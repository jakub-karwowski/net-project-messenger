#include "TuiFrontend.hpp"

#include "Json.hpp"
#include "Utils.hpp"
#include "Worker.hpp"

#include <algorithm>
#include <array>
#include <format>
#include <limits>
#include <tuple>
#include <utility>

namespace {

using Sub = msg::Subscription;

// The active conversation is shown in full; the others only count unread messages.
constexpr Sub kActive{Sub::Inbound::Forward, Sub::Outbound::Notify, Sub::Lifecycle::Notify};
constexpr Sub kBackground{Sub::Inbound::Notify, Sub::Outbound::Ignore, Sub::Lifecycle::Notify};

constexpr std::size_t kPageMessages = 10;

std::string short_id(ConvId conv) {
    return json::hex_id(conv).substr(0, 8);
}

}  // namespace

TuiFrontend::TuiFrontend(ThreadQueue<msg::FrontendMsg>& inbox, ThreadQueue<msg::StoreMsg>& store, Render render,
                         Logger log, std::function<void(std::string_view)> begin_shutdown)
    : inbox_(inbox),
      store_(store),
      render_(std::move(render)),
      log_(log.with_component("tui")),
      begin_shutdown_(std::move(begin_shutdown)),
      screen_(chat_, input_),
      processor_(store, ProcessorMode{kActive, kBackground}) {}

void TuiFrontend::start(const wire::Endpoint& listening, std::size_t pts) {
    // Every conversation counts unread messages until it is opened.
    std::ignore = store_.push(msg::SetDefaultSubscription{kBackground});
    notice(std::format("listening on {}:{} ({} IO workers). /dial <ip> <port> to connect, /help for commands",
                       listening.address, listening.port, pts));
    redraw();
}

void TuiFrontend::run() {
    while (auto m = inbox_.pop()) {
        worker::dispatch(*this, *m);
        // Handle whatever else is already queued (a paste, a burst of messages)
        // before drawing once.
        while (!done_) {
            auto more = inbox_.try_pop();
            if (!more) { break; }
            worker::dispatch(*this, *more);
        }
        redraw();
        if (done_) { return; }
    }
}

void TuiFrontend::redraw() {
    screen_.set_status(status_text());
    screen_.set_notice(StyledText(notice_, TuiScreen::kNoticeStyle));
    if (render_) { render_(screen_); }
}

void TuiFrontend::notice(std::string text) {
    notice_ = std::move(text);
}

// ------------------------------------------------------------------ keys

void TuiFrontend::on(msg::Key& k) {
    const auto [text, size] = input_->get_content();
    const std::string content(text);
    const std::size_t cursor = input_->get_cursor();
    switch (k.type) {
        case msg::Key::Text: {
            const std::array edits{Input::put(cursor, k.text), Input::set_cursor(cursor + k.text.size())};
            input_->apply(edits);
            break;
        }
        case msg::Key::Backspace: {
            if (cursor == 0) { break; }
            const std::size_t prev = utils::utf8::prev(content, cursor);
            const std::array edits{Input::remove(prev, cursor - prev), Input::set_cursor(prev)};
            input_->apply(edits);
            break;
        }
        case msg::Key::Left: {
            const std::array edits{Input::set_cursor(utils::utf8::prev(content, cursor))};
            input_->apply(edits);
            break;
        }
        case msg::Key::Right: {
            const std::array edits{Input::set_cursor(utils::utf8::next(content, cursor))};
            input_->apply(edits);
            break;
        }
        case msg::Key::Enter: {
            const std::array edits{Input::remove(0, content.size()), Input::set_cursor(0)};
            input_->apply(edits);
            if (!content.empty()) { submit(content); }
            break;
        }
        case msg::Key::PageUp:
            scroll_ += kPageMessages;
            apply_scroll();
            break;
        case msg::Key::PageDown:
            scroll_ = scroll_ > kPageMessages ? scroll_ - kPageMessages : 0;
            apply_scroll();
            break;
        case msg::Key::Tab: {
            if (order_.empty()) { break; }
            const auto it = std::ranges::find(order_, active());
            const auto next = (it == order_.end() || std::next(it) == order_.end()) ? order_.begin() : std::next(it);
            if (*next != active()) { open(*next); }
            break;
        }
        case msg::Key::Exit:
            submit("/quit");
            break;
        case msg::Key::Up:
        case msg::Key::Down:
            break;
    }
}

void TuiFrontend::submit(const std::string& line) {
    const ReqId req = next_req_++;
    const auto result = processor_.submit(line, req);
    if (!result) {
        notice("error: " + result.error().text);
        return;
    }
    switch (*result) {
        case CommandProcessor::Local::None:
            if (line.starts_with("/dial")) { dials_.insert(req); }
            if (line.starts_with("/list")) { lists_.insert(req); }
            if (line.starts_with("/open")) {
                // The snapshot that follows replaces the view.
                scroll_ = 0;
                apply_scroll();
            }
            break;
        case CommandProcessor::Local::Blank:
            break;
        case CommandProcessor::Local::Help:
            notice(std::string(cmd::help_text()) + "  Tab: next conversation  PgUp/PgDn: scroll");
            break;
        case CommandProcessor::Local::Quit:
            if (!std::exchange(quitting_, true)) {
                notice("shutting down...");
                if (begin_shutdown_) { begin_shutdown_("quit"); }
            }
            break;
    }
}

void TuiFrontend::open(ConvId conv) {
    submit("/open " + json::hex_id(conv));
}

void TuiFrontend::apply_scroll() {
    const std::size_t total = std::numeric_limits<std::size_t>::max();
    chat_->expose(0, total);
    const std::size_t size = chat_->get_messages().size();
    scroll_ = std::min(scroll_, size == 0 ? 0 : size - 1);
    chat_->expose(0, scroll_ == 0 ? total : size - scroll_);
}

void TuiFrontend::on(msg::InputLine&) {}
void TuiFrontend::on(msg::InputClosed&) {}

// ------------------------------------------------------------------ store events

void TuiFrontend::on(msg::Reply& m) {
    const bool was_dial = dials_.erase(m.req) > 0;
    const bool was_list = lists_.erase(m.req) > 0;
    if (!m.ok) {
        notice("error: " + m.error);
        return;
    }
    if (was_dial && m.conv != kNoId) {
        know(m.conv);
        open(m.conv);
    }
    if (was_list) { show_next_list_ = true; }
}

void TuiFrontend::on(msg::ConvSnapshot& m) {
    know(m.conv);
    convs_[m.conv].status = m.status;
    convs_[m.conv].unread = 0;
    if (m.conv != active()) { return; }
    chat_->clear();
    for (const auto& s : m.msgs) {
        chat_->add_message(s.at, s.sender == kNoId ? label_of_mine(s.state) : label_of_peer(m.conv), s.body);
    }
    apply_scroll();
}

void TuiFrontend::on(msg::MsgAppended& m) {
    if (m.conv != active()) { return; }
    // Message ids are history indexes: the model mirrors the whole history.
    chat_->add_message(m.msg.at, m.msg.sender == kNoId ? label_of_mine(m.msg.state) : label_of_peer(m.conv), m.msg.body);
    if (scroll_ > 0) { ++scroll_; }   // scrolled back: the view stays where it is
    apply_scroll();
}

void TuiFrontend::on(msg::MsgStatus& m) {
    if (m.conv != active()) {
        if (m.state == msg::DeliveryState::Failed) { notice("a message to " + short_id(m.conv) + " could not be sent"); }
        return;
    }
    chat_->set_sender(m.id, label_of_mine(m.state));
}

void TuiFrontend::on(msg::ConvState& m) {
    const bool first = !convs_.contains(m.conv);
    know(m.conv);
    ConvInfo& info = convs_[m.conv];
    info.status = m.status;
    info.detail = m.detail;
    notice(std::format("{}: {}{}{}", short_id(m.conv), msg::to_string(m.status), m.detail.empty() ? "" : " - ", m.detail));
    if (first) {
        // Learn its address for the labels (no request id: no Reply, not shown).
        std::ignore = store_.push(msg::ListReq{0});
        if (active() == kNoId) { open(m.conv); }
    }
}

void TuiFrontend::on(msg::ConvNotify& m) {
    know(m.conv);
    convs_[m.conv].unread = m.unread;
}

void TuiFrontend::on(msg::ConvList& m) {
    for (const auto& c : m.convs) {
        know(c.conv);
        ConvInfo& info = convs_[c.conv];
        info.status = c.status;
        info.remote = c.remote;
        if (c.conv != active()) { info.unread = c.unread; }
    }
    if (!std::exchange(show_next_list_, false)) { return; }
    std::string text = std::format("{} conversations:", m.convs.size());
    for (const auto& c : m.convs) {
        text += std::format("  {} {} {}:{}", json::hex_id(c.conv), msg::to_string(c.status), c.remote.address, c.remote.port);
        if (c.unread > 0) { text += std::format(" ({} unread)", c.unread); }
    }
    notice(std::move(text));
}

void TuiFrontend::on(msg::SystemNotice& m) {
    notice(m.text);
}

void TuiFrontend::on(msg::ShutdownDone&) {
    done_ = true;
}

// ------------------------------------------------------------------ helpers

void TuiFrontend::know(ConvId conv) {
    if (!convs_.contains(conv)) {
        convs_.emplace(conv, ConvInfo{});
        order_.push_back(conv);
    }
}

std::string TuiFrontend::label_of_mine(msg::DeliveryState state) const {
    switch (state) {
        case msg::DeliveryState::PendingSend: return "me (sending)";
        case msg::DeliveryState::Failed: return "me (failed)";
        default: return "me";
    }
}

std::string TuiFrontend::label_of_peer(ConvId conv) const {
    const auto it = convs_.find(conv);
    if (it == convs_.end() || it->second.remote.address.empty()) { return "peer"; }
    return std::format("{}:{}", it->second.remote.address, it->second.remote.port);
}

StyledText TuiFrontend::status_text() const {
    const Style bar = TuiScreen::kBarStyle;
    Style bold = bar;
    bold.attrs |= Style::Bold;
    StyledText text;
    if (quitting_) { text.add(" quitting |", bold); }   // first: the bar may be cut at the right
    const ConvId conv = active();
    if (conv == kNoId) {
        text.add(" no conversation ", bold);
        text.add("- /dial <ip> <port> to start one ", bar);
    } else {
        const ConvInfo& info = convs_.contains(conv) ? convs_.at(conv) : ConvInfo{};
        text.add(" " + json::hex_id(conv) + " ", bold);
        text.add(std::string(msg::to_string(info.status)), bar);
        if (!info.remote.address.empty()) { text.add(std::format(" {}:{}", info.remote.address, info.remote.port), bar); }
        if (scroll_ > 0) { text.add(std::format("  [scrolled back {}]", scroll_), bar); }
    }
    std::string others;
    for (const ConvId c : order_) {
        if (c == conv) { continue; }
        const ConvInfo& info = convs_.at(c);
        others += " " + short_id(c);
        if (info.unread > 0) { others += std::format("({})", info.unread); }
    }
    if (!others.empty()) { text.add("  | others:" + others, bar); }
    return text;
}
