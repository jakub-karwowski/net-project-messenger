// Render.cpp - the render thread: ChatController (commands + store-event
// interpretation), TerminalRender (the TerminalUI front-end behind RenderWorker)
// and HeadlessRender (fd in, JSON lines out, behind HeadlessRenderWorker).

#include "Render.hpp"

#include "Chat.hpp"
#include "Element.hpp"
#include "Utils.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <poll.h>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <atomic>
#include <unistd.h>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

// Display label for a message author: 0 is this node (a locally authored
// message), anything else is the local name of the peer it came from.
std::string sender_label(uint64_t sender_id) {
    return sender_id == 0 ? std::string{"me"} : "peer#" + std::to_string(sender_id);
}

bool parse_u64(std::string_view text, uint64_t& out) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), out);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

bool parse_port(std::string_view text, uint16_t& out) {
    unsigned value = 0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) { return false; }
    if (value > 0xFFFFu) { return false; }
    out = static_cast<uint16_t>(value);
    return true;
}

std::vector<std::string_view> split_spaces(std::string_view line) {
    std::vector<std::string_view> parts;
    for (size_t i = 0; i < line.size();) {
        while (i < line.size() && line[i] == ' ') { ++i; }
        const size_t start = i;
        while (i < line.size() && line[i] != ' ') { ++i; }
        if (i > start) { parts.push_back(line.substr(start, i - start)); }
    }
    return parts;
}

} // namespace

// --- ChatController ---------------------------------------------------------

ChatController::ChatController(RenderSink& sink, ThreadQueue<Event>& store_queue,
                               Logger logger, bool machine)
    : sink_(sink), store_queue_(store_queue), logger_(std::move(logger)), machine_(machine) {}

void ChatController::stop() {
    running_ = false;
    if (!quit_sent_) {
        quit_sent_ = true;
        sink_.quit();
    }
}

bool ChatController::push(Event event) {
    if (store_queue_.emplace(std::move(event))) { return true; }
    stop();
    return false;
}

CommandResult ChatController::submit(std::string_view line) {
    if (line.empty()) { return {}; }
    if (line.front() == '/') { return command(line); }
    compose(std::string{line});
    return {};
}

CommandResult ChatController::command(std::string_view line) {
    const std::vector<std::string_view> parts = split_spaces(line);
    const std::string_view cmd = parts.empty() ? std::string_view{} : parts[0];
    logger_.info("command: " + std::string(line));
    CommandResult result;

    if (cmd == "/quit" || cmd == "/exit") {
        stop();
    } else if (cmd == "/help") {
        sink_.info("/conversations             list known conversations");
        sink_.info("/current                   show the conversation being viewed");
        sink_.info("/switch <conversation id>  subscribe to a conversation");
        sink_.info("/notify-on <conversation id>  announce its new messages");
        sink_.info("/notify-off <conversation id> stay silent about it");
        sink_.info("/connect <ip> <port>       dial a new peer");
        sink_.info("/reconnect <conversation id>  re-dial an existing peer");
        sink_.info("/quit, /exit               leave");
    } else if (cmd == "/conv" || cmd == "/conversations") {
        if (push(EventListConversations{})) {
            list_requests_.push_back(false);
            result.pending = Pending::Conversations;
        }
    } else if (cmd == "/current") {
        // Purely informational: the controller already knows which conversation
        // is viewed, so nothing has to be asked of the store.
        if (current_conversation_id_ == 0) {
            sink_.info("no conversation selected");
        } else {
            sink_.info("current conversation: " + std::to_string(current_conversation_id_));
        }
    } else if (cmd == "/switch") {
        uint64_t conversation_id = 0;
        if (parts.size() < 2 || !parse_u64(parts[1], conversation_id)) {
            sink_.command_error("usage: /switch <conversation id>");
        } else {
            const uint64_t before = current_conversation_id_;
            switch_conversation(conversation_id);
            if (current_conversation_id_ != before) {
                result = {Pending::Switch, conversation_id};
            }
        }
    } else if (cmd == "/notify-on" || cmd == "/notify-off") {
        uint64_t conversation_id = 0;
        if (parts.size() < 2 || !parse_u64(parts[1], conversation_id)) {
            sink_.command_error("usage: " + std::string(cmd) + " <conversation id>");
        } else {
            set_notifications(conversation_id, cmd == "/notify-on");
        }
    } else if (cmd == "/connect") {
        uint16_t port = 0;
        if (parts.size() < 3 || !parse_port(parts[2], port)) {
            sink_.command_error("usage: /connect <ip> <port>");
        } else {
            sink_.info("connecting to " + std::string(parts[1]) + ":" + std::string(parts[2])
                       + "...");
            if (push(EventDialPeer{std::string(parts[1]), port, 0})) {
                result.pending = Pending::Connect;
            }
        }
    } else if (cmd == "/reconnect") {
        uint64_t conversation_id = 0;
        if (parts.size() < 2 || !parse_u64(parts[1], conversation_id)) {
            sink_.command_error("usage: /reconnect <conversation id>");
        } else {
            sink_.info("reconnecting conversation " + std::to_string(conversation_id) + "...");
            if (push(EventReconnectPeer{conversation_id, 0})) {
                result.pending = Pending::Reconnect;
            }
        }
    } else if (machine_ && cmd == "/send") {
        // /send <conv> <text>: the text is everything after the conversation id.
        uint64_t conversation_id = 0;
        size_t pos = line.find(cmd) + cmd.size();
        while (pos < line.size() && line[pos] == ' ') { ++pos; }
        const size_t id_end = std::min(line.find(' ', pos), line.size());
        const std::string_view id_text = line.substr(pos, id_end - pos);
        const std::string_view text =
            id_end < line.size() ? line.substr(id_end + 1) : std::string_view{};
        if (!parse_u64(id_text, conversation_id) || conversation_id == 0 || text.empty()) {
            sink_.command_error("usage: /send <conversation id> <text>");
        } else {
            (void)push(EventComposeMessage{conversation_id, std::string(text)});
        }
    } else if (machine_ && cmd == "/sync") {
        if (push(EventListConversations{})) {
            list_requests_.push_back(true);
            result.pending = Pending::Sync;
        }
    } else if (machine_ && cmd == "/echo") {
        if (parts.size() < 2 || (parts[1] != "on" && parts[1] != "off")) {
            sink_.command_error("usage: /echo on|off");
        } else {
            const bool on = parts[1] == "on";
            if (push(EventSetEcho{on})) { sink_.info(on ? "echo on" : "echo off"); }
        }
    } else {
        sink_.command_error("unknown command: " + std::string(cmd));
    }
    return result;
}

void ChatController::switch_conversation(uint64_t conversation_id) {
    if (conversation_id == 0) {
        sink_.command_error("conversation id must be non-zero");
        return;
    }
    if (conversation_id == current_conversation_id_) { return; }
    current_conversation_id_ = conversation_id;
    // The store answers with a full snapshot, and returns whichever
    // conversation was being viewed to the background default.
    (void)push(EventSubscribe{conversation_id, AllSubscriptions, 0});
}

// Background conversations announce their messages by default (the store
// subscribes every new one that way), so this only switches the exception on
// and off. The conversation being viewed keeps its content subscription.
void ChatController::set_notifications(uint64_t conversation_id, bool enabled) {
    if (conversation_id == 0) {
        sink_.command_error("conversation id must be non-zero");
        return;
    }
    if (conversation_id == current_conversation_id_) {
        sink_.info("conversation " + std::to_string(conversation_id)
                   + " is open: notifications follow the view");
        return;
    }
    const bool queued = enabled
        ? push(EventSubscribe{conversation_id, SubscriptionFlag::Notification, 0})
        : push(EventUnsubscribe{conversation_id});
    if (!queued) { return; }
    sink_.info(std::string{"notifications "} + (enabled ? "on: " : "off: ")
               + "conversation " + std::to_string(conversation_id));
}

void ChatController::compose(const std::string& payload) {
    if (current_conversation_id_ == 0) {
        sink_.command_error("no conversation selected: /conv then /switch, or /connect");
        return;
    }
    // The store echoes the message back through the subscription, so nothing is
    // shown from here.
    (void)push(EventComposeMessage{current_conversation_id_, payload});
}

void ChatController::handle_store(const Event& event) {
    if (const auto* snapshot = std::get_if<EventConversationSnapshot>(&event)) {
        if (!snapshot->store) { return; }
        const ConversationStore& source = *snapshot->store;
        if (source.conversation_id != current_conversation_id_) { return; } // stale
        sink_.messages(source.conversation_id, source.store, true);
    } else if (const auto* delta = std::get_if<EventConversationDelta>(&event)) {
        if (delta->conversation_id != current_conversation_id_) { return; }
        sink_.messages(delta->conversation_id, delta->messages, false);
    } else if (const auto* notice = std::get_if<EventConversationNotification>(&event)) {
        // A background conversation has something new; the content itself is
        // only fetched on /switch (which sends a fresh copy).
        sink_.notification(notice->conversation_id);
    } else if (const auto* list = std::get_if<EventConversationList>(&event)) {
        const bool is_sync = !list_requests_.empty() && list_requests_.front();
        if (!list_requests_.empty()) { list_requests_.pop_front(); }
        if (is_sync) { sink_.sync(); } else { sink_.conversations(*list); }
    } else if (const auto* dialed = std::get_if<EventPeerDialed>(&event)) {
        sink_.peer("dialed", dialed->conversation_id, dialed->peer_id);
        switch_conversation(dialed->conversation_id);
    } else if (const auto* connection = std::get_if<EventPeerConnection>(&event)) {
        sink_.peer(connection->reconnect ? "reconnected" : "connected",
                   connection->conversation_id, connection->peer_id);
    } else if (const auto* gone = std::get_if<EventPeerDisconnected>(&event)) {
        sink_.peer("disconnected", gone->conversation_id, gone->peer_id);
    } else if (const auto* error = std::get_if<EventError>(&event)) {
        sink_.error(error->message);
    }
    // Other events are not addressed to the render thread.
}

// --- TerminalRender ---------------------------------------------------------

namespace {

// The terminal is laid out at this design size and scaled to the real one.
constexpr int kRenderWidth = 80;
constexpr int kRenderHeight = 24;
constexpr int kInputHeight = 5;
constexpr int kChatHeight = kRenderHeight - kInputHeight;

// A fixed-capacity history of submitted input lines. It is a circular buffer:
// once full, each push overwrites the oldest entry, so the buffer never grows
// and always holds the kCapacity most recent lines. Index 0 is the oldest,
// size() - 1 the newest.
template<size_t kCapacity>
class InputHistory {
public:
    void push(std::string entry) {
        entries_[head_] = std::move(entry);
        head_ = (head_ + 1) % kCapacity;
        if (size_ < kCapacity) { ++size_; }
    }

    std::size_t size() const noexcept { return size_; }

    const std::string& at(std::size_t index) const noexcept {
        // Oldest entry sits `size_` slots behind the next write slot.
        const std::size_t oldest = (head_ + kCapacity - size_) % kCapacity;
        return entries_[(oldest + index) % kCapacity];
    }

private:
    std::array<std::string, kCapacity> entries_{};
    std::size_t head_{0}; // next write slot
    std::size_t size_{0};
};

// The terminal session: it owns the widgets and the input line, feeds submitted
// lines to the ChatController and shows what the controller reports. All
// terminal output happens here; no other worker touches the UI.
class TerminalRender final : public RenderSink {
public:
    TerminalRender(TerminalUI& terminal,
                   ThreadQueue<Event>& render_queue,
                   ThreadQueue<Event>& store_queue,
                   Logger logger)
        : terminal_(terminal),
          render_queue_(render_queue),
          logger_(logger),
          controller_(*this, store_queue, std::move(logger)),
          chat_model_(std::make_shared<ChatModel>()),
          input_model_(std::make_shared<InputModel>()),
          chat_window_(std::make_shared<ChatWindow>(
              Element::Position{0, 0}, kRenderWidth, kChatHeight)),
          input_box_(std::make_shared<InputBox>(
              Element::Position{0, kChatHeight}, kRenderWidth, kInputHeight)),
          layout_(Element::Position{0, 0}, kRenderWidth, kRenderHeight) {
        chat_window_->set_model(chat_model_);
        input_box_->set_model(input_model_);
        input_box_->set_border({Element::Border::BorderType::Asterisk, 1, 1, 0, 0});
        input_box_->set_margin({1, 1, 0, 0});
        layout_.add_element(chat_window_);
        layout_.add_element(input_box_);
    }

    void run() {
        logger_.info("render: started");
        system_message("welcome: /help for commands");
        terminal_.clear();
        render();
        while (controller_.running()) {
            auto event = render_queue_.pop();
            if (!event) { break; } // queue shut down and drained
            handle(*event);
            if (controller_.running()) { render(); }
        }
        logger_.info("render: stopped");
    }

    // --- RenderSink ---
    void info(std::string_view text) override { system_message(std::string(text)); }

    void messages(uint64_t conv, std::span<const ConversationStore::Message> items,
                  bool snapshot) override {
        if (snapshot) {
            chat_model_->clear();
            if (items.empty()) {
                system_message("conversation " + std::to_string(conv) + " has no messages yet");
                return;
            }
        }
        for (const auto& message : items) {
            chat_model_->add_message(std::chrono::system_clock::now(),
                                     sender_label(message.sender_id), message.payload);
        }
    }

    void notification(uint64_t conv) override {
        system_message("new message: conversation " + std::to_string(conv));
    }

    void conversations(const EventConversationList& list) override {
        if (list.conversations.empty()) {
            system_message("no conversations");
            return;
        }
        system_message("conversations:");
        for (const ConversationInfo& info : list.conversations) {
            std::string line = "  #" + std::to_string(info.conversation_id);
            for (const uint64_t peer : info.peer_id) {
                line += " peer#" + std::to_string(peer);
            }
            // A fully read conversation shows no count at all.
            if (info.unread != 0) {
                line += " (" + std::to_string(info.unread) + " unread)";
            }
            system_message(std::move(line));
        }
    }

    void peer(std::string_view what, uint64_t conv, uint64_t peer_id) override {
        if (what == "dialed") {
            system_message("connected: conversation " + std::to_string(conv));
        } else if (what == "disconnected") {
            system_message("peer#" + std::to_string(peer_id) + " disconnected"
                           + (conv != 0 ? " (conversation " + std::to_string(conv) + ")"
                                        : std::string{}));
        } else {
            system_message("peer#" + std::to_string(peer_id) + " " + std::string(what)
                           + ": conversation " + std::to_string(conv));
        }
    }

    void error(std::string_view message) override {
        system_message("error: " + std::string(message));
    }

    void quit() override {}

    void command_error(std::string_view text) override { system_message(std::string(text)); }

private:
    void render() { terminal_.render(layout_); }

    void system_message(std::string text) {
        chat_model_->add_message(std::chrono::system_clock::now(), ">", std::move(text));
    }

    void handle(const Event& event) {
        if (const auto* key = std::get_if<EventLocal>(&event)) {
            handle_key(*key);
        } else {
            controller_.handle_store(event);
        }
    }

    void handle_key(const EventLocal& key) {
        switch (key.type) {
            case EventLocal::Type::EXIT:
                controller_.stop();
                return;
            case EventLocal::Type::TEXT: {
                const size_t cursor = input_model_->get_cursor();
                input_model_->apply(std::vector<InputModel::Edit>{
                    Input::put(cursor, key.payload),
                    Input::set_cursor(cursor + key.payload.size())});
                history_pos_ = history_.size(); // editing detaches the recall
                return;
            }
            case EventLocal::Type::BACKSPACE: {
                const size_t cursor = input_model_->get_cursor();
                if (cursor == 0) { return; }
                const std::string_view text = std::get<0>(input_model_->get_content());
                const size_t previous = utils::utf8::prev(text, cursor);
                input_model_->apply(std::vector<InputModel::Edit>{
                    Input::remove(previous, cursor - previous),
                    Input::set_cursor(previous)});
                history_pos_ = history_.size(); // editing detaches the recall
                return;
            }
            case EventLocal::Type::ARROW_LEFT: {
                const std::string_view text = std::get<0>(input_model_->get_content());
                input_model_->apply(std::vector<InputModel::Edit>{
                    Input::set_cursor(utils::utf8::prev(text, input_model_->get_cursor()))});
                return;
            }
            case EventLocal::Type::ARROW_RIGHT: {
                const std::string_view text = std::get<0>(input_model_->get_content());
                input_model_->apply(std::vector<InputModel::Edit>{
                    Input::set_cursor(utils::utf8::next(text, input_model_->get_cursor()))});
                return;
            }
            case EventLocal::Type::ARROW_UP:
                history_prev();
                return;
            case EventLocal::Type::ARROW_DOWN:
                history_next();
                return;
            case EventLocal::Type::ENTER:
                submit();
                return;
        }
    }

    // Enter: an empty line is ignored, a leading '/' is a command, anything
    // else is a message for the current conversation.
    void submit() {
        const std::string line{std::get<0>(input_model_->get_content())};
        input_model_->apply(std::vector<InputModel::Edit>{
            Input::remove(0, line.size()),
            Input::set_cursor(0)});
        if (!line.empty()) { history_.push(line); }
        history_pos_ = history_.size(); // a fresh line starts a new recall
        (void)controller_.submit(line);
    }

    // Overwrites the input field with `text` (rather than appending) and puts
    // the cursor at the end, so a recalled history line is ready to edit.
    void set_input(std::string_view text) {
        const size_t length = std::get<0>(input_model_->get_content()).size();
        input_model_->apply(std::vector<InputModel::Edit>{
            Input::remove(0, length),
            Input::put(0, text),
            Input::set_cursor(text.size())});
    }

    // Up: step towards the oldest remembered line. `history_pos_` is the number
    // of entries from the newest, so decrementing walks backwards in time and 0
    // is the oldest line (or no history at all).
    void history_prev() {
        if (history_pos_ == 0) { return; }
        --history_pos_;
        set_input(history_.at(history_pos_));
    }

    // Down: step towards the newest line; past the newest entry the fresh,
    // empty line is restored.
    void history_next() {
        if (history_pos_ >= history_.size()) { return; }
        ++history_pos_;
        set_input(history_pos_ == history_.size()
                      ? std::string_view{}
                      : std::string_view{history_.at(history_pos_)});
    }

    TerminalUI& terminal_;
    ThreadQueue<Event>& render_queue_;
    Logger logger_;
    ChatController controller_;
    std::shared_ptr<ChatModel> chat_model_;
    std::shared_ptr<InputModel> input_model_;
    std::shared_ptr<ChatWindow> chat_window_;
    std::shared_ptr<InputBox> input_box_;
    AbsoluteLayout layout_;
    // Submitted-line history and the recall cursor into it. `history_pos_` is
    // the number of entries from the newest: history_.size() means the fresh
    // line is being edited, 0 means the oldest remembered line.
    InputHistory<32ULL> history_;
    std::size_t history_pos_{0};
};

} // namespace

// --- RenderWorker -----------------------------------------------------------

RenderWorker::RenderWorker(
    TerminalUI& terminal,
    ThreadQueue<Event>& render_queue,
    ThreadQueue<Event>& store_queue,
    Logger logger)
    : terminal_(terminal),
      render_queue_(render_queue),
      store_queue_(store_queue),
      logger_(std::move(logger)) {}

void RenderWorker::operator()() {
    TerminalRender session{terminal_, render_queue_, store_queue_, logger_};
    session.run();
}

// --- HeadlessRenderWorker ---------------------------------------------------

namespace {

// Appends `text` as a JSON string literal (with quotes). Quotes, backslashes and
// control characters are escaped; bytes that are not valid UTF-8 become U+FFFD so
// the output is always valid JSON.
void append_json_string(std::string& out, std::string_view text) {
    out += '"';
    for (size_t i = 0; i < text.size();) {
        const auto c = static_cast<unsigned char>(text[i]);
        if (c < 0x80) {
            switch (c) {
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                case '\b': out += "\\b"; break;
                case '\f': out += "\\f"; break;
                default:
                    if (c < 0x20 || c == 0x7f) {
                        static constexpr char hex[] = "0123456789abcdef";
                        out += "\\u00";
                        out += hex[c >> 4];
                        out += hex[c & 0xF];
                    } else {
                        out += static_cast<char>(c);
                    }
            }
            ++i;
            continue;
        }
        // Multi-byte sequence: validate length, continuation bytes, overlongs.
        size_t len = 0;
        uint32_t cp = 0;
        if (c >= 0xC2 && c <= 0xDF) { len = 2; cp = c & 0x1F; }
        else if (c >= 0xE0 && c <= 0xEF) { len = 3; cp = c & 0x0F; }
        else if (c >= 0xF0 && c <= 0xF4) { len = 4; cp = c & 0x07; }
        bool valid = len != 0 && i + len <= text.size();
        for (size_t k = 1; valid && k < len; ++k) {
            const auto cont = static_cast<unsigned char>(text[i + k]);
            if ((cont & 0xC0) != 0x80) { valid = false; break; }
            cp = (cp << 6) | (cont & 0x3F);
        }
        if (valid) {
            if ((len == 3 && cp < 0x800) || (len == 4 && (cp < 0x10000 || cp > 0x10FFFF))
                || (cp >= 0xD800 && cp <= 0xDFFF)) {
                valid = false;
            }
        }
        if (valid) {
            out.append(text.substr(i, len));
            i += len;
        } else {
            out += "\xEF\xBF\xBD"; // U+FFFD
            ++i;
        }
    }
    out += '"';
}

std::string json_string(std::string_view text) {
    std::string out;
    append_json_string(out, text);
    return out;
}

} // namespace

struct HeadlessRenderWorker::Impl final : RenderSink {
    struct PendingRequest {
        Pending kind;
        uint64_t arg;
        std::optional<std::string> re;
    };

    Impl(ThreadQueue<Event>& render_queue, ThreadQueue<Event>& store_queue, int in_fd,
         int out_fd, std::string io_name, Logger logger)
        : render_queue_(render_queue),
          in_fd_(in_fd),
          out_fd_(out_fd),
          io_name_(std::move(io_name)),
          logger_(logger),
          controller_(*this, store_queue, std::move(logger), /*machine=*/true) {}

    // --- output ---

    // Writes one JSON line: {"seq":N,"t_ns":T[,"re":..],"event":"<event>"<rest>}.
    // `rest` is either empty or starts with ','. Thread safe.
    void emit(const std::optional<std::string>& re, std::string_view event,
              std::string_view rest = {}) {
        emitted_in_command_ = true;
        const auto now = std::chrono::steady_clock::now().time_since_epoch();
        const auto t_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
        std::string tail;
        if (re) { tail += ",\"re\":"; append_json_string(tail, *re); }
        tail += ",\"event\":";
        append_json_string(tail, event);
        tail += rest;
        tail += "}\n";

        std::lock_guard lock{out_mutex_};
        std::string line = "{\"seq\":" + std::to_string(++seq_) + ",\"t_ns\":"
                           + std::to_string(t_ns) + tail;
        write_all(line);
    }

    void write_all(const std::string& line) {
        size_t done = 0;
        while (done < line.size()) {
            const ssize_t n = ::write(out_fd_, line.data() + done, line.size() - done);
            if (n < 0) {
                if (errno == EINTR) { continue; }
                return; // reader of our output is gone: nothing to do about it
            }
            done += static_cast<size_t>(n);
        }
    }

    // --- request correlation ---

    std::optional<std::string> take(Pending a, Pending b = Pending::None, uint64_t arg = 0,
                                    bool match_arg = false) {
        for (auto it = pending_.begin(); it != pending_.end(); ++it) {
            if (it->kind != a && it->kind != b) { continue; }
            if (match_arg && it->arg != arg) { continue; }
            auto re = std::move(it->re);
            pending_.erase(it);
            return re;
        }
        return std::nullopt;
    }

    // --- RenderSink ---

    void info(std::string_view text) override {
        emit(cur_re_, "info", ",\"text\":" + json_string(text));
    }

    void messages(uint64_t conv, std::span<const ConversationStore::Message> items,
                  bool snapshot) override {
        std::string rest = ",\"conversation\":" + std::to_string(conv)
                           + ",\"snapshot\":" + (snapshot ? "true" : "false") + ",\"items\":[";
        bool first = true;
        for (const auto& message : items) {
            if (!first) { rest += ','; }
            first = false;
            rest += "{\"from\":" + json_string(sender_label(message.sender_id))
                    + ",\"text\":" + json_string(message.payload) + "}";
        }
        rest += ']';
        std::optional<std::string> re;
        if (snapshot) { re = take(Pending::Switch, Pending::None, conv, true); }
        emit(re, "messages", rest);
    }

    void notification(uint64_t conv) override {
        emit(std::nullopt, "notification", ",\"conversation\":" + std::to_string(conv));
    }

    void conversations(const EventConversationList& list) override {
        std::string rest = ",\"items\":[";
        bool first = true;
        for (const ConversationInfo& info : list.conversations) {
            if (!first) { rest += ','; }
            first = false;
            rest += "{\"conversation\":" + std::to_string(info.conversation_id) + ",\"peers\":[";
            for (size_t i = 0; i < info.peer_id.size(); ++i) {
                if (i != 0) { rest += ','; }
                rest += std::to_string(info.peer_id[i]);
            }
            rest += "],\"unread\":" + std::to_string(info.unread) + "}";
        }
        rest += ']';
        emit(take(Pending::Conversations), "conversations", rest);
    }

    void peer(std::string_view what, uint64_t conv, uint64_t peer_id) override {
        std::optional<std::string> re;
        if (what == "dialed") {
            known_peers_.insert(peer_id);
            re = take(Pending::Connect, Pending::Reconnect);
        } else if (what == "disconnected") {
            // A disconnect for a peer we never saw connect is a dial that failed.
            if (known_peers_.find(peer_id) == known_peers_.end()) {
                re = take(Pending::Connect);
                if (!re) { re = take(Pending::Reconnect); }
            }
        } else {
            known_peers_.insert(peer_id);
        }
        emit(re, "peer",
             ",\"what\":" + json_string(what) + ",\"conversation\":" + std::to_string(conv)
             + ",\"peer\":" + std::to_string(peer_id));
    }

    void error(std::string_view message) override {
        // The store reports a reconnect it cannot resolve as an error.
        std::optional<std::string> re;
        if (message.ends_with("no peer to reconnect")) { re = take(Pending::Reconnect); }
        emit(re, "error", ",\"message\":" + json_string(message));
    }

    void command_error(std::string_view text) override {
        emit(cur_re_, "error", ",\"message\":" + json_string(text));
    }

    void sync() override { emit(take(Pending::Sync), "sync"); }

    void quit() override { quit_re_ = cur_re_; }

    // --- input ---

    void start_reader() {
        reader_ = std::thread{[this] { read_lines(); }};
    }

    void stop_reader() {
        stop_.store(true);
        if (reader_.joinable()) { reader_.join(); }
    }

    // Reads `in_fd_` and posts one EventCommandLine per line; EOF posts the eof
    // marker. Polls with a timeout so it can notice `stop_` while blocked.
    void read_lines() {
        std::string buffer;
        char chunk[4096];
        bool eof = false;
        while (!eof && !stop_.load()) {
            pollfd pfd{in_fd_, POLLIN, 0};
            const int ready = ::poll(&pfd, 1, 50);
            if (ready < 0) {
                if (errno == EINTR) { continue; }
                eof = true;
                break;
            }
            if (ready == 0) { continue; }
            const ssize_t n = ::read(in_fd_, chunk, sizeof(chunk));
            if (n < 0) {
                if (errno == EINTR || errno == EAGAIN) { continue; }
                eof = true;
            } else if (n == 0) {
                eof = true;
            } else {
                buffer.append(chunk, static_cast<size_t>(n));
                size_t start = 0;
                for (size_t nl; (nl = buffer.find('\n', start)) != std::string::npos;
                     start = nl + 1) {
                    if (!render_queue_.emplace(
                            EventCommandLine{buffer.substr(start, nl - start), false})) {
                        return;
                    }
                }
                buffer.erase(0, start);
            }
        }
        if (stop_.load()) { return; }
        if (!buffer.empty()) { (void)render_queue_.emplace(EventCommandLine{buffer, false}); }
        (void)render_queue_.emplace(EventCommandLine{{}, true});
    }

    void handle_line(std::string line) {
        while (!line.empty() && (line.back() == '\r' || line.back() == ' '
                                 || line.back() == '\t')) {
            line.pop_back();
        }
        std::string_view text = line;
        cur_re_.reset();
        if (!text.empty() && text.front() == '#') {
            const size_t space = text.find(' ');
            cur_re_ = std::string(text.substr(1, space == std::string_view::npos
                                                      ? text.size() - 1 : space - 1));
            text = space == std::string_view::npos ? std::string_view{}
                                                   : text.substr(space + 1);
            while (!text.empty() && text.front() == ' ') { text.remove_prefix(1); }
        }
        if (text.empty()) {
            if (cur_re_) { emit(cur_re_, "error", ",\"message\":\"empty command\""); }
            cur_re_.reset();
            return;
        }

        emitted_in_command_ = false;
        CommandResult result;
        if (text == "/stats") {
            const std::string metrics = stats_ ? stats_() : std::string{"{}"};
            emit(cur_re_, "stats", ",\"metrics\":" + (metrics.empty() ? "{}" : metrics));
        } else {
            result = controller_.submit(text);
        }
        if (result.pending != Pending::None) {
            pending_.push_back({result.pending, result.arg, cur_re_});
        } else if (!emitted_in_command_ && cur_re_ && controller_.running()) {
            emit(cur_re_, "ok"); // accepted, nothing to report beyond that
        }
        cur_re_.reset();
    }

    void run() {
        logger_.info("headless render: started");
        start_reader();
        while (controller_.running()) {
            auto event = render_queue_.pop();
            if (!event) { break; } // queue shut down and drained
            if (const auto* command = std::get_if<EventCommandLine>(&*event)) {
                if (command->eof) { controller_.stop(); }
                else { handle_line(command->line); }
            } else {
                controller_.handle_store(*event);
            }
        }
        controller_.stop();
        stop_reader();
        emit(quit_re_, "bye");
        logger_.info("headless render: stopped");
    }

    ThreadQueue<Event>& render_queue_;
    int in_fd_;
    int out_fd_;
    std::string io_name_;
    Logger logger_;
    ChatController controller_;

    std::mutex out_mutex_;
    uint64_t seq_{0};

    std::thread reader_;
    std::atomic<bool> stop_{false};

    // Render-thread state (only touched from run()).
    std::deque<PendingRequest> pending_;
    std::optional<std::string> cur_re_;
    std::optional<std::string> quit_re_;
    std::atomic<bool> emitted_in_command_{false};
    std::unordered_set<uint64_t> known_peers_;
    std::function<std::string()> stats_;
};

HeadlessRenderWorker::HeadlessRenderWorker(
    ThreadQueue<Event>& render_queue,
    ThreadQueue<Event>& store_queue,
    int in_fd,
    int out_fd,
    std::string io_name,
    Logger logger)
    : impl_(std::make_unique<Impl>(render_queue, store_queue, in_fd, out_fd,
                                   std::move(io_name), std::move(logger))) {}

HeadlessRenderWorker::~HeadlessRenderWorker() {
    if (impl_) { impl_->stop_reader(); }
}

void HeadlessRenderWorker::operator()() { impl_->run(); }

void HeadlessRenderWorker::announce_ready(std::string listen,
                                          std::optional<std::string> error) {
    std::string rest = ",\"io\":" + json_string(impl_->io_name_);
    if (error) {
        rest += ",\"error\":" + json_string(*error);
    } else {
        rest += ",\"listen\":" + json_string(listen);
    }
    impl_->emit(std::nullopt, "ready", rest);
}

void HeadlessRenderWorker::set_stats_provider(std::function<std::string()> provider) {
    impl_->stats_ = std::move(provider);
}
