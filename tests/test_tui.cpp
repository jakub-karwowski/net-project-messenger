// test_tui.cpp - the terminal frontend without a terminal.
//
// Key decoding is tested as pure functions. The frontend is fed keys and Store
// events by hand and renders into a FrameBuffer, which is read back as text:
// what reaches the Store must be exactly what the CLI would send for the same
// line, and what is on screen must follow the events.
//
// Plain assertions, no framework.

#include "Json.hpp"
#include "Keys.hpp"
#include "TuiFrontend.hpp"

#include <iostream>
#include <string>
#include <vector>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

// ------------------------------------------------------------------ keys

void test_classify() {
    using keys::ByteKind;
    check(keys::classify('a').kind == ByteKind::Key && keys::classify('a').key.type == msg::Key::Text
              && keys::classify('a').key.text == "a",
          "a printable byte is Text");
    check(keys::classify('\r').key.type == msg::Key::Enter && keys::classify('\n').key.type == msg::Key::Enter, "CR and LF are Enter");
    check(keys::classify(0x7f).key.type == msg::Key::Backspace && keys::classify(0x08).key.type == msg::Key::Backspace,
          "DEL and BS are Backspace");
    check(keys::classify('\t').key.type == msg::Key::Tab, "Tab");
    check(keys::classify(0x03).key.type == msg::Key::Exit, "Ctrl+C is Exit");
    check(keys::classify(0x1b).kind == ByteKind::Escape, "ESC starts a sequence");
    check(keys::classify(0xc5).kind == ByteKind::Utf8Lead, "a UTF-8 lead byte");
    check(keys::classify(0x01).kind == ByteKind::Ignore && keys::classify(0x00).kind == ByteKind::Ignore,
          "other control bytes are ignored");
}

void test_sequences() {
    check(keys::sequence("A")->type == msg::Key::Up && keys::sequence("B")->type == msg::Key::Down
              && keys::sequence("C")->type == msg::Key::Right && keys::sequence("D")->type == msg::Key::Left,
          "arrows");
    check(keys::sequence("5~")->type == msg::Key::PageUp && keys::sequence("6~")->type == msg::Key::PageDown, "page keys");
    check(!keys::sequence("1;5C") && !keys::sequence("3~") && !keys::sequence(""), "others are not keys here");
    check(keys::is_final('A') && keys::is_final('~') && !keys::is_final('5') && !keys::is_final(';'), "final bytes");
}

// ------------------------------------------------------------------ the frontend

struct Harness {
    ThreadQueue<msg::FrontendMsg> inbox;
    ThreadQueue<msg::StoreMsg> store;
    FrameBuffer frame;
    int width = 70;
    int height = 14;
    std::vector<std::string> shutdowns;
    TuiFrontend tui{inbox, store, [this](Renderable& r) { r.resize(width, height); std::ignore = r.write_frame(frame); },
                    Logger{}, [this](std::string_view reason) { shutdowns.emplace_back(reason); }};

    void type(std::string_view text) {
        for (const char c : text) {
            msg::Key k{msg::Key::Text, std::string(1, c)};
            tui.on(k);
        }
    }
    void key(msg::Key::Type t) {
        msg::Key k{t, {}};
        tui.on(k);
    }
    void enter(std::string_view line) {
        type(line);
        key(msg::Key::Enter);
    }
    template <class M>
    void event(M m) {
        tui.on(m);
    }
    std::vector<msg::StoreMsg> sent() {
        std::vector<msg::StoreMsg> out;
        while (auto m = store.try_pop()) { out.push_back(std::move(*m)); }
        return out;
    }
    std::string row(int r) {
        tui.redraw();
        std::string out;
        for (int c = 0; c < frame.width; ++c) {
            const auto& cell = frame.at(r, c);
            if (cell.continuation) { continue; }
            out += cell.glyph.empty() ? std::string(" ") : std::string(cell.glyph.text());
        }
        return out;
    }
    std::string screen() {
        tui.redraw();
        std::string out;
        for (int r = 0; r < frame.height; ++r) { out += row(r) + "\n"; }
        return out;
    }
};

bool contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

void test_start_and_layout() {
    Harness h;
    h.tui.start({"127.0.0.1", 7000}, 2);
    auto sent = h.sent();
    check(sent.size() == 1 && std::holds_alternative<msg::SetDefaultSubscription>(sent[0])
              && std::get<msg::SetDefaultSubscription>(sent[0]).sub.inbound == msg::Subscription::Inbound::Notify,
          "start: conversations count unread messages until opened");
    check(h.frame.width == 70 && h.frame.height == 14, "the frame has the terminal's size");
    check(contains(h.row(0), "no conversation"), "row 0: the status bar");
    check(contains(h.row(h.height - 4), "listening on 127.0.0.1:7000"), "the notice row sits above the input");
    check(h.row(h.height - 3).starts_with("####"), "the input box has a top border");
    h.width = 40;
    h.height = 10;
    check(contains(h.row(0), "no conversation") && h.frame.height == 10 && h.row(7).starts_with("####"),
          "after a resize the rows are placed exactly again");
}

void test_editing_and_same_requests_as_cli() {
    Harness h;
    h.type("/dial 10.0.0.2 7001");
    check(std::get<0>(h.tui.input().get_content()) == "/dial 10.0.0.2 7001", "typing fills the input");
    check(contains(h.row(h.height - 2), "/dial 10.0.0.2 7001"), "and it is shown");
    h.key(msg::Key::Enter);
    auto sent = h.sent();
    check(sent.size() == 1 && std::get<msg::DialReq>(sent[0]).req == 1
              && std::get<msg::DialReq>(sent[0]).ep == wire::Endpoint{"10.0.0.2", 7001},
          "Enter: the same DialReq the CLI sends for that line");
    check(std::get<0>(h.tui.input().get_content()).empty(), "and the input is cleared");

    // Editing: "hxy" → Left, Backspace → "hy"; a two-byte character as one unit.
    h.type("hxy");
    h.key(msg::Key::Left);
    h.key(msg::Key::Backspace);
    check(std::get<0>(h.tui.input().get_content()) == "hy" && h.tui.input().get_cursor() == 1, "Left and Backspace edit at the cursor");
    msg::Key z{msg::Key::Text, "\xc5\xbc"};   // ż
    h.tui.on(z);
    h.key(msg::Key::Backspace);
    check(std::get<0>(h.tui.input().get_content()) == "hy", "Backspace removes a whole multi-byte character");
    h.key(msg::Key::Right);
    h.key(msg::Key::Right);
    check(h.tui.input().get_cursor() == 2, "Right stops at the end");
}

void test_dial_opens_and_shows_messages() {
    Harness h;
    h.enter("/dial 10.0.0.2 7001");
    std::ignore = h.sent();
    const ConvId conv = 0x1234;
    h.event(msg::Reply{1, true, conv, {}});
    auto sent = h.sent();
    check(sent.size() == 1 && std::get<msg::Subscribe>(sent[0]).conv == conv
              && std::get<msg::Subscribe>(sent[0]).sub.inbound == msg::Subscription::Inbound::Forward && h.tui.active() == conv,
          "a successful dial opens its conversation");
    h.event(msg::ConvState{conv, msg::ConvStatus::Connected, {}});
    h.event(msg::ConvList{{msg::ConvSummary{conv, msg::ConvStatus::Connected, {"10.0.0.2", 7001}, 0}}});
    std::ignore = h.sent();
    h.event(msg::ConvSnapshot{conv, {msg::StoredMessage{0, 77, "earlier message", msg::WallClock::now(),
                                                         msg::DeliveryState::Received}},
                              msg::ConvStatus::Connected});
    const std::string screen = h.screen();
    check(contains(h.row(0), json::hex_id(conv)) && contains(h.row(0), "connected") && contains(h.row(0), "10.0.0.2:7001"),
          "the status bar shows the active conversation, its state and address");
    check(contains(screen, "10.0.0.2:7001@") && contains(screen, "earlier message"), "the snapshot is shown, labelled by peer");

    // Plain text goes to the active conversation.
    h.enter("hello there");
    sent = h.sent();
    check(sent.size() == 1 && std::get<msg::SendText>(sent[0]).conv == conv && std::get<msg::SendText>(sent[0]).body == "hello there",
          "plain text: SendText to the active conversation");
    h.event(msg::MsgAppended{conv, 1, msg::StoredMessage{1, kNoId, "hello there", msg::WallClock::now(), msg::DeliveryState::PendingSend}});
    check(contains(h.screen(), "me (sending)@"), "my message shows as sending");
    h.event(msg::MsgStatus{conv, 1, msg::DeliveryState::Sent});
    check(!contains(h.screen(), "me (sending)") && contains(h.screen(), "me@"), "then as sent");
    h.event(msg::MsgStatus{conv, 1, msg::DeliveryState::Failed});
    check(contains(h.screen(), "me (failed)@"), "or failed");

    // Peer text with control characters cannot reach the terminal as controls.
    h.event(msg::MsgAppended{conv, 2, msg::StoredMessage{2, 77, "evil \x1b[2J text", msg::WallClock::now(), msg::DeliveryState::Received}});
    const std::string after = h.screen();
    check(contains(after, "evil") && !contains(after, "\x1b"), "an escape byte in a message is shown, not executed");
}

void test_errors_switching_and_quit() {
    Harness h;
    h.enter("/frobnicate");
    check(contains(h.row(h.height - 4), "error: unknown command /frobnicate"), "a parse error is shown as a notice");
    check(h.sent().empty(), "and nothing is sent");
    h.enter("hello");
    check(contains(h.row(h.height - 4), "error: no active conversation"), "text without a conversation: an error");

    // A conversation appears (an inbound peer): it is opened since none is active.
    h.event(msg::ConvState{0xaa, msg::ConvStatus::Dialing, {}});
    auto sent = h.sent();
    check(h.tui.active() == 0xaa && sent.size() == 2 && std::holds_alternative<msg::ListReq>(sent[0])
              && std::get<msg::Subscribe>(sent[1]).conv == 0xaa,
          "the first conversation that appears is opened (and its address asked for)");
    h.event(msg::ConvList{{msg::ConvSummary{0xaa, msg::ConvStatus::Dialing, {"10.0.0.3", 7000}, 0}}});
    check(!contains(h.row(h.height - 4), "conversations:"), "the address lookup is not shown as a list");
    h.event(msg::ConvState{0xbb, msg::ConvStatus::Connected, {}});
    std::ignore = h.sent();
    check(h.tui.active() == 0xaa, "a second one does not take over");
    h.event(msg::ConvNotify{0xbb, 3});
    check(contains(h.row(0), "others: 00000000(3)"), "the others' unread counts are in the status bar");

    h.key(msg::Key::Tab);
    sent = h.sent();
    check(h.tui.active() == 0xbb && sent.size() == 2 && std::get<msg::Subscribe>(sent[0]).conv == 0xaa
              && std::get<msg::Subscribe>(sent[0]).sub.inbound == msg::Subscription::Inbound::Notify
              && std::get<msg::Subscribe>(sent[1]).conv == 0xbb,
          "Tab: the previous conversation goes back to counting, the next is opened");
    h.key(msg::Key::Tab);
    check(h.tui.active() == 0xaa, "Tab wraps around");
    std::ignore = h.sent();

    h.enter("/list");
    const auto list_req = std::get<msg::ListReq>(h.sent().at(0)).req;
    h.event(msg::Reply{list_req, true, kNoId, {}});
    h.event(msg::ConvList{{msg::ConvSummary{0xaa, msg::ConvStatus::Connected, {"10.0.0.3", 7000}, 0}}});
    check(contains(h.row(h.height - 4), "1 conversations:"), "an explicit /list is shown");

    h.key(msg::Key::Exit);
    sent = h.sent();
    check(sent.size() == 1 && std::holds_alternative<msg::QuitReq>(sent[0]) && h.shutdowns == std::vector<std::string>{"quit"},
          "Ctrl+C: QuitReq and the shutdown watchdog is armed");
    check(contains(h.row(0), "quitting"), "the status bar says so");
    h.key(msg::Key::Exit);
    std::ignore = h.sent();
    check(h.shutdowns.size() == 1, "a second Ctrl+C does not arm it twice");
    check(!h.tui.done(), "not done until the Store says so");
    h.event(msg::ShutdownDone{});
    check(h.tui.done(), "ShutdownDone ends it");
}

void test_scrolling() {
    Harness h;
    h.event(msg::ConvState{0xaa, msg::ConvStatus::Connected, {}});
    std::vector<msg::StoredMessage> history;
    for (MsgId i = 0; i < 40; ++i) {
        history.push_back(msg::StoredMessage{i, 77, "line " + std::to_string(i), msg::WallClock::now(), msg::DeliveryState::Received});
    }
    h.event(msg::ConvSnapshot{0xaa, history, msg::ConvStatus::Connected});
    check(contains(h.screen(), "line 39") && !contains(h.screen(), "line 20"), "the newest messages are shown");
    h.key(msg::Key::PageUp);
    const std::string up = h.screen();
    check(!contains(up, "line 39") && contains(up, "line 29") && contains(h.row(0), "scrolled back 10"),
          "PageUp shows older messages and says so");
    h.event(msg::MsgAppended{0xaa, 40, msg::StoredMessage{40, 77, "line 40", msg::WallClock::now(), msg::DeliveryState::Received}});
    check(contains(h.screen(), "line 29") && !contains(h.screen(), "line 40"), "a new message does not move a scrolled view");
    h.key(msg::Key::PageDown);
    h.key(msg::Key::PageDown);
    check(contains(h.screen(), "line 40") && !contains(h.row(0), "scrolled"), "PageDown returns to the newest");
}

}  // namespace

int main() {
    std::cout << "test_tui\n";
    test_classify();
    test_sequences();
    test_start_and_layout();
    test_editing_and_same_requests_as_cli();
    test_dial_opens_and_shows_messages();
    test_errors_switching_and_quit();
    test_scrolling();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
