// test_headless.cpp - harness for HeadlessRenderWorker.
//
// Plain assertions (no framework). The worker is driven through a pair of pipes
// plus the render / store queues, exactly as main would wire it:
//   * every command emits the matching store event
//   * every output line is well-formed JSON (checked by a tiny parser below) with
//     a monotonic "seq" and a "t_ns"
//   * "#<req-id>" correlation, including answers that arrive asynchronously
//   * /sync as a barrier, /echo, /stats, EOF == /quit, announce_ready
//   * JSON escaping of quotes, backslashes, control characters, bad UTF-8

#include "Events.hpp"
#include "Render.hpp"
#include "ThreadQueue.hpp"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "FAIL: " << what << '\n';
    }
}

// --- a tiny JSON parser (validator) -----------------------------------------

struct Json {
    enum class Type { Null, Bool, Number, String, Array, Object } type{Type::Null};
    bool b{};
    double n{};
    std::string s;
    std::vector<Json> arr;
    std::vector<std::pair<std::string, Json>> obj;

    const Json* get(std::string_view key) const {
        for (const auto& entry : obj) { if (entry.first == key) { return &entry.second; } }
        return nullptr;
    }
    std::string str(std::string_view key) const {
        const Json* v = get(key);
        return (v && v->type == Type::String) ? v->s : std::string{};
    }
    double num(std::string_view key) const {
        const Json* v = get(key);
        return (v && v->type == Type::Number) ? v->n : -1;
    }
};

class Parser {
public:
    explicit Parser(std::string_view text) : t_(text) {}
    // Parses exactly one value spanning the whole input.
    std::optional<Json> parse() {
        Json out;
        skip();
        if (!value(out)) { return std::nullopt; }
        skip();
        if (p_ != t_.size()) { return std::nullopt; }
        return out;
    }

private:
    void skip() { while (p_ < t_.size() && (t_[p_] == ' ' || t_[p_] == '\t')) { ++p_; } }
    bool lit(std::string_view word) {
        if (t_.substr(p_, word.size()) != word) { return false; }
        p_ += word.size();
        return true;
    }
    bool value(Json& out) {
        if (p_ >= t_.size()) { return false; }
        const char c = t_[p_];
        if (c == '{') { return object(out); }
        if (c == '[') { return array(out); }
        if (c == '"') { out.type = Json::Type::String; return string(out.s); }
        if (c == 't') { out.type = Json::Type::Bool; out.b = true; return lit("true"); }
        if (c == 'f') { out.type = Json::Type::Bool; return lit("false"); }
        if (c == 'n') { return lit("null"); }
        return number(out);
    }
    bool number(Json& out) {
        const size_t start = p_;
        if (p_ < t_.size() && t_[p_] == '-') { ++p_; }
        while (p_ < t_.size() && ((t_[p_] >= '0' && t_[p_] <= '9') || t_[p_] == '.'
                                  || t_[p_] == 'e' || t_[p_] == 'E' || t_[p_] == '+'
                                  || t_[p_] == '-')) { ++p_; }
        if (p_ == start) { return false; }
        out.type = Json::Type::Number;
        out.n = std::stod(std::string(t_.substr(start, p_ - start)));
        return true;
    }
    static void put_utf8(std::string& out, uint32_t cp) {
        if (cp < 0x80) { out += static_cast<char>(cp); }
        else if (cp < 0x800) {
            out += static_cast<char>(0xC0 | (cp >> 6));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        } else {
            out += static_cast<char>(0xE0 | (cp >> 12));
            out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        }
    }
    bool string(std::string& out) {
        ++p_; // opening quote
        while (p_ < t_.size()) {
            const auto c = static_cast<unsigned char>(t_[p_++]);
            if (c == '"') { return true; }
            if (c < 0x20) { return false; } // raw control characters are illegal
            if (c != '\\') { out += static_cast<char>(c); continue; }
            if (p_ >= t_.size()) { return false; }
            const char e = t_[p_++];
            switch (e) {
                case '"': out += '"'; break;
                case '\\': out += '\\'; break;
                case '/': out += '/'; break;
                case 'b': out += '\b'; break;
                case 'f': out += '\f'; break;
                case 'n': out += '\n'; break;
                case 'r': out += '\r'; break;
                case 't': out += '\t'; break;
                case 'u': {
                    if (p_ + 4 > t_.size()) { return false; }
                    uint32_t cp = 0;
                    for (int i = 0; i < 4; ++i) {
                        const char h = t_[p_++];
                        cp <<= 4;
                        if (h >= '0' && h <= '9') { cp |= static_cast<uint32_t>(h - '0'); }
                        else if (h >= 'a' && h <= 'f') { cp |= static_cast<uint32_t>(h - 'a' + 10); }
                        else if (h >= 'A' && h <= 'F') { cp |= static_cast<uint32_t>(h - 'A' + 10); }
                        else { return false; }
                    }
                    put_utf8(out, cp);
                    break;
                }
                default: return false;
            }
        }
        return false;
    }
    bool array(Json& out) {
        out.type = Json::Type::Array;
        ++p_;
        skip();
        if (p_ < t_.size() && t_[p_] == ']') { ++p_; return true; }
        while (true) {
            Json item;
            skip();
            if (!value(item)) { return false; }
            out.arr.push_back(std::move(item));
            skip();
            if (p_ >= t_.size()) { return false; }
            if (t_[p_] == ',') { ++p_; continue; }
            if (t_[p_] == ']') { ++p_; return true; }
            return false;
        }
    }
    bool object(Json& out) {
        out.type = Json::Type::Object;
        ++p_;
        skip();
        if (p_ < t_.size() && t_[p_] == '}') { ++p_; return true; }
        while (true) {
            skip();
            if (p_ >= t_.size() || t_[p_] != '"') { return false; }
            std::string key;
            if (!string(key)) { return false; }
            skip();
            if (p_ >= t_.size() || t_[p_] != ':') { return false; }
            ++p_;
            skip();
            Json item;
            if (!value(item)) { return false; }
            out.obj.emplace_back(std::move(key), std::move(item));
            skip();
            if (p_ >= t_.size()) { return false; }
            if (t_[p_] == ',') { ++p_; continue; }
            if (t_[p_] == '}') { ++p_; return true; }
            return false;
        }
    }

    std::string_view t_;
    size_t p_{0};
};

// --- harness ----------------------------------------------------------------

using namespace std::chrono_literals;

struct Harness {
    ThreadQueue<Event> render;
    ThreadQueue<Event> store;
    int in_pipe[2]{-1, -1};  // test writes in_pipe[1], worker reads in_pipe[0]
    int out_pipe[2]{-1, -1}; // worker writes out_pipe[1], test reads out_pipe[0]
    std::unique_ptr<HeadlessRenderWorker> worker;
    std::thread thread;
    std::string buffer;
    uint64_t last_seq{0};
    bool monotonic{true};
    bool well_formed{true};
    bool stamped{true};
    bool input_closed{false};

    Harness() {
        if (::pipe(in_pipe) != 0 || ::pipe(out_pipe) != 0) { std::abort(); }
        worker = std::make_unique<HeadlessRenderWorker>(
            render, store, in_pipe[0], out_pipe[1], "test-io");
        thread = std::thread{[this] { (*worker)(); }};
    }

    ~Harness() {
        render.graceful_shutdown();
        if (thread.joinable()) { thread.join(); }
        if (!input_closed) { ::close(in_pipe[1]); }
        ::close(in_pipe[0]);
        ::close(out_pipe[0]);
        ::close(out_pipe[1]);
    }

    void send(std::string_view text) {
        (void)!::write(in_pipe[1], text.data(), text.size());
    }
    void line(std::string_view text) { send(std::string(text) + "\n"); }
    void close_input() { ::close(in_pipe[1]); input_closed = true; }

    // Next complete output line parsed as JSON (validated), or nullopt on timeout.
    std::optional<Json> next(std::chrono::milliseconds timeout = 2000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (true) {
            const size_t nl = buffer.find('\n');
            if (nl != std::string::npos) {
                const std::string text = buffer.substr(0, nl);
                buffer.erase(0, nl + 1);
                auto json = Parser{text}.parse();
                if (!json || json->type != Json::Type::Object) {
                    well_formed = false;
                    std::cout << "bad JSON line: " << text << '\n';
                    return std::nullopt;
                }
                const double seq = json->num("seq");
                if (seq <= static_cast<double>(last_seq)) { monotonic = false; }
                last_seq = static_cast<uint64_t>(seq);
                if (json->num("t_ns") < 0 || json->str("event").empty()) { stamped = false; }
                return json;
            }
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            if (left.count() <= 0) { return std::nullopt; }
            pollfd pfd{out_pipe[0], POLLIN, 0};
            if (::poll(&pfd, 1, static_cast<int>(left.count())) > 0) {
                char chunk[4096];
                const ssize_t n = ::read(out_pipe[0], chunk, sizeof(chunk));
                if (n > 0) { buffer.append(chunk, static_cast<size_t>(n)); }
                else if (n == 0) { return std::nullopt; }
            }
        }
    }

    std::optional<Event> store_event(std::chrono::milliseconds timeout = 2000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (auto event = store.try_pop()) { return event; }
            std::this_thread::sleep_for(2ms);
        }
        return std::nullopt;
    }
};

template<typename T>
const T* as(const std::optional<Event>& event) {
    return event ? std::get_if<T>(&*event) : nullptr;
}

// --- tests ------------------------------------------------------------------

void test_commands_emit_store_events() {
    Harness h;

    h.line("/conversations");
    check(as<EventListConversations>(h.store_event()) != nullptr, "/conversations -> EventListConversations");

    h.line("/connect 10.0.0.1 5000");
    auto dial_event = h.store_event();
    const auto* dial = as<EventDialPeer>(dial_event);
    check(dial && dial->ip == "10.0.0.1" && dial->port == 5000 && dial->peer_id == 0,
          "/connect -> EventDialPeer");

    h.line("/reconnect 7");
    auto reconnect_event = h.store_event();
    const auto* reconnect = as<EventReconnectPeer>(reconnect_event);
    check(reconnect && reconnect->conversation_id == 7 && reconnect->peer_id == 0,
          "/reconnect -> EventReconnectPeer");

    h.line("/switch 7");
    auto switch_event = h.store_event();
    const auto* sub = as<EventSubscribe>(switch_event);
    check(sub && sub->conversation_id == 7 && sub->level == AllSubscriptions,
          "/switch -> EventSubscribe(all)");

    h.line("/notify-off 5");
    auto off_event = h.store_event();
    const auto* off = as<EventUnsubscribe>(off_event);
    check(off && off->conversation_id == 5, "/notify-off -> EventUnsubscribe");

    h.line("/notify-on 5");
    auto on_event = h.store_event();
    const auto* on = as<EventSubscribe>(on_event);
    check(on && on->conversation_id == 5 && on->level == SubscriptionFlag::Notification,
          "/notify-on -> EventSubscribe(Notification)");

    h.line("/send 7 hello world  with  spaces");
    auto send_event = h.store_event();
    const auto* send = as<EventComposeMessage>(send_event);
    check(send && send->conversation_id == 7 && send->payload == "hello world  with  spaces",
          "/send -> EventComposeMessage keeps the text verbatim");

    h.line("plain text");
    auto plain_event = h.store_event();
    const auto* plain = as<EventComposeMessage>(plain_event);
    check(plain && plain->conversation_id == 7 && plain->payload == "plain text",
          "a plain line is composed into the current conversation");

    h.line("/echo on");
    auto echo_on = h.store_event();
    check(as<EventSetEcho>(echo_on) && as<EventSetEcho>(echo_on)->on, "/echo on -> EventSetEcho{true}");
    h.line("/echo off");
    auto echo_off = h.store_event();
    check(as<EventSetEcho>(echo_off) && !as<EventSetEcho>(echo_off)->on,
          "/echo off -> EventSetEcho{false}");

    h.line("/current");
    h.line("/bogus");
    h.line("/echo maybe");
    h.line("/send 7");
    check(!h.store_event(150ms).has_value(), "/current, unknown and malformed commands emit no store event");

    check(h.well_formed && h.monotonic && h.stamped, "all lines well-formed, seq monotonic, t_ns present");
}

void test_replies_and_correlation() {
    Harness h;

    // Synchronous replies carry the req-id.
    h.line("#a1 /current");
    auto reply = h.next();
    check(reply && reply->str("event") == "info" && reply->str("re") == "a1",
          "/current replies with an info event carrying re");

    h.line("#a2 /bogus");
    reply = h.next();
    check(reply && reply->str("event") == "error" && reply->str("re") == "a2",
          "unknown command -> error carrying re");

    h.line("#a3 /send 4 hi");
    reply = h.next();
    check(reply && reply->str("event") == "ok" && reply->str("re") == "a3",
          "/send is acknowledged with ok carrying re");
    (void)h.store_event();

    h.line("/send 4 hi"); // no re: no ack line
    (void)h.store_event();

    // /stats
    h.line("#a4 /stats");
    reply = h.next();
    check(reply && reply->str("event") == "stats" && reply->str("re") == "a4"
              && reply->get("metrics") && reply->get("metrics")->type == Json::Type::Object,
          "/stats emits stats with a metrics object");

    // Asynchronous: /connect answered by EventPeerDialed, then the switch snapshot.
    h.line("#c1 /connect 1.2.3.4 99");
    reply = h.next();
    check(reply && reply->str("event") == "info" && reply->str("re") == "c1",
          "/connect first reports 'connecting' with its re");
    (void)h.store_event();
    (void)h.render.emplace(EventPeerDialed{3, 11});
    reply = h.next();
    check(reply && reply->str("event") == "peer" && reply->str("what") == "dialed"
              && reply->str("re") == "c1" && reply->num("conversation") == 3
              && reply->num("peer") == 11,
          "EventPeerDialed resolves the pending /connect");
    auto sub_event = h.store_event();
    check(as<EventSubscribe>(sub_event) && as<EventSubscribe>(sub_event)->conversation_id == 3,
          "a dialed conversation is switched to");

    // Two /conversations in flight take their answers in order.
    h.line("#l1 /conversations");
    h.line("#l2 /conversations");
    (void)h.store_event();
    (void)h.store_event();
    (void)h.render.emplace(EventConversationList{{ConversationInfo{3, {11}, 2}}});
    (void)h.render.emplace(EventConversationList{});
    reply = h.next();
    check(reply && reply->str("event") == "conversations" && reply->str("re") == "l1"
              && reply->get("items") && reply->get("items")->arr.size() == 1,
          "first list answers the oldest /conversations");
    if (reply && reply->get("items") && !reply->get("items")->arr.empty()) {
        const Json& item = reply->get("items")->arr[0];
        check(item.num("conversation") == 3 && item.num("unread") == 2
                  && item.get("peers") && item.get("peers")->arr.size() == 1
                  && item.get("peers")->arr[0].n == 11,
              "conversations item fields");
    }
    reply = h.next();
    check(reply && reply->str("event") == "conversations" && reply->str("re") == "l2"
              && reply->get("items") && reply->get("items")->arr.empty(),
          "second list answers the next one (empty)");

    // /switch: the snapshot is the reply.
    h.line("#s1 /switch 9");
    (void)h.store_event();
    auto snapshot = std::make_unique<ConversationStore>();
    snapshot->conversation_id = 9;
    snapshot->store.push_back({"hi there", 0});
    snapshot->store.push_back({"yo", 5});
    (void)h.render.emplace(EventConversationSnapshot{std::move(snapshot)});
    reply = h.next();
    check(reply && reply->str("event") == "messages" && reply->str("re") == "s1"
              && reply->num("conversation") == 9
              && reply->get("snapshot") && reply->get("snapshot")->b,
          "snapshot answers /switch with re");
    if (reply && reply->get("items") && reply->get("items")->arr.size() == 2) {
        const auto& items = reply->get("items")->arr;
        check(items[0].str("from") == "me" && items[0].str("text") == "hi there"
                  && items[1].str("from") == "peer#5" && items[1].str("text") == "yo",
              "snapshot items carry from/text");
    } else {
        check(false, "snapshot has two items");
    }

    // Unsolicited events carry no re.
    (void)h.render.emplace(EventConversationDelta{9, {{"later", 5}}});
    reply = h.next();
    check(reply && reply->str("event") == "messages" && reply->get("re") == nullptr
              && reply->get("snapshot") && !reply->get("snapshot")->b,
          "delta is an unsolicited messages event");
    (void)h.render.emplace(EventConversationNotification{4});
    reply = h.next();
    check(reply && reply->str("event") == "notification" && reply->num("conversation") == 4,
          "notification event");
    (void)h.render.emplace(EventPeerConnection{4, 12, false});
    reply = h.next();
    check(reply && reply->str("event") == "peer" && reply->str("what") == "connected"
              && reply->num("peer") == 12,
          "peer connected event");
    (void)h.render.emplace(EventPeerConnection{4, 12, true});
    reply = h.next();
    check(reply && reply->str("what") == "reconnected", "peer reconnected event");
    (void)h.render.emplace(EventPeerDisconnected{4, 12});
    reply = h.next();
    check(reply && reply->str("what") == "disconnected", "peer disconnected event");
    (void)h.render.emplace(EventError{"boom"});
    reply = h.next();
    check(reply && reply->str("event") == "error" && reply->str("message") == "boom",
          "worker error event");

    // A failed dial resolves the pending /connect as an error-bearing peer event.
    h.line("#c2 /connect 5.5.5.5 1");
    (void)h.next(); // connecting info
    (void)h.store_event();
    (void)h.render.emplace(EventPeerDisconnected{0, 77});
    reply = h.next();
    check(reply && reply->str("what") == "disconnected" && reply->str("re") == "c2",
          "a disconnect of an unknown peer resolves the pending /connect");

    // A reconnect to nowhere comes back as an error.
    h.line("#r1 /reconnect 42");
    (void)h.next();
    (void)h.store_event();
    (void)h.render.emplace(EventError{"conversation 42 has no peer to reconnect"});
    reply = h.next();
    check(reply && reply->str("event") == "error" && reply->str("re") == "r1",
          "reconnect error carries re");

    check(h.well_formed && h.monotonic && h.stamped, "correlation: lines well-formed and monotonic");
}

void test_sync_barrier() {
    Harness h;
    h.line("#b1 /sync");
    auto request = h.store_event();
    check(as<EventListConversations>(request) != nullptr, "/sync -> EventListConversations");
    check(!h.next(150ms).has_value(), "/sync stays silent until the list returns");
    (void)h.render.emplace(EventConversationList{});
    auto reply = h.next();
    check(reply && reply->str("event") == "sync" && reply->str("re") == "b1",
          "list answers the barrier with sync + re");
    check(!h.next(100ms).has_value(), "the barrier produces no conversations event");

    // Mixed with /conversations, answers stay in order.
    h.line("#b2 /conversations");
    h.line("#b3 /sync");
    (void)h.store_event();
    (void)h.store_event();
    (void)h.render.emplace(EventConversationList{});
    (void)h.render.emplace(EventConversationList{});
    auto first = h.next();
    auto second = h.next();
    check(first && first->str("event") == "conversations" && first->str("re") == "b2",
          "ordered: conversations first");
    check(second && second->str("event") == "sync" && second->str("re") == "b3",
          "ordered: sync second");
}

void test_ready_and_bye() {
    Harness h;
    h.worker->announce_ready("127.0.0.1:9001");
    auto ready = h.next();
    check(ready && ready->str("event") == "ready" && ready->str("io") == "test-io"
              && ready->str("listen") == "127.0.0.1:9001" && ready->num("seq") == 1,
          "ready line (seq 1)");

    h.line("#q1 /quit");
    auto bye = h.next();
    check(bye && bye->str("event") == "bye" && bye->str("re") == "q1", "/quit -> bye with re");
    h.thread.join();
    check(!h.next(100ms).has_value(), "nothing after bye");
}

void test_ready_error() {
    Harness h;
    h.worker->announce_ready("", "bind failed: address in use");
    auto ready = h.next();
    check(ready && ready->str("event") == "ready" && ready->str("error") == "bind failed: address in use"
              && ready->get("listen") == nullptr,
          "ready line carries the bind error");
}

void test_eof_quits() {
    Harness h;
    h.send("/current"); // an unterminated last line is still a line
    h.close_input();
    auto info = h.next();
    check(info && info->str("event") == "info", "unterminated last line is handled");
    auto bye = h.next();
    check(bye && bye->str("event") == "bye", "EOF on input quits with bye");
    h.thread.join();
}

void test_escaping() {
    Harness h;
    const std::string nasty = std::string("q\"uote back\\slash tab\t nl\\n ctl\x01\x1f del\x7f ")
                              + "utf8 \xC3\xA9\xE2\x82\xAC bad\xFF\xC3 end";
    h.line("/switch 1");
    (void)h.store_event();
    (void)h.render.emplace(EventConversationDelta{1, {{nasty, 3}}});
    // Raw line must contain no raw control characters; the parser rejects them.
    auto reply = h.next();
    check(h.well_formed, "escaped output is valid JSON");
    if (reply && reply->get("items") && reply->get("items")->arr.size() == 1) {
        const std::string text = reply->get("items")->arr[0].str("text");
        check(text.find("q\"uote back\\slash tab\t nl\\n ctl\x01\x1f del\x7f ") == 0,
              "quotes, backslashes and control characters round-trip");
        check(text.find("utf8 \xC3\xA9\xE2\x82\xAC bad") != std::string::npos,
              "valid UTF-8 is preserved");
        check(text.find("\xEF\xBF\xBD") != std::string::npos && text.find('\xFF') == std::string::npos,
              "invalid UTF-8 is replaced with U+FFFD");
    } else {
        check(false, "escaping: got the message item");
    }

    // The req-id is escaped too.
    h.line("#we\"ird /current");
    reply = h.next();
    check(h.well_formed && reply && reply->str("re") == "we\"ird", "re is escaped");
}

} // namespace

int main() {
    test_commands_emit_store_events();
    test_replies_and_correlation();
    test_sync_barrier();
    test_ready_and_bye();
    test_ready_error();
    test_eof_quits();
    test_escaping();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
