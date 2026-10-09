// test_render.cpp - harness for RenderWorker.
//
// Plain assertions (no framework): every failed check is printed and the process
// exits non-zero if anything failed. Covers the render thread's command handling
// by driving it with the same EventLocal values the keyboard listener would push:
//   * /conversations -> EventListConversations
//   * /connect <ip> <port> -> EventDialPeer (peer id left for the store to mint)
//   * /switch <conversation id> -> EventSubscribe with every subscription flag (the
//     store returns the conversation left behind to the notification default)
//   * /notify-on|/notify-off <conversation id> -> EventSubscribe with the Notification
//     flag / EventUnsubscribe (never for the conversation being viewed)
//   * /current -> nothing at all (the render already knows the id it is viewing)
//
// RenderWorker only needs the TerminalUI for output, but TerminalUI's constructor
// captures STDIN's terminal attributes (it calls tcgetattr), so these tests open a
// pseudo-terminal and point STDIN at its slave side exactly like test_workers.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE // posix_openpt / grantpt / unlockpt / ptsname_r
#endif

#include "Events.hpp"
#include "TerminalUI.hpp"
#include "ThreadQueue.hpp"
#include "ThreadWorkers.hpp"

#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>

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

// A pseudo-terminal pair; the slave is handed to TerminalUI via STDIN.
struct Pty {
    int master{-1};
    int slave{-1};

    bool open() {
        master = ::posix_openpt(O_RDWR | O_NOCTTY);
        if (master < 0) { return false; }
        if (::grantpt(master) != 0 || ::unlockpt(master) != 0) { return false; }
        char name[128] = {};
        if (::ptsname_r(master, name, sizeof(name)) != 0) { return false; }
        slave = ::open(name, O_RDWR | O_NOCTTY);
        return slave >= 0;
    }

    ~Pty() {
        if (slave >= 0) { ::close(slave); }
        if (master >= 0) { ::close(master); }
    }
};

// Points STDIN_FILENO at `fd` for the enclosing scope, restoring it on exit.
// TerminalUI must be constructed only after this redirect is in place.
struct StdinRedirect {
    int saved{-1};
    bool active{false};

    explicit StdinRedirect(int fd) {
        saved = ::dup(STDIN_FILENO);
        if (saved < 0) { return; }
        if (::dup2(fd, STDIN_FILENO) < 0) { ::close(saved); saved = -1; return; }
        active = true;
    }
    StdinRedirect(const StdinRedirect&) = delete;
    StdinRedirect& operator=(const StdinRedirect&) = delete;
    ~StdinRedirect() {
        if (saved >= 0) { ::dup2(saved, STDIN_FILENO); ::close(saved); }
    }
};

// Types `text` one character per EventLocal, then a final ENTER.
void type(ThreadQueue<Event>& queue, std::string_view text) {
    for (const char ch : text) {
        (void)queue.emplace(EventLocal{EventLocal::Type::TEXT, std::string(1, ch)});
    }
    (void)queue.emplace(EventLocal{EventLocal::Type::ENTER, {}});
}

// Pops the next store event without blocking forever.
std::optional<Event> wait_for(ThreadQueue<Event>& queue, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (auto event = queue.try_pop()) { return event; }
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------

void test_commands_emit_store_events() {
    Pty pty;
    check(pty.open(), "render: pty opened");
    if (pty.master < 0) { return; }

    ThreadQueue<Event> render_queue;
    ThreadQueue<Event> store_queue;
    StdinRedirect redirect(pty.slave);
    check(redirect.active, "render: stdin redirected");
    if (!redirect.active) { return; }

    TerminalUI ui;
    RenderWorker worker{ui, render_queue, store_queue};
    std::thread thread{[&worker] { worker(); }};

    const auto timeout = std::chrono::seconds{2};

    // /conversations asks the store for the list.
    type(render_queue, "/conversations");
    auto list_request = wait_for(store_queue, timeout);
    check(list_request.has_value(), "render: /conversations emitted an event");
    check(list_request && std::holds_alternative<EventListConversations>(*list_request),
          "render: /conversations emits EventListConversations");

    // /connect <ip> <port> asks the store to dial, leaving the peer id at 0.
    type(render_queue, "/connect 10.0.0.1 5000");
    auto dial = wait_for(store_queue, timeout);
    check(dial.has_value(), "render: /connect emitted an event");
    if (dial) {
        const auto* request = std::get_if<EventDialPeer>(&*dial);
        check(request != nullptr, "render: /connect emits EventDialPeer");
        check(request && request->ip == "10.0.0.1", "render: /connect keeps the ip");
        check(request && request->port == 5000, "render: /connect keeps the port");
        check(request && request->peer_id == 0, "render: /connect leaves the peer id unset");
    }

    // /switch <id> subscribes to the conversation with every flag.
    type(render_queue, "/switch 7");
    auto subscribe = wait_for(store_queue, timeout);
    check(subscribe.has_value(), "render: /switch emitted an event");
    if (subscribe) {
        const auto* request = std::get_if<EventSubscribe>(&*subscribe);
        check(request != nullptr, "render: /switch emits EventSubscribe");
        check(request && request->conversation_id == 7, "render: /switch keeps the id");
        check(request && request->level == AllSubscriptions,
              "render: /switch subscribes to every flag");
    }

    // /notify-off mutes a background conversation, /notify-on brings it back.
    type(render_queue, "/notify-off 5");
    auto mute = wait_for(store_queue, timeout);
    check(mute.has_value(), "render: /notify-off emitted an event");
    if (mute) {
        const auto* request = std::get_if<EventUnsubscribe>(&*mute);
        check(request != nullptr, "render: /notify-off drops the subscription");
        check(request && request->conversation_id == 5, "render: /notify-off keeps the id");
    }

    type(render_queue, "/notify-on 5");
    auto unmute = wait_for(store_queue, timeout);
    check(unmute.has_value(), "render: /notify-on emitted an event");
    if (unmute) {
        const auto* request = std::get_if<EventSubscribe>(&*unmute);
        check(request != nullptr, "render: /notify-on subscribes");
        check(request && request->conversation_id == 5, "render: /notify-on keeps the id");
        check(request && request->level == SubscriptionFlag::Notification,
              "render: /notify-on subscribes in notification mode");
    }

    type(render_queue, "/notify-on 7");
    check(!wait_for(store_queue, std::chrono::milliseconds{200}).has_value(),
          "render: /notify-on the open conversation emits nothing");

    // /switch only moves the content subscription: the store returns the
    // conversation left behind to the background default.
    type(render_queue, "/switch 3");
    auto arrived = wait_for(store_queue, timeout);
    check(arrived.has_value(), "render: /switch emitted an event");
    if (arrived) {
        const auto* request = std::get_if<EventSubscribe>(&*arrived);
        check(request != nullptr, "render: /switch emits EventSubscribe");
        check(request && request->conversation_id == 3, "render: /switch keeps the new id");
        check(request && request->level == AllSubscriptions,
              "render: /switch subscribes to every content flag");
    }
    check(!wait_for(store_queue, std::chrono::milliseconds{200}).has_value(),
          "render: /switch sends nothing else (the store handles the old conversation)");

    // /current is display-only: it reports the conversation being viewed and asks
    // the store for nothing (the render owns that id).
    type(render_queue, "/current");
    check(!wait_for(store_queue, std::chrono::milliseconds{200}).has_value(),
          "render: /current emits no store event");

    // An unknown command yields no store event.
    type(render_queue, "/bogus");
    check(!wait_for(store_queue, std::chrono::milliseconds{200}).has_value(),
          "render: unknown command emits nothing");

    render_queue.graceful_shutdown();
    thread.join();
}

} // namespace

int main() {
    test_commands_emit_store_events();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
