// test_workers.cpp - unit tests for KeyboardListenerWorker.
//
// Plain assertions (no framework): every failed check is collected and printed
// at the end; the process exits non-zero if anything failed.
//
// The worker reads STDIN_FILENO through TerminalUI, whose constructor needs a
// real tty (it calls tcgetattr). These tests therefore open a pseudo-terminal,
// point STDIN at its slave side, and feed keystrokes by writing to the master
// side. Every scenario ends with Ctrl+C so the worker terminates and the queue
// drain returns; a returning drain also proves the worker shut the queue down.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE // posix_openpt / grantpt / unlockpt / ptsname_r
#endif

#include "Events.hpp"
#include "TerminalUI.hpp"
#include "ThreadQueue.hpp"
#include "ThreadWorkers.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

std::vector<std::string> g_failures;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) { g_failures.push_back(what); }
}

// A pseudo-terminal pair. The slave is handed to TerminalUI (via STDIN); the
// master is how the test types.
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

    // Types `bytes` on the terminal.
    bool feed(std::string_view bytes) const {
        return ::write(master, bytes.data(), bytes.size())
            == static_cast<ssize_t>(bytes.size());
    }

    // Closing the master makes the worker's next read observe EOF.
    void close_master() {
        if (master >= 0) { ::close(master); master = -1; }
    }

    ~Pty() {
        if (slave >= 0) { ::close(slave); }
        if (master >= 0) { ::close(master); }
    }
};

// Points STDIN_FILENO at `fd` for the enclosing scope, restoring the previous
// descriptor on exit. TerminalUI captures stdin's terminal attributes, so it
// must be constructed only after this redirect is in place.
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

const EventLocal* as_local(const Event& event) { return std::get_if<EventLocal>(&event); }

// Types `input`, then Ctrl+C, and drains everything the worker delivered.
std::optional<std::vector<Event>> run_scenario(Pty& pty, std::string_view input) {
    if (!pty.open()) { return std::nullopt; }

    ThreadQueue<Event> queue;
    StdinRedirect redirect(pty.slave);
    if (!redirect.active) { return std::nullopt; }

    TerminalUI ui;
    KeyboardListenerWorker listener{ui, queue};
    std::thread worker{[&listener] { listener(); }};

    (void)pty.feed(input);
    (void)pty.feed("\x03");

    std::vector<Event> events;
    while (auto event = queue.pop()) { events.push_back(std::move(*event)); }
    worker.join();
    return events;
}

// ---------------------------------------------------------------------------

void test_text_ascii() {
    Pty pty;
    auto events = run_scenario(pty, "hi");
    check(events.has_value(), "text: pty opened");
    if (!events) { return; }

    check(events->size() == 3, "text: 'hi' -> TEXT 'h', TEXT 'i', EXIT");
    if (events->size() != 3) { return; }
    const EventLocal* h = as_local((*events)[0]);
    const EventLocal* i = as_local((*events)[1]);
    const EventLocal* quit = as_local((*events)[2]);
    check(h && h->type == EventLocal::Type::TEXT && h->payload == "h", "text: first key is 'h'");
    check(i && i->type == EventLocal::Type::TEXT && i->payload == "i", "text: second key is 'i'");
    check(quit && quit->type == EventLocal::Type::EXIT, "text: trailing EXIT");
}

void test_text_utf8() {
    Pty pty;
    auto events = run_scenario(pty, "\xC3\xA9"); // U+00E9 LATIN SMALL LETTER E WITH ACUTE
    check(events.has_value(), "utf8: pty opened");
    if (!events) { return; }

    check(events->size() == 2, "utf8: one TEXT + EXIT");
    if (events->size() != 2) { return; }
    const EventLocal* text = as_local((*events)[0]);
    check(text && text->type == EventLocal::Type::TEXT, "utf8: first event is TEXT");
    check(text && text->payload == "\xC3\xA9", "utf8: both bytes of the codepoint delivered");
}

void test_enter() {
    Pty pty;
    auto events = run_scenario(pty, "\r\n");
    check(events.has_value(), "enter: pty opened");
    if (!events) { return; }

    check(events->size() == 3, "enter: CR and LF -> two ENTER + EXIT");
    if (events->size() != 3) { return; }
    const EventLocal* cr = as_local((*events)[0]);
    const EventLocal* lf = as_local((*events)[1]);
    check(cr && cr->type == EventLocal::Type::ENTER, "enter: CR is ENTER");
    check(lf && lf->type == EventLocal::Type::ENTER, "enter: LF is ENTER");
}

void test_backspace() {
    Pty pty;
    auto events = run_scenario(pty, "\x7F\x08");
    check(events.has_value(), "backspace: pty opened");
    if (!events) { return; }

    check(events->size() == 3, "backspace: DEL and BS -> two BACKSPACE + EXIT");
    if (events->size() != 3) { return; }
    const EventLocal* del = as_local((*events)[0]);
    const EventLocal* bs = as_local((*events)[1]);
    check(del && del->type == EventLocal::Type::BACKSPACE, "backspace: 0x7F is BACKSPACE");
    check(bs && bs->type == EventLocal::Type::BACKSPACE, "backspace: 0x08 is BACKSPACE");
}

void test_arrows() {
    Pty pty;
    auto events = run_scenario(pty, "\x1b[D\x1b[C\x1b[A\x1b[B");
    check(events.has_value(), "arrows: pty opened");
    if (!events) { return; }

    check(events->size() == 5, "arrows: left, right, up and down + EXIT");
    if (events->size() != 5) { return; }
    const EventLocal* left = as_local((*events)[0]);
    const EventLocal* right = as_local((*events)[1]);
    const EventLocal* up = as_local((*events)[2]);
    const EventLocal* down = as_local((*events)[3]);
    check(left && left->type == EventLocal::Type::ARROW_LEFT, "arrows: ESC [ D is ARROW_LEFT");
    check(right && right->type == EventLocal::Type::ARROW_RIGHT, "arrows: ESC [ C is ARROW_RIGHT");
    check(up && up->type == EventLocal::Type::ARROW_UP, "arrows: ESC [ A is ARROW_UP");
    check(down && down->type == EventLocal::Type::ARROW_DOWN, "arrows: ESC [ B is ARROW_DOWN");
}

void test_ignores_unmapped_input() {
    Pty pty;
    // 0x01 is a control byte; ESC [ H is the (unmapped) home key; then 'a'.
    auto events = run_scenario(pty, "\x01\x1b[Ha");
    check(events.has_value(), "ignore: pty opened");
    if (!events) { return; }

    check(events->size() == 2, "ignore: only the printable key survives");
    if (events->size() != 2) { return; }
    const EventLocal* text = as_local((*events)[0]);
    check(text && text->type == EventLocal::Type::TEXT && text->payload == "a",
          "ignore: 'a' delivered, control byte and home key dropped");
    const EventLocal* quit = as_local((*events)[1]);
    check(quit && quit->type == EventLocal::Type::EXIT, "ignore: trailing EXIT");
}

void test_exit_shuts_queue() {
    Pty pty;
    if (!pty.open()) { check(false, "exit: pty opened"); return; }

    ThreadQueue<Event> queue;
    StdinRedirect redirect(pty.slave);
    check(redirect.active, "exit: stdin redirected");
    if (!redirect.active) { return; }

    TerminalUI ui;
    KeyboardListenerWorker listener{ui, queue};
    std::thread worker{[&listener] { listener(); }};

    check(pty.feed("\x03"), "exit: Ctrl+C written");

    auto first = queue.pop();
    check(first.has_value(), "exit: an event is delivered");
    if (first) {
        const EventLocal* local = as_local(*first);
        check(local && local->type == EventLocal::Type::EXIT, "exit: first event is EXIT");
    }
    check(!queue.pop().has_value(), "exit: queue is shut down after EXIT");
    worker.join();
}

void test_eof_quits() {
    Pty pty;
    if (!pty.open()) { check(false, "eof: pty opened"); return; }

    ThreadQueue<Event> queue;
    StdinRedirect redirect(pty.slave);
    check(redirect.active, "eof: stdin redirected");
    if (!redirect.active) { return; }

    TerminalUI ui;
    KeyboardListenerWorker listener{ui, queue};
    std::thread worker{[&listener] { listener(); }};

    pty.close_master(); // the worker's next read sees EOF

    auto first = queue.pop();
    check(first.has_value(), "eof: an event is delivered");
    if (first) {
        const EventLocal* local = as_local(*first);
        check(local && local->type == EventLocal::Type::EXIT, "eof: event is EXIT");
    }
    check(!queue.pop().has_value(), "eof: queue is shut down");
    worker.join();
}

void test_stops_when_queue_closed() {
    Pty pty;
    if (!pty.open()) { check(false, "closed-queue: pty opened"); return; }

    ThreadQueue<Event> queue;
    queue.graceful_shutdown(); // the consumer is already gone

    StdinRedirect redirect(pty.slave);
    check(redirect.active, "closed-queue: stdin redirected");
    if (!redirect.active) { return; }

    TerminalUI ui;
    KeyboardListenerWorker listener{ui, queue};

    std::atomic<bool> finished{false};
    std::thread worker{[&listener, &finished] {
        listener();
        finished.store(true, std::memory_order_release);
    }};

    check(pty.feed("a"), "closed-queue: key written");

    for (int i = 0; i < 300 && !finished.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    check(finished.load(std::memory_order_acquire),
          "closed-queue: worker returns once the queue is closed");
    if (finished.load(std::memory_order_acquire)) {
        worker.join();
    } else {
        worker.detach(); // do not hang the run; the failure is already recorded
    }
}

} // namespace

int main() {
    test_text_ascii();
    test_text_utf8();
    test_enter();
    test_backspace();
    test_arrows();
    test_ignores_unmapped_input();
    test_exit_shuts_queue();
    test_eof_quits();
    test_stops_when_queue_closed();

    for (const std::string& failure : g_failures) {
        std::cout << "FAIL: " << failure << '\n';
    }
    std::cout << (g_checks - static_cast<int>(g_failures.size())) << '/' << g_checks
              << " checks passed\n";
    return g_failures.empty() ? 0 : 1;
}
