// test_main.cpp - end-to-end smoke test for p2p::MainWorker.
//
// Plain assertions (no framework): every failed check is printed and the process
// exits non-zero if anything failed. Covers:
//   * the whole stack (terminal, queues, peer table and all workers) starting up
//     and shutting down cleanly: Ctrl+C on the terminal reaches the keyboard
//     worker, which ends the render loop and lets MainWorker join every thread.
//   * a busy listen port being reported (not crashing) and still shutting down.
//
// MainWorker owns a real TerminalUI, whose constructor needs a tty (it calls
// tcgetattr), so the test opens a pseudo-terminal and points STDIN at its slave
// side exactly like test_workers / test_render.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE // posix_openpt / grantpt / unlockpt / ptsname_r
#endif

#include "TerminalUI.hpp"
#include "ThreadWorkers.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>

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

    // Types `bytes` on the terminal (written to the master side).
    bool feed(std::string_view bytes) const {
        return ::write(master, bytes.data(), bytes.size())
            == static_cast<ssize_t>(bytes.size());
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

// ---------------------------------------------------------------------------

void test_main_worker_starts_and_stops() {
    constexpr unsigned kPort = 9996;

    Pty pty;
    check(pty.open(), "main: pty opened");
    if (pty.master < 0) { return; }

    StdinRedirect redirect(pty.slave);
    check(redirect.active, "main: stdin redirected");
    if (!redirect.active) { return; }

    p2p::MainWorker worker{"127.0.0.1", kPort, "/dev/null"};
    std::atomic<bool> finished{false};
    std::thread thread{[&worker, &finished] {
        worker();
        finished.store(true, std::memory_order_release);
    }};

    // Let the listener bind and the workers come up before quitting.
    std::this_thread::sleep_for(std::chrono::milliseconds{200});

    // Ctrl+C (ISIG off, so it arrives as 0x03) makes the keyboard worker push
    // EXIT, end the render loop and trigger MainWorker's orderly shutdown.
    check(pty.feed("\x03"), "main: quit key written");

    for (int i = 0; i < 500 && !finished.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    check(finished.load(std::memory_order_acquire), "main: worker returned after quit");
    if (finished.load(std::memory_order_acquire)) {
        thread.join();
    } else {
        thread.detach(); // do not hang the run; the failure is already recorded
    }
}

// A busy listen port must be reported, not crash: the listener thread fails, the
// app keeps running, and Ctrl+C still shuts everything down.
void test_main_worker_survives_bind_failure() {
    constexpr unsigned kPort = 9995;

    // Occupy the port so MainWorker's listener cannot bind it.
    TcpListener blocker{"127.0.0.1", kPort, 1};

    Pty pty;
    check(pty.open(), "bind-fail: pty opened");
    if (pty.master < 0) { return; }

    StdinRedirect redirect(pty.slave);
    check(redirect.active, "bind-fail: stdin redirected");
    if (!redirect.active) { return; }

    p2p::MainWorker worker{"127.0.0.1", kPort, "/dev/null"};
    std::atomic<bool> finished{false};
    std::thread thread{[&worker, &finished] {
        worker();
        finished.store(true, std::memory_order_release);
    }};

    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    check(pty.feed("\x03"), "bind-fail: quit key written");

    for (int i = 0; i < 500 && !finished.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    check(finished.load(std::memory_order_acquire),
          "bind-fail: worker returned despite the busy port");
    if (finished.load(std::memory_order_acquire)) {
        thread.join();
    } else {
        thread.detach();
    }
}

} // namespace

int main() {
    test_main_worker_starts_and_stops();
    test_main_worker_survives_bind_failure();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
