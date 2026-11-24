// test_tui_e2e.cpp - the terminal UI in a real pseudo-terminal.
//
// A `node` (the TUI) runs on a pty; the test types into it and reads what it
// draws. Its peer is a `node --cli`, driven through pipes. Covers: start-up on
// a terminal, /dial typed as keys, chat both ways, Ctrl+C ending the process
// with exit 0 and the terminal reset, and refusing to start without a terminal.
//
// Plain assertions, no framework. Every wait is bounded.

#include "CliProcess.hpp"

#include <iostream>
#include <pty.h>
#include <regex>
#include <string>
#include <sys/ioctl.h>

namespace {

using namespace cli_process;
using namespace std::chrono_literals;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

// A node on a pseudo-terminal. Everything it writes is kept, escapes and all.
class TuiProcess {
public:
    explicit TuiProcess(std::vector<std::string> extra = {}) {
        winsize ws{};
        ws.ws_col = 100;
        ws.ws_row = 30;
        pid_ = ::forkpty(&master_, nullptr, nullptr, &ws);
        if (pid_ == 0) {
            std::vector<std::string> args{NODE_BINARY, "--listen", "127.0.0.1:0"};
            args.insert(args.end(), extra.begin(), extra.end());
            std::vector<char*> argv;
            for (auto& a : args) { argv.push_back(a.data()); }
            argv.push_back(nullptr);
            ::execv(argv[0], argv.data());
            ::_exit(127);
        }
        ::fcntl(master_, F_SETFD, FD_CLOEXEC);   // later children must not hold it open
    }

    ~TuiProcess() {
        if (!exit_code_) {
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
        }
        ::close(master_);
    }

    void type(const std::string& text) {
        // One write per byte, like typing: the UI reads key by key.
        for (const char c : text) {
            std::ignore = ::write(master_, &c, 1);
            std::this_thread::sleep_for(1ms);
        }
    }

    // Waits until the output so far matches `re`; returns its first group (or
    // the whole match when it has none).
    std::optional<std::string> wait_for(const std::regex& re, std::chrono::milliseconds limit = 5s) {
        const auto until = Clock::now() + limit;
        for (;;) {
            std::smatch m;
            if (std::regex_search(output_, m, re)) { return m.size() > 1 ? m[1].str() : m[0].str(); }
            if (Clock::now() >= until || !read_more(until)) { return std::nullopt; }
        }
    }

    bool wait_text(const std::string& text, std::chrono::milliseconds limit = 5s) {
        const auto until = Clock::now() + limit;
        while (output_.find(text) == std::string::npos) {
            if (Clock::now() >= until || !read_more(until)) { return false; }
        }
        return true;
    }

    std::optional<int> wait_exit(std::chrono::milliseconds limit = 5s) {
        const auto until = Clock::now() + limit;
        while (!exit_code_ && Clock::now() < until) {
            read_more(Clock::now() + 10ms);
            int status = 0;
            if (::waitpid(pid_, &status, WNOHANG) == pid_) {
                exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
            }
        }
        if (exit_code_) { while (read_more(Clock::now() + 50ms)) {} }
        return exit_code_;
    }

    const std::string& output() const { return output_; }

private:
    bool read_more(Clock::time_point until) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(until - Clock::now()).count();
        pollfd p{master_, POLLIN, 0};
        if (::poll(&p, 1, static_cast<int>(std::max<long long>(left, 0))) <= 0) { return false; }
        char buf[65536];
        const ssize_t n = ::read(master_, buf, sizeof buf);
        if (n <= 0) { return false; }   // EIO once the child has closed the terminal
        output_.append(buf, static_cast<std::size_t>(n));
        return true;
    }

    pid_t pid_ = -1;
    int master_ = -1;
    std::string output_;
    std::optional<int> exit_code_;
};

void test_session() {
    Node peer;
    const auto ready = peer.wait_event("ready");
    const std::string listen = ready ? str_field(*ready, "listen") : "";
    const std::string peer_port = listen.substr(listen.rfind(':') + 1);

    TuiProcess tui;
    const auto port = tui.wait_for(std::regex(R"(listening on 127\.0\.0\.1:(\d+))"));
    check(port.has_value(), "the TUI starts on a terminal and shows where it listens");
    check(tui.wait_text("no conversation"), "its status bar says there is no conversation yet");

    tui.type("/dial 127.0.0.1 " + peer_port + "\r");
    const auto up = peer.wait_event("conv_state", [](const std::string& l) { return str_field(l, "status") == "connected"; });
    const std::string peer_conv = up ? str_field(*up, "conv") : "";
    check(!peer_conv.empty(), "typing /dial connects to the peer");
    check(tui.wait_text("connected"), "the TUI shows the conversation connected");

    tui.type("hello from the tui\r");
    check(peer.wait_event("msg", [](const std::string& l) { return str_field(l, "body") == "hello from the tui"; }).has_value(),
          "plain text typed in the TUI reaches the peer");
    peer.send("/send " + peer_conv + " hi there, terminal");
    check(tui.wait_text("hi there, terminal"), "the peer's reply is drawn in the TUI");

    tui.type("\x03");   // Ctrl+C
    const auto code = tui.wait_exit();
    check(code == 0, "Ctrl+C: the TUI shuts down normally, exit 0");
    check(tui.output().rfind("\033[?25h") != std::string::npos && tui.output().rfind("\033[2J") != std::string::npos,
          "and leaves the terminal with the cursor shown and the screen cleared");
    check(peer.wait_event("conv_state", [&](const std::string& l) {
        return str_field(l, "conv") == peer_conv && str_field(l, "status") == "disconnected";
    }).has_value(), "the peer sees the TUI go");
    peer.send("/quit");
    check(peer.wait_exit() == 0, "peer exits");
}

void test_no_terminal() {
    // stdin is a pipe here, not a terminal.
    int in[2];
    int err[2];
    check(::pipe2(in, O_CLOEXEC) == 0 && ::pipe2(err, O_CLOEXEC) == 0, "pipes");
    const pid_t pid = ::fork();
    if (pid == 0) {
        ::dup2(in[0], STDIN_FILENO);
        ::dup2(err[1], STDERR_FILENO);
        ::execl(NODE_BINARY, NODE_BINARY, "--listen", "127.0.0.1:0", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    ::close(in[0]);
    ::close(err[1]);
    int status = 0;
    ::waitpid(pid, &status, 0);
    char buf[512] = {};
    const ssize_t n = ::read(err[0], buf, sizeof buf - 1);
    const std::string text(buf, n > 0 ? static_cast<std::size_t>(n) : 0);
    check(WIFEXITED(status) && WEXITSTATUS(status) == 1, "without a terminal: exit 1");
    check(text.find("needs a terminal") != std::string::npos, "with a message that says why: " + text);
    ::close(in[1]);
    ::close(err[0]);
}

}  // namespace

int main() {
    std::cout << "test_tui_e2e\n";
    test_session();
    test_no_terminal();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
