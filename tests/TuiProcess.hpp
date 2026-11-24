#pragma once

// Test helper: a node running the terminal UI on a pseudo-terminal, typed into
// and read from. NODE_BINARY must name the node executable.

#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <optional>
#include <poll.h>
#include <pty.h>
#include <regex>
#include <string>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#ifndef NODE_BINARY
#error "NODE_BINARY must name the node executable"
#endif

namespace tui_process {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

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
    void signal(int sig) { ::kill(pid_, sig); }

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

}  // namespace tui_process
