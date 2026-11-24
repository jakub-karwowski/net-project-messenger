#pragma once

// Test helper: a `node --cli` process driven through pipes, and the little
// JSON reading the tests need. NODE_BINARY must name the node executable.

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <optional>
#include <poll.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#ifndef NODE_BINARY
#error "NODE_BINARY must name the node executable"
#endif

namespace cli_process {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

// ------------------------------------------------------------------ minimal JSON reading

// The string value of "key" in a one-line JSON object (escapes \" and \\ undone;
// others left as written). Empty when absent.
std::string str_field(const std::string& line, const std::string& key) {
    const std::string tag = "\"" + key + "\":\"";
    const auto at = line.find(tag);
    if (at == std::string::npos) { return {}; }
    std::string out;
    for (std::size_t i = at + tag.size(); i < line.size(); ++i) {
        if (line[i] == '"') { return out; }
        if (line[i] == '\\' && i + 1 < line.size() && (line[i + 1] == '"' || line[i + 1] == '\\')) { ++i; }
        out += line[i];
    }
    return {};
}

std::optional<long long> num_field(const std::string& line, const std::string& key) {
    const std::string tag = "\"" + key + "\":";
    const auto at = line.find(tag);
    if (at == std::string::npos) { return std::nullopt; }
    try {
        return std::stoll(line.substr(at + tag.size()));
    } catch (...) {
        return std::nullopt;
    }
}

bool is_event(const std::string& line, const std::string& ev) {
    return str_field(line, "ev") == ev;
}

// ------------------------------------------------------------------ a node process

class Node {
public:
    explicit Node(std::vector<std::string> extra = {}) {
        int in[2];
        int out[2];
        // Close-on-exec: a node started later must not inherit this one's pipe
        // ends (holding its stdin open would hide end of input from it). dup2()
        // clears the flag on the copies the child uses as stdin and stdout.
        if (::pipe2(in, O_CLOEXEC) != 0 || ::pipe2(out, O_CLOEXEC) != 0) { std::exit(2); }
        pid_ = ::fork();
        if (pid_ == 0) {
            ::dup2(in[0], STDIN_FILENO);
            ::dup2(out[1], STDOUT_FILENO);
            ::close(in[0]); ::close(in[1]); ::close(out[0]); ::close(out[1]);
            std::vector<std::string> args{NODE_BINARY, "--cli", "--listen", "127.0.0.1:0", "--handshake-timeout", "1000"};
            args.insert(args.end(), extra.begin(), extra.end());
            std::vector<char*> argv;
            for (auto& a : args) { argv.push_back(a.data()); }
            argv.push_back(nullptr);
            ::execv(argv[0], argv.data());
            ::_exit(127);
        }
        ::close(in[0]);
        ::close(out[1]);
        in_ = in[1];
        out_ = out[0];
    }

    ~Node() {
        close_stdin();
        if (!exit_code_) {
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
        }
        if (out_ >= 0) { ::close(out_); }
    }

    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

    // Writes one command; returns its line number (its request id).
    long long send(const std::string& line) {
        const std::string bytes = line + "\n";
        std::ignore = ::write(in_, bytes.data(), bytes.size());
        return ++lines_sent_;
    }

    void close_stdin() {
        if (in_ >= 0) { ::close(in_); in_ = -1; }
    }

    void close_stdout() {
        if (out_ >= 0) { ::close(out_); out_ = -1; }
    }

    // The first line from index `from` on that satisfies `pred`; reads more
    // output as needed. Returns its index.
    std::optional<std::size_t> wait_for(const std::function<bool(const std::string&)>& pred, std::size_t from = 0,
                                        std::chrono::milliseconds limit = 5s) {
        const auto until = Clock::now() + limit;
        for (;;) {
            for (std::size_t i = from; i < lines_.size(); ++i) {
                if (pred(lines_[i])) { return i; }
            }
            from = std::max(from, lines_.size());
            if (Clock::now() >= until || !read_more(until)) { return std::nullopt; }
        }
    }

    std::optional<std::string> wait_event(const std::string& ev, const std::function<bool(const std::string&)>& pred = {},
                                          std::size_t from = 0) {
        const auto i = wait_for([&](const std::string& l) { return is_event(l, ev) && (!pred || pred(l)); }, from);
        if (!i) { return std::nullopt; }
        return lines_[*i];
    }

    std::optional<std::string> reply(long long req) {
        return wait_event("reply", [req](const std::string& l) { return num_field(l, "req") == req; });
    }

    std::size_t mark() const { return lines_.size(); }
    const std::vector<std::string>& lines() const { return lines_; }

    // Waits for the process to exit; its exit code, or nullopt if it did not.
    std::optional<int> wait_exit(std::chrono::milliseconds limit = 5s) {
        const auto until = Clock::now() + limit;
        while (!exit_code_ && Clock::now() < until) {
            int status = 0;
            if (::waitpid(pid_, &status, WNOHANG) == pid_) {
                exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
                break;
            }
            if (out_ >= 0) { read_more(Clock::now() + 10ms); } else { std::this_thread::sleep_for(5ms); }
        }
        if (exit_code_ && out_ >= 0) { while (read_more(Clock::now() + 50ms)) {} }
        return exit_code_;
    }

    void signal(int sig) { ::kill(pid_, sig); }

private:
    // Reads what is available (waiting until `until`); false at end of output or timeout.
    bool read_more(Clock::time_point until) {
        if (out_ < 0) { return false; }
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(until - Clock::now()).count();
        pollfd p{out_, POLLIN, 0};
        if (::poll(&p, 1, static_cast<int>(std::max<long long>(left, 0))) <= 0) { return false; }
        char buf[65536];
        const ssize_t n = ::read(out_, buf, sizeof buf);
        if (n <= 0) { return false; }
        partial_.append(buf, static_cast<std::size_t>(n));
        for (auto nl = partial_.find('\n'); nl != std::string::npos; nl = partial_.find('\n')) {
            lines_.push_back(partial_.substr(0, nl));
            partial_.erase(0, nl + 1);
        }
        return true;
    }

    pid_t pid_ = -1;
    int in_ = -1;
    int out_ = -1;
    std::string partial_;
    std::vector<std::string> lines_;
    long long lines_sent_ = 0;
    std::optional<int> exit_code_;
};

std::optional<int> ready_port(Node& n) {
    const auto ready = n.wait_event("ready");
    if (!ready) { return std::nullopt; }
    const std::string listen = str_field(*ready, "listen");
    const auto colon = listen.rfind(':');
    if (colon == std::string::npos) { return std::nullopt; }
    return std::stoi(listen.substr(colon + 1));
}

bool all_json_lines(const Node& n) {
    for (const auto& l : n.lines()) {
        if (l.empty() || l.front() != '{' || l.back() != '}') { return false; }
    }
    return true;
}

std::function<bool(const std::string&)> conv_is(const std::string& conv, const std::string& status) {
    return [=](const std::string& l) { return str_field(l, "conv") == conv && str_field(l, "status") == status; };
}

}  // namespace cli_process
