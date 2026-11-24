// test_cli_e2e.cpp - real `node --cli` processes, driven the way an agent
// drives them: commands written to stdin, JSON events read from stdout.
//
// Covers: ready with the bound port, dial, chat both ways (with text that must
// be escaped), /open with history, /list, /close and /reconnect keeping the
// conversations, errors with their line numbers, end of input → shutdown_done
// and exit 0, /quit, SIGTERM with --stay, a closed stdout, and a start-up
// failure.
//
// Plain assertions, no framework. Every wait is bounded; a child that hangs
// is killed.

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
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

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

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

// ------------------------------------------------------------------ tests

void test_session() {
    Node a, b;
    const auto pa = ready_port(a);
    const auto pb = ready_port(b);
    check(pa && pb && *pa != 0 && *pb != 0 && a.lines().size() == 1, "ready is the first line and names the bound port");
    if (!pa || !pb) { return; }
    check(a.lines().front().find("\"pts\":1") != std::string::npos, "ready reports the IO workers");

    // Dial.
    const auto dial = a.send("/dial 127.0.0.1 " + std::to_string(*pb));
    const auto r = a.reply(dial);
    const std::string ca = r ? str_field(*r, "conv") : "";
    check(r && r->find("\"ok\":true") != std::string::npos && ca.size() == 16, "dial: reply with the new conversation");
    check(a.wait_event("conv_state", conv_is(ca, "connected")).has_value(), "A: connected");
    const auto b_up = b.wait_event("conv_state", [](const std::string& l) { return str_field(l, "status") == "connected"; });
    const std::string cb = b_up ? str_field(*b_up, "conv") : "";
    check(cb.size() == 16, "B: a new conversation, connected");

    // A → B.
    auto from = a.mark();
    const auto send = a.send("/send " + ca + " hello B");
    check(a.reply(send).has_value(), "send: reply");
    check(b.wait_event("msg", [&](const std::string& l) {
        return str_field(l, "conv") == cb && str_field(l, "body") == "hello B" && str_field(l, "from") == "peer";
    }).has_value(), "B receives the message");
    check(a.wait_event("msg_status", [&](const std::string& l) {
        return str_field(l, "conv") == ca && str_field(l, "state") == "sent";
    }, from).has_value(), "A sees it sent");

    // B opens the conversation: its history comes as a snapshot; then plain text goes there.
    const auto open = b.send("/open " + cb);
    check(b.reply(open).has_value(), "/open: reply");
    const auto snap = b.wait_event("snapshot", [&](const std::string& l) { return str_field(l, "conv") == cb; });
    check(snap && snap->find("hello B") != std::string::npos, "/open: a snapshot with the history");
    b.send("hi \"A\" \x1b[2J\\o/");
    const auto got = a.wait_event("msg", [&](const std::string& l) { return str_field(l, "from") == "peer"; });
    check(got && str_field(*got, "body") == "hi \"A\" \\u001b[2J\\o/", "A receives B's plain text (escaped)");
    check(got && got->find('\x1b') == std::string::npos, "no raw escape byte reaches A's output");

    // List.
    const auto list = a.send("/list");
    check(a.reply(list).has_value(), "/list: reply");
    const auto convs = a.wait_event("convs");
    check(convs && convs->find(ca) != std::string::npos && convs->find("\"status\":\"connected\"") != std::string::npos,
          "/list: the conversation, connected");

    // Close and reconnect keep the conversations.
    from = a.mark();
    auto b_from = b.mark();
    a.send("/close " + ca);
    check(a.wait_event("conv_state", conv_is(ca, "disconnected"), from).has_value(), "A: disconnected after /close");
    check(b.wait_event("conv_state", conv_is(cb, "disconnected"), b_from).has_value(), "B: disconnected");
    from = a.mark();
    b_from = b.mark();
    a.send("/reconnect " + ca);
    check(a.wait_event("conv_state", conv_is(ca, "connected"), from).has_value(), "A: connected again");
    check(b.wait_event("conv_state", conv_is(cb, "connected"), b_from).has_value(), "B: connected again, same conversation");

    // Errors carry their line number; blank lines count.
    const auto bad = a.send("/frobnicate");
    const auto bad_reply = a.reply(bad);
    check(bad_reply && bad_reply->find("\"ok\":false") != std::string::npos
              && str_field(*bad_reply, "error").starts_with("unknown command"),
          "an unknown command: reply ok=false with the error");
    a.send("");
    const auto help = a.send("/help");
    check(a.reply(help).has_value() && a.wait_event("notice").has_value(), "/help after a blank line: its line number");

    // End of input: A shuts down cleanly; B sees it go.
    b_from = b.mark();
    a.close_stdin();
    const auto code = a.wait_exit();
    check(code == 0, "end of input: exit 0 (got " + (code ? std::to_string(*code) : std::string("no exit")) + ", last line: "
                         + (a.lines().empty() ? std::string() : a.lines().back()) + ")");
    check(!a.lines().empty() && is_event(a.lines().back(), "shutdown_done"), "shutdown_done is A's last line");
    check(b.wait_event("conv_state", conv_is(cb, "disconnected"), b_from).has_value(), "B: disconnected");

    const auto quit = b.send("/quit");
    check(b.reply(quit).has_value(), "/quit: reply");
    check(b.wait_exit() == 0 && is_event(b.lines().back(), "shutdown_done"), "/quit: shutdown_done, exit 0");
    check(all_json_lines(a) && all_json_lines(b), "every output line is one JSON object");
}

void test_sigterm_with_stay() {
    Node c({"--stay"});
    check(ready_port(c).has_value(), "ready");
    c.close_stdin();
    std::this_thread::sleep_for(200ms);
    check(!c.wait_exit(100ms).has_value(), "--stay: still running after end of input");
    c.signal(SIGTERM);
    check(c.wait_exit() == 0 && is_event(c.lines().back(), "shutdown_done"), "SIGTERM: normal shutdown, exit 0");
}

void test_stdout_closed() {
    Node d;
    check(ready_port(d).has_value(), "ready");
    d.close_stdout();
    d.send("/list");   // its output hits a closed pipe
    check(d.wait_exit() == 0, "stdout closed: the node quits normally");
}

void test_listen_failure() {
    Node first;
    const auto port = ready_port(first);
    Node second({"--listen", "127.0.0.1:" + std::to_string(port.value_or(1))});
    const auto fatal = second.wait_event("fatal");
    check(fatal && str_field(*fatal, "error").starts_with("cannot listen"), "a port in use: fatal event");
    check(second.wait_exit() == 1, "and exit code 1");
}

}  // namespace

int main() {
    std::cout << "test_cli_e2e\n";
    test_session();
    test_sigterm_with_stay();
    test_stdout_closed();
    test_listen_failure();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
