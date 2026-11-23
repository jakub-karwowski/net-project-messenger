// test_listener.cpp - TcpListener::stop()/accept_client()/local_port() and the
// remote address a TcpConnection records when it is created.
//
// Plain assertions, no framework. Blocking accept_client() calls are guarded by a
// watchdog that calls stop(), so a hang turns into a failed check.

#include "TcpSocket.hpp"

#include <sys/resource.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

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

// Calls listener.stop() after `limit` unless destroyed first.
class Watchdog {
    public:
        Watchdog(TcpListener& listener, std::chrono::milliseconds limit)
            : thread_([this, &listener, limit] {
                  std::unique_lock lock(mutex_);
                  if (!cv_.wait_for(lock, limit, [this] { return done_; })) {
                      fired_ = true;
                      listener.stop();
                  }
              }) {}
        ~Watchdog() {
            { std::lock_guard lock(mutex_); done_ = true; }
            cv_.notify_all();
            thread_.join();
            if (fired_) { check(false, "watchdog fired: accept_client() did not return in time"); }
        }
    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        bool done_ = false;
        bool fired_ = false;
        std::thread thread_;
};

// A loopback listener on an ephemeral port.
struct Listening {
    TcpListener listener;
    uint16_t port;
};

Listening make_listener(int backlog = 16) {
    TcpListener listener("127.0.0.1", 0, backlog);
    const auto port = listener.local_port();
    if (!port || *port == 0) {
        std::cout << "FAIL: local_port() of an ephemeral listener\n";
        std::exit(1);
    }
    return {std::move(listener), *port};
}

bool is_error(const std::expected<TcpConnection, SocketError>& r, SocketError e) {
    return !r && r.error() == e;
}

void run(const char* title, const std::function<void()>& test) {
    std::cout << title << '\n';
    test();
}

// --- stop ----------------------------------------------------------------------

void test_accept_still_works() {
    auto l = make_listener();
    auto client = TcpConnection::connect("127.0.0.1", l.port);
    check(client.has_value(), "client connects to the ephemeral port");
    Watchdog dog(l.listener, 2s);
    auto accepted = l.listener.accept_client();
    check(accepted.has_value(), "accept_client() returns the connection");
}

void test_stop_wakes_blocked_accept() {
    auto l = make_listener();
    std::thread t([&] { std::this_thread::sleep_for(50ms); l.listener.stop(); });
    Watchdog dog(l.listener, 2s);
    const auto start = Clock::now();
    const auto r = l.listener.accept_client();
    const auto elapsed = Clock::now() - start;
    t.join();
    check(is_error(r, SocketError::Stopped), "a blocked accept_client() returns Stopped");
    check(elapsed < 1s, "promptly");
}

void test_stop_is_permanent_and_idempotent() {
    auto l = make_listener();
    l.listener.stop();
    l.listener.stop();
    check(is_error(l.listener.accept_client(), SocketError::Stopped), "Stopped after stop()");
    check(is_error(l.listener.accept_client(), SocketError::Stopped), "and every time after");
}

void test_stop_beats_a_queued_client() {
    auto l = make_listener();
    auto client = TcpConnection::connect("127.0.0.1", l.port);   // completes in the backlog
    check(client.has_value(), "client queued");
    l.listener.stop();
    check(is_error(l.listener.accept_client(), SocketError::Stopped), "stop() wins over a waiting client");
}

void test_moved_listener_keeps_working() {
    auto l = make_listener();
    TcpListener moved = std::move(l.listener);
    auto client = TcpConnection::connect("127.0.0.1", l.port);
    Watchdog dog(moved, 2s);
    check(moved.accept_client().has_value(), "a moved-to listener accepts");
    moved.stop();
    check(is_error(moved.accept_client(), SocketError::Stopped), "and can be stopped");
}

// With every descriptor slot below the soft limit taken, accept() has nowhere to
// put the new socket: EMFILE. The client stays queued and is accepted once a
// descriptor is free again.
void test_too_many_open_files() {
    auto l = make_listener();
    auto client = TcpConnection::connect("127.0.0.1", l.port);
    check(client.has_value(), "client queued");

    rlimit original{};
    ::getrlimit(RLIMIT_NOFILE, &original);
    const int lowest_free = ::dup(0);   // the next descriptor accept() would get
    ::close(lowest_free);
    rlimit tight = original;
    tight.rlim_cur = static_cast<rlim_t>(lowest_free);
    check(::setrlimit(RLIMIT_NOFILE, &tight) == 0, "lower the descriptor limit");

    const auto r = l.listener.accept_client();
    ::setrlimit(RLIMIT_NOFILE, &original);
    check(is_error(r, SocketError::TooManyOpenFiles), "accept_client() reports TooManyOpenFiles");

    Watchdog dog(l.listener, 2s);
    check(l.listener.accept_client().has_value(), "the queued client is accepted once descriptors are free");
}

// --- remote_endpoint -----------------------------------------------------------

void test_remote_endpoint_both_sides() {
    auto l = make_listener();
    auto client = TcpConnection::connect("127.0.0.1", l.port);
    Watchdog dog(l.listener, 2s);
    auto server = l.listener.accept_client();
    check(client && server, "loopback pair");
    if (!client || !server) { return; }

    check(client->remote_endpoint() == TcpEndpoint{"127.0.0.1", l.port}, "client side: the listener's address");
    const TcpEndpoint& s = server->remote_endpoint();
    check(s.address == "127.0.0.1" && s.port != 0 && s.port != l.port,
          "server side: the client's address and its own ephemeral port");
}

void test_remote_endpoint_survives_peer_close_and_move() {
    auto l = make_listener();
    auto client = TcpConnection::connect("127.0.0.1", l.port);
    Watchdog dog(l.listener, 2s);
    auto server = l.listener.accept_client();
    if (!client || !server) { check(false, "loopback pair"); return; }
    const auto expected = server->remote_endpoint();

    { TcpConnection gone = std::move(*client); }   // the peer goes away
    check(server->remote_endpoint() == expected, "still known after the peer closed");

    TcpConnection moved = std::move(*server);
    check(moved.remote_endpoint() == expected, "moves with the connection");
    check(server->remote_endpoint() == TcpEndpoint{}, "a moved-from connection holds an empty endpoint");

    EpollConnectionPool pool;
    const auto id = pool.connection_add(std::move(moved));
    check(id.has_value(), "into a pool");
    if (!id) { return; }
    auto released = pool.connection_release(*id);
    check(released && released->connection.remote_endpoint() == expected, "and out of it again");
}

void test_remote_endpoint_while_dialling() {
    auto l = make_listener();
    auto dialling = TcpConnection::connect_start("127.0.0.1", l.port, {});
    check(dialling && dialling->remote_endpoint() == TcpEndpoint{"127.0.0.1", l.port},
          "connect_start() records the destination at once");
}

}  // namespace

int main() {
    run("accept still works", test_accept_still_works);
    run("stop wakes a blocked accept", test_stop_wakes_blocked_accept);
    run("stop is permanent and idempotent", test_stop_is_permanent_and_idempotent);
    run("stop beats a queued client", test_stop_beats_a_queued_client);
    run("a moved listener keeps working", test_moved_listener_keeps_working);
    run("too many open files", test_too_many_open_files);
    run("remote_endpoint: both sides", test_remote_endpoint_both_sides);
    run("remote_endpoint: survives peer close, move and pool", test_remote_endpoint_survives_peer_close_and_move);
    run("remote_endpoint: while dialling", test_remote_endpoint_while_dialling);

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
