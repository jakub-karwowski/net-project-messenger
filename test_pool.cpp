// test_pool.cpp - harness for TcpConnectionPool and EpollConnectionPool.
//
// Plain assertions (no framework). Every scenario runs against both pools:
//   * echo round trip through send()/receive()
//   * a peer close is reported as bytes_read == 0
//   * an idle-timeout prune is reported exactly once
//   * an LRU over-capacity prune is reported
//   * remove_connection() is NOT reported
//   * a fatal write inside send() is reported once through receive()
//   * backpressure: send() returns WouldBlock once the tx cap is exceeded
//   * close_pool() unblocks a waiting receive()

#include "TcpSocket.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "FAIL: " << what << '\n';
    }
}

std::span<const std::byte> as_bytes(const std::string& s) {
    return std::as_bytes(std::span<const char>(s.data(), s.size()));
}

// Listener on a random high port; retries on a collision with another process.
struct Endpoint {
    std::unique_ptr<TcpListener> listener;
    unsigned port{0};
};

Endpoint make_endpoint() {
    std::mt19937 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 20; ++attempt) {
        const unsigned port = 20000 + rng() % 30000;
        try {
            return {std::make_unique<TcpListener>("127.0.0.1", port, 16), port};
        } catch (...) {
            // address in use (or similar): try another port
        }
    }
    std::cout << "FAIL: no free port\n";
    std::exit(1);
}

// Connected pair: `server` is handed to the pool, `peer` is the test's end.
struct Pair {
    TcpConnection server;
    TcpConnection peer;
};

Pair make_pair(Endpoint& ep) {
    auto peer = TcpConnection::connect("127.0.0.1", ep.port);
    if (!peer) { std::cout << "FAIL: connect\n"; std::exit(1); }
    auto server = ep.listener->accept_client();
    if (!server) { std::cout << "FAIL: accept\n"; std::exit(1); }
    return {std::move(*server), std::move(*peer)};
}

// Runs receive() on its own thread and records everything it returns.
template <typename Pool>
class Driver {
public:
    struct Event { int id; std::size_t n; std::string data; };

    explicit Driver(Pool& pool) : pool_(pool), thread_([this] { run(); }) {}
    ~Driver() { stop(); }

    void stop() {
        pool_.close_pool();
        if (thread_.joinable()) { thread_.join(); }
    }

    template <typename Pred>
    bool wait_for(Pred pred, std::chrono::milliseconds timeout = 3s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (pred()) { return true; }
            std::this_thread::sleep_for(5ms);
        }
        return pred();
    }

    int closes(int id) {
        std::lock_guard lock(mutex_);
        int c = 0;
        for (const auto& e : events_) { c += (e.id == id && e.n == 0); }
        return c;
    }
    int total_closes() {
        std::lock_guard lock(mutex_);
        int c = 0;
        for (const auto& e : events_) { c += (e.n == 0); }
        return c;
    }
    std::string data(int id) {
        std::lock_guard lock(mutex_);
        std::string s;
        for (const auto& e : events_) { if (e.id == id) { s += e.data; } }
        return s;
    }
    bool ended_with_closed() const { return ended_closed_.load(); }

private:
    void run() {
        std::array<std::byte, 4096> buf{};
        for (;;) {
            auto r = pool_.receive(buf);
            if (!r) {
                ended_closed_ = (r.error() == SocketError::ConnectionClosed);
                return;
            }
            std::lock_guard lock(mutex_);
            events_.push_back({r->connection_id, r->bytes_read,
                               std::string(reinterpret_cast<const char*>(buf.data()), r->bytes_read)});
        }
    }

    Pool& pool_;
    std::mutex mutex_;
    std::vector<Event> events_;
    std::atomic<bool> ended_closed_{false};
    std::thread thread_;
};

template <typename Pool>
void test_echo(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool;
    Driver<Pool> driver(pool);
    auto pr = make_pair(ep);
    const int id = pool.add_connection(std::move(pr.server));
    check(id > 0, name + ": add_connection returns an id");

    const std::string msg = "hello pool";
    check(pr.peer.send(as_bytes(msg)).has_value(), name + ": peer send");
    check(driver.wait_for([&] { return driver.data(id).size() >= msg.size(); }),
          name + ": receive sees the bytes");
    check(driver.data(id) == msg, name + ": received payload matches");

    check(pool.send(id, as_bytes(driver.data(id))).has_value(), name + ": pool send");
    pr.peer.with_receive_timeout(2s);
    std::array<std::byte, 64> back{};
    auto n = pr.peer.receive(back);
    check(n && *n == msg.size()
              && std::string(reinterpret_cast<const char*>(back.data()), *n) == msg,
          name + ": echo round trip");
    check(pool.stats().queued_sends == 0 && pool.stats().flushes == 0,
          name + ": small send completes on the immediate write (no queueing)");
}

template <typename Pool>
void test_tx_stats(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool(16, 60s, 64u * 1024 * 1024);
    Driver<Pool> driver(pool);
    auto pr = make_pair(ep);
    const int id = pool.add_connection(std::move(pr.server));
    const std::string chunk(64 * 1024, 's');
    std::size_t sent = 0;
    while (sent < 16u * 1024 * 1024) {   // peer not reading: kernel buffers fill, the rest queues
        if (!pool.send(id, as_bytes(chunk))) { break; }
        sent += chunk.size();
    }
    check(pool.stats().queued_sends > 0, name + ": a full socket buffer counts queued sends");
    pr.peer.with_receive_timeout(3s);
    std::vector<std::byte> buf(256 * 1024);
    std::size_t got = 0;
    while (got < sent) {
        auto n = pr.peer.receive(buf);
        if (!n) { break; }
        got += *n;
    }
    check(got == sent, name + ": queued bytes all arrive");
    check(pool.stats().flushes > 0, name + ": flushes on writability counted");
}

template <typename Pool>
void test_shared_ptr_add(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool;
    Driver<Pool> driver(pool);
    auto pr = make_pair(ep);
    auto shared = std::make_shared<TcpConnection>(std::move(pr.server));
    const int id = pool.add_connection(shared);
    check(id > 0, name + ": shared_ptr add_connection returns an id");
    check(shared->options().non_blocking, name + ": shared connection set non-blocking");
    check(pool.add_connection(std::shared_ptr<TcpConnection>{}) == -1,
          name + ": null shared_ptr rejected");

    const std::string msg = "shared hello";
    check(pr.peer.send(as_bytes(msg)).has_value(), name + ": shared peer send");
    check(driver.wait_for([&] { return driver.data(id).size() >= msg.size(); }),
          name + ": shared connection receives");
    check(pool.send(id, as_bytes(msg)).has_value(), name + ": shared connection sends");
    pr.peer.with_receive_timeout(2s);
    std::array<std::byte, 64> back{};
    auto n = pr.peer.receive(back);
    check(n && *n == msg.size(), name + ": shared echo round trip");

    check(shared.use_count() == 2, name + ": pool holds one reference");
    check(pool.remove_connection(id), name + ": remove shared connection");
    check(shared.use_count() == 1, name + ": pool released its reference");
}

template <typename Pool>
void test_peer_close(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool;
    Driver<Pool> driver(pool);
    int id;
    {
        auto pr = make_pair(ep);
        id = pool.add_connection(std::move(pr.server));
        // peer goes out of scope: FIN
    }
    check(driver.wait_for([&] { return driver.closes(id) >= 1; }),
          name + ": peer close reported as 0");
    std::this_thread::sleep_for(150ms);
    check(driver.closes(id) == 1, name + ": peer close reported once");
    check(pool.send(id, as_bytes("x")).error_or(SocketError::Unknown) == SocketError::ConnectionNotFound,
          name + ": send to closed id is ConnectionNotFound");
}

template <typename Pool>
void test_idle_timeout(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool(16, 100ms);
    Driver<Pool> driver(pool);
    auto pr = make_pair(ep);
    const int id = pool.add_connection(std::move(pr.server));
    check(driver.wait_for([&] { return driver.closes(id) >= 1; }),
          name + ": idle prune reported");
    std::this_thread::sleep_for(400ms);
    check(driver.closes(id) == 1, name + ": idle prune reported exactly once");
    check(driver.total_closes() == 1, name + ": no other closes");
}

template <typename Pool>
void test_lru(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool(2, 60s);
    Driver<Pool> driver(pool);
    auto p1 = make_pair(ep);
    auto p2 = make_pair(ep);
    auto p3 = make_pair(ep);
    const int a = pool.add_connection(std::move(p1.server));
    const int b = pool.add_connection(std::move(p2.server));
    const int c = pool.add_connection(std::move(p3.server));
    check(driver.wait_for([&] { return driver.total_closes() >= 1; }),
          name + ": LRU eviction reported");
    std::this_thread::sleep_for(150ms);
    check(driver.total_closes() == 1, name + ": exactly one eviction reported");
    check(driver.closes(c) == 0, name + ": newest connection survives");
    check(driver.closes(a) + driver.closes(b) == 1, name + ": evicted one of the older ids");
}

template <typename Pool>
void test_remove_not_reported(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool;
    Driver<Pool> driver(pool);
    auto pr = make_pair(ep);
    const int id = pool.add_connection(std::move(pr.server));
    check(pool.remove_connection(id), name + ": remove_connection true");
    check(!pool.remove_connection(id), name + ": second remove is false");
    std::this_thread::sleep_for(300ms);
    check(driver.total_closes() == 0, name + ": explicit removal not reported");
}

template <typename Pool>
void test_send_failure_reported(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool;
    auto pr = make_pair(ep);
    const int id = pool.add_connection(std::move(pr.server));
    {
        TcpConnection dying = std::move(pr.peer); // closes: FIN, then RST on our writes
    }
    // Driver starts only afterwards, so the drop is certainly noticed in send()
    // (or, if the kernel already flagged EOF, by the read path once it runs).
    bool failed = false;
    for (int i = 0; i < 200 && !failed; ++i) {
        auto r = pool.send(id, as_bytes(std::string(1024, 'x')));
        if (!r && r.error() != SocketError::WouldBlock) { failed = true; }
        std::this_thread::sleep_for(5ms);
    }
    check(failed, name + ": send eventually fails on a dead peer");

    Driver<Pool> driver(pool);
    check(driver.wait_for([&] { return driver.closes(id) >= 1; }),
          name + ": drop is reported through receive()");
    std::this_thread::sleep_for(150ms);
    check(driver.closes(id) == 1, name + ": reported exactly once");
}

template <typename Pool>
void test_backpressure(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool(16, 60s, 64 * 1024);
    auto pr = make_pair(ep); // peer never reads
    const int id = pool.add_connection(std::move(pr.server));

    const std::string chunk(16 * 1024, 'b');
    bool would_block = false;
    for (int i = 0; i < 100000 && !would_block; ++i) {
        auto r = pool.send(id, as_bytes(chunk));
        if (!r) {
            would_block = (r.error() == SocketError::WouldBlock);
            if (!would_block) { break; }
        }
    }
    check(would_block, name + ": send reports WouldBlock past the tx cap");
    check(pool.send(id, as_bytes(std::string(64 * 1024 + 1, 'x'))).error_or(SocketError::Unknown)
              == SocketError::WouldBlock,
          name + ": oversized buffer rejected with WouldBlock");
}

template <typename Pool>
void test_close_unblocks(const std::string& name) {
    Endpoint ep = make_endpoint();
    Pool pool;
    Driver<Pool> driver(pool);
    std::this_thread::sleep_for(50ms);
    pool.close_pool();
    check(driver.wait_for([&] { return driver.ended_with_closed(); }, 2s),
          name + ": close_pool unblocks receive with ConnectionClosed");
    auto pr = make_pair(ep);
    check(pool.add_connection(std::move(pr.server)) == -1, name + ": add after close fails");
}

template <typename Pool>
void run_all(const std::string& name) {
    test_echo<Pool>(name);
    test_tx_stats<Pool>(name);
    test_shared_ptr_add<Pool>(name);
    test_peer_close<Pool>(name);
    test_idle_timeout<Pool>(name);
    test_lru<Pool>(name);
    test_remove_not_reported<Pool>(name);
    test_send_failure_reported<Pool>(name);
    test_backpressure<Pool>(name);
    test_close_unblocks<Pool>(name);
}

// connect() to port 0 must fail at once. Linux never refuses it: the SYN goes
// unanswered and a plain blocking connect() hangs for the SYN-retry budget (~130 s),
// which is how a dial-back to a duplex-only peer (advertised port 0) wedged the
// sender. Run on a detached thread so a regression fails the check instead of
// hanging the test binary.
void test_connect_port_zero_fails_fast() {
    auto done = std::make_shared<std::atomic<int>>(0); // 0 pending, 1 failed, 2 connected
    std::thread{[done] {
        auto connected = TcpConnection::connect("127.0.0.1", 0);
        done->store(connected ? 2 : 1);
    }}.detach();
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (done->load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }
    check(done->load() == 1, "connect to port 0 fails within 2 s (state "
                                 + std::to_string(done->load()) + ")");
}

} // namespace

int main() {
    test_connect_port_zero_fails_fast();
    run_all<TcpConnectionPool>("poll");
    run_all<EpollConnectionPool>("epoll");

    std::cout << (g_checks - g_failures) << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
