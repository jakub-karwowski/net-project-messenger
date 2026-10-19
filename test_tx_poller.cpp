// test_tx_poller.cpp - harness for TxPoller (poll and epoll backends).
//
// Plain assertions (no framework). Every scenario runs against both backends:
//   * immediate send (fast path)
//   * queued write flushed once the peer drains
//   * WouldBlock at the tx cap (nothing queued)
//   * fatal write (peer RST) shuts the socket down and drops the entry
//   * remove() leaves no dangling reference
//   * an EpollConnectionPool (rx) and a TxPoller (tx) share one connection

#include "TcpSocket.hpp"
#include "TxPoller.hpp"

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <random>
#include <span>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
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
        } catch (...) {}
    }
    std::cout << "FAIL: no free port\n";
    std::exit(1);
}

// Connected pair: `server` is shared with the poller, `peer` is the test's end.
struct Pair {
    std::shared_ptr<TcpConnection> server;
    TcpConnection peer;
};

Pair make_pair(Endpoint& ep) {
    auto peer = TcpConnection::connect("127.0.0.1", ep.port);
    if (!peer) { std::cout << "FAIL: connect\n"; std::exit(1); }
    auto server = ep.listener->accept_client();
    if (!server) { std::cout << "FAIL: accept\n"; std::exit(1); }
    return {std::make_shared<TcpConnection>(std::move(*server)), std::move(*peer)};
}

// Raw client socket so the test can force an RST (SO_LINGER 0) on close.
struct RawPair {
    std::shared_ptr<TcpConnection> server;
    int peer_fd;
};

RawPair make_raw_pair(Endpoint& ep) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(ep.port));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (fd < 0 || ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cout << "FAIL: raw connect\n";
        std::exit(1);
    }
    auto server = ep.listener->accept_client();
    if (!server) { std::cout << "FAIL: accept\n"; std::exit(1); }
    return {std::make_shared<TcpConnection>(std::move(*server)), fd};
}

void reset_close(int fd) {
    linger lg{1, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    ::close(fd);
}

// Runs TxPoller::run() on a thread; stops and joins on destruction.
template <typename Poller>
struct RunGuard {
    explicit RunGuard(Poller& p) : poller(p), thread([&p] { p.run(); }) {}
    ~RunGuard() { poller.close(); if (thread.joinable()) { thread.join(); } }
    Poller& poller;
    std::thread thread;
};

template <typename Poller>
void test_immediate(const std::string& name) {
    Endpoint ep = make_endpoint();
    Poller poller;
    RunGuard<Poller> guard(poller);
    auto pr = make_pair(ep);
    check(poller.add(7, pr.server), name + ": add");
    check(!poller.add(7, pr.server), name + ": duplicate id rejected");
    check(poller.send(99, as_bytes("x")).error_or(SocketError::Unknown) == SocketError::ConnectionNotFound,
          name + ": unknown id is ConnectionNotFound");

    const std::string msg = "hello tx";
    check(poller.send(7, as_bytes(msg)).has_value(), name + ": send");
    pr.peer.with_receive_timeout(2s);
    std::array<std::byte, 64> buf{};
    auto n = pr.peer.receive(buf);
    check(n && *n == msg.size()
              && std::string(reinterpret_cast<const char*>(buf.data()), *n) == msg,
          name + ": peer receives the bytes");
}

template <typename Poller>
void test_queued(const std::string& name) {
    Endpoint ep = make_endpoint();
    constexpr std::size_t kTotal = 24u * 1024 * 1024;
    Poller poller(64u * 1024 * 1024);
    RunGuard<Poller> guard(poller);
    auto pr = make_pair(ep);
    check(poller.add(1, pr.server), name + ": add");

    // Far more than the kernel buffers hold while the peer is not reading.
    std::string chunk(64 * 1024, 'q');
    std::size_t sent = 0;
    bool ok = true;
    while (sent < kTotal && ok) {
        for (std::size_t i = 0; i < chunk.size(); ++i) {
            chunk[i] = static_cast<char>('a' + ((sent + i) % 26));
        }
        ok = poller.send(1, as_bytes(chunk)).has_value();
        sent += chunk.size();
    }
    check(ok, name + ": all sends accepted (queued)");

    pr.peer.with_receive_timeout(3s);
    std::vector<std::byte> buf(256 * 1024);
    std::size_t got = 0;
    bool intact = true;
    while (got < sent) {
        auto n = pr.peer.receive(buf);
        if (!n) { break; }
        for (std::size_t i = 0; i < *n; ++i) {
            if (static_cast<char>(buf[i]) != static_cast<char>('a' + ((got + i) % 26))) { intact = false; }
        }
        got += *n;
    }
    check(got == sent, name + ": every queued byte flushed once the peer drains");
    check(intact, name + ": bytes arrive in order");
}

template <typename Poller>
void test_would_block(const std::string& name) {
    Endpoint ep = make_endpoint();
    Poller poller(64 * 1024);
    RunGuard<Poller> guard(poller);
    auto pr = make_pair(ep); // peer never reads
    poller.add(1, pr.server);

    const std::string chunk(16 * 1024, 'b');
    bool would_block = false;
    for (int i = 0; i < 100000 && !would_block; ++i) {
        auto r = poller.send(1, as_bytes(chunk));
        if (!r) {
            would_block = (r.error() == SocketError::WouldBlock);
            if (!would_block) { break; }
        }
    }
    check(would_block, name + ": WouldBlock past the tx cap");
    check(poller.send(1, as_bytes(std::string(64 * 1024 + 1, 'x'))).error_or(SocketError::Unknown)
              == SocketError::WouldBlock,
          name + ": oversized buffer rejected with WouldBlock");
}

template <typename Poller>
void test_fatal_fast_path(const std::string& name) {
    Endpoint ep = make_endpoint();
    Poller poller;
    auto rp = make_raw_pair(ep);
    poller.add(1, rp.server);
    reset_close(rp.peer_fd);

    bool failed = false;
    for (int i = 0; i < 200 && !failed; ++i) {
        auto r = poller.send(1, as_bytes(std::string(1024, 'x')));
        if (!r && r.error() != SocketError::WouldBlock) { failed = true; }
        std::this_thread::sleep_for(5ms);
    }
    check(failed, name + ": fast-path send fails on a reset peer");
    check(poller.send(1, as_bytes("x")).error_or(SocketError::Unknown) == SocketError::ConnectionNotFound,
          name + ": failed entry removed");
    std::array<std::byte, 16> b{};
    auto r = rp.server->receive(b);
    check(!r && r.error() != SocketError::WouldBlock,
          name + ": reader on the shared connection sees EOF/error, not WouldBlock");
}

template <typename Poller>
void test_fatal_in_run(const std::string& name) {
    Endpoint ep = make_endpoint();
    Poller poller(64u * 1024 * 1024);
    RunGuard<Poller> guard(poller);
    auto rp = make_raw_pair(ep);
    poller.add(1, rp.server);

    // Fill the pipe so bytes are queued and POLLOUT armed, then reset the peer.
    const std::string chunk(64 * 1024, 'z');
    for (int i = 0; i < 200; ++i) { (void)poller.send(1, as_bytes(chunk)); }
    reset_close(rp.peer_fd);

    bool gone = false;
    for (int i = 0; i < 400 && !gone; ++i) {
        gone = poller.send(1, as_bytes("x")).error_or(SocketError::Unknown)
               == SocketError::ConnectionNotFound;
        std::this_thread::sleep_for(5ms);
    }
    check(gone, name + ": run() drops the entry on a fatal write");
    std::array<std::byte, 16> b{};
    auto r = rp.server->receive(b);
    check(!r && r.error() != SocketError::WouldBlock,
          name + ": socket shut down, reader sees EOF/error");
}

template <typename Poller>
void test_remove(const std::string& name) {
    Endpoint ep = make_endpoint();
    Poller poller(64 * 1024 * 1024);
    RunGuard<Poller> guard(poller);
    auto pr = make_pair(ep); // peer never reads, so bytes stay queued/armed
    std::weak_ptr<TcpConnection> weak = pr.server;
    poller.add(1, pr.server);
    for (int i = 0; i < 100; ++i) { (void)poller.send(1, as_bytes(std::string(64 * 1024, 'r'))); }
    check(pr.server.use_count() == 2, name + ": poller holds one reference");

    check(poller.remove(1), name + ": remove true");
    check(!poller.remove(1), name + ": second remove false");
    check(pr.server.use_count() == 1, name + ": poller released its reference");
    std::this_thread::sleep_for(100ms); // run() must keep going without the fd

    // remove() does not shut the socket down: the peer sees no EOF yet.
    pr.peer.with_receive_timeout(200ms);
    std::vector<std::byte> buf(1 << 20);
    for (;;) {
        auto n = pr.peer.receive(buf);
        if (!n) {
            check(n.error() != SocketError::ConnectionClosed, name + ": remove() did not shut the socket down");
            break;
        }
    }

    pr.server.reset(); // the "rx side" drops its ref: last owner, fd closes
    check(weak.expired(), name + ": no dangling connection after the last ref is dropped");
    pr.peer.with_receive_timeout(2s);
    std::array<std::byte, 16> b{};
    auto n = pr.peer.receive(b);
    check(!n, name + ": peer sees the close once the fd is gone");
}

// Echo peer: reads everything it receives and writes it straight back.
class EchoPeer {
public:
    explicit EchoPeer(TcpConnection conn) : conn_(std::move(conn)), thread_([this] { run(); }) {}
    ~EchoPeer() { stop_ = true; conn_.shutdown(); if (thread_.joinable()) { thread_.join(); } }
private:
    void run() {
        conn_.with_receive_timeout(100ms);
        std::array<std::byte, 8192> buf{};
        while (!stop_) {
            auto n = conn_.receive(buf);
            if (!n) {
                if (n.error() == SocketError::ConnectionClosed) { return; }
                continue;
            }
            std::size_t off = 0;
            while (off < *n) {
                auto s = conn_.send(std::span<const std::byte>(buf.data() + off, *n - off));
                if (!s) { return; }
                off += *s;
            }
        }
    }
    TcpConnection conn_;
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

template <typename Poller>
void test_duplex_shared(const std::string& name) {
    Endpoint ep = make_endpoint();
    EpollConnectionPool rx;
    Poller tx(256 * 1024);

    auto peer = TcpConnection::connect("127.0.0.1", ep.port);
    if (!peer) { std::cout << "FAIL: connect\n"; std::exit(1); }
    auto server = ep.listener->accept_client();
    if (!server) { std::cout << "FAIL: accept\n"; std::exit(1); }
    EchoPeer echo(std::move(*peer));

    auto shared = std::make_shared<TcpConnection>(std::move(*server));
    const int id = rx.add_connection(shared);
    check(id > 0, name + ": rx add");
    check(tx.add(id, shared), name + ": tx add with the rx id");

    std::string received;
    std::mutex m;
    std::thread rx_thread([&] {
        std::array<std::byte, 4096> buf{};
        for (;;) {
            auto r = rx.receive(buf);
            if (!r) { return; }
            if (r->bytes_read == 0) { return; }
            std::lock_guard lock(m);
            received.append(reinterpret_cast<const char*>(buf.data()), r->bytes_read);
        }
    });
    std::thread tx_thread([&] { tx.run(); });

    constexpr std::size_t kTotal = 4u * 1024 * 1024;
    std::string expected;
    expected.reserve(kTotal);
    std::string chunk(8192, ' ');
    while (expected.size() < kTotal) {
        for (std::size_t i = 0; i < chunk.size(); ++i) {
            chunk[i] = static_cast<char>('A' + ((expected.size() + i) % 26));
        }
        auto r = tx.send(id, as_bytes(chunk));
        if (!r) {
            if (r.error() == SocketError::WouldBlock) { std::this_thread::sleep_for(1ms); continue; }
            break;
        }
        expected += chunk;
    }

    bool complete = false;
    for (int i = 0; i < 1000 && !complete; ++i) {
        { std::lock_guard lock(m); complete = received.size() >= expected.size(); }
        if (!complete) { std::this_thread::sleep_for(10ms); }
    }
    {
        std::lock_guard lock(m);
        check(expected.size() >= kTotal, name + ": duplex all chunks accepted");
        check(received == expected, name + ": echo returns exactly what was sent");
    }

    tx.close();
    rx.close_pool();
    tx_thread.join();
    rx_thread.join();
}

template <typename Poller>
void run_all(const std::string& name) {
    test_immediate<Poller>(name);
    test_queued<Poller>(name);
    test_would_block<Poller>(name);
    test_fatal_fast_path<Poller>(name);
    test_fatal_in_run<Poller>(name);
    test_remove<Poller>(name);
    test_duplex_shared<Poller>(name);
}

} // namespace

int main() {
    run_all<PollTxPoller>("poll-tx");
    run_all<EpollTxPoller>("epoll-tx");

    std::cout << (g_checks - g_failures) << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
