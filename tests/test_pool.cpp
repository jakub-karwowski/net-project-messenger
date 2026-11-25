// test_pool.cpp - EpollConnectionPool: interrupt, non-blocking dial, connection
// hand-over, timeouts, stats, receive with a deadline and connection_count,
// plus a few regressions of the existing behaviour.
//
// Plain assertions, no framework. Every receive() is guarded by a watchdog that
// stops the pool, so a hang turns into a failed check instead of a stuck test.

#include "TcpSocket.hpp"

#include <array>
#include <atomic>
#include <ctime>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Pool = EpollConnectionPool;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

std::span<const std::byte> as_bytes(const std::string& s) {
    return std::as_bytes(std::span<const char>(s.data(), s.size()));
}

std::string as_string(std::span<const std::byte> bytes) {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

const char* name(SocketError e) {
    switch (e) {
        case SocketError::ConnectionRefused: return "ConnectionRefused";
        case SocketError::ConnectionClosed:  return "ConnectionClosed";
        case SocketError::ConnectionNotFound:return "ConnectionNotFound";
        case SocketError::ReadTimedOut:      return "ReadTimedOut";
        case SocketError::WriteTimedOut:     return "WriteTimedOut";
        case SocketError::ConnectTimedOut:   return "ConnectTimedOut";
        case SocketError::HostUnreachable:   return "HostUnreachable";
        case SocketError::NotConnected:      return "NotConnected";
        case SocketError::Interrupted:       return "Interrupted";
        case SocketError::Stopped:           return "Stopped";
        default:                             return "other";
    }
}

bool is_error(const Pool::ReceiveOutcome& r, uint64_t id, SocketError e) {
    return !r && r.error().connection_id == id && r.error().error == e;
}

std::string describe(const Pool::ReceiveOutcome& r) {
    if (r) {
        return std::string(r->kind == Pool::ReceiveResult::Kind::Connected ? "Connected" : "Data")
             + " id=" + std::to_string(r->connection_id) + " bytes=" + std::to_string(r->bytes_read);
    }
    return std::string("error ") + name(r.error().error) + " id=" + std::to_string(r.error().connection_id);
}

// Stops `pool` after `limit` unless destroyed first, so receive() cannot hang.
class Watchdog {
    public:
        Watchdog(Pool& pool, std::chrono::milliseconds limit)
            : thread_([this, &pool, limit] {
                  std::unique_lock lock(mutex_);
                  if (!cv_.wait_for(lock, limit, [this] { return done_; })) {
                      fired_ = true;
                      pool.stop();
                  }
              }) {}
        ~Watchdog() {
            { std::lock_guard lock(mutex_); done_ = true; }
            cv_.notify_all();
            thread_.join();
            if (fired_) { check(false, "watchdog fired: receive() did not return in time"); }
        }
    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        bool done_ = false;
        bool fired_ = false;
        std::thread thread_;
};

// Listener on a random high port; retries on a collision.
struct Listening {
    std::unique_ptr<TcpListener> listener;
    unsigned port{0};
};

Listening make_listener(int backlog = 16) {
    std::mt19937 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 50; ++attempt) {
        const unsigned port = 20000 + rng() % 30000;
        try {
            return {std::make_unique<TcpListener>("127.0.0.1", port, backlog), port};
        } catch (...) {}
    }
    std::cout << "FAIL: no free port\n";
    std::exit(1);
}

// Connected pair over loopback: `pooled` goes into a pool, `peer` is the test's end.
struct Pair {
    TcpConnection pooled;
    TcpConnection peer;
};

Pair make_pair() {
    auto l = make_listener();
    auto peer = TcpConnection::connect("127.0.0.1", l.port);
    auto pooled = l.listener->accept_client();
    if (!peer || !pooled) {
        std::cout << "FAIL: could not build a loopback pair\n";
        std::exit(1);
    }
    return {std::move(*pooled), std::move(*peer)};
}

void run(const char* title, const std::function<void()>& test) {
    std::cout << title << '\n';
    test();
}

// ---------------------------------------------------------------------------

void test_interrupt_wakes_blocked_receive() {
    Pool pool;
    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 2s);
    std::thread t([&] { std::this_thread::sleep_for(50ms); pool.interrupt(); });
    const auto r = pool.receive(buf);
    t.join();
    check(is_error(r, 0, SocketError::Interrupted), "blocked receive() returns Interrupted, got " + describe(r));
}

void test_interrupts_coalesce() {
    Pool pool;
    auto pair = make_pair();
    const auto id = pool.connection_add(std::move(pair.pooled));
    check(id.has_value(), "connection_add");

    pool.interrupt();
    pool.interrupt();
    pool.interrupt();
    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 2s);
    const auto first = pool.receive(buf);
    check(is_error(first, 0, SocketError::Interrupted), "first receive() is Interrupted, got " + describe(first));

    check(pair.peer.send(as_bytes("x")).has_value(), "peer send");
    const auto second = pool.receive(buf);
    check(second && second->connection_id == *id && second->bytes_read == 1,
          "three interrupts are reported once; next is the data, got " + describe(second));
}

void test_dial_success() {
    auto l = make_listener();
    Pool pool;
    const auto id = pool.connection_dial("127.0.0.1", l.port, {}, {}, 2s);
    check(id.has_value(), "connection_dial returns an id");
    if (!id) { return; }

    // Not reported yet: still Connecting from the pool's point of view.
    const auto early = pool.send(*id, as_bytes("too early"));
    check(!early && early.error() == SocketError::NotConnected, "send() before Connected fails with NotConnected");

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    const auto r = pool.receive(buf);
    check(r && r->connection_id == *id && r->kind == Pool::ReceiveResult::Kind::Connected && r->bytes_read == 0,
          "receive() reports Connected, got " + describe(r));

    check(pool.send(*id, as_bytes("ping")).has_value(), "send() after Connected");
    auto accepted = l.listener->accept_client();
    check(accepted.has_value(), "listener accepts the dialled connection");
    if (!accepted) { return; }
    std::array<std::byte, 16> pbuf{};
    const auto got = accepted->receive(pbuf);
    check(got && as_string(std::span(pbuf).first(*got)) == "ping", "peer receives what the pool sent");

    check(accepted->send(as_bytes("pong")).has_value(), "peer send");
    const auto data = pool.receive(buf);
    check(data && data->kind == Pool::ReceiveResult::Kind::Data
              && as_string(std::span(buf).first(data->bytes_read)) == "pong",
          "pool receives data on the dialled connection, got " + describe(data));
}

void test_dial_refused() {
    unsigned port = 0;
    { auto l = make_listener(); port = l.port; }   // closed again: nobody listens there now

    Pool pool;
    const auto id = pool.connection_dial("127.0.0.1", port, {}, {}, 2s);
    check(id.has_value(), "connection_dial returns an id even when the dial will fail");
    if (!id) { return; }
    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    const auto r = pool.receive(buf);
    check(is_error(r, *id, SocketError::ConnectionRefused), "refused dial is reported, got " + describe(r));
    const auto after = pool.send(*id, as_bytes("x"));
    check(!after && after.error() == SocketError::ConnectionNotFound, "a failed dial is gone from the pool");
}

void test_dial_invalid_address() {
    Pool pool;
    const auto bad_ip = pool.connection_dial("not-an-ip", 80, {}, {}, 1s);
    check(!bad_ip && bad_ip.error() == EpollError::InvalidAddress, "a non-IPv4 address is rejected at once");
    const auto port0 = pool.connection_dial("127.0.0.1", 0, {}, {}, 1s);
    check(!port0 && port0.error() == EpollError::InvalidAddress, "port 0 is rejected at once");
    const auto too_long = pool.connection_dial("127.000.000.000.001", 80, {}, {}, 1s);
    check(!too_long && too_long.error() == EpollError::InvalidAddress, "an over-long address is rejected at once");
}

// A listener whose accept queue is full drops new SYNs, so further dials hang
// until the pool's connect timer gives up on them. The queue only counts as full
// once a handshake has completed: dials fired back to back all get in, so the
// first dial fills it and the rest go in one by one afterwards.
void test_dial_timeout() {
    auto l = make_listener(/*backlog=*/0);
    Pool pool;
    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 5s);

    const auto filler = pool.connection_dial("127.0.0.1", l.port, {}, {}, 2s);
    const auto filled = pool.receive(buf);
    check(filler && filled && filled->kind == Pool::ReceiveResult::Kind::Connected,
          "the first dial fills the accept queue, got " + describe(filled));

    constexpr int kDials = 4;
    for (int i = 0; i < kDials; ++i) {
        check(pool.connection_dial("127.0.0.1", l.port, {}, {}, 300ms).has_value(), "dial into a full backlog");
        std::this_thread::sleep_for(20ms);
    }

    int connected = 0, timed_out = 0, other = 0;
    const auto start = Clock::now();
    for (int i = 0; i < kDials; ++i) {
        const auto r = pool.receive(buf);
        if (r && r->kind == Pool::ReceiveResult::Kind::Connected) { ++connected; }
        else if (!r && r.error().error == SocketError::ConnectTimedOut) { ++timed_out; }
        else { ++other; std::cout << "  unexpected: " << describe(r) << '\n'; }
    }
    const auto elapsed = Clock::now() - start;
    std::cout << "  " << connected << " connected, " << timed_out << " timed out\n";
    check(other == 0, "every dial ends Connected or ConnectTimedOut");
    check(timed_out >= 1, "dials past the backlog time out");
    check(elapsed >= 250ms && elapsed < 3s, "the connect timeout, not the kernel's SYN retries, ends them");
}

// A Connected notice still queued when the caller drops the connection must not
// be reported afterwards.
void test_drop_forgets_queued_notice() {
    auto l = make_listener();
    Pool pool;
    const auto a = pool.connection_dial("127.0.0.1", l.port, {}, {}, 2s);
    const auto b = pool.connection_dial("127.0.0.1", l.port, {}, {}, 2s);
    check(a && b, "two dials");
    if (!a || !b) { return; }
    std::this_thread::sleep_for(100ms);   // both complete in the kernel: one epoll batch

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    const auto first = pool.receive(buf);
    check(first && first->kind == Pool::ReceiveResult::Kind::Connected, "first Connected, got " + describe(first));
    if (!first) { return; }
    const uint64_t other = (first->connection_id == *a) ? *b : *a;
    pool.connection_drop(other);

    // Something else to report: if the other dial's notice survived, it would come first.
    auto pair = make_pair();
    const auto c = pool.connection_add(std::move(pair.pooled));
    check(pair.peer.send(as_bytes("z")).has_value(), "peer send");
    const auto next = pool.receive(buf);
    check(next && c && next->connection_id == *c && next->kind == Pool::ReceiveResult::Kind::Data,
          "the dropped dial's Connected is not reported, got " + describe(next));
}

// The rebalance path: a connection with unsent bytes leaves pool A and joins
// pool B. Bytes go out in order, and bytes the peer sent before the move are
// read by pool B.
void test_release_and_adopt_keep_the_stream() {
    auto pair = make_pair();
    Pool a(Pool::Timeouts{.read = 0ms, .write = 5s}, /*max_tx_buffer=*/64u << 20);
    const auto id_a = a.connection_add(std::move(pair.pooled));
    check(id_a.has_value(), "add to pool A");
    if (!id_a) { return; }

    check(pair.peer.send(as_bytes("pre")).has_value(), "peer sends before the move");

    // The peer does not read yet, so the kernel buffers fill and the rest queues in A.
    constexpr size_t kChunk = 1u << 20, kChunks = 16, kTotal = kChunk * kChunks;
    std::vector<std::byte> chunk(kChunk);
    size_t offset = 0;
    for (size_t c = 0; c < kChunks; ++c) {
        for (size_t i = 0; i < kChunk; ++i) { chunk[i] = static_cast<std::byte>((offset + i) % 251); }
        check(a.send(*id_a, chunk).has_value(), "send chunk " + std::to_string(c));
        offset += kChunk;
    }

    auto released = a.connection_release(*id_a);
    check(released.has_value(), "connection_release");
    if (!released) { return; }
    std::cout << "  unsent at release: " << released->unsent_tx.size() << " bytes, sent: "
              << released->stats.bytes_sent << '\n';
    check(!released->unsent_tx.empty(), "some bytes were still queued (the test exercises the carry-over)");
    check(released->stats.bytes_sent + released->unsent_tx.size() == kTotal, "sent + unsent == everything queued");
    check(released->stats.bytes_received == 0, "pool A read nothing");

    const auto gone = a.send(*id_a, as_bytes("x"));
    check(!gone && gone.error() == SocketError::ConnectionNotFound, "pool A no longer knows the connection");

    Pool b;
    const auto id_b = b.connection_add(std::move(released->connection), Pool::Timeouts{.read = 0ms, .write = 5s},
                                       released->unsent_tx);
    check(id_b.has_value(), "add to pool B with the unsent bytes");
    if (!id_b) { return; }

    std::atomic<bool> stream_ok{true};
    std::thread reader([&] {
        std::vector<std::byte> rbuf(64 * 1024);
        size_t seen = 0;
        while (seen < kTotal) {
            const auto n = pair.peer.receive(rbuf);
            if (!n) { stream_ok = false; return; }
            for (size_t i = 0; i < *n; ++i) {
                if (rbuf[i] != static_cast<std::byte>((seen + i) % 251)) { stream_ok = false; return; }
            }
            seen += *n;
        }
        std::ignore = pair.peer.send(as_bytes("hello"));
    });

    std::string collected;
    std::array<std::byte, 64> buf{};
    {
        Watchdog dog(b, 10s);
        while (collected.size() < 8) {
            const auto r = b.receive(buf);
            if (!r) { check(false, "pool B: " + describe(r)); break; }
            collected += as_string(std::span(buf).first(r->bytes_read));
        }
    }
    reader.join();
    check(stream_ok, "the peer received every byte in order across the move");
    check(collected == "prehello", "pool B read the pre-move bytes, then the reply; got \"" + collected + "\"");
}

void test_release_refusals() {
    auto l = make_listener();
    Pool pool;
    const auto missing = pool.connection_release(42);
    check(!missing && missing.error() == SocketError::ConnectionNotFound, "release of an unknown id");
    const auto dialling = pool.connection_dial("127.0.0.1", l.port, {}, {}, 2s);
    check(dialling.has_value(), "dial");
    if (!dialling) { return; }
    const auto r = pool.connection_release(*dialling);
    check(!r && r.error() == SocketError::NotConnected, "release while still dialling");
}

void test_set_timeouts_enables_read_timeout() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled), Pool::Timeouts{.read = 0ms, .write = 5s});
    check(id.has_value(), "add without a read timeout");
    if (!id) { return; }
    check(pool.set_timeouts(*id, Pool::Timeouts{.read = 150ms, .write = 5s}).has_value(), "set_timeouts");

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    const auto start = Clock::now();
    const auto r = pool.receive(buf);
    const auto elapsed = Clock::now() - start;
    check(is_error(r, *id, SocketError::ReadTimedOut), "the new read timeout fires, got " + describe(r));
    check(elapsed >= 140ms && elapsed < 2s, "after about the new timeout");
}

void test_set_timeouts_disables_read_timeout() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled), Pool::Timeouts{.read = 150ms, .write = 5s});
    check(id.has_value(), "add with a read timeout");
    if (!id) { return; }
    check(pool.set_timeouts(*id, Pool::Timeouts{.read = 0ms, .write = 5s}).has_value(), "set_timeouts");

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    std::thread t([&] { std::this_thread::sleep_for(400ms); pool.interrupt(); });
    const auto r = pool.receive(buf);
    t.join();
    check(is_error(r, 0, SocketError::Interrupted), "the retired read timer does not fire, got " + describe(r));
    check(pool.send(*id, as_bytes("x")).has_value(), "the connection is still there");
}

void test_take_stats() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled));
    check(id.has_value(), "add");
    if (!id) { return; }
    check(pool.send(*id, as_bytes("hello")).has_value(), "pool send");
    check(pair.peer.send(as_bytes("abc")).has_value(), "peer send");

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 2s);
    const auto r = pool.receive(buf);
    check(r && r->bytes_read == 3, "receive 3 bytes, got " + describe(r));

    const auto s = pool.take_stats(*id);
    check(s && s->bytes_received == 3 && s->bytes_sent == 5, "stats count 3 in / 5 out");
    const auto again = pool.take_stats(*id);
    check(again && again->bytes_received == 0 && again->bytes_sent == 0, "take_stats resets the counters");
    const auto missing = pool.take_stats(999);
    check(!missing && missing.error() == SocketError::ConnectionNotFound, "take_stats of an unknown id");
}

// The two-step connect on its own, without the pool: connect_status() says
// "still in flight" (false) until the handshake ends, then the outcome.
void test_connect_start_and_status() {
    const auto settle = [](const TcpConnection& c) {
        const auto deadline = Clock::now() + 2s;
        auto status = c.connect_status();
        while (status && !*status && Clock::now() < deadline) {
            std::this_thread::sleep_for(5ms);
            status = c.connect_status();
        }
        return status;
    };

    auto l = make_listener();
    auto ok = TcpConnection::connect_start("127.0.0.1", l.port, {});
    check(ok.has_value(), "connect_start to a listener");
    if (ok) {
        const auto s = settle(*ok);
        check(s && *s, "connect_status() becomes true");
    }

    unsigned closed_port = 0;
    { auto gone = make_listener(); closed_port = gone.port; }
    auto refused = TcpConnection::connect_start("127.0.0.1", closed_port, {});
    check(refused.has_value(), "connect_start does not wait for the outcome");
    if (refused) {
        const auto s = settle(*refused);
        check(!s && s.error() == SocketError::ConnectionRefused, "connect_status() reports the refusal");
    }

    const auto bad = TcpConnection::connect_start("300.1.1.1", 80, {});
    check(!bad && bad.error() == SocketError::InvalidAddress, "connect_start rejects a malformed address");
    const auto blocking_bad = TcpConnection::connect("nope", 80);
    check(!blocking_bad && blocking_bad.error() == SocketError::InvalidAddress,
          "blocking connect() now reports InvalidAddress too");
}

void test_peer_close_still_reported() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled));
    check(id.has_value(), "add");
    if (!id) { return; }
    { TcpConnection closing = std::move(pair.peer); }

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 2s);
    const auto r = pool.receive(buf);
    check(is_error(r, *id, SocketError::ConnectionClosed), "peer close is reported, got " + describe(r));
}

void test_stop_still_wins() {
    Pool pool;
    pool.interrupt();
    pool.stop();
    std::array<std::byte, 64> buf{};
    const auto r = pool.receive(buf);
    check(is_error(r, 0, SocketError::Stopped), "stop() takes precedence over a pending interrupt, got " + describe(r));
}

}  // namespace

void test_receive_deadline() {
    Pool pool;
    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);

    auto start = Clock::now();
    auto r = pool.receive(buf, start + 60ms);
    auto waited = Clock::now() - start;
    check(is_error(r, 0, SocketError::WouldBlock), "nothing to report: WouldBlock at the deadline, got " + describe(r));
    check(waited >= 55ms && waited < 1s, "receive(deadline) waits until the deadline");

    start = Clock::now();
    r = pool.receive(buf, start - 1ms);
    check(is_error(r, 0, SocketError::WouldBlock) && Clock::now() - start < 50ms, "a past deadline returns at once");

    pool.interrupt();
    r = pool.receive(buf, Clock::now() + 1s);
    check(is_error(r, 0, SocketError::Interrupted), "an interrupt is still reported before the deadline, got " + describe(r));

    auto pair = make_pair();
    const auto id = pool.connection_add(std::move(pair.pooled));
    check(pair.peer.send(as_bytes("x")).has_value(), "peer send");
    r = pool.receive(buf, Clock::now() + 2s);
    check(r && id && r->connection_id == *id && r->bytes_read == 1, "data before the deadline is reported, got " + describe(r));
}

// --- read pausing (backpressure) -------------------------------------------

// CPU time used by the calling thread.
std::chrono::nanoseconds thread_cpu() {
    timespec ts{};
    ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
}

void test_pause_keeps_data_in_the_kernel() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled));
    check(id.has_value(), "add");
    if (!id) { return; }
    const uint64_t ids[] = {*id, 999};   // an unknown id is skipped
    pool.pause_reading(ids);
    pool.pause_reading(ids);              // and pausing twice is harmless
    check(pair.peer.send(as_bytes("abc")).has_value(), "peer sends while paused");

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    const auto cpu_before = thread_cpu();
    auto r = pool.receive(buf, Clock::now() + 150ms);
    const auto cpu = thread_cpu() - cpu_before;
    check(is_error(r, 0, SocketError::WouldBlock), "nothing is read while paused, got " + describe(r));
    // Unread data on a level-triggered socket would be reported again and again
    // if it were still watched for reading: the wait would spin.
    check(cpu < 30ms, "the paused wait sleeps rather than spins (" +
                          std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(cpu).count()) + " ms CPU)");
    pool.resume_reading(ids);
    r = pool.receive(buf, Clock::now() + 2s);
    check(r && r->connection_id == *id && as_string(std::span(buf).first(r->bytes_read)) == "abc",
          "resumed: the data was waiting, got " + describe(r));
}

void test_pause_lets_writes_flow() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled));
    check(id.has_value(), "add");
    if (!id) { return; }
    const uint64_t ids[] = {*id};
    pool.pause_reading(ids);
    check(pair.peer.send(as_bytes("unread")).has_value(), "peer sends while paused");

    // More than the socket buffers hold: the rest waits in the TX buffer and
    // only the pool's EPOLLOUT handling can move it.
    const std::string big(768 * 1024, 'z');
    check(pool.send(*id, as_bytes(big)).has_value(), "queue 768 KiB");

    std::atomic<bool> data_seen{false};
    std::thread loop([&] {
        std::array<std::byte, 4096> buf{};
        for (;;) {
            const auto r = pool.receive(buf);
            if (!r && r.error().connection_id == 0) { return; }   // stopped
            if (r && r->connection_id == *id && r->bytes_read > 0) { data_seen = true; }
        }
    });
    pair.peer.with_receive_timeout(2s);
    std::size_t got = 0;
    std::array<std::byte, 65536> in{};
    while (got < big.size()) {
        const auto n = pair.peer.receive(in);
        if (!n) { break; }
        got += *n;
    }
    pool.stop();
    loop.join();
    check(got == big.size(), "every queued byte reached the peer while paused (" + std::to_string(got) + ")");
    check(!data_seen, "and nothing was read meanwhile");
}

void test_pause_freezes_the_read_timeout() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled), Pool::Timeouts{.read = 100ms, .write = 5s});
    check(id.has_value(), "add with a 100 ms read timeout");
    if (!id) { return; }
    const uint64_t ids[] = {*id};
    pool.pause_reading(ids);

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    auto r = pool.receive(buf, Clock::now() + 400ms);
    check(is_error(r, 0, SocketError::WouldBlock), "four timeouts of silence while paused: no ReadTimedOut, got " + describe(r));
    check(pool.timer_count() == 1, "the read timer kept itself alive, once (" + std::to_string(pool.timer_count()) + ")");
    const auto resumed = Clock::now();
    pool.resume_reading(ids);
    r = pool.receive(buf, Clock::now() + 2s);
    const auto after = Clock::now() - resumed;
    check(is_error(r, *id, SocketError::ReadTimedOut), "resumed: the timeout counts again, got " + describe(r));
    check(after >= 90ms && after < 1s, "from the resume, not from before the pause");
}

void test_pause_cycles_keep_one_read_timer() {
    auto pair = make_pair();
    Pool pool;
    const auto added = Clock::now();
    const auto id = pool.connection_add(std::move(pair.pooled), Pool::Timeouts{.read = 200ms, .write = 5s});
    check(id.has_value(), "add with a 200 ms read timeout");
    if (!id) { return; }
    std::this_thread::sleep_for(150ms);
    const uint64_t ids[] = {*id};
    for (int i = 0; i < 50; ++i) {   // many cycles, each must leave no live timer behind
        pool.pause_reading(ids);
        pool.resume_reading(ids);
    }
    check(pool.timer_count() == 1, "still one read timer after 50 cycles, not one per cycle (" +
                                       std::to_string(pool.timer_count()) + " queued)");
    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    const auto r = pool.receive(buf, Clock::now() + 2s);
    const auto since = Clock::now() - added;
    check(is_error(r, *id, SocketError::ReadTimedOut), "the timeout still fires, got " + describe(r));
    check(since >= 330ms, "200 ms after the last resume, not at the original deadline");
}

void test_hang_up_while_paused() {
    auto pair = make_pair();
    Pool pool;
    const auto id = pool.connection_add(std::move(pair.pooled));
    check(id.has_value(), "add");
    if (!id) { return; }
    const uint64_t ids[] = {*id};
    pool.pause_reading(ids);
    check(pair.peer.send(as_bytes("bye")).has_value(), "peer sends its last words");
    std::ignore = pair.peer.shutdown(TcpConnection::Shutdown::Both);
    { TcpConnection gone = std::move(pair.peer); }

    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 3s);
    // A plain close shows only as readable, so it waits; a reset or a full
    // hang-up is reported. Either way nothing spins and the data is not lost.
    auto r = pool.receive(buf, Clock::now() + 200ms);
    if (is_error(r, 0, SocketError::WouldBlock)) {
        pool.resume_reading(ids);
        r = pool.receive(buf, Clock::now() + 2s);
    }
    check(r && as_string(std::span(buf).first(r->bytes_read)) == "bye", "the last data is delivered, got " + describe(r));
    r = pool.receive(buf, Clock::now() + 2s);
    check(is_error(r, *id, SocketError::ConnectionClosed), "then the close, got " + describe(r));
}

void test_release_while_paused() {
    auto pair = make_pair();
    Pool a;
    Pool b;
    const auto id = a.connection_add(std::move(pair.pooled));
    check(id.has_value(), "add to A");
    if (!id) { return; }
    const uint64_t ids[] = {*id};
    a.pause_reading(ids);
    check(pair.peer.send(as_bytes("moved")).has_value(), "peer sends while paused on A");
    std::this_thread::sleep_for(20ms);
    auto released = a.connection_release(*id);
    check(released.has_value(), "release from A while paused");
    if (!released) { return; }
    const auto id_b = b.connection_add(std::move(released->connection));
    std::array<std::byte, 64> buf{};
    Watchdog dog(b, 3s);
    const auto r = b.receive(buf, Clock::now() + 2s);
    check(id_b && r && r->connection_id == *id_b && as_string(std::span(buf).first(r->bytes_read)) == "moved",
          "B reads it, data intact, got " + describe(r));
}

int rcvbuf_of(const TcpConnection& c) {
    return c.receive_buffer_size().value_or(-1);
}

void test_receive_buffer_option() {
    auto pair = make_pair();
    const int before = rcvbuf_of(pair.pooled);
    pair.pooled.with_receive_buffer(16 * 1024);
    check(pair.pooled.options().receive_buffer == 16 * 1024, "the builder records the size");
    check(pair.pooled.apply_options_now().has_value(), "and applies it");
    // Linux doubles the value it is given, for its own bookkeeping.
    const int after = rcvbuf_of(pair.pooled);
    check(after >= 16 * 1024 && after <= 2 * 16 * 1024 && after != before,
          "SO_RCVBUF is set (" + std::to_string(before) + " -> " + std::to_string(after) + ")");
}

void test_connection_count() {
    Pool pool(Pool::Timeouts{0ms, 5s}, 64 * 1024);
    check(pool.connection_count() == 0, "an empty pool holds no connections");
    auto a = make_pair();
    auto b = make_pair();
    const auto ida = pool.connection_add(std::move(a.pooled));
    const auto idb = pool.connection_add(std::move(b.pooled));
    check(ida && idb && pool.connection_count() == 2, "two added");
    pool.connection_drop(*ida);
    check(pool.connection_count() == 1, "drop removes at once");

    // Queue more than the kernel takes while the peer does not read, then remove:
    // the connection stays counted until its buffer has been flushed.
    const std::vector<std::byte> chunk(16 * 1024, std::byte{'x'});
    bool queued = false;
    for (int i = 0; i < 10000 && !queued; ++i) {
        const auto sent = pool.send(*idb, chunk);
        if (!sent && sent.error() == SocketError::BufferFull) { queued = true; }
    }
    check(queued, "the TX buffer filled up");
    pool.connection_remove(*idb);
    check(pool.connection_count() == 1, "a removed connection that is still flushing is counted");

    std::atomic<size_t> drained{0};
    std::thread reader([&] {
        std::array<std::byte, 64 * 1024> sink{};
        for (;;) {
            const auto n = b.peer.receive(sink);
            if (!n || *n == 0) { return; }
            drained += *n;
        }
    });
    std::array<std::byte, 64> buf{};
    Watchdog dog(pool, 5s);
    const auto until = Clock::now() + 4s;
    while (pool.connection_count() > 0 && Clock::now() < until) {
        std::ignore = pool.receive(buf, Clock::now() + 20ms);   // flushing happens inside receive()
    }
    check(pool.connection_count() == 0, "the count drops to 0 once the flush is done");
    reader.join();
    check(drained > 0, "the peer received the flushed data");
}

int main() {
    run("interrupt wakes a blocked receive", test_interrupt_wakes_blocked_receive);
    run("interrupts coalesce", test_interrupts_coalesce);
    run("dial: success", test_dial_success);
    run("dial: refused", test_dial_refused);
    run("dial: invalid address", test_dial_invalid_address);
    run("dial: connect timeout", test_dial_timeout);
    run("drop forgets a queued notice", test_drop_forgets_queued_notice);
    run("release + adopt keep the stream intact", test_release_and_adopt_keep_the_stream);
    run("release refusals", test_release_refusals);
    run("set_timeouts enables a read timeout", test_set_timeouts_enables_read_timeout);
    run("set_timeouts disables a read timeout", test_set_timeouts_disables_read_timeout);
    run("take_stats", test_take_stats);
    run("TcpConnection: connect_start + connect_status", test_connect_start_and_status);
    run("peer close (regression)", test_peer_close_still_reported);
    run("stop beats interrupt (regression)", test_stop_still_wins);
    run("receive with a deadline", test_receive_deadline);
    run("connection_count", test_connection_count);
    run("receive buffer option", test_receive_buffer_option);
    run("pause: data stays in the kernel", test_pause_keeps_data_in_the_kernel);
    run("pause: writes still flow", test_pause_lets_writes_flow);
    run("pause: no read timeout while paused", test_pause_freezes_the_read_timeout);
    run("pause: cycles keep one read timer", test_pause_cycles_keep_one_read_timer);
    run("pause: a hang-up while paused", test_hang_up_while_paused);
    run("pause: release while paused", test_release_while_paused);

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
