// test_stressgen - runs stressgen against an in-process fake node that follows
// the protocol (read ConnectInfo, dial back, Ack with the token, echo chats).

#include "stressgen.hpp"

#include <algorithm>
#include <atomic>
#include <random>
#include <cstdio>
#include <cmath>
#include <cstdlib>
#include <mutex>
#include <thread>

static int failures = 0;
static void check(bool ok, const char* what) {
    std::printf("[%s] %s\n", ok ? "ok" : "FAIL", what);
    if (!ok) { ++failures; }
}

static bool send_all(int fd, const std::vector<std::byte>& b) {
    std::size_t off = 0;
    while (off < b.size()) {
        const ssize_t n = ::send(fd, b.data() + off, b.size() - off, MSG_NOSIGNAL);
        if (n <= 0) { return false; }
        off += static_cast<std::size_t>(n);
    }
    return true;
}

class FakeNode {
public:
    // ack_every: Ack only peers whose index (peer_id - 1'000'000) % ack_every == 0 (1 = all).
    // dup_at: echo the dup_at-th message twice (0 = never).
    explicit FakeNode(bool duplex = false, int ack_every = 1, int dup_at = 0)
        : duplex_(duplex), ack_every_(ack_every), dup_at_(dup_at) {
        lfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(lfd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ::bind(lfd_, reinterpret_cast<sockaddr*>(&a), sizeof a);
        ::listen(lfd_, 1024);
        socklen_t l = sizeof a;
        ::getsockname(lfd_, reinterpret_cast<sockaddr*>(&a), &l);
        port = ntohs(a.sin_port);
        acceptor_ = std::thread([this] {
            for (;;) {
                const int fd = ::accept(lfd_, nullptr, nullptr);
                if (fd < 0) { return; }
                std::lock_guard lk(m_);
                fds_.push_back(fd);
                threads_.emplace_back([this, fd] { serve(fd); });
            }
        });
    }
    ~FakeNode() {
        stopping_ = true;
        ::shutdown(lfd_, SHUT_RDWR);
        acceptor_.join();
        ::close(lfd_);
        {
            std::lock_guard lk(m_);
            for (int fd : fds_) { ::shutdown(fd, SHUT_RDWR); }
        }
        for (auto& t : threads_) { t.join(); }
        for (int fd : fds_) { ::close(fd); }
    }
    std::uint16_t port = 0;
    std::atomic<int> echoed{0};
    std::atomic<int> eagain{0};   // echo sends that found the peer's socket full (backpressure)

private:
    // Non-blocking first; on EAGAIN count it, then wait for writability (until teardown).
    bool send_echo(int fd, const std::vector<std::byte>& b) {
        std::size_t off = 0;
        while (off < b.size()) {
            const ssize_t n = ::send(fd, b.data() + off, b.size() - off, MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n > 0) { off += static_cast<std::size_t>(n); continue; }
            if (n < 0 && errno == EINTR) { continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                ++eagain;
                pollfd p{fd, POLLOUT, 0};
                while (!stopping_ && ::poll(&p, 1, 100) == 0) {}
                if (stopping_) { return false; }
                continue;
            }
            return false;
        }
        return true;
    }
    void serve(int fd) {
        wire::Decoder dec;
        std::vector<std::byte> buf(65536);
        int ret = -1;
        for (;;) {
            const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
            if (n <= 0) { break; }
            dec.feed(std::span<const std::byte>(buf.data(), static_cast<std::size_t>(n)));
            for (;;) {
                auto item = dec.next();
                if (!item) { continue; }
                if (!*item) { break; }
                if (const auto* ci = wire::as_connect(**item)) {
                    if (ack_every_ > 1 && (ci->peer_id - 1'000'000u) % static_cast<unsigned>(ack_every_) != 0) { continue; }
                    if (duplex_) {   // answer on the accepted socket, never dial back
                        ret = fd;
                        auto ack = wire::encode(wire::make_connect_ack(77, ci->peer_id));
                        send_all(ret, *ack);
                        continue;
                    }
                    if (ci->endpoints.empty()) { continue; }
                    ret = ::socket(AF_INET, SOCK_STREAM, 0);
                    sockaddr_in a{};
                    a.sin_family = AF_INET;
                    a.sin_port = htons(ci->endpoints[0].port);
                    ::inet_pton(AF_INET, ci->endpoints[0].address.c_str(), &a.sin_addr);
                    if (::connect(ret, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) { ::close(ret); ret = -1; continue; }
                    {
                        std::lock_guard lk(m_);
                        fds_.push_back(ret);   // shut down + closed at teardown
                    }
                    auto ack = wire::encode(wire::make_connect_ack(77, ci->peer_id));
                    send_all(ret, *ack);
                } else if (const auto* m = wire::as_message(**item)) {
                    if (ret < 0) { continue; }
                    auto e = wire::encode(wire::make_chat_message(m->conversation_id, m->sender_id, m->body));
                    if (send_echo(ret, *e)) { ++echoed; }
                    if (dup_at_ > 0 && ++seen_msgs_ == dup_at_) { send_echo(ret, *e); }
                }
            }
        }
    }
    bool duplex_ = false;
    int ack_every_ = 1, dup_at_ = 0;
    std::atomic<int> seen_msgs_{0};
    std::atomic<bool> stopping_{false};
    int lfd_ = -1;
    std::thread acceptor_;
    std::mutex m_;
    std::vector<int> fds_;
    std::vector<std::thread> threads_;
};

static int run(const stress::Config& c, stress::Result& r, std::string& err) {
    stress::Generator g(c);
    return g.run(r, err);
}

// Absolute-correctness check of the histogram against a sorted copy of the samples.
static bool hist_ok(const std::vector<std::int64_t>& v, bool print = false) {
    stress::Histogram h;
    double sum = 0;
    for (auto x : v) { h.record(x); sum += static_cast<double>(x); }
    auto s = v;
    std::sort(s.begin(), s.end());
    if (h.count() != v.size() || h.max() != s.back()) { return false; }
    if (std::abs(h.mean() - sum / static_cast<double>(v.size())) > 1e-6 * (1 + sum / static_cast<double>(v.size()))) { return false; }
    for (double q : {0.0001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999, 1.0}) {
        auto rank = static_cast<std::size_t>(std::ceil(q * static_cast<double>(v.size())));
        rank = std::clamp<std::size_t>(rank, 1, v.size());
        const std::int64_t truth = s[rank - 1];
        const std::int64_t got = h.quantile(q);
        if (got < truth) { if (print) { std::printf("  q=%g got %lld < true %lld\n", q, (long long)got, (long long)truth); } return false; }
        if (static_cast<double>(got - truth) > static_cast<double>(truth) / 32.0) {
            if (print) { std::printf("  q=%g got %lld vs true %lld\n", q, (long long)got, (long long)truth); }
            return false;
        }
    }
    return true;
}

static void histogram_tests() {
    for (std::int64_t c : {0LL, 1LL, 31LL, 63LL, 64LL, 100LL, 1'000'000LL, 123'456'789LL, 40'000'000'000LL}) {
        check(hist_ok(std::vector<std::int64_t>(1000, c), true), ("histogram constant " + std::to_string(c)).c_str());
    }
    std::mt19937_64 rng(42);
    for (auto [lo, hi] : {std::pair<std::int64_t, std::int64_t>{1, 60}, {1, 1'000'000}, {1000, 1'000'000'000}, {50'000, 60'000}}) {
        std::vector<std::int64_t> v(20000);
        for (auto& x : v) { x = lo + static_cast<std::int64_t>(rng() % static_cast<std::uint64_t>(hi - lo + 1)); }
        check(hist_ok(v, true), ("histogram uniform [" + std::to_string(lo) + "," + std::to_string(hi) + "]").c_str());
    }
    bool edges = true;
    for (int k = 1; k <= 45; ++k) {
        const std::int64_t p = 1ll << k;
        for (std::int64_t d : {-1LL, 0LL, 1LL}) {
            // single value at an edge (exact upper bound check) and a pair straddling it
            edges = edges && hist_ok({p + d}, true) && hist_ok({p + d, p + d, p + d + 1}, true);
        }
    }
    check(edges, "histogram powers-of-two edges (2^k-1, 2^k, 2^k+1)");
    {
        stress::Histogram h;
        h.record(-5); h.record(10);
        check(h.quantile(0.5) == 0 && h.max() == 10 && h.count() == 2, "histogram: negative clamps to 0");
        check(stress::Histogram{}.quantile(0.5) == 0, "histogram: empty -> 0");
    }
}

static bool has(const std::string& j, const char* key) { return j.find(key) != std::string::npos; }

static void partial_ramp(bool duplex) {
    const std::string tag = duplex ? "duplex " : "simplex ";
    FakeNode node(duplex, 2);   // Acks every second peer only
    stress::Config c;
    c.target_port = node.port; c.duplex = duplex;
    c.peers = 16; c.rate = 100; c.duration = 1; c.warmup = 0.2; c.grace = 0.5; c.ramp_timeout = 1;
    stress::Result r; std::string err;
    check(run(c, r, err) == 0, (tag + "partial ramp: rc 0").c_str());
    check(r.peers_total == 16 && r.peers_ready == 8, (tag + "partial ramp: peers_ready == half").c_str());
    check(r.handshake_failures >= 8, (tag + "partial ramp: handshake_failures counts un-Acked peers").c_str());
    check(r.sent > 0 && r.lost == 0, (tag + "partial ramp: Ready peers still carry traffic").c_str());
    check(has(stress::to_json(c, r), "\"handshake_failures\":"), (tag + "partial ramp: json field").c_str());
}

static void slow_backpressure(bool duplex) {
    const std::string tag = duplex ? "duplex " : "simplex ";
    FakeNode node(duplex);
    stress::Config c;
    c.target_port = node.port; c.duplex = duplex;
    c.peers = 4; c.slow_peers = 4; c.rate = 100; c.size = 8000; c.duration = 2; c.warmup = 0.3; c.grace = 0.5;
    stress::Result r; std::string err;
    check(run(c, r, err) == 0, (tag + "slow: run completes").c_str());
    check(node.eagain > 0, (tag + "slow: node's echo send hit EAGAIN (backpressure)").c_str());
    check(r.slow_unread_bytes > 0, (tag + "slow: slow_unread_bytes > 0").c_str());
    check(r.slow_unread_bytes < 4 * 64 * 1024, (tag + "slow: unread bounded by the small rcvbuf").c_str());
    check(r.received > 0 && r.slow_sent > 0, (tag + "slow: active peers echo, slow peers send").c_str());
    std::fprintf(stderr, "%s slow: eagain=%d unread=%llu\n", tag.c_str(), node.eagain.load(), (unsigned long long)r.slow_unread_bytes);
}

static void duplicates(bool duplex) {
    const std::string tag = duplex ? "duplex " : "simplex ";
    FakeNode node(duplex, 1, 50);   // the 50th message is echoed twice
    stress::Config c;
    c.target_port = node.port; c.duplex = duplex;
    c.peers = 4; c.rate = 100; c.duration = 1.5; c.warmup = 0; c.grace = 1;
    stress::Result r; std::string err;
    check(run(c, r, err) == 0, (tag + "dup: run completes").c_str());
    check(r.duplicates == 1, (tag + "dup: duplicates == 1").c_str());
    check(r.received_raw == r.received && r.received == r.sent + 1, (tag + "dup: received_raw counts both copies").c_str());
    check(r.lost == 0, (tag + "dup: lost keeps its clamped rule (0)").c_str());
    const std::string j = stress::to_json(c, r);
    check(has(j, "\"duplicates\":1") && has(j, "\"received_raw\":"), (tag + "dup: json fields").c_str());
}

static void window_fields() {
    FakeNode node;
    stress::Config c;
    c.target_port = node.port;
    c.peers = 4; c.rate = 100; c.duration = 1; c.warmup = 0.4; c.grace = 0.3;
    stress::Result r; std::string err;
    const auto t0 = stress::now_ns();
    check(run(c, r, err) == 0, "window: run completes");
    const auto t1 = stress::now_ns();
    check(r.window_start_mono_ns >= t0 + 400'000'000 && r.window_end_mono_ns <= t1, "window: inside the run, after warmup");
    check(r.window_start_mono_ns < r.window_end_mono_ns, "window: start < end");
    check(std::llabs((r.window_end_mono_ns - r.window_start_mono_ns) - 1'000'000'000) < 1'000'000, "window: end - start ~= duration");
    const std::string j = stress::to_json(c, r);
    check(has(j, "\"window_start_mono_ns\":") && has(j, "\"window_end_mono_ns\":") && has(j, "\"gen_cpu\":{\"utime_us\":") &&
          has(j, "\"slow_unread_bytes\":0") && has(j, "\"handshake_failures\":0"), "window: json fields present");
    check(r.gen_utime_us + r.gen_stime_us > 0, "gen_cpu: nonzero CPU over the window");
}

int main() {
    histogram_tests();
    partial_ramp(false); partial_ramp(true);
    slow_backpressure(false); slow_backpressure(true);
    duplicates(false); duplicates(true);
    window_fields();
    {   // steady rate, 64 peers
        FakeNode node;
        stress::Config c;
        c.target_port = node.port;
        c.peers = 64; c.rate = 200; c.duration = 2; c.warmup = 0.5; c.size = 64;
        stress::Result r; std::string err;
        const int rc = run(c, r, err);
        std::fprintf(stderr, "%s", stress::to_json(c, r).c_str());
        check(rc == 0, "rate run completes");
        check(r.peers_ready == 64, "all peers ready");
        const double expect = 64.0 * 200 * 2;
        check(r.sent > expect * 0.8 && r.sent < expect * 1.2, "sent ~= peers*rate*duration");
        check(r.lost == 0 && r.received == r.sent, "received == sent");
        check(r.rtt.count() == r.received, "one sample per echo");
        check(r.rtt.quantile(0.5) > 0 && r.rtt.quantile(0.5) < 50'000'000, "p50 sane (<50ms)");
        check(r.rtt.quantile(0.99) >= r.rtt.quantile(0.5) && r.rtt.max() >= r.rtt.quantile(0.999), "quantiles monotone");
        check(r.handshake.count() >= 64 && r.handshake.quantile(0.5) > 0, "handshake samples present and positive");
        check(stress::to_json(c, r).find("\"handshake_us\":{\"p50\":") != std::string::npos, "json has handshake_us");
    }
    {   // closed loop window
        FakeNode node;
        stress::Config c;
        c.target_port = node.port;
        c.peers = 8; c.rate = 0; c.window = 4; c.duration = 1; c.warmup = 0.2;
        stress::Result r; std::string err;
        check(run(c, r, err) == 0, "window run completes");
        check(r.sent > 100 && r.lost == 0, "window: traffic flows, nothing lost");
    }
    {   // idle + slow + churn
        FakeNode node;
        stress::Config c;
        c.target_port = node.port;
        c.peers = 16; c.idle_peers = 4; c.slow_peers = 4; c.rate = 100; c.duration = 1.5; c.warmup = 0.3;
        c.churn = 20; c.grace = 1;
        stress::Result r; std::string err;
        check(run(c, r, err) == 0, "mixed run completes");
        check(r.churn_events > 10, "churn happened");
        check(r.handshake.count() > 16 + 4 + 4 && r.handshake.quantile(0.5) > 0, "churn redials add handshake samples");
        check(r.slow_sent > 0 && r.received > 0, "slow peers send, active peers echo");
        check(r.received <= r.sent, "received <= sent");
    }
    {   // duplex: single connection per peer
        FakeNode node(true);
        stress::Config c;
        c.target_port = node.port; c.duplex = true;
        c.peers = 32; c.rate = 200; c.duration = 1.5; c.warmup = 0.5;
        stress::Result r; std::string err;
        check(run(c, r, err) == 0, "duplex rate run completes");
        check(r.peers_ready == 32, "duplex: all peers ready");
        check(r.sent > 0 && r.lost == 0 && r.received == r.sent, "duplex: received == sent");
        check(r.rtt.count() == r.received, "duplex: one sample per echo");
        check(r.handshake.count() >= 32 && r.handshake.quantile(0.5) > 0, "duplex: handshake samples present and positive");
    }
    {   // duplex: idle + slow + churn
        FakeNode node(true);
        stress::Config c;
        c.target_port = node.port; c.duplex = true;
        c.peers = 16; c.idle_peers = 4; c.slow_peers = 4; c.rate = 100; c.duration = 1.5; c.warmup = 0.3;
        c.churn = 20; c.grace = 1;
        stress::Result r; std::string err;
        check(run(c, r, err) == 0, "duplex mixed run completes");
        check(r.churn_events > 10, "duplex: churn happened");
        check(r.handshake.count() > 16 + 4 + 4 && r.handshake.quantile(0.5) > 0, "duplex: churn redials add handshake samples");
        check(r.slow_sent > 0 && r.received > 0, "duplex: slow peers send, active peers echo");
        check(r.received <= r.sent, "duplex: received <= sent");
    }
    {   // unreachable target
        stress::Config c;
        {   // a port that was just free: nothing listens there
            FakeNode dead;
            c.target_port = dead.port;
        }
        stress::Result r; std::string err;
        check(run(c, r, err) == 2, "unreachable target -> rc 2");
    }
    std::printf(failures ? "FAILED\n" : "all passed\n");
    return failures ? 1 : 0;
}
