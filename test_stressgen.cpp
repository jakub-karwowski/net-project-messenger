// test_stressgen - runs stressgen against an in-process fake node that follows
// the protocol (read ConnectInfo, dial back, Ack with the token, echo chats).

#include "stressgen.hpp"

#include <atomic>
#include <cstdio>
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
    explicit FakeNode(bool duplex = false) : duplex_(duplex) {
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

private:
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
                    if (send_all(ret, *e)) { ++echoed; }
                }
            }
        }
    }
    bool duplex_ = false;
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

int main() {
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
