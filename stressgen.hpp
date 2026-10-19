#pragma once

// stressgen.hpp - load generator that speaks the chat wire protocol directly.
//
// Simulated peer i opens a write connection to the node and sends
// ConnectInfo{peer_id = token_i, endpoints = [our listen addr]}. The node dials
// our (single, shared) listener back and sends ConnectInfoAck whose
// ack_peer_id == token_i, which tells us which peer that return connection
// belongs to. Chat messages go out on the write connection; the node (running
// with --echo) echoes them back on the return connection, giving an RTT.
//
// Everything runs on ONE epoll thread (non-blocking sockets), so the generator
// scales to thousands of peers without a thread per peer.

#include "Wire.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace stress {

inline std::int64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}
inline std::int64_t wall_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// --- Config / Result ---------------------------------------------------------

struct Config {
    std::string target_ip = "127.0.0.1";
    std::uint16_t target_port = 0;
    std::string listen_ip = "127.0.0.1";   // address advertised to the node
    std::uint16_t listen_port = 0;         // 0 = ephemeral
    int peers = 16;                        // active (sending) peers
    double rate = 100;                     // msgs/s/peer, 0 = as fast as possible
    int window = 0;                        // max in-flight per peer, 0 = unlimited
    int size = 64;                         // body bytes
    double duration = 5;                   // measurement window, seconds
    double warmup = 1;                     // excluded from stats, seconds
    int idle_peers = 0;                    // extra peers: handshake only
    int slow_peers = 0;                    // extra peers: send, never read return conn
    double churn = 0;                      // reconnects per second
    double grace = 2;                      // wait for late echoes after the run
    double ramp_timeout = 10;              // max wait for handshakes
    bool duplex = false;                   // one connection per peer: Ack + echoes come back on the dialed socket
    std::string out;
};

// Log-bucketed histogram (32 sub-buckets per power of two, ~3% resolution).
class Histogram {
public:
    void record(std::int64_t ns) {
        if (ns < 0) { ns = 0; }
        ++buckets_[index(static_cast<std::uint64_t>(ns))];
        ++count_;
        max_ = std::max(max_, ns);
        sum_ += static_cast<double>(ns);
    }
    std::uint64_t count() const noexcept { return count_; }
    std::int64_t max() const noexcept { return max_; }
    double mean() const noexcept { return count_ ? sum_ / static_cast<double>(count_) : 0; }
    // Upper edge of the bucket holding the q-quantile, in ns.
    std::int64_t quantile(double q) const noexcept {
        if (count_ == 0) { return 0; }
        auto target = static_cast<std::uint64_t>(q * static_cast<double>(count_));
        if (static_cast<double>(target) < q * static_cast<double>(count_)) { ++target; }
        target = std::max<std::uint64_t>(target, 1);
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < buckets_.size(); ++i) {
            seen += buckets_[i];
            if (seen >= target) { return std::min<std::int64_t>(upper(i), max_); }
        }
        return max_;
    }
private:
    static std::size_t index(std::uint64_t v) noexcept {
        const int msb = 63 - std::countl_zero(v | 1);
        const int shift = msb > 5 ? msb - 5 : 0;
        return std::min<std::size_t>(static_cast<std::size_t>(shift) * 32 + static_cast<std::size_t>(v >> shift),
                                     64 * 32 - 1);
    }
    static std::int64_t upper(std::size_t idx) noexcept {
        const std::size_t shift = idx < 64 ? 0 : idx / 32 - 1;
        const std::uint64_t base = static_cast<std::uint64_t>(idx - shift * 32) << shift;
        return static_cast<std::int64_t>(base + ((1ull << shift) - 1));
    }
    std::vector<std::uint64_t> buckets_ = std::vector<std::uint64_t>(64 * 32, 0);
    std::uint64_t count_ = 0;
    std::int64_t max_ = 0;
    double sum_ = 0;
};

struct Result {
    std::uint64_t sent = 0;           // measured-window messages sent (excl. slow peers)
    std::uint64_t received = 0;       // of those, echoes that arrived (incl. grace)
    std::uint64_t lost = 0;           // sent - received
    double msgs_per_s = 0;            // received / duration
    Histogram rtt;
    std::uint64_t total_sent = 0;     // everything, warmup included
    std::uint64_t total_received = 0;
    std::uint64_t slow_sent = 0;
    std::uint64_t send_skipped = 0;   // generator-side drops (output buffer full)
    int peers_total = 0, peers_ready = 0;
    std::uint64_t connect_failures = 0, disconnects = 0, churn_events = 0, bad_frames = 0;
    std::int64_t start_wall_ns = 0, end_wall_ns = 0;
    double measured_seconds = 0;
};

// --- Generator ---------------------------------------------------------------

class Generator {
public:
    explicit Generator(const Config& cfg) : cfg_(cfg), rng_(std::random_device{}()) {}
    ~Generator() {
        for (std::size_t fd = 0; fd < conns_.size(); ++fd) { if (conns_[fd]) { ::close(static_cast<int>(fd)); } }
        if (lfd_ >= 0) { ::close(lfd_); }
        if (ep_ >= 0) { ::close(ep_); }
    }

    // 0 ok, 2 target unreachable, 3 no handshakes completed, 1 other.
    int run(Result& res, std::string& err);

private:
    enum class Kind { Active, Idle, Slow };
    enum class State { Dead, Connecting, Handshaking, Ready };
    enum class CType { Write, Ret };
    struct Peer {
        std::uint64_t token = 0;
        Kind kind = Kind::Active;
        State state = State::Dead;
        int wfd = -1, rfd = -1;
        std::vector<std::byte> out;
        std::size_t out_off = 0;
        bool want_out = false;
        bool no_read = false;      // duplex slow peer: EPOLLIN disabled, socket kept open
        std::uint64_t seq = 0;
        std::uint64_t inflight = 0;
        std::int64_t next_send = 0;
    };
    struct Conn {
        CType type;
        int peer;                 // -1 while a return connection is unmatched
        wire::Decoder dec;
    };

    void ctl(int op, int fd, std::uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.u64 = static_cast<std::uint64_t>(fd);
        ::epoll_ctl(ep_, op, fd, &ev);
    }
    void add_conn(int fd, CType t, int peer, std::uint32_t events) {
        if (conns_.size() <= static_cast<std::size_t>(fd)) { conns_.resize(static_cast<std::size_t>(fd) * 2 + 16); }
        conns_[static_cast<std::size_t>(fd)] = std::make_unique<Conn>(Conn{t, peer, {}});
        ctl(EPOLL_CTL_ADD, fd, events);
    }
    void close_conn(int fd) {
        if (fd < 0) { return; }
        ::epoll_ctl(ep_, EPOLL_CTL_DEL, fd, nullptr);
        ::close(fd);
        if (static_cast<std::size_t>(fd) < conns_.size()) { conns_[static_cast<std::size_t>(fd)].reset(); }
    }

    bool setup(std::string& err);
    void dial(int i);
    void kill_peer(int i, bool count);
    void maybe_ready(int i);
    void flush(int i);
    void arm_write(int i);
    void on_accept();
    void on_write_conn(int fd, std::uint32_t ev);
    void on_ret_conn(int fd);
    void on_duplex_read(int fd);
    void on_message(int peer, const wire::ChatMessage& m, std::int64_t now);
    void send_one(int i, std::int64_t now);
    void tick_send(std::int64_t now);
    void tick_churn(std::int64_t now);

    Config cfg_;
    std::mt19937_64 rng_;
    int ep_ = -1, lfd_ = -1;
    std::uint16_t lport_ = 0;
    std::vector<std::unique_ptr<Conn>> conns_;
    std::vector<Peer> peers_;
    std::unordered_map<std::uint64_t, int> by_token_;
    Result* res_ = nullptr;
    std::int64_t ms_ = 0, me_ = 0;   // measurement window [ms_, me_)
    bool sending_ = false;
    std::uint64_t sent_meas_ = 0;
    std::vector<std::byte> rbuf_ = std::vector<std::byte>(64 * 1024);
    std::int64_t churn_next_ = 0;
    int dial_cursor_ = 0;
};

inline bool Generator::setup(std::string& err) {
    rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &rl);
    }
    ep_ = ::epoll_create1(0);
    if (cfg_.duplex) {   // no listener: the node answers on the dialed socket; endpoint advertised is ignored
        if (ep_ < 0) { err = "epoll_create failed"; return false; }
        lport_ = cfg_.listen_port;
        return true;
    }
    lfd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (ep_ < 0 || lfd_ < 0) { err = "socket/epoll_create failed"; return false; }
    int one = 1;
    ::setsockopt(lfd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(cfg_.listen_port);
    if (::inet_pton(AF_INET, cfg_.listen_ip.c_str(), &a.sin_addr) != 1) { err = "bad --listen-ip"; return false; }
    if (::bind(lfd_, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0 || ::listen(lfd_, 4096) != 0) {
        err = std::string("listen failed: ") + std::strerror(errno);
        return false;
    }
    socklen_t len = sizeof a;
    ::getsockname(lfd_, reinterpret_cast<sockaddr*>(&a), &len);
    lport_ = ntohs(a.sin_port);
    ctl(EPOLL_CTL_ADD, lfd_, EPOLLIN);
    return true;
}

inline void Generator::dial(int i) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) { ++res_->connect_failures; p.state = State::Dead; return; }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(cfg_.target_port);
    ::inet_pton(AF_INET, cfg_.target_ip.c_str(), &a.sin_addr);
    const int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    if (rc != 0 && errno != EINPROGRESS) { ::close(fd); ++res_->connect_failures; p.state = State::Dead; return; }
    p.wfd = fd;
    p.state = State::Connecting;
    p.out.clear(); p.out_off = 0; p.want_out = true; p.no_read = false; p.inflight = 0;
    add_conn(fd, CType::Write, i, EPOLLIN | EPOLLOUT);
}

inline void Generator::kill_peer(int i, bool count) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    if (p.state == State::Dead) { return; }
    close_conn(p.wfd); close_conn(p.rfd);
    p.wfd = p.rfd = -1;
    p.state = State::Dead;
    p.out.clear(); p.out_off = 0; p.want_out = false; p.inflight = 0;
    if (count) { ++res_->disconnects; }
}

inline void Generator::maybe_ready(int i) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    if (cfg_.duplex) { return; }   // duplex: Ready is set when the Ack arrives
    if (p.state == State::Handshaking && p.rfd >= 0) { p.state = State::Ready; }
}

inline void Generator::arm_write(int i) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    const std::uint32_t in = p.no_read ? 0u : static_cast<std::uint32_t>(EPOLLIN);
    ctl(EPOLL_CTL_MOD, p.wfd, p.want_out ? (in | EPOLLOUT) : in);
}

inline void Generator::flush(int i) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    while (p.out_off < p.out.size()) {
        const ssize_t n = ::send(p.wfd, p.out.data() + p.out_off, p.out.size() - p.out_off, MSG_NOSIGNAL);
        if (n > 0) { p.out_off += static_cast<std::size_t>(n); continue; }
        if (n < 0 && errno == EINTR) { continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!p.want_out) { p.want_out = true; arm_write(i); }
            return;
        }
        kill_peer(i, true);
        return;
    }
    p.out.clear(); p.out_off = 0;
    if (p.want_out) { p.want_out = false; arm_write(i); }
}

inline void Generator::on_accept() {
    for (;;) {
        const int fd = ::accept4(lfd_, nullptr, nullptr, SOCK_NONBLOCK);
        if (fd < 0) { return; }
        add_conn(fd, CType::Ret, -1, EPOLLIN);
    }
}

inline void Generator::on_write_conn(int fd, std::uint32_t ev) {
    const int i = conns_[static_cast<std::size_t>(fd)]->peer;
    Peer& p = peers_[static_cast<std::size_t>(i)];
    if (p.state == State::Connecting) {
        int soerr = 0; socklen_t l = sizeof soerr;
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &l);
        if (soerr != 0 || (ev & (EPOLLERR | EPOLLHUP))) {
            ++res_->connect_failures;
            kill_peer(i, false);
            return;
        }
        if (!(ev & EPOLLOUT)) { return; }
        p.state = State::Handshaking;
        wire::encode_into(p.out, wire::make_connect(p.token, 0, {wire::Endpoint{cfg_.listen_ip, lport_}}));
        flush(i);
        if (p.state != State::Dead) { maybe_ready(i); }
        return;
    }
    if (cfg_.duplex) {
        if (ev & EPOLLIN) { on_duplex_read(fd); if (p.state == State::Dead) { return; } }
        if (ev & (EPOLLERR | EPOLLHUP)) { kill_peer(i, true); return; }
        if (ev & EPOLLOUT) { flush(i); }
        return;
    }
    if (ev & EPOLLIN) {   // the node never sends on this connection; drain / detect EOF
        const ssize_t n = ::recv(fd, rbuf_.data(), rbuf_.size(), 0);
        if (n == 0 || (n < 0 && errno != EAGAIN && errno != EINTR)) { kill_peer(i, true); return; }
    }
    if (ev & (EPOLLERR | EPOLLHUP)) { kill_peer(i, true); return; }
    if (ev & EPOLLOUT) { flush(i); }
}

inline void Generator::on_message(int peer, const wire::ChatMessage& m, std::int64_t now) {
    Peer& p = peers_[static_cast<std::size_t>(peer)];
    unsigned pid = 0; unsigned long long seq = 0; long long ts = 0;
    if (std::sscanf(m.body.c_str(), "%u:%llu:%lld:", &pid, &seq, &ts) != 3) { return; }
    ++res_->total_received;
    if (p.inflight > 0) { --p.inflight; }
    if (p.kind == Kind::Slow) { return; }
    if (ts >= ms_ && ts < me_) {
        ++res_->received;
        res_->rtt.record(now - ts);
    }
}

inline void Generator::on_ret_conn(int fd) {
    for (;;) {
        Conn& c = *conns_[static_cast<std::size_t>(fd)];
        const ssize_t n = ::recv(fd, rbuf_.data(), rbuf_.size(), 0);
        if (n > 0) {
            const std::int64_t now = now_ns();
            c.dec.feed(std::span<const std::byte>(rbuf_.data(), static_cast<std::size_t>(n)));
            for (;;) {
                auto item = c.dec.next();
                if (!item) { ++res_->bad_frames; continue; }
                if (!*item) { break; }
                if (const auto* ack = wire::as_connect_ack(**item)) {
                    if (c.peer >= 0) { continue; }
                    auto it = by_token_.find(ack->ack_peer_id);
                    if (it == by_token_.end()) { continue; }
                    Peer& p = peers_[static_cast<std::size_t>(it->second)];
                    if (p.state == State::Dead) { close_conn(fd); return; }
                    if (p.rfd >= 0 && p.rfd != fd) { close_conn(p.rfd); }
                    c.peer = it->second;
                    p.rfd = fd;
                    if (p.kind == Kind::Slow) {   // accept, but never read again
                        ::epoll_ctl(ep_, EPOLL_CTL_DEL, fd, nullptr);
                        maybe_ready(it->second);
                        return;
                    }
                    maybe_ready(it->second);
                } else if (const auto* m = wire::as_message(**item)) {
                    if (c.peer >= 0) { on_message(c.peer, *m, now); }
                }
            }
            if (static_cast<std::size_t>(n) < rbuf_.size()) { return; }
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { return; }
        if (n < 0 && errno == EINTR) { continue; }
        const int peer = c.peer;   // EOF or error
        if (peer >= 0 && peers_[static_cast<std::size_t>(peer)].rfd == fd) { kill_peer(peer, true); }
        else { close_conn(fd); }
        return;
    }
}

// Duplex: the dialed socket carries ConnectAck and echoes back.
inline void Generator::on_duplex_read(int fd) {
    for (;;) {
        Conn& c = *conns_[static_cast<std::size_t>(fd)];
        const int i = c.peer;
        Peer& p = peers_[static_cast<std::size_t>(i)];
        const ssize_t n = ::recv(fd, rbuf_.data(), rbuf_.size(), 0);
        if (n > 0) {
            const std::int64_t now = now_ns();
            c.dec.feed(std::span<const std::byte>(rbuf_.data(), static_cast<std::size_t>(n)));
            for (;;) {
                auto item = c.dec.next();
                if (!item) { ++res_->bad_frames; continue; }
                if (!*item) { break; }
                if (const auto* ack = wire::as_connect_ack(**item)) {
                    if (ack->ack_peer_id == p.token && p.state == State::Handshaking) { p.state = State::Ready; }
                } else if (const auto* m = wire::as_message(**item)) {
                    if (p.state == State::Ready) { on_message(i, *m, now); }
                }
                if (p.kind == Kind::Slow && p.state == State::Ready && !p.no_read) {
                    p.no_read = true;   // accept the handshake, then never read again
                    arm_write(i);
                    return;
                }
            }
            if (static_cast<std::size_t>(n) < rbuf_.size()) { return; }
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { return; }
        if (n < 0 && errno == EINTR) { continue; }
        kill_peer(i, true);   // EOF or error
        return;
    }
}

inline void Generator::send_one(int i, std::int64_t now) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    if (p.out.size() - p.out_off > (1u << 20)) { ++res_->send_skipped; return; }
    char head[64];
    const int hl = std::snprintf(head, sizeof head, "%u:%llu:%lld:", static_cast<unsigned>(i),
                                 static_cast<unsigned long long>(p.seq++), static_cast<long long>(now));
    std::string body(head, static_cast<std::size_t>(hl));
    if (static_cast<int>(body.size()) < cfg_.size) { body.resize(static_cast<std::size_t>(cfg_.size), 'x'); }
    wire::encode_into(p.out, wire::make_chat_message(0, p.token, std::move(body)));
    ++p.inflight;
    ++res_->total_sent;
    if (p.kind == Kind::Slow) { ++res_->slow_sent; }
    else if (now >= ms_ && now < me_) { ++res_->sent; ++sent_meas_; }
}

inline void Generator::tick_send(std::int64_t now) {
    const std::int64_t interval = cfg_.rate > 0 ? static_cast<std::int64_t>(1e9 / cfg_.rate) : 0;
    const std::uint64_t win = static_cast<std::uint64_t>(cfg_.window);
    for (std::size_t k = 0; k < peers_.size(); ++k) {
        Peer& p = peers_[k];
        if (p.state != State::Ready || p.kind == Kind::Idle) { continue; }
        const int i = static_cast<int>(k);
        if (interval > 0) {
            if (now - p.next_send > 100'000'000) { p.next_send = now; }   // don't replay a long stall
            int budget = 256;
            while (p.next_send <= now && budget-- > 0) {
                if (win && p.inflight >= win) { break; }
                send_one(i, now);
                p.next_send += interval;
            }
            if (win && p.inflight >= win && p.next_send < now) { p.next_send = now; }
        } else {
            int budget = 64;
            while (budget-- > 0 && (!win || p.inflight < win) && p.out.size() - p.out_off < (64u << 10)) {
                send_one(i, now);
            }
        }
        if (!p.out.empty()) { flush(i); }
    }
}

inline void Generator::tick_churn(std::int64_t now) {
    if (cfg_.churn <= 0 || !sending_) { return; }
    const auto step = static_cast<std::int64_t>(1e9 / cfg_.churn);
    int budget = 16;
    while (churn_next_ <= now && budget-- > 0) {
        churn_next_ += step;
        std::vector<int> ready;
        for (std::size_t k = 0; k < peers_.size(); ++k) {
            if (peers_[k].state == State::Ready && peers_[k].kind != Kind::Idle) { ready.push_back(static_cast<int>(k)); }
        }
        if (ready.empty()) { continue; }
        const int i = ready[rng_() % ready.size()];
        kill_peer(i, false);
        ++res_->churn_events;
        dial(i);
    }
    if (churn_next_ < now - 1'000'000'000) { churn_next_ = now; }
}

inline int Generator::run(Result& res, std::string& err) {
    res_ = &res;
    // Reachability probe first so we can fail fast with a clear exit code.
    {
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port = htons(cfg_.target_port);
        bool ok = fd >= 0 && ::inet_pton(AF_INET, cfg_.target_ip.c_str(), &a.sin_addr) == 1;
        if (ok) {
            int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
            if (rc != 0 && errno == EINPROGRESS) {
                pollfd pfd{fd, POLLOUT, 0};
                int soerr = 1; socklen_t l = sizeof soerr;
                rc = (::poll(&pfd, 1, 3000) == 1 &&
                      ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &l) == 0 && soerr == 0) ? 0 : -1;
            }
            ok = rc == 0;
        }
        if (!ok) {
            err = "target " + cfg_.target_ip + ":" + std::to_string(cfg_.target_port) + " unreachable";
            if (fd >= 0) { ::close(fd); }
            return 2;
        }
        ::close(fd);
    }
    if (!setup(err)) { return 1; }

    const int total = cfg_.peers + cfg_.idle_peers + cfg_.slow_peers;
    peers_.resize(static_cast<std::size_t>(total));
    for (int i = 0; i < total; ++i) {
        Peer& p = peers_[static_cast<std::size_t>(i)];
        p.token = 1'000'000u + static_cast<std::uint64_t>(i);
        p.kind = i < cfg_.peers ? Kind::Active : (i < cfg_.peers + cfg_.idle_peers ? Kind::Idle : Kind::Slow);
        by_token_[p.token] = i;
    }
    res.peers_total = total;
    res.start_wall_ns = wall_ns();

    enum class Phase { Ramp, Run, Grace, Done } phase = Phase::Ramp;
    const std::int64_t ramp_end = now_ns() + static_cast<std::int64_t>(cfg_.ramp_timeout * 1e9);
    std::int64_t send_end = 0, grace_end = 0;
    std::vector<epoll_event> evs(512);

    while (phase != Phase::Done) {
        const int n = ::epoll_wait(ep_, evs.data(), static_cast<int>(evs.size()), 1);
        for (int k = 0; k < n; ++k) {
            const int fd = static_cast<int>(evs[static_cast<std::size_t>(k)].data.u64);
            const std::uint32_t ev = evs[static_cast<std::size_t>(k)].events;
            if (fd == lfd_) { on_accept(); continue; }
            if (static_cast<std::size_t>(fd) >= conns_.size() || !conns_[static_cast<std::size_t>(fd)]) { continue; }
            if (conns_[static_cast<std::size_t>(fd)]->type == CType::Write) { on_write_conn(fd, ev); }
            else { on_ret_conn(fd); }
        }
        const std::int64_t now = now_ns();
        // Stagger dials so we don't overflow the node's accept backlog.
        for (int d = 0; d < 128 && dial_cursor_ < total; ++d, ++dial_cursor_) { dial(dial_cursor_); }

        if (phase == Phase::Ramp) {
            int ready = 0, pending = 0;
            for (const Peer& p : peers_) {
                if (p.state == State::Ready) { ++ready; }
                else if (p.state != State::Dead) { ++pending; }
            }
            if (dial_cursor_ >= total && (pending == 0 || now >= ramp_end)) {
                if (ready == 0) {
                    err = res.connect_failures ? "target unreachable (all connects failed)"
                                               : cfg_.duplex ? "no peer completed the handshake (no ConnectInfoAck on the dialed socket)"
                                                         : "no peer completed the handshake (no ConnectInfoAck on return connection)";
                    return res.connect_failures ? 2 : 3;
                }
                ms_ = now + static_cast<std::int64_t>(cfg_.warmup * 1e9);
                me_ = ms_ + static_cast<std::int64_t>(cfg_.duration * 1e9);
                send_end = me_;
                churn_next_ = now;
                for (Peer& p : peers_) { p.next_send = now; }
                sending_ = true;
                phase = Phase::Run;
            }
        } else if (phase == Phase::Run) {
            tick_send(now);
            tick_churn(now);
            if (now >= send_end) {
                sending_ = false;
                grace_end = now + static_cast<std::int64_t>(cfg_.grace * 1e9);
                phase = Phase::Grace;
            }
        } else if (phase == Phase::Grace) {
            for (std::size_t k = 0; k < peers_.size(); ++k) {
                if (peers_[k].state != State::Dead && !peers_[k].out.empty()) { flush(static_cast<int>(k)); }
            }
            if (res.received >= sent_meas_ || now >= grace_end) { phase = Phase::Done; }
        }
    }
    for (const Peer& p : peers_) { if (p.state == State::Ready) { ++res.peers_ready; } }
    res.lost = res.sent - std::min(res.sent, res.received);
    res.measured_seconds = cfg_.duration;
    res.msgs_per_s = cfg_.duration > 0 ? static_cast<double>(res.received) / cfg_.duration : 0;
    res.end_wall_ns = wall_ns();
    return 0;
}

inline std::string to_json(const Config& c, const Result& r) {
    char b[2048];
    auto us = [](std::int64_t ns) { return static_cast<double>(ns) / 1000.0; };
    std::snprintf(b, sizeof b,
        "{\"sent\":%llu,\"received\":%llu,\"lost\":%llu,\"msgs_per_s\":%.1f,"
        "\"rtt_us\":{\"p50\":%.1f,\"p90\":%.1f,\"p99\":%.1f,\"p999\":%.1f,\"max\":%.1f,\"mean\":%.1f,\"samples\":%llu},"
        "\"total_sent\":%llu,\"total_received\":%llu,\"slow_sent\":%llu,\"send_skipped\":%llu,"
        "\"peers_total\":%d,\"peers_ready\":%d,\"connect_failures\":%llu,\"disconnects\":%llu,"
        "\"churn_events\":%llu,\"bad_frames\":%llu,"
        "\"config\":{\"target\":\"%s:%u\",\"peers\":%d,\"idle_peers\":%d,\"slow_peers\":%d,\"rate\":%g,"
        "\"window\":%d,\"size\":%d,\"duration\":%g,\"warmup\":%g,\"churn\":%g,\"grace\":%g},"
        "\"start_unix_ns\":%lld,\"end_unix_ns\":%lld}\n",
        (unsigned long long)r.sent, (unsigned long long)r.received, (unsigned long long)r.lost, r.msgs_per_s,
        us(r.rtt.quantile(0.5)), us(r.rtt.quantile(0.9)), us(r.rtt.quantile(0.99)), us(r.rtt.quantile(0.999)),
        us(r.rtt.max()), r.rtt.mean() / 1000.0, (unsigned long long)r.rtt.count(),
        (unsigned long long)r.total_sent, (unsigned long long)r.total_received, (unsigned long long)r.slow_sent,
        (unsigned long long)r.send_skipped, r.peers_total, r.peers_ready,
        (unsigned long long)r.connect_failures, (unsigned long long)r.disconnects,
        (unsigned long long)r.churn_events, (unsigned long long)r.bad_frames,
        c.target_ip.c_str(), c.target_port, c.peers, c.idle_peers, c.slow_peers, c.rate, c.window, c.size,
        c.duration, c.warmup, c.churn, c.grace, (long long)r.start_wall_ns, (long long)r.end_wall_ns);
    return b;
}

}  // namespace stress
