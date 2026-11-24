#include "LoadGen.hpp"

#include "Json.hpp"
#include "TcpSocket.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <format>
#include <optional>
#include <queue>
#include <random>
#include <span>
#include <sys/resource.h>
#include <tuple>
#include <unordered_map>

namespace loadgen {

// ------------------------------------------------------------------ histogram

void Histogram::record(std::int64_t ns) {
    ns = std::max<std::int64_t>(ns, 0);
    ++buckets_[index(static_cast<std::uint64_t>(ns))];
    ++count_;
    max_ = std::max(max_, ns);
    sum_ += static_cast<double>(ns);
}

std::int64_t Histogram::quantile(double q) const noexcept {
    if (count_ == 0) { return 0; }
    const auto target = std::max<std::uint64_t>(
        static_cast<std::uint64_t>(std::ceil(q * static_cast<double>(count_))), 1);
    std::uint64_t seen = 0;
    for (std::size_t i = 0; i < buckets_.size(); ++i) {
        seen += buckets_[i];
        if (seen >= target) { return std::min<std::int64_t>(upper(i), max_); }
    }
    return max_;
}

std::size_t Histogram::index(std::uint64_t v) noexcept {
    const int msb = 63 - std::countl_zero(v | 1);
    const int shift = msb > 5 ? msb - 5 : 0;
    return std::min<std::size_t>(static_cast<std::size_t>(shift) * 32 + static_cast<std::size_t>(v >> shift), 64 * 32 - 1);
}

std::int64_t Histogram::upper(std::size_t idx) noexcept {
    const std::size_t shift = idx < 64 ? 0 : idx / 32 - 1;
    const std::uint64_t base = static_cast<std::uint64_t>(idx - shift * 32) << shift;
    return static_cast<std::int64_t>(base + ((1ull << shift) - 1));
}

// ------------------------------------------------------------------ the run

namespace {

using Clock = std::chrono::steady_clock;
using Pool = EpollConnectionPool;

std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

std::int64_t seconds_ns(double s) {
    return static_cast<std::int64_t>(s * 1e9);
}

Clock::time_point at(std::int64_t ns) {
    return Clock::time_point{std::chrono::nanoseconds{ns}};
}

void raise_open_file_limit() {
    rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur < limit.rlim_max) {
        limit.rlim_cur = limit.rlim_max;
        std::ignore = ::setrlimit(RLIMIT_NOFILE, &limit);
    }
}

struct Peer {
    enum class Kind : std::uint8_t { Active, Idle, Slow };
    enum class State : std::uint8_t { Down, Dialing, AwaitAck, Ready, Released };

    Kind kind = Kind::Active;
    State state = State::Down;
    std::uint64_t our_name = 0;     // what we call the node: hello.peer_id
    std::uint64_t node_name = 0;    // what the node calls us: ack.peer_id, the chat sender_id
    std::uint64_t conn = 0;         // pool connection id
    wire::Decoder dec;
    std::int64_t dial_ns = 0;
    bool reconnecting = false;
    std::uint64_t next_seq = 1;
    std::uint64_t last_echo_seq = 0;
    int inflight = 0;            // every message not answered yet (for the window)
    int measured_inflight = 0;   // those of them sent in the measurement window
    // A slow peer after the handshake: out of the pool, never read.
    std::optional<TcpConnection> raw;
    std::vector<std::byte> raw_tx;  // what send_no_block has not taken yet (frames stay whole)
};

class Run {
public:
    explicit Run(const Config& c) : c_(c), pool_(Pool::Timeouts{std::chrono::milliseconds{0}, std::chrono::seconds{5}}, 4 << 20) {
        rng_.seed(std::random_device{}());
        const int total = c_.peers + c_.idle_peers + c_.slow_peers;
        peers_.resize(static_cast<std::size_t>(total));
        for (int i = 0; i < total; ++i) {
            Peer& p = peers_[static_cast<std::size_t>(i)];
            p.kind = i < c_.peers ? Peer::Kind::Active : (i < c_.peers + c_.idle_peers ? Peer::Kind::Idle : Peer::Kind::Slow);
            do { p.our_name = rng_(); } while (p.our_name == 0);
        }
        period_ns_ = c_.rate > 0 ? static_cast<std::int64_t>(1e9 / c_.rate) : 0;
        body_size_ = static_cast<std::size_t>(std::max(c_.size, 16));
    }

    Result go() {
        ramp();
        traffic();
        pool_.stop();
        r_.measured_seconds = c_.duration;
        r_.measure_begin_ns = measure_begin_;
        r_.measure_end_ns = measure_end_;
        return std::move(r_);
    }

private:
    // ---------------------------------------------------------------- phases

    void ramp() {
        const auto until = now_ns() + seconds_ns(c_.ramp_timeout);
        std::size_t next = 0;
        while (now_ns() < until) {
            while (next < peers_.size() && dialing_ < c_.dial_batch) { dial(next++); }
            if (next == peers_.size() && dialing_ == 0) { break; }
            poll_once(std::min(until, now_ns() + 50'000'000));
        }
        for (const Peer& p : peers_) {
            const bool up = p.state == Peer::State::Ready || p.state == Peer::State::Released;
            if (!up) { continue; }
            if (p.kind == Peer::Kind::Active) { ++r_.active_ready; }
            if (p.kind == Peer::Kind::Idle) { ++r_.idle_ready; }
            if (p.kind == Peer::Kind::Slow) { ++r_.slow_ready; }
        }
    }

    void traffic() {
        const std::int64_t t0 = now_ns();
        measure_begin_ = t0 + seconds_ns(c_.warmup);
        measure_end_ = measure_begin_ + seconds_ns(c_.duration);
        const std::int64_t grace_end = measure_end_ + seconds_ns(c_.grace);
        r_.seconds.resize(static_cast<std::size_t>(std::ceil(c_.duration)));
        sending_ = true;

        // Spread the first sends over one period, so peers do not fire in step.
        std::uniform_int_distribution<std::int64_t> jitter(0, std::max<std::int64_t>(period_ns_, 1) - 1);
        for (std::size_t i = 0; i < peers_.size(); ++i) {
            const Peer& p = peers_[i];
            if (p.kind == Peer::Kind::Idle) { continue; }
            if (period_ns_ > 0) {
                due_.push({t0 + jitter(rng_), i});
            } else if (p.kind == Peer::Kind::Active) {
                fill_window(i);
            }
        }
        if (c_.churn > 0) { next_churn_ = t0 + static_cast<std::int64_t>(1e9 / c_.churn); }

        for (;;) {
            const std::int64_t now = now_ns();
            if (now >= measure_end_) { sending_ = false; }
            if (now >= grace_end) { break; }
            if (!sending_ && r_.echoed + r_.send_failed >= r_.sent && measured_inflight() == 0) { break; }

            while (sending_ && !due_.empty() && due_.top().first <= now) {
                const auto [due, i] = due_.top();
                due_.pop();
                on_due(i, due, now);
            }
            if (sending_ && c_.churn > 0 && now >= next_churn_) {
                churn_one();
                next_churn_ += static_cast<std::int64_t>(1e9 / c_.churn);
            }
            flush_slow();

            std::int64_t wake = std::min(now + 20'000'000, grace_end);
            if (sending_ && !due_.empty()) { wake = std::min(wake, due_.top().first); }
            if (sending_ && c_.churn > 0) { wake = std::min(wake, next_churn_); }
            if (sending_) { wake = std::min(wake, measure_end_); }
            poll_once(wake);
        }
    }

    // Messages of the measurement window still unanswered.
    std::uint64_t measured_inflight() const {
        std::uint64_t n = 0;
        for (const Peer& p : peers_) { n += static_cast<std::uint64_t>(p.measured_inflight); }
        return n;
    }

    // ---------------------------------------------------------------- connections

    void dial(std::size_t i) {
        Peer& p = peers_[i];
        TcpConnection::Options o;
        o.non_blocking = true;
        o.tcp_nodelay = true;
        if (p.kind == Peer::Kind::Slow) { o.receive_buffer = c_.slow_receive_buffer; }
        const auto id = pool_.connection_dial(c_.target.address, c_.target.port, o,
                                              Pool::Timeouts{std::chrono::milliseconds{0}, std::chrono::seconds{5}},
                                              std::chrono::seconds{5});
        if (!id) {
            ++r_.dial_failures;
            return;
        }
        p.conn = *id;
        p.state = Peer::State::Dialing;
        p.dial_ns = now_ns();
        p.dec = wire::Decoder{};
        by_conn_[*id] = i;
        ++dialing_;
    }

    void poll_once(std::int64_t until_ns) {
        auto r = pool_.receive(rx_, at(until_ns));
        if (r) {
            const auto it = by_conn_.find(r->connection_id);
            if (it == by_conn_.end()) { return; }
            if (r->kind == Pool::ReceiveResult::Kind::Connected) {
                on_connected(it->second);
            } else {
                on_bytes(it->second, std::span<const std::byte>(rx_).first(r->bytes_read));
            }
            return;
        }
        const auto [id, err] = r.error();
        if (id == 0) { return; }   // the deadline (WouldBlock), or an interrupt
        const auto it = by_conn_.find(id);
        if (it == by_conn_.end()) { return; }
        on_down(it->second, err);
    }

    void on_connected(std::size_t i) {
        Peer& p = peers_[i];
        if (p.state != Peer::State::Dialing) { return; }
        auto hello = wire::encode(wire::ConnectInfo{p.our_name, p.node_name, {}});
        if (!hello || !pool_.send(p.conn, *hello)) {
            on_down(i, SocketError::Unknown);
            return;
        }
        p.state = Peer::State::AwaitAck;
    }

    void on_down(std::size_t i, SocketError) {
        Peer& p = peers_[i];
        if (p.state == Peer::State::Dialing || p.state == Peer::State::AwaitAck) {
            --dialing_;
            if (!p.reconnecting) { ++r_.dial_failures; }
        } else if (p.state == Peer::State::Ready) {
            ++r_.disconnects;
        }
        by_conn_.erase(p.conn);
        abandon(p);
        p.state = Peer::State::Down;
    }

    // The connection is gone with whatever was in flight on it. Only messages
    // of the measurement window count; earlier ones were never counted as sent.
    void abandon(Peer& p) {
        r_.abandoned += static_cast<std::uint64_t>(p.measured_inflight);
        p.inflight = 0;
        p.measured_inflight = 0;
    }

    void on_bytes(std::size_t i, std::span<const std::byte> data) {
        Peer& p = peers_[i];
        p.dec.feed(data);
        for (;;) {
            wire::Decoded next = p.dec.next();
            if (std::holds_alternative<wire::NeedMore>(next)) { return; }
            if (std::holds_alternative<wire::WireError>(next)) {
                ++r_.protocol_errors;
                continue;
            }
            auto& m = std::get<wire::Message>(next);
            if (p.state == Peer::State::AwaitAck) {
                if (const auto* ack = std::get_if<wire::ConnectInfoAck>(&m)) {
                    on_ack(i, *ack);
                    if (p.state == Peer::State::Released) { return; }   // out of the pool now
                    continue;
                }
            }
            if (const auto* chat = std::get_if<wire::ChatMessage>(&m); chat != nullptr && p.state == Peer::State::Ready) {
                on_echo(i, chat->body);
                continue;
            }
            ++r_.protocol_errors;
        }
    }

    void on_ack(std::size_t i, const wire::ConnectInfoAck& ack) {
        Peer& p = peers_[i];
        p.node_name = ack.peer_id;
        --dialing_;
        const std::int64_t took = now_ns() - p.dial_ns;
        if (p.reconnecting) {
            r_.reconnect.record(took);
            p.reconnecting = false;
        } else {
            r_.handshake.record(took);
        }
        p.state = Peer::State::Ready;
        if (p.kind == Peer::Kind::Slow) {
            // From now on nobody reads it: take it out of the pool.
            auto released = pool_.connection_release(p.conn);
            by_conn_.erase(p.conn);
            if (!released) {
                p.state = Peer::State::Down;
                return;
            }
            p.raw.emplace(std::move(released->connection));
            p.raw_tx = std::move(released->unsent_tx);
            p.state = Peer::State::Released;
            return;
        }
        if (sending_ && period_ns_ == 0 && p.kind == Peer::Kind::Active) { fill_window(i); }
    }

    void churn_one() {
        std::vector<std::size_t> candidates;
        for (std::size_t i = 0; i < peers_.size(); ++i) {
            if (peers_[i].kind == Peer::Kind::Active && peers_[i].state == Peer::State::Ready) { candidates.push_back(i); }
        }
        if (candidates.empty()) { return; }
        const std::size_t i = candidates[std::uniform_int_distribution<std::size_t>(0, candidates.size() - 1)(rng_)];
        Peer& p = peers_[i];
        pool_.connection_drop(p.conn);
        by_conn_.erase(p.conn);
        abandon(p);
        p.state = Peer::State::Down;
        p.reconnecting = true;
        ++r_.reconnects;
        dial(i);   // re-presents node_name, so the node recognises the peer
    }

    // ---------------------------------------------------------------- traffic

    void on_due(std::size_t i, std::int64_t due, std::int64_t now) {
        Peer& p = peers_[i];
        std::int64_t next = due + period_ns_;
        if (next < now - 1'000'000'000) {   // more than a second behind: skip ahead
            if (in_window(now)) { ++r_.behind; }
            next = now + period_ns_;
        }
        due_.push({next, i});
        if (p.state == Peer::State::Released) {
            queue_slow(p);
            return;
        }
        if (p.state != Peer::State::Ready) { return; }
        if (c_.window > 0 && p.inflight >= c_.window) {
            if (in_window(now)) { ++r_.window_blocked; }
            return;
        }
        send_one(i);
    }

    void fill_window(std::size_t i) {
        while (peers_[i].state == Peer::State::Ready && peers_[i].inflight < std::max(c_.window, 1) && sending_) {
            if (!send_one(i)) { return; }
        }
    }

    std::vector<std::byte> frame_for(Peer& p, std::int64_t sent_ns) {
        std::string body(body_size_, 'x');
        const std::uint64_t seq = p.next_seq++;
        std::memcpy(body.data(), &sent_ns, sizeof sent_ns);
        std::memcpy(body.data() + 8, &seq, sizeof seq);
        auto frame = wire::encode(wire::ChatMessage{0, p.node_name, std::move(body)});
        return frame ? std::move(*frame) : std::vector<std::byte>{};
    }

    bool send_one(std::size_t i) {
        Peer& p = peers_[i];
        const std::int64_t sent_ns = now_ns();
        const bool counted = in_window(sent_ns);
        const auto frame = frame_for(p, sent_ns);
        const auto ok = pool_.send(p.conn, frame);
        if (!ok) {
            if (counted) {
                ++r_.sent;
                ++r_.send_failed;
            }
            return false;
        }
        ++p.inflight;
        if (counted) {
            ++p.measured_inflight;
            ++r_.sent;
            ++second_of(sent_ns).sent;
        }
        return true;
    }

    void on_echo(std::size_t i, const std::string& body) {
        Peer& p = peers_[i];
        if (body.size() < 16) {
            ++r_.protocol_errors;
            return;
        }
        std::int64_t sent_ns = 0;
        std::uint64_t seq = 0;
        std::memcpy(&sent_ns, body.data(), sizeof sent_ns);
        std::memcpy(&seq, body.data() + 8, sizeof seq);
        if (p.inflight > 0) { --p.inflight; }
        if (seq <= p.last_echo_seq) { return; }   // a repeat: count each message once
        p.last_echo_seq = seq;
        const std::int64_t now = now_ns();
        if (in_window(sent_ns)) {
            if (p.measured_inflight > 0) { --p.measured_inflight; }
            ++r_.echoed;
            r_.rtt.record(now - sent_ns);
            auto& s = second_of(std::min(now, measure_end_ - 1));
            ++s.echoed;
            s.rtt.record(now - sent_ns);
        }
        if (period_ns_ == 0 && sending_) { fill_window(i); }
    }

    // Slow peers queue frames themselves and push what the socket takes.
    void queue_slow(Peer& p) {
        if (!p.raw || p.raw_tx.size() > (1u << 20)) { return; }   // it is stuck: stop piling up
        const auto frame = frame_for(p, now_ns());
        p.raw_tx.insert(p.raw_tx.end(), frame.begin(), frame.end());
    }

    void flush_slow() {
        for (Peer& p : peers_) {
            if (!p.raw || p.raw_tx.empty()) { continue; }
            const auto n = p.raw->send_no_block(p.raw_tx);
            if (n && *n > 0) { p.raw_tx.erase(p.raw_tx.begin(), p.raw_tx.begin() + static_cast<std::ptrdiff_t>(*n)); }
        }
    }

    bool in_window(std::int64_t ns) const { return ns >= measure_begin_ && ns < measure_end_; }

    SecondSample& second_of(std::int64_t ns) {
        const auto idx = static_cast<std::size_t>(std::max<std::int64_t>(ns - measure_begin_, 0) / 1'000'000'000);
        return r_.seconds[std::min(idx, r_.seconds.size() - 1)];
    }

    const Config& c_;
    Pool pool_;
    std::vector<Peer> peers_;
    std::unordered_map<std::uint64_t, std::size_t> by_conn_;
    std::priority_queue<std::pair<std::int64_t, std::size_t>, std::vector<std::pair<std::int64_t, std::size_t>>,
                        std::greater<>> due_;
    std::array<std::byte, 64 * 1024> rx_{};
    std::mt19937_64 rng_;
    Result r_;
    std::int64_t period_ns_ = 0;
    std::size_t body_size_ = 16;
    int dialing_ = 0;
    bool sending_ = false;
    std::int64_t measure_begin_ = 0;
    std::int64_t measure_end_ = 0;
    std::int64_t next_churn_ = 0;
};

double ms(std::int64_t ns) {
    return static_cast<double>(ns) / 1e6;
}

std::string histogram_json(const Histogram& h) {
    return std::format(R"({{"count":{},"mean_ms":{:.3f},"p50_ms":{:.3f},"p90_ms":{:.3f},"p99_ms":{:.3f},"p999_ms":{:.3f},"max_ms":{:.3f}}})",
                       h.count(), h.mean() / 1e6, ms(h.quantile(0.5)), ms(h.quantile(0.9)), ms(h.quantile(0.99)),
                       ms(h.quantile(0.999)), ms(h.max()));
}

}  // namespace

Result run(const Config& config) {
    raise_open_file_limit();
    Run r(config);
    return r.go();
}

std::string to_json(const Config& c, const Result& r) {
    json::Array seconds;
    for (std::size_t i = 0; i < r.seconds.size(); ++i) {
        const auto& s = r.seconds[i];
        seconds.raw(std::format(R"({{"t":{},"sent":{},"echoed":{},"rtt_p50_ms":{:.3f},"rtt_p99_ms":{:.3f}}})", i, s.sent,
                                s.echoed, ms(s.rtt.quantile(0.5)), ms(s.rtt.quantile(0.99))));
    }
    const std::uint64_t lost = r.sent - std::min(r.sent, r.echoed + r.send_failed + r.abandoned);
    const std::string config_json = std::format(
        R"({{"target":"{}:{}","peers":{},"rate":{},"window":{},"size":{},"duration":{},"warmup":{},"grace":{},"idle_peers":{},"slow_peers":{},"churn":{}}})",
        c.target.address, c.target.port, c.peers, c.rate, c.window, c.size, c.duration, c.warmup, c.grace, c.idle_peers,
        c.slow_peers, c.churn);
    return json::Object{}
        .raw("config", config_json)
        .num("active_ready", static_cast<std::uint64_t>(r.active_ready))
        .num("idle_ready", static_cast<std::uint64_t>(r.idle_ready))
        .num("slow_ready", static_cast<std::uint64_t>(r.slow_ready))
        .num("dial_failures", static_cast<std::uint64_t>(r.dial_failures))
        .raw("handshake", histogram_json(r.handshake))
        .num("reconnects", r.reconnects)
        .raw("reconnect", histogram_json(r.reconnect))
        .num("sent", r.sent)
        .num("echoed", r.echoed)
        .num("lost", lost)
        .num("send_failed", r.send_failed)
        .num("abandoned", r.abandoned)
        .num("behind", r.behind)
        .num("window_blocked", r.window_blocked)
        .num("disconnects", r.disconnects)
        .num("protocol_errors", r.protocol_errors)
        .num("measure_begin_ns", static_cast<std::uint64_t>(r.measure_begin_ns))
        .num("measure_end_ns", static_cast<std::uint64_t>(r.measure_end_ns))
        .raw("echoed_per_s", std::format("{:.1f}", static_cast<double>(r.echoed) / std::max(r.measured_seconds, 1e-9)))
        .raw("rtt", histogram_json(r.rtt))
        .raw("seconds", seconds.done())
        .done();
}

}  // namespace loadgen
