// loadgen: drives a `node --cli --echo` with simulated peers and prints one
// JSON object with the results. See LoadGen.hpp.

#include "LoadGen.hpp"

#include <charconv>
#include <cstdio>
#include <format>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr std::string_view kUsage =
    "usage: loadgen --target HOST:PORT [options]\n"
    "  --peers N          active peers (default 16)\n"
    "  --rate R           messages per second per active peer; 0: closed loop (default 100)\n"
    "  --window W         most messages in flight per peer; 0: no limit (default 0; needed with --rate 0)\n"
    "  --size B           body bytes, at least 16 (default 64)\n"
    "  --duration S       measurement seconds (default 5)\n"
    "  --warmup S         seconds of traffic before measuring (default 1)\n"
    "  --grace S          seconds to wait for late echoes (default 2)\n"
    "  --idle N           peers that only complete the handshake (default 0)\n"
    "  --slow N           peers that send but never read (default 0)\n"
    "  --churn C          reconnects per second (default 0)\n"
    "  --ramp-timeout S   most seconds for all handshakes (default 10)\n";

template <class T>
std::optional<T> number(std::string_view text) {
    T value{};
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size()) { return std::nullopt; }
    return value;
}

int fail(const std::string& message) {
    std::fprintf(stderr, "loadgen: %s\n%s", message.c_str(), std::string(kUsage).c_str());
    return 1;
}

}  // namespace

int main(int argc, char** argv) {
    loadgen::Config c;
    bool have_target = false;
    const std::vector<std::string_view> args(argv + 1, argv + argc);
    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string_view opt = args[i];
        if (opt == "--help" || opt == "-h") {
            std::fputs(std::string(kUsage).c_str(), stdout);
            return 0;
        }
        if (i + 1 >= args.size()) { return fail(std::format("{} needs a value", opt)); }
        const std::string_view v = args[++i];
        const auto bad = [&] { return fail(std::format("bad value for {}: {}", opt, v)); };
        if (opt == "--target") {
            const auto colon = v.rfind(':');
            const auto port = colon == std::string_view::npos ? std::nullopt : number<unsigned>(v.substr(colon + 1));
            if (!port || *port == 0 || *port > 65535) { return bad(); }
            c.target = wire::Endpoint{std::string(v.substr(0, colon)), static_cast<std::uint16_t>(*port)};
            have_target = true;
        } else if (opt == "--peers" || opt == "--window" || opt == "--size" || opt == "--idle" || opt == "--slow") {
            const auto n = number<int>(v);
            if (!n || *n < 0) { return bad(); }
            if (opt == "--peers") { c.peers = *n; }
            else if (opt == "--window") { c.window = *n; }
            else if (opt == "--size") { c.size = *n; }
            else if (opt == "--idle") { c.idle_peers = *n; }
            else { c.slow_peers = *n; }
        } else if (opt == "--rate" || opt == "--duration" || opt == "--warmup" || opt == "--grace" || opt == "--churn"
                   || opt == "--ramp-timeout") {
            const auto d = number<double>(v);
            if (!d || *d < 0) { return bad(); }
            if (opt == "--rate") { c.rate = *d; }
            else if (opt == "--duration") { c.duration = *d; }
            else if (opt == "--warmup") { c.warmup = *d; }
            else if (opt == "--grace") { c.grace = *d; }
            else if (opt == "--churn") { c.churn = *d; }
            else { c.ramp_timeout = *d; }
        } else {
            return fail(std::format("unknown option {}", opt));
        }
    }
    if (!have_target) { return fail("--target is required"); }
    if (c.rate == 0 && c.window == 0) { return fail("--rate 0 (closed loop) needs --window"); }
    if (c.duration <= 0) { return fail("--duration must be positive"); }

    const auto result = loadgen::run(c);
    std::fputs((loadgen::to_json(c, result) + "\n").c_str(), stdout);
    return result.active_ready == c.peers ? 0 : 2;
}
