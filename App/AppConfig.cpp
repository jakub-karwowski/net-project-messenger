#include "AppConfig.hpp"

#include <charconv>
#include <format>
#include <optional>

namespace {

template <class T>
std::optional<T> number(std::string_view text) {
    T value{};
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size()) { return std::nullopt; }
    return value;
}

// "host:port", port 0-65535 (0: any free port, for --listen and --advertise).
std::optional<wire::Endpoint> endpoint(std::string_view text) {
    const auto colon = text.rfind(':');
    if (colon == std::string_view::npos || colon == 0) { return std::nullopt; }
    const auto port = number<unsigned>(text.substr(colon + 1));
    if (!port || *port > 65535) { return std::nullopt; }
    return wire::Endpoint{std::string(text.substr(0, colon)), static_cast<std::uint16_t>(*port)};
}

}  // namespace

std::expected<AppConfig, std::string> parse_args(std::span<const std::string_view> args) {
    AppConfig c;
    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string_view opt = args[i];
        // Options that take a value read it from the next argument.
        const auto value = [&]() -> std::expected<std::string_view, std::string> {
            if (i + 1 >= args.size()) { return std::unexpected(std::format("{} needs a value", opt)); }
            return args[++i];
        };
        const auto bad = [&](std::string_view v) { return std::unexpected(std::format("bad value for {}: {}", opt, v)); };

        if (opt == "--cli") { c.frontend = AppConfig::Frontend::Cli; continue; }
        if (opt == "--tui") { c.frontend = AppConfig::Frontend::Tui; continue; }
        if (opt == "--stay") { c.stay = true; continue; }
        if (opt == "--help" || opt == "-h") { c.help = true; continue; }

        constexpr std::string_view kWithValue[] = {"--listen", "--advertise", "--log", "--log-level", "--min-pts",
                                                   "--max-pts", "--shutdown-grace", "--handshake-timeout",
                                                   "--connect-timeout", "--balance-period"};
        if (!std::ranges::contains(kWithValue, opt)) { return std::unexpected(std::format("unknown option {}", opt)); }
        const auto v = value();
        if (!v) { return std::unexpected(v.error()); }

        if (opt == "--listen" || opt == "--advertise") {
            const auto ep = endpoint(*v);
            if (!ep) { return bad(*v); }
            if (opt == "--listen") { c.listen = *ep; } else { c.advertise.push_back(*ep); }
        } else if (opt == "--log") {
            c.log_path = std::string(*v);
        } else if (opt == "--log-level") {
            const auto level = parse_log_level(*v);
            if (!level) { return bad(*v); }
            c.log_level = *level;
        } else if (opt == "--min-pts" || opt == "--max-pts") {
            const auto n = number<std::size_t>(*v);
            if (!n || *n == 0 || *n > 256) { return bad(*v); }
            (opt == "--min-pts" ? c.min_pts : c.max_pts) = *n;
        } else if (opt == "--shutdown-grace" || opt == "--handshake-timeout" || opt == "--connect-timeout"
                   || opt == "--balance-period") {
            const auto ms = number<unsigned>(*v);
            if (!ms || (opt == "--balance-period" && *ms == 0)) { return bad(*v); }
            const std::chrono::milliseconds d{*ms};
            if (opt == "--shutdown-grace") { c.shutdown_grace = d; }
            else if (opt == "--handshake-timeout") { c.handshake_timeout = d; }
            else if (opt == "--connect-timeout") { c.connect_timeout = d; }
            else { c.balance_period = d; }
        } else {
            return std::unexpected(std::format("unknown option {}", opt));
        }
    }
    if (c.max_pts < c.min_pts) { return std::unexpected(std::string("--max-pts is below --min-pts")); }
    return c;
}

std::string_view usage() noexcept {
    return "usage: node [options]\n"
           "  --cli                    agent mode: commands on stdin, JSON events on stdout\n"
           "  --stay                   (--cli) keep running after end of input\n"
           "  --listen HOST:PORT       accept connections here (default 0.0.0.0:7000; port 0 = any)\n"
           "  --advertise HOST:PORT    endpoint peers should dial back (repeatable; default: the\n"
           "                           listen address, 127.0.0.1 for 0.0.0.0; port 0 = the bound port)\n"
           "  --log PATH               write a log file (default: no log)\n"
           "  --log-level LEVEL        debug, info, metric, warn, error, fatal, off (default info)\n"
           "  --min-pts N / --max-pts N  IO worker threads (default 1 / 4)\n"
           "  --shutdown-grace MS      time to flush links on quit (default 1000)\n"
           "  --handshake-timeout MS   (default 5000)\n"
           "  --connect-timeout MS     per dial attempt (default 5000)\n"
           "  --balance-period MS      how often the Balancer probes and decides (default 500)\n"
           "  --help\n";
}
