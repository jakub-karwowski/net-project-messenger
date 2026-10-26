#pragma once

// Command line of the app:
//   app [--io=threads|pool-sender|poll|epoll|poll-duplex|epoll-duplex|poll-split|epoll-split|peer-threads] [--headless] [--echo] [--max-peers=N]
//       [--backlog=N] [--address=A] [--port=P] [--log=PATH] [--metrics=PATH]
// The old positional form `app [address] [port] [log]` still works.

#include "IoMode.hpp"

#include <charconv>
#include <cstddef>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>

namespace p2p {

struct AppConfig {
    static constexpr std::size_t kDefaultMaxPeers = 8;

    IoMode io{IoMode::Threads};
    bool headless{false};
    bool echo{false};                      // initial store echo mode
    std::size_t max_peers{kDefaultMaxPeers};
    int backlog{16};                       // listen() backlog
    std::string address{"127.0.0.1"};
    unsigned port{9000};
    std::string log_path{"/dev/null"};
    std::string metrics_path;              // empty: metrics file disabled
};

namespace detail {
template <class T>
inline bool parse_number(std::string_view text, T& out) {
    T value{};
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size()) { return false; }
    out = value;
    return true;
}
} // namespace detail

// Parses argv[1..]. Returns an error message for an unknown flag or bad value.
inline std::expected<AppConfig, std::string> parse_app_config(int argc, char** argv) {
    AppConfig config;
    unsigned positional = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        auto bad = [&](std::string_view what) {
            return std::unexpected(std::string{what} + ": " + std::string{arg});
        };
        if (!arg.starts_with("--")) {
            switch (positional++) {
                case 0: config.address = std::string{arg}; break;
                case 1:
                    // Lenient, as before: an unparsable port keeps the default.
                    detail::parse_number(arg, config.port);
                    break;
                case 2: config.log_path = std::string{arg}; break;
                default: return bad("unexpected argument");
            }
            continue;
        }
        const auto eq = arg.find('=');
        const bool has_value = eq != std::string_view::npos;
        const std::string_view name = arg.substr(0, eq);
        const std::string_view value = has_value ? arg.substr(eq + 1) : std::string_view{};

        if (name == "--headless" && !has_value) { config.headless = true; }
        else if (name == "--echo" && !has_value) { config.echo = true; }
        else if (name == "--io" && has_value) {
            const auto mode = parse_io_mode(value);
            if (!mode) { return bad("unknown io mode"); }
            config.io = *mode;
        } else if (name == "--max-peers" && has_value) {
            std::size_t peers = 0;
            if (!detail::parse_number(value, peers) || peers == 0) { return bad("bad --max-peers"); }
            config.max_peers = peers;
        } else if (name == "--backlog" && has_value) {
            if (!detail::parse_number(value, config.backlog) || config.backlog <= 0) { return bad("bad --backlog"); }
        } else if (name == "--address" && has_value) { config.address = std::string{value}; }
        else if (name == "--port" && has_value) {
            if (!detail::parse_number(value, config.port)) { return bad("bad --port"); }
        } else if (name == "--log" && has_value) { config.log_path = std::string{value}; }
        else if (name == "--metrics" && has_value) { config.metrics_path = std::string{value}; }
        else { return bad("unknown option"); }
    }
    return config;
}

} // namespace p2p
