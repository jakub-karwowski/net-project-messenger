#pragma once

// Command-line options of the node.

#include "Logger.hpp"
#include "Wire.hpp"

#include <chrono>
#include <cstddef>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct AppConfig {
    enum class Frontend { Tui, Cli };

    Frontend frontend = Frontend::Tui;
    bool stay = false;                       // CLI: keep running after end of input
    bool echo = false;                       // CLI: load-test target, echoes every message
    bool balancer = true;                    // false: fixed workers, own placement only
    wire::Endpoint listen{"0.0.0.0", 7000};  // port 0: any free port
    std::vector<wire::Endpoint> advertise;   // empty: derived from listen (port 0 = the bound port)
    std::string log_path;                    // empty: no log
    LogLevel log_level = LogLevel::Info;
    std::size_t min_pts = 1;
    std::size_t max_pts = 4;
    std::size_t max_links = 1024;
    std::size_t max_links_per_pt = 0;        // 0: max_links
    std::chrono::milliseconds shutdown_grace{1000};
    std::chrono::milliseconds handshake_timeout{5000};
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds balance_period{500};
    std::size_t balance_max_moves = 2;       // links the Balancer may move per tick
    std::size_t inbox_batch = 256;           // most messages a worker takes from its inbox at once
    // Backpressure: IO workers stop reading while the Store's or Router's inbox
    // holds inbox_high messages, until it is back at inbox_low (0: high / 2).
    // inbox_high 0 turns it off.
    std::size_t inbox_high = 16384;
    std::size_t inbox_low = 0;
    bool help = false;
};

// Parses argv (without the program name). The error text names the bad option.
std::expected<AppConfig, std::string> parse_args(std::span<const std::string_view> args);

std::string_view usage() noexcept;
