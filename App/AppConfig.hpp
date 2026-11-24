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
    wire::Endpoint listen{"0.0.0.0", 7000};  // port 0: any free port
    std::vector<wire::Endpoint> advertise;   // empty: derived from listen (port 0 = the bound port)
    std::string log_path;                    // empty: no log
    LogLevel log_level = LogLevel::Info;
    std::size_t min_pts = 1;
    std::size_t max_pts = 4;
    std::chrono::milliseconds shutdown_grace{1000};
    std::chrono::milliseconds handshake_timeout{5000};
    std::chrono::milliseconds connect_timeout{5000};
    bool help = false;
};

// Parses argv (without the program name). The error text names the bad option.
std::expected<AppConfig, std::string> parse_args(std::span<const std::string_view> args);

std::string_view usage() noexcept;
