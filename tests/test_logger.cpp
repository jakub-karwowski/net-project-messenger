// test_logger.cpp - Logger handles, the level filter (applied before
// formatting), escaping, the line format, and LogWriter: per-thread ordering,
// batching, and draining on a graceful shutdown.
//
// Plain assertions, no framework.

#include "Logger.hpp"

#include <atomic>
#include <cstdio>
#include <fcntl.h>
#include <format>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

// Counts how often it is formatted.
struct Expensive {};
std::atomic<int> g_formatted{0};

}  // namespace

template <>
struct std::formatter<Expensive> : std::formatter<std::string_view> {
    auto format(const Expensive&, std::format_context& ctx) const {
        g_formatted.fetch_add(1);
        return std::formatter<std::string_view>::format("expensive", ctx);
    }
};

namespace {

// A temporary file the writer logs to; read back as lines.
struct TempLog {
    std::string path;
    int fd = -1;
    TempLog() {
        char name[] = "/tmp/test_logger_XXXXXX";
        fd = ::mkstemp(name);
        path = name;
    }
    ~TempLog() { ::unlink(path.c_str()); }
    std::vector<std::string> lines() const {
        std::vector<std::string> out;
        FILE* f = std::fopen(path.c_str(), "r");
        if (f == nullptr) { return out; }
        char buf[4096];
        while (std::fgets(buf, sizeof buf, f) != nullptr) { out.emplace_back(buf); }
        std::fclose(f);
        return out;
    }
};

void test_level_filter() {
    LogCore core(LogLevel::Warn);
    Logger log(core, "t");
    g_formatted = 0;
    log.info("{}", Expensive{});
    log.debug("{}", Expensive{});
    check(g_formatted == 0, "arguments of a filtered-out level are never formatted");
    check(core.queue().size() == 0, "a filtered-out record is not queued");
    log.warn("{}", Expensive{});
    check(g_formatted == 1 && core.queue().size() == 1, "an enabled level is formatted and queued once");
    core.set_level(LogLevel::Debug);
    check(log.enabled(LogLevel::Debug), "set_level takes effect for existing handles");
    core.set_level(LogLevel::Off);
    check(!log.enabled(LogLevel::Fatal), "Off disables every level");

    Logger disabled;
    disabled.fatal("{}", Expensive{});   // must not crash
    check(!disabled.enabled(LogLevel::Fatal), "a default-constructed Logger is disabled");
}

void test_parse_level() {
    check(parse_log_level("debug") == LogLevel::Debug, "parse 'debug'");
    check(parse_log_level("WARN") == LogLevel::Warn, "parse 'WARN' (case-insensitive)");
    check(parse_log_level("Metric") == LogLevel::Metric, "parse 'Metric'");
    check(parse_log_level("off") == LogLevel::Off, "parse 'off'");
    check(!parse_log_level("verbose").has_value(), "reject an unknown level");
    check(!parse_log_level("").has_value(), "reject empty text");
}

void test_short_name() {
    ShortName n("a-very-long-thread-name");
    check(n.view() == "a-very-long-thr", "ShortName keeps at most 15 bytes");
    check(ShortName("pt-3").view() == "pt-3", "ShortName keeps a short name intact");
}

void test_escaping() {
    std::string out;
    append_log_escaped(out, "a\nb\tc\rd");
    check(out == "a\\nb\\tc\\rd", "newline, tab and CR are escaped, so a record stays one line");
    out.clear();
    append_log_escaped(out, "\x1b[2J\x07");
    check(out == "\\x1b[2J\\x07", "ESC and BEL are hex-escaped");
    out.clear();
    append_log_escaped(out, "x\xc2\x9by");
    check(out == "x\\xc2\\x9by", "a UTF-8 C1 control (CSI) is escaped");
    out.clear();
    append_log_escaped(out, "za\xc5\xbc\xc3\xb3\xc5\x82\xc4\x87 \xc2\xa9");
    check(out == "za\xc5\xbc\xc3\xb3\xc5\x82\xc4\x87 \xc2\xa9", "ordinary UTF-8 (including U+00A9) is kept");
}

void test_line_format() {
    LogRecord r;
    r.ts = std::chrono::steady_clock::time_point{} + std::chrono::microseconds{12345678};
    r.wall = std::chrono::system_clock::time_point{} + std::chrono::milliseconds{1'764'000'000'123};
    r.thread = ShortName("store");
    r.level = LogLevel::Warn;
    r.component = ShortName("router");
    r.text = "hello\nworld";
    const std::string line = format_log_line(r, std::chrono::steady_clock::time_point{});
    check(line == "2025-11-24T16:00:00.123Z     12.345678 WARN   [store] router: hello\\nworld\n",
          "line format: " + line);
}

void test_writer_orders_and_drains() {
    TempLog file;
    LogCore core(LogLevel::Debug);
    LogWriter writer(core, file.fd, /*owns_fd=*/true);
    check(core.raw_fd() == file.fd, "the writer publishes its fd for the hard-exit path");

    constexpr int kThreads = 4;
    constexpr int kPerThread = 2000;
    std::thread writer_thread([&] { writer.run(); });
    std::vector<std::thread> producers;
    for (int t = 0; t < kThreads; ++t) {
        producers.emplace_back([&core, t] {
            Logger log(core, std::format("p{}", t));
            for (int i = 0; i < kPerThread; ++i) { log.info("seq={}", i); }
        });
    }
    for (auto& p : producers) { p.join(); }
    core.queue().graceful_shutdown();   // the writer must still drain what is queued
    writer_thread.join();

    const auto lines = file.lines();
    check(lines.size() == static_cast<size_t>(kThreads * kPerThread),
          "every record is written exactly once (" + std::to_string(lines.size()) + " lines)");
    std::vector<int> next(kThreads, 0);
    bool ordered = true;
    bool terminated = true;
    for (const auto& line : lines) {
        terminated = terminated && !line.empty() && line.back() == '\n';
        const auto tpos = line.find("[p");
        const auto spos = line.find("seq=");
        if (tpos == std::string::npos || spos == std::string::npos) { ordered = false; continue; }
        const int t = line[tpos + 2] - '0';
        const int seq = std::stoi(line.substr(spos + 4));
        if (t < 0 || t >= kThreads || seq != next[t]) { ordered = false; continue; }
        ++next[t];
    }
    check(ordered, "records from one thread are written in the order they were logged");
    check(terminated, "every line ends with a newline");

    Logger late(core, "late");
    late.error("after shutdown");   // dropped, must not block or crash
    check(core.queue().size() == 0, "a record logged after shutdown is dropped");
}

}  // namespace

int main() {
    std::cout << "test_logger\n";
    test_level_filter();
    test_parse_level();
    test_short_name();
    test_escaping();
    test_line_format();
    test_writer_orders_and_drains();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
