#include "Logger.hpp"

#include <algorithm>
#include <cerrno>
#include <ctime>
#include <fcntl.h>
#include <tuple>
#include <unistd.h>

namespace {

constexpr std::array<std::string_view, 7> kLevelNames{
    "DEBUG", "INFO", "METRIC", "WARN", "ERROR", "FATAL", "OFF"};

constexpr char kHex[] = "0123456789abcdef";

void append_hex_escape(std::string& out, unsigned char byte) {
    out += "\\x";
    out += kHex[byte >> 4];
    out += kHex[byte & 0x0f];
}

}  // namespace

std::string_view to_string(LogLevel level) noexcept {
    const auto index = static_cast<std::size_t>(level);
    return index < kLevelNames.size() ? kLevelNames[index] : "?";
}

std::optional<LogLevel> parse_log_level(std::string_view text) noexcept {
    // ASCII only, unlike std::tolower: no locale, no undefined behaviour for
    // negative chars.
    const auto lower = [](char c) { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c; };
    const auto it = std::ranges::find_if(kLevelNames, [&](std::string_view name) {
        return std::ranges::equal(name, text, {}, lower, lower);
    });
    if (it == kLevelNames.end()) { return std::nullopt; }
    return static_cast<LogLevel>(it - kLevelNames.begin());
}

void Logger::emit(LogLevel level, std::string text) const noexcept {
    LogRecord record;
    record.ts = std::chrono::steady_clock::now();
    record.wall = std::chrono::system_clock::now();
    record.thread = thread_;
    record.level = level;
    record.component = component_;
    record.text = std::move(text);
    // false: the Logger is shutting down. The record is dropped on purpose.
    std::ignore = core_->queue().push(std::move(record));
}

void append_log_escaped(std::string& out, std::string_view text) {
    for (std::size_t i = 0; i < text.size(); ++i) {
        const auto byte = static_cast<unsigned char>(text[i]);
        if (byte == '\n') { out += "\\n"; continue; }
        if (byte == '\t') { out += "\\t"; continue; }
        if (byte == '\r') { out += "\\r"; continue; }
        if (byte < 0x20 || byte == 0x7f) { append_hex_escape(out, byte); continue; }
        // C1 controls U+0080..U+009F are encoded as C2 80..C2 9F. Some terminals
        // act on them (CSI is U+009B), so they are escaped too.
        if (byte == 0xc2 && i + 1 < text.size()) {
            const auto next = static_cast<unsigned char>(text[i + 1]);
            if (next >= 0x80 && next <= 0x9f) {
                append_hex_escape(out, byte);
                append_hex_escape(out, next);
                ++i;
                continue;
            }
        }
        out += static_cast<char>(byte);
    }
}

std::string format_log_line(const LogRecord& record, std::chrono::steady_clock::time_point epoch) {
    using namespace std::chrono;
    const auto wall_ms = duration_cast<milliseconds>(record.wall.time_since_epoch()).count();
    const std::time_t seconds = static_cast<std::time_t>(wall_ms / 1000);
    std::tm utc{};
    gmtime_r(&seconds, &utc);
    const double mono = duration<double>(record.ts - epoch).count();

    std::string line = std::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z {:13.6f} {:<6} [{}] {}: ",
                                   utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
                                   utc.tm_hour, utc.tm_min, utc.tm_sec, wall_ms % 1000,
                                   mono, to_string(record.level),
                                   record.thread.view(), record.component.view());
    append_log_escaped(line, record.text);
    line += '\n';
    return line;
}

LogWriter::LogWriter(LogCore& core, int fd, bool owns_fd) noexcept
    : core_(core), fd_(fd), owns_fd_(owns_fd) {
    core_.set_raw_fd(fd_);
}

LogWriter::~LogWriter() {
    core_.set_raw_fd(-1);
    if (owns_fd_ && fd_ >= 0) { ::close(fd_); }
}

void LogWriter::run() {
    auto& queue = core_.queue();
    std::string batch;
    while (auto record = queue.pop()) {
        batch.clear();
        batch += format_log_line(*record, core_.epoch());
        // Coalesce whatever else is already queued into one write.
        for (int i = 0; i < 256; ++i) {
            auto more = queue.try_pop();
            if (!more) { break; }
            batch += format_log_line(*more, core_.epoch());
        }
        write_all(batch);
    }
}

void LogWriter::write_all(const std::string& bytes) noexcept {
    if (fd_ < 0) { return; }
    std::size_t done = 0;
    while (done < bytes.size()) {
        const ssize_t n = ::write(fd_, bytes.data() + done, bytes.size() - done);
        if (n > 0) { done += static_cast<std::size_t>(n); continue; }
        if (n < 0 && errno == EINTR) { continue; }
        return;   // the sink is gone; logging must not stop the process
    }
}

int open_log_file(const std::string& path) noexcept {
    return ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
}
