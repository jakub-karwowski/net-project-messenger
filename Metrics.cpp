#include "Metrics.hpp"

#include <dirent.h>
#include <sys/resource.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <utility>
#include <string>

namespace p2p {
namespace {

long thread_count() {
    std::ifstream status{"/proc/self/status"};
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0) { return std::atol(line.c_str() + 8); }
    }
    return 0;
}

long fd_count() {
    DIR* dir = ::opendir("/proc/self/fd");
    if (!dir) { return 0; }
    long n = 0;
    while (const dirent* e = ::readdir(dir)) {
        if (e->d_name[0] != '.') { ++n; }
    }
    ::closedir(dir);
    return n > 0 ? n - 1 : 0; // minus the descriptor opendir itself holds
}

long long tv_us(const timeval& tv) {
    return static_cast<long long>(tv.tv_sec) * 1'000'000 + tv.tv_usec;
}

} // namespace

std::string Metrics::snapshot_json(const QueueMarks& marks) const {
    rusage ru{};
    ::getrusage(RUSAGE_SELF, &ru);
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    const long long t_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    constexpr auto r = std::memory_order_relaxed;
    using ull = unsigned long long;

    char buf[1280];
    const int n = std::snprintf(buf, sizeof buf,
        "{\"t_ns\":%lld,\"accepts\":%llu,\"connections_live\":%lld,\"bytes_in\":%llu,"
        "\"frames_in\":%llu,\"frames_out\":%llu,\"send_failures\":%llu,"
        "\"send_backpressure_drops\":%llu,\"reconnects\":%llu,\"evictions\":%llu,"
        "\"echoed\":%llu,\"render_q_hwm\":%zu,\"store_q_hwm\":%zu,\"sender_q_hwm\":%zu,"
        "\"utime_us\":%lld,\"stime_us\":%lld,\"nvcsw\":%ld,\"nivcsw\":%ld,"
        "\"maxrss_kb\":%ld,\"threads\":%ld,\"fds\":%ld,"
        "\"tx_queued_sends\":%llu,\"tx_flushes\":%llu}",
        t_ns, (ull)accepts.load(r), (long long)connections_live.load(r),
        (ull)bytes_in.load(r), (ull)frames_in.load(r), (ull)frames_out.load(r),
        (ull)send_failures.load(r), (ull)send_backpressure_drops.load(r),
        (ull)reconnects.load(r), (ull)evictions.load(r), (ull)echoed.load(r),
        marks.render, marks.store, marks.sender,
        tv_us(ru.ru_utime), tv_us(ru.ru_stime), ru.ru_nvcsw, ru.ru_nivcsw, ru.ru_maxrss,
        thread_count(), fd_count(),
        (ull)tx_queued_sends.load(r), (ull)tx_flushes.load(r));
    return std::string(buf, n > 0 ? static_cast<std::size_t>(n) : 0);
}

void MetricsWorker::operator()() {
    std::ofstream out{path_, std::ios::trunc};
    auto write = [&] {
        if (out) { out << snapshot_() << '\n'; out.flush(); }
    };
    write();
    std::unique_lock lock(mutex_);
    while (!cv_.wait_for(lock, interval_, [this] { return stop_; })) {
        write();
    }
    write(); // final line
}

void MetricsWorker::stop() noexcept {
    {
        std::scoped_lock lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
}

} // namespace p2p
