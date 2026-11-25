#include "QueueMonitor.hpp"

#include "Worker.hpp"

#include <algorithm>
#include <format>
#include <iterator>
#include <utility>

QueueMonitor::QueueMonitor(Logger log, std::chrono::milliseconds sample, int samples_per_line)
    : log_(std::move(log)), period_(sample), samples_per_line_(std::max(samples_per_line, 1)) {}

void QueueMonitor::watch(std::string name, const QueueDepth& queue) {
    queues_.push_back(Watched{std::move(name), &queue});
}

void QueueMonitor::run() {
    inbox_.every(period_, Tick{});
    worker::run_inbox(*this, inbox_, 1);
}

void QueueMonitor::stop() {
    inbox_.graceful_shutdown();
}

void QueueMonitor::on(Tick&) {
    if (const std::string line = sample(); !line.empty()) { log_.metric("{}", line); }
}

std::string QueueMonitor::sample() {
    for (Watched& w : queues_) {
        w.last = w.queue->approx_size();
        w.max = std::max(w.max, w.last);
    }
    if (++samples_ < samples_per_line_) { return {}; }
    samples_ = 0;
    std::string line = "queue_depth";
    for (Watched& w : queues_) {
        std::format_to(std::back_inserter(line), " {0}={1} {0}_max={2}", w.name, w.last, w.max);
        w.max = 0;
    }
    return line;
}
