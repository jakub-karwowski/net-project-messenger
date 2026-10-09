// test_logger.cpp - harness for LoggerWorker.
//
// Plain assertions (no framework): every failed check is printed and the process
// exits non-zero if anything failed. Covers:
//   * records reaching the sink, one line each, tagged by level, in order
//   * a default-constructed Logger being a harmless no-op
//   * the worker draining the queue and returning once it is shut down

#include "ThreadWorkers.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "FAIL: " << what << '\n';
    }
}

void test_records_reach_the_sink_in_order() {
    ThreadQueue<LogRecord> queue;
    std::ostringstream sink;
    LoggerWorker worker{queue, sink};

    std::thread thread{[&worker] { worker(); }};

    Logger logger{&queue};
    logger.info("first line");
    logger.warn("second line");
    logger.error("third line");

    queue.graceful_shutdown();
    thread.join(); // the sink is only safe to read once the worker has stopped

    const std::string text = sink.str();
    check(text.find("[INFO] first line") != std::string::npos, "logger: info line written");
    check(text.find("[WARN] second line") != std::string::npos, "logger: warn line written");
    check(text.find("[ERROR] third line") != std::string::npos, "logger: error line written");

    const std::size_t first = text.find("first line");
    const std::size_t second = text.find("second line");
    const std::size_t third = text.find("third line");
    check(first != std::string::npos && second != std::string::npos &&
              third != std::string::npos && first < second && second < third,
          "logger: records keep their order");
}

void test_default_logger_is_a_no_op() {
    Logger logger; // no queue attached
    logger.info("dropped");
    logger.warn("dropped");
    logger.error("dropped");
    check(true, "logger: default logger does not crash");
}

void test_worker_stops_when_queue_is_shut_down() {
    ThreadQueue<LogRecord> queue;
    std::ostringstream sink;
    LoggerWorker worker{queue, sink};

    std::atomic<bool> finished{false};
    std::thread thread{[&worker, &finished] {
        worker();
        finished.store(true, std::memory_order_release);
    }};

    queue.graceful_shutdown();
    for (int i = 0; i < 500 && !finished.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    check(finished.load(std::memory_order_acquire), "logger: worker returns after shutdown");
    thread.join();
}

} // namespace

int main() {
    test_records_reach_the_sink_in_order();
    test_default_logger_is_a_no_op();
    test_worker_stops_when_queue_is_shut_down();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
