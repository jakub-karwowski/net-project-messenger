#pragma once

// Logs how deep the node's inboxes are, as Metric lines, so a run shows where a
// backlog builds up. Samples every `sample` period and writes one line every
// `samples_per_line` samples with each queue's current depth and the deepest
// it was seen since the previous line:
//
//   queue_depth store=12 store_max=3011 router=0 router_max=7 ...
//
// Runs on its own thread (run() / stop()); it only reads QueueDepth counters,
// so it never takes another queue's lock.

#include "Logger.hpp"
#include "ThreadQueue.hpp"

#include <chrono>
#include <cstddef>
#include <string>
#include <variant>
#include <vector>

class QueueMonitor {
public:
    explicit QueueMonitor(Logger log, std::chrono::milliseconds sample = std::chrono::milliseconds{100},
                          int samples_per_line = 5);

    // Adds a queue under `name`. Call before run(); the queue must outlive it.
    void watch(std::string name, const QueueDepth& queue);

    void run();    // the thread body: samples until stop()
    void stop();   // any thread; run() returns soon after

    // One sample, and the line it completes (empty when it completes none).
    // run() uses them; tests call them directly.
    std::string sample();

    struct Tick {};
    void on(Tick&);   // for worker::dispatch

private:
    struct Watched {
        std::string name;
        const QueueDepth* queue;
        std::size_t last = 0;   // the latest sample
        std::size_t max = 0;    // the deepest sample since the last line
    };

    Logger log_;
    std::chrono::milliseconds period_;
    int samples_per_line_;
    int samples_ = 0;
    std::vector<Watched> queues_;
    ThreadQueue<std::variant<Tick>> inbox_;
};
