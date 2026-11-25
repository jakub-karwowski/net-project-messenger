#pragma once

// Admission control on a data path: producers that bring new work into the
// node (IO workers reading sockets) ask the gate whether the queues downstream
// can take more. Two watermarks give hysteresis: stop at `high`, start again
// only once every queue is back at `low`, so a queue hovering around one value
// does not switch the producers on and off on every message.
//
// The gate keeps no state of its own beyond the queues it watches: each
// producer remembers whether it has stopped. Reads are a few relaxed atomic
// loads (QueueDepth), cheap enough for every receive round.

#include "ThreadQueue.hpp"

#include <cstddef>
#include <stdexcept>
#include <vector>

class FlowGate {
public:
    // Requires low < high.
    FlowGate(std::size_t high, std::size_t low) : high_(high), low_(low) {
        if (low >= high) { throw std::invalid_argument("FlowGate: the low watermark must be below the high one"); }
    }

    // Adds a queue to watch. Call before any thread reads the gate; the queue
    // must outlive it.
    void watch(const QueueDepth& queue) { queues_.push_back(&queue); }

    // Some watched queue is at or above the high watermark.
    [[nodiscard]] bool over_high() const noexcept {
        for (const QueueDepth* q : queues_) {
            if (q->approx_size() >= high_) { return true; }
        }
        return false;
    }

    // Every watched queue is at or below the low watermark.
    [[nodiscard]] bool under_low() const noexcept {
        for (const QueueDepth* q : queues_) {
            if (q->approx_size() > low_) { return false; }
        }
        return true;
    }

    std::size_t high() const noexcept { return high_; }
    std::size_t low() const noexcept { return low_; }

private:
    std::vector<const QueueDepth*> queues_;
    std::size_t high_;
    std::size_t low_;
};
