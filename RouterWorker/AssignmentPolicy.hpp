#pragma once

// Which IO worker the next new link goes to. The Balancer sends a list of
// worker ids (weighted by spare capacity); the Router takes them in turn. The
// Router owns the only copy, so no lock: the Balancer replaces it by message.

#include "Ids.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

class AssignmentPolicy {
public:
    // A newer list replaces the current one; an older epoch is ignored.
    void set(std::vector<PtId> next_assignments, std::uint64_t epoch) {
        if (epoch < epoch_) { return; }
        list_ = std::move(next_assignments);
        epoch_ = epoch;
        pos_ = 0;
    }

    // The next listed worker that `usable` accepts, skipping the rest (retiring,
    // gone, full). Empty when the list is exhausted or names no usable worker:
    // the caller then falls back to its own choice.
    std::optional<PtId> next(const std::function<bool(PtId)>& usable) {
        while (pos_ < list_.size()) {
            const PtId pt = list_[pos_++];
            if (usable(pt)) { return pt; }
        }
        return std::nullopt;
    }

    std::uint64_t epoch() const noexcept { return epoch_; }

private:
    std::vector<PtId> list_;
    std::size_t pos_ = 0;
    std::uint64_t epoch_ = 0;
};
