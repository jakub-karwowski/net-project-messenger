#include <chrono>
#include <cstdint>
#include <map>
#include <sys/types.h>

class BalancerWorker {
    using Clock = std::chrono::steady_clock;
    struct LoadMetrics {
        Clock::time_point last_read;
        size_t bytes_in;
        size_t bytes_out;
        uint64_t io_worker_id;
    };
    std::map<uint64_t, LoadMetrics> conversatoin_metrics_;
    void assignment_policy(std::vector<uint64_t>& buffer, size_t num_workers) noexcept; // conusme the queue and fill the internal buffer
public:
    std::vector<uint64_t> get_assignment_policy(size_t num_workers);
};