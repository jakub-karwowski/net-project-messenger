#include <cstdint>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

struct RoutingTable {
    struct RoutingEntry {
        uint64_t io_worker_id;
        enum struct Status { Normal, Moving, Closed } status;
    };
    struct IO_WorkerHandle {
        std::thread worker;
        std::vector<uint64_t> conversations;
    };
    class AssignmentPolicy {
    public:
        uint64_t get_next_worker_id();
        void update_policy(std::vector<uint64_t> policy);
    private:
        size_t index_{};
        std::vector<uint64_t> worker_ids_;
        std::mutex mutex_;
    };
    std::map<uint64_t, RoutingEntry> conversation_routing; //conversation_id key
    std::map<uint64_t, IO_WorkerHandle> io_workers; // io_worker_id key
};