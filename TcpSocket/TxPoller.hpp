#pragma once

#include "TcpSocket.hpp"

#include <atomic>
#include <cstddef>
#include <expected>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>

// Write-only multiplexer for TCP connections that are shared (via
// std::shared_ptr<TcpConnection>) with a read-side pool.
//
// Division of labour per socket: the rx pool only reads, the TxPoller only writes.
// A fatal write error calls shutdown(Both) on the socket so the reader side sees EOF
// and reports the disconnect itself. The poller never reads from the socket.
//
// Rule: a descriptor is always removed from the poll/epoll set BEFORE the poller
// releases its shared_ptr, so a poll set never holds a closed or reused fd number.
//
// Thread safety: everything except run() may be called from any thread; run() is
// driven by exactly one thread.

// poll(2) backend: reuses PollSet. Interest is registered only while bytes are pending.
class PollBackend {
public:
    void arm(int id, int fd);     // start watching for writability
    void disarm(int id, int fd) noexcept;
    void wake() const noexcept;
    // Blocks until something is writable/failed or wake(); ready ids are appended.
    void wait(std::vector<int>& ready);
    // Called under the poller's lock right before wait().
    void prepare() { set_.prepare(); }
private:
    PollSet set_;
};

// epoll(7) backend with an eventfd for wake-ups.
class EpollBackend {
public:
    EpollBackend();
    ~EpollBackend();
    EpollBackend(const EpollBackend&) = delete;
    EpollBackend& operator=(const EpollBackend&) = delete;

    void arm(int id, int fd);
    void disarm(int id, int fd) noexcept;
    void wake() const noexcept;
    void wait(std::vector<int>& ready);
    void prepare() {}
private:
    int epoll_fd_{-1};
    int wake_fd_{-1};
};

template <class Backend>
class TxPoller {
public:
    explicit TxPoller(std::size_t max_tx_buffer_bytes = 1024 * 1024);
    ~TxPoller();

    TxPoller(const TxPoller&) = delete;
    TxPoller& operator=(const TxPoller&) = delete;

    // Registers a shared connection under a caller-chosen id (normally the rx pool's
    // id). Ensures O_NONBLOCK and applies the options eagerly. False when the id
    // already exists, the poller is closed, or the connection is unusable.
    bool add(int id, std::shared_ptr<TcpConnection> connection);

    // Fast path: immediate non-blocking send while nothing is queued; the remainder is
    // queued and flushed by run(). WouldBlock (nothing queued) when it would exceed the
    // tx cap; ConnectionNotFound for an unknown id; ConnectionClosed after close().
    // A fatal write error shuts the socket down (Both), removes the entry and is returned.
    std::expected<void, SocketError> send(int id, std::span<const std::byte> source) noexcept;

    // Drops the entry (pending bytes are discarded). Does NOT shut the socket down.
    bool remove(int id) noexcept;

    // Stops run() and rejects further add()/send().
    void close() noexcept;

    // Write-path counters (see TxStats); safe from any thread.
    TxStats stats() const noexcept {
        return {queued_sends_.load(std::memory_order_relaxed), flushes_.load(std::memory_order_relaxed)};
    }

    // Flush loop; returns after close(). Never reads from a socket.
    void run();

private:
    struct Entry {
        std::shared_ptr<TcpConnection> connection;
        std::vector<std::byte> tx_buffer;
        std::size_t tx_offset{0};
        bool armed{false};
    };
    using Map = std::unordered_map<int, Entry>;

    // false on fatal write error
    bool flush_locked(Entry& entry) noexcept;
    void arm_locked(int id, Entry& entry) noexcept;
    void disarm_locked(int id, Entry& entry) noexcept;
    // disarm, optionally shutdown(Both), erase
    void drop_locked(Map::iterator it, bool shutdown_socket) noexcept;

    std::mutex mutex_;
    std::size_t max_tx_buffer_bytes_;
    std::atomic<bool> closed_{false};
    std::atomic<std::uint64_t> queued_sends_{0};
    std::atomic<std::uint64_t> flushes_{0};
    Backend backend_;
    Map entries_;
};

using PollTxPoller = TxPoller<PollBackend>;
using EpollTxPoller = TxPoller<EpollBackend>;

extern template class TxPoller<PollBackend>;
extern template class TxPoller<EpollBackend>;
