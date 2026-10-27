#pragma once

// Names the calling thread for /proc/<pid>/task/<tid>/comm, top(1), perf and
// debuggers, so a stuck node can be diagnosed from a thread dump. Linux keeps at
// most 15 characters (plus NUL); longer names are truncated here rather than
// rejected (pthread_setname_np fails with ERANGE otherwise). Best effort: errors
// are ignored.

#include <pthread.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace p2p {

inline void set_thread_name(std::string_view name) noexcept {
    char buffer[16]{};
    const std::size_t n = name.size() < 15 ? name.size() : 15;
    for (std::size_t i = 0; i < n; ++i) { buffer[i] = name[i]; }
    (void)::pthread_setname_np(::pthread_self(), buffer);
}

// "<prefix>-<n>" for one of many threads of the same kind (readers, owners).
inline void set_thread_name(std::string_view prefix, std::uint64_t n) noexcept {
    try {
        std::string name{prefix};
        name += '-';
        name += std::to_string(n);
        set_thread_name(name);
    } catch (...) {}
}

} // namespace p2p
