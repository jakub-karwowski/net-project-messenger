#pragma once

// Test helper: listeners on random loopback ports that tolerate collisions.
//
// Tests draw random ports in [12000, 27000). Under `ctest -j8` several binaries
// (test_multiplexed, test_peer_threads, ...) listen in that range at once, so a
// draw can hit a port another process holds. A bare
//     TcpListener server("127.0.0.1", random_port(), 4);
// then throws SocketException("Address already in use") -> std::terminate ->
// "Subprocess aborted" (seen once in a full ctest run; reproduced by running six
// test_multiplexed copies side by side). These helpers redraw instead.
//
// (Probing a port by bind+listen+close and handing the number out was tried and
// rejected: closing a listening socket takes 10-100 ms on this WSL2 kernel, which
// delayed the node's own bind past the tests' 100 ms settle time.)

#include "TcpSocket.hpp"
#include "Transport.hpp"

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

namespace test_ports {

inline unsigned random_port() {
    static std::mt19937 rng{std::random_device{}()};
    return 12000 + rng() % 15000; // below the ephemeral range
}

// A listener on a random free port; `port` receives the number. Throws only when
// every draw is taken (then the test aborts, which is the right signal).
inline TcpListener listen_on_random_port(unsigned& port, int backlog) {
    for (int attempt = 0;; ++attempt) {
        port = random_port();
        try {
            return TcpListener("127.0.0.1", port, backlog);
        } catch (const SocketException&) {
            if (attempt >= 100) { throw; }
        }
    }
}

// A port nobody listens on: bound once (so it was free) and released.
inline unsigned closed_port() {
    unsigned port = 0;
    { TcpListener probe = listen_on_random_port(port, 1); }
    return port;
}

// Starts `transport.run()` on `thread` and waits (up to `timeout`, returning as soon
// as it is known) for the listen socket: true once bound, false when the bind
// failed (port taken by another process: the caller redraws) or timed out. Replaces
// fixed "let it listen" sleeps, which expired under parallel load.
inline bool start_listening(InboundTransport& transport, std::thread& thread,
                            std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
    auto ready = std::make_shared<std::promise<bool>>();
    auto bound = ready->get_future();
    transport.set_ready_callback([ready](std::string, std::optional<std::string> error) {
        ready->set_value(!error.has_value()); // called once, from the run() thread
    });
    thread = std::thread{[&transport] { transport.run(); }};
    return bound.wait_for(timeout) == std::future_status::ready && bound.get();
}

} // namespace test_ports
