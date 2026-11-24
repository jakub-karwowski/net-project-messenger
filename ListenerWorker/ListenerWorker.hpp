#pragma once

// Accepts inbound connections and hands each to the Router. It reads nothing
// from them: the handshake runs on an IO worker, so a slow peer can never hold
// up accept().
//
// The thread body is run(); listener.stop() (from any thread) ends it.

#include "Logger.hpp"
#include "Messages.hpp"
#include "TcpSocket.hpp"
#include "ThreadQueue.hpp"

#include <chrono>

struct ListenerConfig {
    // After running out of descriptors: wait before trying again (the client
    // stays queued in the kernel meanwhile).
    std::chrono::milliseconds fd_backoff{100};
    // After any other accept() failure.
    std::chrono::milliseconds error_backoff{10};
};

class ListenerWorker {
public:
    ListenerWorker(TcpListener& listener, ThreadQueue<msg::RouterMsg>& router, Logger log, ListenerConfig config = {});

    void run();

private:
    TcpListener& listener_;
    ThreadQueue<msg::RouterMsg>& router_;
    Logger log_;
    ListenerConfig config_;
};
