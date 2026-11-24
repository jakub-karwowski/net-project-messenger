#include "ListenerWorker.hpp"

#include <thread>
#include <tuple>

ListenerWorker::ListenerWorker(TcpListener& listener, ThreadQueue<msg::RouterMsg>& router, Logger log,
                               ListenerConfig config)
    : listener_(listener), router_(router), log_(log.with_component("listener")), config_(config) {}

void ListenerWorker::run() {
    for (;;) {
        auto conn = listener_.accept_client();
        if (conn) {
            // Applied when the IO worker's pool adopts it.
            conn->with_non_blocking().with_tcp_nodelay().with_keep_alive();
            const auto& from = conn->remote_endpoint();
            log_.debug("accepted {}:{}", from.address, from.port);
            std::ignore = router_.push(msg::InboundAccepted{std::move(*conn)});   // false: shutting down
            continue;
        }
        switch (conn.error()) {
            case SocketError::Stopped:
                return;
            case SocketError::TooManyOpenFiles:
                log_.warn("accept: out of file descriptors; retrying in {} ms", config_.fd_backoff.count());
                std::this_thread::sleep_for(config_.fd_backoff);
                break;
            default:
                log_.warn("accept failed: {}", to_string(conn.error()));
                std::this_thread::sleep_for(config_.error_backoff);
                break;
        }
    }
}
