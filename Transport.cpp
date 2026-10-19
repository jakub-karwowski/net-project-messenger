// Transport.cpp - ThreadedInbound and the make_io_stack factory.

#include "Transport.hpp"
#include "PoolOutbound.hpp"

#include <stdexcept>
#include <string>
#include <utility>

namespace p2p {

ThreadedInbound::ThreadedInbound(const NetConfig& net, ThreadQueue<Event>& store_queue,
                                 PeerTable& peers, Logger logger)
    : listener_(net.address, net.port, store_queue, peers, net.max_peers, net.backlog,
                std::move(logger)) {
    listener_.set_metrics(net.metrics);
}

IoStack make_io_stack(IoMode mode, const NetConfig& net, ThreadQueue<Event>& store_queue,
                      PeerTable& peers, Logger logger) {
    switch (mode) {
        case IoMode::Threads:
            return IoStack{
                std::make_unique<ThreadedInbound>(net, store_queue, peers, std::move(logger)),
                std::make_unique<BlockingOutbound>(net.send_timeout)};
        case IoMode::PoolSender: {
            PoolOutboundOptions options;
            options.max_connections = 2 * net.max_peers;
            return IoStack{
                std::make_unique<ThreadedInbound>(net, store_queue, peers, logger),
                std::make_unique<PoolOutbound<TcpConnectionPool>>(options, std::move(logger))};
        }
        case IoMode::Poll:
        case IoMode::Epoll:
            break;
    }
    throw std::runtime_error("io mode not implemented yet: " + std::string{to_string(mode)});
}

} // namespace p2p
