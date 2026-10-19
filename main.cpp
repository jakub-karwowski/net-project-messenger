// main.cpp - the p2p chat entry point. MainWorker builds the whole stack and runs
// the render loop on this (the main) thread; everything else is a worker thread.
//
//   app [--io=threads|pool-sender|poll|epoll] [--headless] [--echo] [--max-peers=N]
//       [--address=A] [--port=P] [--log=PATH] [--metrics=PATH]
//   app [address] [port] [log file]     (old positional form; defaults: 127.0.0.1 9000 /dev/null)

#include "AppConfig.hpp"
#include "ThreadWorkers.hpp"

#include <iostream>
#include <utility>

int main(int argc, char** argv) {
    auto config = p2p::parse_app_config(argc, argv);
    if (!config) {
        std::cerr << "app: " << config.error() << '\n';
        return 2;
    }
    // TODO(headless task): wire --headless (HeadlessRender, no TerminalUI), --echo
    // and --metrics, then drop this guard.
    if (config->headless) {
        std::cerr << "app: --headless is not implemented yet\n";
        return 2;
    }
    // TODO(io task): the non-threads variants are added by make_io_stack().
    if (config->io != IoMode::Threads) {
        std::cerr << "app: io mode not implemented yet: " << to_string(config->io) << '\n';
        return 2;
    }

    p2p::MainWorker main_worker{std::move(*config)};
    main_worker();
    return 0;
}
