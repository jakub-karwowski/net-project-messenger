// main.cpp - the p2p chat entry point. MainWorker builds the whole stack and runs
// the render loop on this (the main) thread; everything else is a worker thread.
//
//   app [address] [port] [log file]     (defaults: 127.0.0.1 9000 app.log)

#include "ThreadWorkers.hpp"

#include <charconv>
#include <string>
#include <string_view>
#include <utility>

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "127.0.0.1";
    unsigned port = 9000;
    if (argc > 2) {
        const std::string_view text = argv[2];
        (void)std::from_chars(text.data(), text.data() + text.size(), port);
    }
    std::string log_path = (argc > 3) ? argv[3] : "/dev/null";

    p2p::MainWorker main_worker{std::move(address), port, std::move(log_path)};
    main_worker();
    return 0;
}