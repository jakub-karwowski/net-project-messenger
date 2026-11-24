#pragma once

// The agent-facing frontend: no keyboard, no screen. Each input line is a
// command whose request id is its line number; every event is one JSON line on
// stdout (see CliFormat.hpp).
//
// It runs on the main thread: run() handles the frontend inbox until the Store
// reports ShutdownDone (or the inbox is closed by an emergency shutdown).
//
// Every conversation is forwarded (SetDefaultSubscription at start), so an
// agent sees every message and status change without subscribing.
// End of input, /quit and a closed stdout all start the normal shutdown.

#include "CommandProcessor.hpp"
#include "JsonlWriter.hpp"
#include "Logger.hpp"
#include "Messages.hpp"
#include "ThreadQueue.hpp"

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

struct CliOptions {
    bool stay = false;   // keep running after end of input (until /quit or a signal)
    // Load-test target: every received message goes straight back to its
    // sender, nothing is stored or printed per message or per peer.
    bool echo = false;
};

class CliFrontend {
public:
    // `begin_shutdown` arms the shutdown watchdog (Supervisor::begin_shutdown);
    // `progress` tells it that another event went out (Supervisor::progress),
    // so a long backlog during a shutdown is not taken for a hang.
    CliFrontend(CliOptions options, ThreadQueue<msg::FrontendMsg>& inbox, ThreadQueue<msg::StoreMsg>& store,
                JsonlWriter& out, Logger log, std::function<void(std::string_view)> begin_shutdown,
                std::function<void()> progress = {});

    // Writes "ready" and makes the Store forward every conversation.
    void start(const wire::Endpoint& listening, std::size_t pts);
    void run();
    bool done() const noexcept { return done_; }

    // from the input thread
    void on(msg::InputLine& m);
    void on(msg::InputClosed& m);
    void on(msg::Key& m);   // not used by this frontend
    // from the Store
    void on(msg::Reply& m);
    void on(msg::ConvSnapshot& m);
    void on(msg::MsgAppended& m);
    void on(msg::MsgStatus& m);
    void on(msg::ConvState& m);
    void on(msg::ConvNotify& m);
    void on(msg::ConvList& m);
    void on(msg::SystemNotice& m);
    void on(msg::ShutdownDone& m);

private:
    void write(const std::string& line);
    void quit(std::string_view reason);

    CliOptions options_;
    ThreadQueue<msg::FrontendMsg>& inbox_;
    ThreadQueue<msg::StoreMsg>& store_;
    JsonlWriter& out_;
    Logger log_;
    std::function<void(std::string_view)> begin_shutdown_;
    std::function<void()> progress_;
    CommandProcessor processor_;
    bool quitting_ = false;
    bool done_ = false;
};
