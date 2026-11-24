#pragma once

// Turns command lines into Store requests: parses, stamps the request id,
// pushes. Keeps the one piece of session state commands need, the active
// conversation. No I/O of its own: the frontend gets back what it must do
// locally (show help, start the shutdown watchdog) or the error to report.

#include "Commands.hpp"
#include "Messages.hpp"
#include "ThreadQueue.hpp"

#include <expected>
#include <optional>
#include <string_view>

struct ProcessorMode {
    // Subscription of the conversation /open makes active.
    msg::Subscription active{msg::Subscription::Inbound::Forward, msg::Subscription::Outbound::Notify,
                             msg::Subscription::Lifecycle::Notify};
    // Subscription the previously active conversation goes back to; none keeps
    // it as it is (the CLI forwards every conversation anyway).
    std::optional<msg::Subscription> previous;
};

class CommandProcessor {
public:
    // What the frontend still has to do itself after a successful submit().
    enum class Local {
        None,    // a request went to the Store; its Reply will follow
        Blank,   // nothing happened (no Reply will come)
        Help,    // show the help text, and reply yourself
        Quit,    // QuitReq went to the Store; start the shutdown watchdog
    };

    CommandProcessor(ThreadQueue<msg::StoreMsg>& store, ProcessorMode mode = {});

    std::expected<Local, cmd::CommandError> submit(std::string_view line, ReqId req);

    ConvId active() const noexcept { return active_; }

private:
    bool push(msg::StoreMsg m);

    ThreadQueue<msg::StoreMsg>& store_;
    ProcessorMode mode_;
    ConvId active_ = kNoId;
};
