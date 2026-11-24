#include "CliFrontend.hpp"

#include "CliFormat.hpp"
#include "Worker.hpp"

#include <tuple>
#include <utility>

namespace {

using Sub = msg::Subscription;

// Every conversation, all the time: there is no "active" view to save work for.
constexpr Sub kForwardAll{Sub::Inbound::Forward, Sub::Outbound::Notify, Sub::Lifecycle::Notify};

}  // namespace

CliFrontend::CliFrontend(CliOptions options, ThreadQueue<msg::FrontendMsg>& inbox, ThreadQueue<msg::StoreMsg>& store,
                         JsonlWriter& out, Logger log, std::function<void(std::string_view)> begin_shutdown,
                         std::function<void()> progress)
    : options_(options),
      inbox_(inbox),
      store_(store),
      out_(out),
      log_(log.with_component("cli")),
      begin_shutdown_(std::move(begin_shutdown)),
      progress_(std::move(progress)),
      processor_(store, ProcessorMode{kForwardAll, std::nullopt}) {}

void CliFrontend::start(const wire::Endpoint& listening, std::size_t pts) {
    std::ignore = store_.push(msg::SetDefaultSubscription{kForwardAll});   // false: already shutting down
    write(cli::ready(listening, pts));
}

void CliFrontend::run() {
    worker::run_inbox(*this, inbox_);
}

void CliFrontend::write(const std::string& line) {
    if (out_.write(line)) {
        // Also when the shutdown came from a signal, which this frontend does
        // not see; outside a shutdown the Supervisor ignores it.
        if (progress_) { progress_(); }
        return;
    }
    // Nobody reads our output any more: there is no point in carrying on.
    if (!quitting_) {
        log_.info("stdout closed; quitting");
        quit("stdout closed");
    }
}

void CliFrontend::quit(std::string_view reason) {
    if (std::exchange(quitting_, true)) { return; }
    std::ignore = store_.push(msg::QuitReq{0});
    if (begin_shutdown_) { begin_shutdown_(reason); }
}

// ------------------------------------------------------------------ input

void CliFrontend::on(msg::InputLine& m) {
    const auto result = processor_.submit(m.text, m.line_no);
    if (!result) {
        write(cli::reply_error(m.line_no, result.error().text));
        return;
    }
    switch (*result) {
        case CommandProcessor::Local::None:
        case CommandProcessor::Local::Blank:
            return;
        case CommandProcessor::Local::Help:
            write(cli::reply_ok(m.line_no));
            write(cli::notice(cmd::help_text()));
            return;
        case CommandProcessor::Local::Quit:
            // The QuitReq is on its way to the Store (its Reply follows); arm the watchdog.
            quitting_ = true;
            if (begin_shutdown_) { begin_shutdown_("quit"); }
            return;
    }
}

void CliFrontend::on(msg::InputClosed&) {
    if (options_.stay) {
        log_.info("end of input; staying up (--stay)");
        return;
    }
    quit("end of input");
}

void CliFrontend::on(msg::Key&) {}

// ------------------------------------------------------------------ store events

void CliFrontend::on(msg::Reply& m) { write(cli::reply(m)); }
void CliFrontend::on(msg::ConvSnapshot& m) { write(cli::snapshot(m)); }
void CliFrontend::on(msg::MsgAppended& m) { write(cli::message(m)); }
void CliFrontend::on(msg::MsgStatus& m) { write(cli::msg_status(m)); }
void CliFrontend::on(msg::ConvState& m) { write(cli::conv_state(m)); }
void CliFrontend::on(msg::ConvNotify&) {}   // every conversation is forwarded: unread counts carry nothing new
void CliFrontend::on(msg::ConvList& m) { write(cli::convs(m)); }
void CliFrontend::on(msg::SystemNotice& m) { write(cli::notice(m.text)); }

void CliFrontend::on(msg::ShutdownDone&) {
    write(cli::shutdown_done());
    done_ = true;
}
