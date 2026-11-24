#pragma once

// The terminal frontend: keys edit the input line; Enter hands the line to the
// same CommandProcessor the CLI uses; Store events update the screen.
//
// One conversation is active: its messages are shown (subscription Forward);
// the others only count unread messages (Notify). Tab switches to the next
// conversation; a successful /dial opens the new one; the first conversation
// that appears is opened when none is active.
//
// It renders through a callback, so tests can draw into a FrameBuffer instead
// of a terminal. Runs on the main thread: run() handles the inbox until the
// Store reports ShutdownDone, redrawing after every batch of messages.

#include "CommandProcessor.hpp"
#include "Logger.hpp"
#include "Messages.hpp"
#include "ThreadQueue.hpp"
#include "TuiScreen.hpp"

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

class TuiFrontend {
public:
    using Render = std::function<void(Renderable&)>;

    // `progress` tells the shutdown watchdog that another frame was drawn
    // (Supervisor::progress); it matters only during a shutdown.
    TuiFrontend(ThreadQueue<msg::FrontendMsg>& inbox, ThreadQueue<msg::StoreMsg>& store, Render render, Logger log,
                std::function<void(std::string_view)> begin_shutdown, std::function<void()> progress = {});

    // Shows where the node listens and draws the first frame.
    void start(const wire::Endpoint& listening, std::size_t pts);
    void run();
    bool done() const noexcept { return done_; }
    // Brings the bars up to date and renders.
    void redraw();

    // from the input thread
    void on(msg::Key& m);
    void on(msg::InputLine& m);    // not used by this frontend
    void on(msg::InputClosed& m);  // not used by this frontend
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

    // For tests.
    TuiScreen& screen() noexcept { return screen_; }
    const InputModel& input() const noexcept { return *input_; }
    const ChatModel& chat() const noexcept { return *chat_; }
    ConvId active() const noexcept { return processor_.active(); }

private:
    struct ConvInfo {
        msg::ConvStatus status = msg::ConvStatus::New;
        std::string detail;
        wire::Endpoint remote;
        std::size_t unread = 0;
    };

    void submit(const std::string& line);
    void open(ConvId conv);
    void notice(std::string text);
    void know(ConvId conv);
    void apply_scroll();
    std::string label_of_mine(msg::DeliveryState state) const;
    std::string label_of_peer(ConvId conv) const;
    StyledText status_text() const;

    ThreadQueue<msg::FrontendMsg>& inbox_;
    ThreadQueue<msg::StoreMsg>& store_;
    Render render_;
    Logger log_;
    std::function<void(std::string_view)> begin_shutdown_;
    std::function<void()> progress_;

    std::shared_ptr<ChatModel> chat_ = std::make_shared<ChatModel>();
    std::shared_ptr<InputModel> input_ = std::make_shared<InputModel>();
    TuiScreen screen_;
    CommandProcessor processor_;

    std::map<ConvId, ConvInfo> convs_;
    std::vector<ConvId> order_;          // in the order they became known
    std::set<ReqId> dials_;              // /dial requests whose conversation opens on success
    std::set<ReqId> lists_;              // /list requests: their ConvList is shown
    // The Store answers a request with its Reply before the events it causes,
    // so a ConvList right after the Reply to a /list is the one to show. The
    // address lookups (ListReq without a request id) get no Reply.
    bool show_next_list_ = false;
    ReqId next_req_ = 1;
    std::size_t scroll_ = 0;             // messages hidden below the view (PageUp/PageDown)
    std::string notice_;
    bool quitting_ = false;
    bool done_ = false;
};
