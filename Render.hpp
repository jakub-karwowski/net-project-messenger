#pragma once

// Render.hpp - the render side of the node, split in two.
//
// ChatController is the front-end independent half: it parses commands, remembers
// the conversation being viewed, talks to the store through the store queue and
// interprets what the store sends back. It never prints; everything it wants
// shown goes through a RenderSink. TerminalRender (Render.cpp, behind
// RenderWorker) draws on the TerminalUI; HeadlessRender (HeadlessRenderWorker)
// reads command lines from an fd and writes JSON lines to another.

#include "Events.hpp"
#include "ThreadQueue.hpp"
#include "ThreadWorkers.hpp" // Logger, RenderWorker

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

struct RenderSink {
    virtual ~RenderSink() = default;
    virtual void info(std::string_view text) = 0;
    virtual void messages(uint64_t conv, std::span<const ConversationStore::Message>,
                          bool snapshot) = 0;
    virtual void notification(uint64_t conv) = 0;
    virtual void conversations(const EventConversationList&) = 0;
    // what: "connected" | "reconnected" | "dialed" | "disconnected"
    virtual void peer(std::string_view what, uint64_t conv, uint64_t peer) = 0;
    virtual void error(std::string_view message) = 0; // a worker-level failure
    virtual void quit() = 0;                          // called once, when the session ends
    // A rejected command (bad usage, unknown command). Terminal: a plain system
    // message; headless: an error event. Defaults to error().
    virtual void command_error(std::string_view text) { error(text); }
    // Reply to a /sync barrier (headless only; the terminal never sends one).
    virtual void sync() {}
};

// What a command is still waiting for from the store (so a front-end can match
// the asynchronous answer to the request that caused it).
enum class Pending : uint8_t { None, Connect, Reconnect, Switch, Conversations, Sync };

struct CommandResult {
    Pending pending{Pending::None};
    uint64_t arg{}; // Switch: the conversation asked for
};

class ChatController {
public:
    // `machine` enables the headless-only commands (/send, /sync, /echo).
    ChatController(RenderSink& sink, ThreadQueue<Event>& store_queue, Logger logger,
                   bool machine = false);

    // Runs one command line (starting with '/').
    CommandResult command(std::string_view line);
    // Enter on a line: empty is ignored, '/' is a command, anything else is
    // composed into the current conversation.
    CommandResult submit(std::string_view line);
    // Interprets a store -> render event.
    void handle_store(const Event& event);

    bool running() const noexcept { return running_; }
    void stop();
    uint64_t current_conversation() const noexcept { return current_conversation_id_; }

private:
    void switch_conversation(uint64_t conversation_id);
    void set_notifications(uint64_t conversation_id, bool enabled);
    void compose(const std::string& payload);
    bool push(Event event); // to the store; ends the session when it is gone

    RenderSink& sink_;
    ThreadQueue<Event>& store_queue_;
    Logger logger_;
    bool machine_;
    uint64_t current_conversation_id_{0};
    std::deque<bool> list_requests_; // true = /sync, false = /conversations
    bool running_{true};
    bool quit_sent_{false};
};

// Headless front-end: runs on the caller's thread (operator()). Input lines come
// from `in_fd` via a small reader thread that posts EventCommandLine into the
// render queue, so commands and store events are handled in one total order.
// Output is JSONL on `out_fd`; every line carries "seq" and "t_ns". EOF on the
// input is /quit.
struct HeadlessRenderWorker {
    HeadlessRenderWorker(
        ThreadQueue<Event>& render_queue,
        ThreadQueue<Event>& store_queue,
        int in_fd,
        int out_fd,
        std::string io_name,
        Logger logger = {});
    ~HeadlessRenderWorker();
    HeadlessRenderWorker(const HeadlessRenderWorker&) = delete;
    HeadlessRenderWorker& operator=(const HeadlessRenderWorker&) = delete;

    // Blocks until /quit, EOF on the input, or the render queue is shut down.
    // Emits the final {"event":"bye"}. The fds are not closed.
    void operator()();

    // Emits the {"event":"ready"} line: `listen` is "ip:port" once the listener is
    // bound; on a bind failure pass the reason in `error`. Thread safe, and may be
    // called before or after operator() starts (MainWorker calls it from its own
    // thread once the listener is wired). Call it at most once.
    void announce_ready(std::string listen, std::optional<std::string> error = std::nullopt);

    // Optional provider of the "metrics" object for /stats: returns the text of a
    // JSON object (e.g. {"accepts":3}). Default is {}. Set before operator() runs.
    void set_stats_provider(std::function<std::string()> provider);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
