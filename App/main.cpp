// The node: wires every worker together and runs the frontend (the terminal UI,
// or with --cli the agent CLI) on the main thread.
//
// Start order: Supervisor (it sets the signal mask, so it comes before every
// other thread), Logger, Store, Router (with its IO workers), Balancer,
// Listener, input, then the frontend loop. Shutdown is the reverse, driven by
// messages; the Supervisor bounds how long it may take. Objects are declared in
// start order, so they are destroyed in reverse: threads are joined before the
// workers and queues they use go away.

#include "AppConfig.hpp"
#include "BalancerWorker.hpp"
#include "CliFormat.hpp"
#include "CliFrontend.hpp"
#include "JsonlWriter.hpp"
#include "KeyboardWorker.hpp"
#include "LineReader.hpp"
#include "ListenerWorker.hpp"
#include "Logger.hpp"
#include "Messages.hpp"
#include "QueueMonitor.hpp"
#include "RouterWorker.hpp"
#include "StoreWorker.hpp"
#include "Supervisor.hpp"
#include "TerminalState.hpp"
#include "TerminalUI.hpp"
#include "ThreadQueue.hpp"
#include "TuiFrontend.hpp"
#include "Worker.hpp"

#include <cstdio>
#include <sys/resource.h>
#include <exception>
#include <format>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace {

std::vector<wire::Endpoint> advertised(const AppConfig& cfg, std::uint16_t bound_port) {
    if (cfg.advertise.empty()) {
        const std::string host = cfg.listen.address == "0.0.0.0" ? "127.0.0.1" : cfg.listen.address;
        return {wire::Endpoint{host, bound_port}};
    }
    std::vector<wire::Endpoint> out = cfg.advertise;
    for (auto& ep : out) {
        if (ep.port == 0) { ep.port = bound_port; }
    }
    return out;
}

// Many peers need many descriptors: use as many as the hard limit allows.
void raise_open_file_limit() {
    rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur < limit.rlim_max) {
        limit.rlim_cur = limit.rlim_max;
        std::ignore = ::setrlimit(RLIMIT_NOFILE, &limit);
    }
}

// Until a Balancer decides anything, or with --no-balancer: take its messages
// off the queue so it does not grow, and stop when told to.
void discard_balancer_messages(ThreadQueue<msg::BalancerMsg>& q) {
    while (auto m = q.pop()) {
        if (std::holds_alternative<msg::StopBalancer>(*m)) { return; }
    }
}

int run_node(const AppConfig& cfg) {
    raise_open_file_limit();
    const bool tui = cfg.frontend == AppConfig::Frontend::Tui;
    // stdout belongs to the frontend: the CLI's events, or the TUI's screen.
    JsonlWriter out(STDOUT_FILENO);
    const auto fatal = [&](const std::string& error) {
        if (tui) {
            std::fprintf(stderr, "node: %s\n", error.c_str());
        } else {
            std::ignore = out.write(cli::fatal(error));
        }
        return kExitStartupFailed;
    };

    LogCore core(cfg.log_path.empty() ? LogLevel::Off : cfg.log_level);
    const int log_fd = cfg.log_path.empty() ? -1 : open_log_file(cfg.log_path);
    if (!cfg.log_path.empty() && log_fd < 0) { return fatal(std::format("cannot open log file {}", cfg.log_path)); }
    if (tui && !terminal_state::save()) { return fatal("the terminal UI needs a terminal on stdin (use --cli otherwise)"); }

    Supervisor supervisor(ShutdownTimings::from_grace(cfg.shutdown_grace), Logger(core, "supervisor"));
    supervisor.start();

    LogWriter log_writer(core, log_fd, /*owns_fd=*/true);
    WorkerThread log_thread("logger", [&] { log_writer.run(); }, [&] { core.queue().graceful_shutdown(); }, &supervisor);
    supervisor.register_log_queue([&] { core.queue().graceful_shutdown(); });
    const Logger log(core, "main");

    std::optional<TcpListener> listener;
    try {
        listener.emplace(cfg.listen.address, cfg.listen.port, 4096);   // the kernel caps it at somaxconn
    } catch (const std::exception& e) {
        return fatal(std::format("cannot listen on {}:{}: {}", cfg.listen.address, cfg.listen.port, e.what()));
    }
    const std::uint16_t port = listener->local_port().value_or(cfg.listen.port);
    log.info("listening on {}:{}", cfg.listen.address, port);

    // The terminal goes into raw mode here and back in ~TerminalUI(); if the
    // process has to exit without destructors, the Supervisor restores it.
    std::optional<TerminalUI> terminal;
    if (tui) {
        terminal.emplace();
        supervisor.set_terminal_restore(&terminal_state::restore);
    }

    ThreadQueue<msg::FrontendMsg> frontend_q;
    ThreadQueue<msg::StoreMsg> store_q;
    ThreadQueue<msg::RouterMsg> router_q;
    ThreadQueue<msg::BalancerMsg> balancer_q;
    // Backpressure: the IO workers stop reading while either is congested.
    // Received chat fills the Store's inbox; the Router's fills only when the
    // node sends as much as it receives (--echo).
    if (cfg.inbox_high > 0) {
        const std::size_t low = cfg.inbox_low > 0 ? cfg.inbox_low : cfg.inbox_high / 2;
        store_q.set_watermarks(cfg.inbox_high, low);
        router_q.set_watermarks(cfg.inbox_high, low);
    }

    StoreConfig store_cfg;
    store_cfg.advertised = advertised(cfg, port);
    store_cfg.shutdown_grace = cfg.shutdown_grace;
    NullPersistence persistence;
    StoreWorker store(store_cfg, frontend_q, router_q, persistence, Logger(core, "store"));

    RouterConfig router_cfg;
    router_cfg.min_pts = cfg.min_pts;
    router_cfg.max_pts = cfg.balancer ? cfg.max_pts : cfg.min_pts;
    router_cfg.max_links = cfg.max_links;
    router_cfg.inbox_batch = cfg.inbox_batch;
    router_cfg.max_links_per_pt = cfg.max_links_per_pt == 0 ? cfg.max_links : cfg.max_links_per_pt;
    router_cfg.io.handshake_timeout = cfg.handshake_timeout;
    router_cfg.io.connect_timeout = cfg.connect_timeout;
    RouterWorker router(router_cfg, router_q, store_q, balancer_q, &supervisor, Logger(core, "router"),
                        [&] { listener->stop(); });
    balance::Config balancer_cfg;
    balancer_cfg.period = cfg.balance_period;
    balancer_cfg.min_pts = cfg.min_pts;
    balancer_cfg.max_pts = cfg.max_pts;
    balancer_cfg.max_moves_per_tick = cfg.balance_max_moves;
    BalancerWorker balancer(balancer_cfg, balancer_q, router_q, Logger(core, "balancer"), /*timer=*/cfg.balancer);
    ListenerWorker accept(*listener, router_q, Logger(core, "listener"));

    const auto begin_shutdown = [&](std::string_view reason) { supervisor.begin_shutdown(reason); };
    const auto progress = [&] { supervisor.progress(); };
    std::optional<LineReader> lines;
    std::optional<KeyboardWorker> keyboard;
    std::optional<CliFrontend> cli_frontend;
    std::optional<TuiFrontend> tui_frontend;
    if (tui) {
        keyboard.emplace(*terminal, frontend_q, Logger(core, "keyboard"));
        tui_frontend.emplace(frontend_q, store_q, [&](Renderable& r) { std::ignore = terminal->render(r); },
                             Logger(core, "tui"), begin_shutdown, progress);
    } else {
        lines.emplace(STDIN_FILENO, frontend_q, Logger(core, "input"));
        cli_frontend.emplace(CliOptions{cfg.stay, cfg.echo, cfg.inbox_batch}, frontend_q, store_q, out, Logger(core, "cli"), begin_shutdown, progress);
    }

    WorkerThread store_thread("store", [&] { worker::run_inbox(store, store_q, cfg.inbox_batch); },
                              [&] { store_q.graceful_shutdown(); }, &supervisor);
    WorkerThread router_thread("router", [&] { router.run(); }, [&] { router_q.graceful_shutdown(); }, &supervisor);
    WorkerThread balancer_thread("balancer", [&] {
            if (cfg.balancer) { worker::run_inbox(balancer, balancer_q, cfg.inbox_batch); } else { discard_balancer_messages(balancer_q); }
        },
                                 [&] { balancer_q.graceful_shutdown(); }, &supervisor);
    WorkerThread listener_thread("listener", [&] { accept.run(); }, [&] { listener->stop(); }, &supervisor);
    // Inbox depths as Metric lines, to see where a backlog builds up; only when
    // they are logged. Not registered with the Supervisor: it has nothing to
    // finish, so it must never be what a shutdown waits for.
    QueueMonitor monitor(Logger(core, "queues"));
    monitor.watch("store", store_q);
    monitor.watch("router", router_q);
    monitor.watch("frontend", frontend_q);
    monitor.watch("balancer", balancer_q);
    std::optional<WorkerThread> monitor_thread;
    if (log.enabled(LogLevel::Metric)) {
        monitor_thread.emplace("queues", [&] { monitor.run(); }, [&] { monitor.stop(); }, &supervisor);
    }
    WorkerThread input_thread("input", [&] { if (tui) { keyboard->run(); } else { lines->run(); } },
                              [&] { if (tui) { keyboard->stop(); } else { lines->stop(); } }, &supervisor);

    supervisor.register_queue([&] { frontend_q.force_shutdown(); });
    supervisor.register_queue([&] { store_q.force_shutdown(); });
    supervisor.register_queue([&] { router_q.force_shutdown(); });
    supervisor.register_queue([&] { balancer_q.force_shutdown(); });
    for (WorkerThread* t : {&store_thread, &router_thread, &balancer_thread, &listener_thread, &input_thread}) {
        supervisor.register_thread(*t);
    }
    // A signal starts the same shutdown /quit does.
    supervisor.set_graceful_trigger([&] { std::ignore = store_q.push(msg::QuitReq{0}); });
    if (!tui) {
        supervisor.set_escalation_hook([&](const Escalation& e) { std::ignore = out.try_write(cli::shutdown_forced(e)); });
    }

    const wire::Endpoint listening{cfg.listen.address, port};
    if (tui) {
        tui_frontend->start(listening, router.pt_count());
        tui_frontend->run();
    } else {
        cli_frontend->start(listening, router.pt_count());
        cli_frontend->run();
    }

    // Reverse start order. The Store and Router have finished by now (their
    // loops end with the shutdown); the stops matter after an emergency.
    if (monitor_thread) {
        monitor_thread->request_stop();
        monitor_thread->join();
    }
    for (WorkerThread* t : {&input_thread, &listener_thread, &balancer_thread, &router_thread, &store_thread}) {
        t->request_stop();
        t->join();
    }
    // The last line of every complete log: everything logged before it has
    // been queued, and the Logger drains the queue before it stops. The
    // Supervisor still guards that drain.
    log.info("all workers stopped; exit code {}", supervisor.exit_code());
    core.queue().graceful_shutdown();
    log_thread.join();
    return supervisor.finish();
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string_view> args(argv + 1, argv + argc);
    const auto cfg = parse_args(args);
    if (!cfg) {
        std::fprintf(stderr, "node: %s\n%s", cfg.error().c_str(), std::string(usage()).c_str());
        if (std::ranges::contains(args, std::string_view{"--cli"})) {
            JsonlWriter out(STDOUT_FILENO);
            std::ignore = out.write(cli::fatal(cfg.error()));
        }
        return kExitStartupFailed;
    }
    if (cfg->help) {
        std::fputs(std::string(usage()).c_str(), stdout);
        return 0;
    }
    try {
        return run_node(*cfg);
    } catch (const std::exception& e) {
        // Start-up failures that are not handled above (no eventfd, a terminal
        // that cannot be configured, ...).
        terminal_state::restore();
        if (cfg->frontend == AppConfig::Frontend::Tui) {
            std::fprintf(stderr, "node: %s\n", e.what());
        } else {
            JsonlWriter out(STDOUT_FILENO);
            std::ignore = out.write(cli::fatal(e.what()));
        }
        return kExitStartupFailed;
    }
}
