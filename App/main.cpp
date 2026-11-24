// The node: wires every worker together and runs the frontend on the main
// thread.
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
#include "LineReader.hpp"
#include "ListenerWorker.hpp"
#include "Logger.hpp"
#include "Messages.hpp"
#include "RouterWorker.hpp"
#include "StoreWorker.hpp"
#include "Supervisor.hpp"
#include "ThreadQueue.hpp"
#include "Worker.hpp"

#include <cstdio>
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

int run_cli(const AppConfig& cfg) {
    JsonlWriter out(STDOUT_FILENO);

    LogCore core(cfg.log_path.empty() ? LogLevel::Off : cfg.log_level);
    const int log_fd = cfg.log_path.empty() ? -1 : open_log_file(cfg.log_path);
    if (!cfg.log_path.empty() && log_fd < 0) {
        std::ignore = out.write(cli::fatal(std::format("cannot open log file {}", cfg.log_path)));
        return kExitStartupFailed;
    }

    Supervisor supervisor(ShutdownTimings::from_grace(cfg.shutdown_grace), Logger(core, "supervisor"));
    supervisor.start();

    LogWriter log_writer(core, log_fd, /*owns_fd=*/true);
    WorkerThread log_thread("logger", [&] { log_writer.run(); }, [&] { core.queue().graceful_shutdown(); }, &supervisor);
    supervisor.register_log_queue([&] { core.queue().graceful_shutdown(); });
    const Logger log(core, "main");

    std::optional<TcpListener> listener;
    try {
        listener.emplace(cfg.listen.address, cfg.listen.port, 128);
    } catch (const std::exception& e) {
        std::ignore = out.write(cli::fatal(std::format("cannot listen on {}:{}: {}", cfg.listen.address, cfg.listen.port, e.what())));
        return kExitStartupFailed;
    }
    const std::uint16_t port = listener->local_port().value_or(cfg.listen.port);
    log.info("listening on {}:{}", cfg.listen.address, port);

    ThreadQueue<msg::FrontendMsg> frontend_q;
    ThreadQueue<msg::StoreMsg> store_q;
    ThreadQueue<msg::RouterMsg> router_q;
    ThreadQueue<msg::BalancerMsg> balancer_q;

    StoreConfig store_cfg;
    store_cfg.advertised = advertised(cfg, port);
    store_cfg.shutdown_grace = cfg.shutdown_grace;
    NullPersistence persistence;
    StoreWorker store(store_cfg, frontend_q, router_q, persistence, Logger(core, "store"));

    RouterConfig router_cfg;
    router_cfg.min_pts = cfg.min_pts;
    router_cfg.max_pts = cfg.max_pts;
    router_cfg.io.handshake_timeout = cfg.handshake_timeout;
    router_cfg.io.connect_timeout = cfg.connect_timeout;
    RouterWorker router(router_cfg, router_q, store_q, balancer_q, &supervisor, Logger(core, "router"),
                        [&] { listener->stop(); });
    balance::Config balancer_cfg;
    balancer_cfg.period = cfg.balance_period;
    balancer_cfg.min_pts = cfg.min_pts;
    balancer_cfg.max_pts = cfg.max_pts;
    BalancerWorker balancer(balancer_cfg, balancer_q, router_q, Logger(core, "balancer"));
    ListenerWorker accept(*listener, router_q, Logger(core, "listener"));
    LineReader input(STDIN_FILENO, frontend_q, Logger(core, "input"));
    CliFrontend frontend(CliOptions{cfg.stay}, frontend_q, store_q, out, Logger(core, "cli"),
                         [&](std::string_view reason) { supervisor.begin_shutdown(reason); });

    WorkerThread store_thread("store", [&] { worker::run_inbox(store, store_q); },
                              [&] { store_q.graceful_shutdown(); }, &supervisor);
    WorkerThread router_thread("router", [&] { router.run(); }, [&] { router_q.graceful_shutdown(); }, &supervisor);
    WorkerThread balancer_thread("balancer", [&] { worker::run_inbox(balancer, balancer_q); },
                                 [&] { balancer_q.graceful_shutdown(); }, &supervisor);
    WorkerThread listener_thread("listener", [&] { accept.run(); }, [&] { listener->stop(); }, &supervisor);
    WorkerThread input_thread("input", [&] { input.run(); }, [&] { input.stop(); }, &supervisor);

    supervisor.register_queue([&] { frontend_q.force_shutdown(); });
    supervisor.register_queue([&] { store_q.force_shutdown(); });
    supervisor.register_queue([&] { router_q.force_shutdown(); });
    supervisor.register_queue([&] { balancer_q.force_shutdown(); });
    for (WorkerThread* t : {&store_thread, &router_thread, &balancer_thread, &listener_thread, &input_thread}) {
        supervisor.register_thread(*t);
    }
    // A signal starts the same shutdown /quit does.
    supervisor.set_graceful_trigger([&] { std::ignore = store_q.push(msg::QuitReq{0}); });
    supervisor.set_escalation_hook([&](const Escalation& e) { std::ignore = out.try_write(cli::shutdown_forced(e)); });

    frontend.start(wire::Endpoint{cfg.listen.address, port}, router.pt_count());
    frontend.run();

    // Reverse start order. The Store and Router have finished by now (their
    // loops end with the shutdown); the stops matter after an emergency.
    for (WorkerThread* t : {&input_thread, &listener_thread, &balancer_thread, &router_thread, &store_thread}) {
        t->request_stop();
        t->join();
    }
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
    if (cfg->frontend == AppConfig::Frontend::Tui) {
        std::fputs("node: the terminal UI is not built yet; use --cli\n", stderr);
        return kExitStartupFailed;
    }
    try {
        return run_cli(*cfg);
    } catch (const std::exception& e) {
        // Start-up failures that are not handled above (e.g. no eventfd).
        JsonlWriter out(STDOUT_FILENO);
        std::ignore = out.write(cli::fatal(e.what()));
        return kExitStartupFailed;
    }
}
