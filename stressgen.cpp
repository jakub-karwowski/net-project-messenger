// stressgen - wire-protocol load generator for the chat node (see stressgen.hpp).

#include "stressgen.hpp"

#include <cstdlib>
#include <fstream>
#include <iostream>

static void usage() {
    std::cerr <<
        "usage: stressgen --target ip:port [--peers N] [--rate R] [--window W] [--size B]\n"
        "                 [--duration S] [--warmup S] [--idle-peers M] [--slow-peers K]\n"
        "                 [--churn C] [--grace S] [--listen-ip IP] [--listen-port P] [--out file.json]\n"
        "                 [--duplex]\n"
        "  rate: msgs/s/peer (0 = as fast as possible); window: max in-flight/peer (0 = unlimited)\n"
        "  --duplex: single connection per peer (node replies Ack+echoes on the dialed socket; no listener)\n"
        "  idle/slow peers are extra peers on top of --peers. exit: 0 ok, 2 unreachable, 3 no handshakes\n";
}

int main(int argc, char** argv) {
    stress::Config c;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i], v;
        if (a == "-h" || a == "--help") { usage(); return 0; }
        if (a == "--duplex") { c.duplex = true; continue; }
        const auto eq = a.find('=');
        if (eq != std::string::npos) { v = a.substr(eq + 1); a.resize(eq); }
        else if (i + 1 < argc) { v = argv[++i]; }
        else { usage(); return 1; }
        if (a == "--target") {
            const auto colon = v.rfind(':');
            if (colon == std::string::npos) { usage(); return 1; }
            c.target_ip = v.substr(0, colon);
            c.target_port = static_cast<std::uint16_t>(std::atoi(v.c_str() + colon + 1));
        }
        else if (a == "--peers") { c.peers = std::atoi(v.c_str()); }
        else if (a == "--rate") { c.rate = std::atof(v.c_str()); }
        else if (a == "--window") { c.window = std::atoi(v.c_str()); }
        else if (a == "--size") { c.size = std::atoi(v.c_str()); }
        else if (a == "--duration") { c.duration = std::atof(v.c_str()); }
        else if (a == "--warmup") { c.warmup = std::atof(v.c_str()); }
        else if (a == "--idle-peers") { c.idle_peers = std::atoi(v.c_str()); }
        else if (a == "--slow-peers") { c.slow_peers = std::atoi(v.c_str()); }
        else if (a == "--churn") { c.churn = std::atof(v.c_str()); }
        else if (a == "--grace") { c.grace = std::atof(v.c_str()); }
        else if (a == "--listen-ip") { c.listen_ip = v; }
        else if (a == "--listen-port") { c.listen_port = static_cast<std::uint16_t>(std::atoi(v.c_str())); }
        else if (a == "--out") { c.out = v; }
        else { std::cerr << "unknown option " << a << "\n"; usage(); return 1; }
    }
    if (c.target_port == 0) { usage(); return 1; }

    stress::Result r;
    std::string err;
    stress::Generator g(c);
    const int rc = g.run(r, err);
    if (rc != 0) { std::cerr << "stressgen: " << err << "\n"; return rc; }

    const std::string json = stress::to_json(c, r);
    if (c.out.empty()) { std::cout << json; }
    else {
        std::ofstream f(c.out);
        f << json;
        if (!f) { std::cerr << "stressgen: cannot write " << c.out << "\n"; return 1; }
    }
    std::fprintf(stderr,
        "stressgen: peers=%d/%d sent=%llu recv=%llu lost=%llu %.0f msg/s rtt_us p50=%.0f p99=%.0f p999=%.0f max=%.0f\n",
        r.peers_ready, r.peers_total, (unsigned long long)r.sent, (unsigned long long)r.received,
        (unsigned long long)r.lost, r.msgs_per_s, r.rtt.quantile(0.5) / 1e3, r.rtt.quantile(0.99) / 1e3,
        r.rtt.quantile(0.999) / 1e3, r.rtt.max() / 1e3);
    return 0;
}
