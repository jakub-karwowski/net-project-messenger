// test_ids.cpp - IdMinter (random, non-zero, never repeated ids) and the
// message definitions in Messages.hpp (they compile, keep the inbox size
// budget, and move through a ThreadQueue).
//
// Plain assertions, no framework.

#include "Ids.hpp"
#include "Messages.hpp"
#include "ThreadQueue.hpp"

#include <iostream>
#include <string>
#include <unordered_set>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

void test_minter_unique_nonzero() {
    IdMinter minter;
    std::unordered_set<std::uint64_t> seen;
    bool all_nonzero = true;
    bool all_new = true;
    for (int i = 0; i < 100000; ++i) {
        const auto id = minter.next();
        all_nonzero = all_nonzero && id != kNoId;
        all_new = all_new && seen.insert(id).second;
    }
    check(all_nonzero, "minted ids are never 0");
    check(all_new, "100k minted ids are all distinct");
    check(minter.size() == 100000, "the minter remembers every id it issued");
}

void test_minter_reserve() {
    // Same seed twice: the second minter would produce the first one's ids,
    // unless they are reserved.
    IdMinter a(42);
    const auto first = a.next();
    const auto second = a.next();

    IdMinter b(42);
    check(b.reserve(first), "reserve() accepts an unknown id");
    check(!b.reserve(first), "reserve() reports an id it already knows");
    check(!b.reserve(kNoId), "reserve() ignores 0");
    check(b.known(first), "a reserved id is known");
    const auto next = b.next();
    check(next != first, "next() skips a reserved id");
    check(next == second, "next() continues with the sequence after the skipped id");
}

void test_minter_randomly_seeded() {
    IdMinter a;
    IdMinter b;
    check(a.next() != b.next(), "two default-seeded minters start at different ids");
}

void test_messages_move_through_queue() {
    ThreadQueue<msg::StoreMsg> queue;
    msg::SendText send{7, 1234, std::string(100, 'x')};
    check(queue.push(std::move(send)), "a StoreMsg can be pushed");
    check(queue.push(msg::QuitReq{8}), "a second StoreMsg can be pushed");
    auto a = queue.pop();
    auto b = queue.pop();
    check(a && std::holds_alternative<msg::SendText>(*a) && std::get<msg::SendText>(*a).body.size() == 100
              && std::get<msg::SendText>(*a).req == 7,
          "SendText arrives intact");
    check(b && std::holds_alternative<msg::QuitReq>(*b) && std::get<msg::QuitReq>(*b).req == 8,
          "QuitReq arrives in order");

    // Adopt holds a TcpConnection, which only exists for a real socket.
    TcpListener listener("127.0.0.1", 0, 4);
    auto conn = TcpConnection::connect("127.0.0.1", listener.local_port().value_or(0));
    check(conn.has_value(), "loopback connection for the Adopt message");
    if (!conn) { return; }
    ThreadQueue<msg::PtCommand> pt;
    msg::Adopt adopt{.link = 3, .peer = 0, .role = msg::Role::Inbound, .conn = std::move(*conn),
                     .pending_tx = msg::Bytes(10), .dec = {}, .hs = msg::HandshakeState::Established};
    check(pt.push(std::move(adopt)), "a move-only PtCommand (Adopt holds a TcpConnection) can be pushed");
    auto c = pt.pop();
    check(c && std::get<msg::Adopt>(*c).link == 3 && std::get<msg::Adopt>(*c).pending_tx.size() == 10,
          "Adopt arrives intact");
}

void test_size_budget() {
    // The static_asserts in Messages.hpp already enforce this; repeated here so
    // the numbers show up in the test output when they change.
    std::cout << "  sizeof FrontendMsg=" << sizeof(msg::FrontendMsg)
              << " StoreMsg=" << sizeof(msg::StoreMsg)
              << " RouterMsg=" << sizeof(msg::RouterMsg)
              << " PtCommand=" << sizeof(msg::PtCommand)
              << " BalancerMsg=" << sizeof(msg::BalancerMsg) << '\n';
    check(msg::inbox_ok<msg::FrontendMsg> && msg::inbox_ok<msg::StoreMsg> && msg::inbox_ok<msg::RouterMsg>
              && msg::inbox_ok<msg::PtCommand> && msg::inbox_ok<msg::BalancerMsg>,
          "every inbox keeps the size and nothrow-move budget");
    check(msg::Subscription{} == msg::Subscription{msg::Subscription::Inbound::Notify,
                                                   msg::Subscription::Outbound::Ignore,
                                                   msg::Subscription::Lifecycle::Notify},
          "the default subscription is {Notify, Ignore, Notify}");
}

}  // namespace

int main() {
    std::cout << "test_ids\n";
    test_minter_unique_nonzero();
    test_minter_reserve();
    test_minter_randomly_seeded();
    test_messages_move_through_queue();
    test_size_budget();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
