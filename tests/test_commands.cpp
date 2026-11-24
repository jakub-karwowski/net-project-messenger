// test_commands.cpp - the command language (parse_command) and the
// CommandProcessor that stamps request ids and turns commands into Store
// requests.
//
// Plain assertions, no framework.

#include "CommandProcessor.hpp"
#include "Commands.hpp"

#include <iostream>
#include <string>
#include <variant>
#include <vector>

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

constexpr ConvId kActive = 0xabc;
constexpr ConvId kOther = 0x123456789abcdef0;

template <class T>
const T* as(const std::expected<cmd::Command, cmd::CommandError>& r) {
    return r ? std::get_if<T>(&*r) : nullptr;
}

std::string error_of(const std::expected<cmd::Command, cmd::CommandError>& r) {
    return r ? std::string{} : r.error().text;
}

void test_text_and_send() {
    auto r = cmd::parse_command("hello there", kActive);
    check(as<msg::SendText>(r) && as<msg::SendText>(r)->conv == kActive && as<msg::SendText>(r)->body == "hello there",
          "plain text goes to the active conversation");
    r = cmd::parse_command("  spaced  ", kActive);
    check(as<msg::SendText>(r) && as<msg::SendText>(r)->body == "  spaced  ", "plain text keeps its spacing");
    r = cmd::parse_command("hello", kNoId);
    check(error_of(r).starts_with("no active conversation"), "plain text without an active conversation is an error");
    r = cmd::parse_command("//not a command", kActive);
    check(as<msg::SendText>(r) && as<msg::SendText>(r)->body == "/not a command", "// sends text starting with /");

    r = cmd::parse_command("/send 123456789abcdef0 hi  there ", kNoId);
    check(as<msg::SendText>(r) && as<msg::SendText>(r)->conv == kOther && as<msg::SendText>(r)->body == "hi  there ",
          "/send names the conversation; the text is kept as typed");
    r = cmd::parse_command("/send 123456789ABCDEF0 x", kNoId);
    check(as<msg::SendText>(r) && as<msg::SendText>(r)->conv == kOther, "conversation ids are accepted in either case");
    check(!cmd::parse_command("/send xyz hi", kActive), "/send with a bad id");
    check(!cmd::parse_command("/send 1a", kActive), "/send without text");
    check(!cmd::parse_command("/send", kActive), "/send without anything");
    check(!cmd::parse_command("/send 0 hi", kActive), "id 0 is not a conversation");
    check(!cmd::parse_command("/send 11112222333344445 hi", kActive), "an id longer than 16 digits");
}

void test_dial() {
    auto r = cmd::parse_command("/dial 10.0.0.2 7000", kNoId);
    check(as<msg::DialReq>(r) && as<msg::DialReq>(r)->ep == wire::Endpoint{"10.0.0.2", 7000}, "/dial <ip> <port>");
    r = cmd::parse_command("/dial 10.0.0.2:7001", kNoId);
    check(as<msg::DialReq>(r) && as<msg::DialReq>(r)->ep == wire::Endpoint{"10.0.0.2", 7001}, "/dial <ip>:<port>");
    check(!cmd::parse_command("/dial 10.0.0.2", kNoId), "/dial without a port");
    check(!cmd::parse_command("/dial 10.0.0.2 0", kNoId), "/dial to port 0");
    check(!cmd::parse_command("/dial 10.0.0.2 70000", kNoId), "/dial to a port above 65535");
    check(!cmd::parse_command("/dial 10.0.0.2 70x", kNoId), "/dial to a port that is not a number");
    check(!cmd::parse_command("/dial a b c", kNoId), "/dial with too many arguments");
}

void test_conversation_commands() {
    auto r = cmd::parse_command("/open 1a", kNoId);
    check(as<cmd::OpenConv>(r) && as<cmd::OpenConv>(r)->conv == 0x1a, "/open <conv>");
    check(!cmd::parse_command("/open", kActive), "/open needs a conversation");

    r = cmd::parse_command("/reconnect", kActive);
    check(as<msg::ReconnectReq>(r) && as<msg::ReconnectReq>(r)->conv == kActive, "/reconnect: the active conversation");
    r = cmd::parse_command("/close 123456789abcdef0", kActive);
    check(as<msg::CloseReq>(r) && as<msg::CloseReq>(r)->conv == kOther, "/close <conv>");
    check(error_of(cmd::parse_command("/close", kNoId)).starts_with("no active conversation"),
          "/close without a conversation and none active");

    r = cmd::parse_command("/echo on", kActive);
    check(as<cmd::EchoMode>(r) && as<cmd::EchoMode>(r)->on && as<cmd::EchoMode>(r)->conv == kActive, "/echo on");
    r = cmd::parse_command("/echo off 1a", kActive);
    check(as<cmd::EchoMode>(r) && !as<cmd::EchoMode>(r)->on && as<cmd::EchoMode>(r)->conv == 0x1a, "/echo off <conv>");
    check(!cmd::parse_command("/echo maybe", kActive), "/echo with a bad mode");
}

void test_simple_commands() {
    check(as<msg::ListReq>(cmd::parse_command("/list", kNoId)) != nullptr, "/list");
    check(as<msg::QuitReq>(cmd::parse_command("/quit", kNoId)) != nullptr, "/quit");
    check(as<cmd::Help>(cmd::parse_command("/help", kNoId)) != nullptr, "/help");
    check(!cmd::parse_command("/list now", kNoId), "/list takes no arguments");
    check(error_of(cmd::parse_command("/frobnicate", kNoId)).starts_with("unknown command /frobnicate"), "unknown command");
    check(as<cmd::Blank>(cmd::parse_command("", kActive)) != nullptr, "an empty line is blank");
    check(as<cmd::Blank>(cmd::parse_command(" \t ", kActive)) != nullptr, "a line of blanks is blank");
    check(!cmd::parse_command("/", kActive), "a lone / is an unknown command");
}

std::vector<msg::StoreMsg> sent(ThreadQueue<msg::StoreMsg>& q) {
    std::vector<msg::StoreMsg> out;
    while (auto m = q.try_pop()) { out.push_back(std::move(*m)); }
    return out;
}

void test_processor_stamps_and_forwards() {
    ThreadQueue<msg::StoreMsg> store;
    CommandProcessor p(store);
    auto r = p.submit("/dial 10.0.0.2 7000", 7);
    auto out = sent(store);
    check(r && *r == CommandProcessor::Local::None && out.size() == 1 && std::get<msg::DialReq>(out[0]).req == 7,
          "a Store request is pushed with the request id");

    r = p.submit("", 8);
    check(r && *r == CommandProcessor::Local::Blank && sent(store).empty(), "blank: nothing is pushed");
    r = p.submit("/help", 9);
    check(r && *r == CommandProcessor::Local::Help && sent(store).empty(), "help: handled by the frontend");
    r = p.submit("/quit", 10);
    out = sent(store);
    check(r && *r == CommandProcessor::Local::Quit && out.size() == 1 && std::get<msg::QuitReq>(out[0]).req == 10,
          "quit: QuitReq pushed, the frontend is told to start the watchdog");
    r = p.submit("/bogus", 11);
    check(!r && sent(store).empty(), "a parse error pushes nothing");
}

void test_processor_open_and_echo() {
    using Sub = msg::Subscription;
    const Sub active{Sub::Inbound::Forward, Sub::Outbound::Notify, Sub::Lifecycle::Notify};
    const Sub background{Sub::Inbound::Notify, Sub::Outbound::Ignore, Sub::Lifecycle::Notify};

    // TUI-like: the previous conversation goes back to notifications.
    ThreadQueue<msg::StoreMsg> store;
    CommandProcessor p(store, ProcessorMode{active, background});
    check(p.submit("/open 1a", 1).has_value(), "/open");
    auto out = sent(store);
    check(out.size() == 1 && std::get<msg::Subscribe>(out[0]).conv == 0x1a && std::get<msg::Subscribe>(out[0]).req == 1
              && std::get<msg::Subscribe>(out[0]).sub == active && p.active() == 0x1a,
          "/open subscribes the conversation for content and makes it active");
    check(p.submit("plain", 2).has_value(), "plain text after /open");
    out = sent(store);
    check(out.size() == 1 && std::get<msg::SendText>(out[0]).conv == 0x1a, "plain text goes to the opened conversation");

    check(p.submit("/open 2b", 3).has_value(), "/open another");
    out = sent(store);
    check(out.size() == 2 && std::get<msg::Subscribe>(out[0]).conv == 0x1a && std::get<msg::Subscribe>(out[0]).req == 0
              && std::get<msg::Subscribe>(out[0]).sub == background && std::get<msg::Subscribe>(out[1]).conv == 0x2b,
          "the previous conversation goes back to notifications (no reply wanted), then the new one");

    check(p.submit("/echo on", 4).has_value(), "/echo on");
    out = sent(store);
    check(out.size() == 1 && std::get<msg::Subscribe>(out[0]).sub.outbound == Sub::Outbound::Echo
              && std::get<msg::Subscribe>(out[0]).sub.inbound == Sub::Inbound::Forward,
          "/echo on: the active subscription with outbound Echo");

    // CLI-like: no previous subscription to restore.
    ThreadQueue<msg::StoreMsg> store2;
    CommandProcessor cli(store2, ProcessorMode{active, std::nullopt});
    std::ignore = cli.submit("/open 1a", 1);
    std::ignore = cli.submit("/open 2b", 2);
    out = sent(store2);
    check(out.size() == 2, "without a previous mode, /open does not touch the old conversation");

    store2.graceful_shutdown();
    const auto r = cli.submit("/list", 3);
    check(!r && r.error().text == "shutting down", "a closed Store queue: shutting down");
}

}  // namespace

int main() {
    std::cout << "test_commands\n";
    test_text_and_send();
    test_dial();
    test_conversation_commands();
    test_simple_commands();
    test_processor_stamps_and_forwards();
    test_processor_open_and_echo();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
