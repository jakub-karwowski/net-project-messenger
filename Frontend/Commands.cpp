#include "Commands.hpp"

#include "Json.hpp"

#include <charconv>
#include <optional>
#include <vector>

namespace cmd {

namespace {

bool is_space(char c) { return c == ' ' || c == '\t'; }

std::string_view trim(std::string_view s) {
    while (!s.empty() && is_space(s.front())) { s.remove_prefix(1); }
    while (!s.empty() && is_space(s.back())) { s.remove_suffix(1); }
    return s;
}

// Splits off the first blank-separated word; `rest` keeps everything after the
// single separator that follows it (so message text keeps its own spacing).
std::string_view next_word(std::string_view& rest) {
    while (!rest.empty() && is_space(rest.front())) { rest.remove_prefix(1); }
    std::size_t end = 0;
    while (end < rest.size() && !is_space(rest[end])) { ++end; }
    const std::string_view word = rest.substr(0, end);
    rest.remove_prefix(end);
    if (!rest.empty()) { rest.remove_prefix(1); }
    return word;
}

std::vector<std::string_view> words(std::string_view s) {
    std::vector<std::string_view> out;
    while (true) {
        const auto w = next_word(s);
        if (w.empty()) { return out; }
        out.push_back(w);
    }
}

std::unexpected<CommandError> error(std::string text) {
    return std::unexpected(CommandError{std::move(text)});
}

std::optional<std::uint16_t> parse_port(std::string_view text) {
    unsigned value = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size() || value == 0 || value > 65535) { return std::nullopt; }
    return static_cast<std::uint16_t>(value);
}

// The conversation named by `word`, or the active one when `word` is empty.
std::expected<ConvId, CommandError> conv_or_active(std::string_view word, ConvId active) {
    if (word.empty()) {
        if (active == kNoId) { return error("no active conversation: give one, or /open one first"); }
        return active;
    }
    const auto id = json::parse_hex_id(word);
    if (!id) { return error("not a conversation id: " + std::string(word)); }
    return *id;
}

std::expected<Command, CommandError> parse_dial(const std::vector<std::string_view>& args) {
    std::string_view host;
    std::string_view port;
    if (args.size() == 1) {
        const auto colon = args[0].rfind(':');
        if (colon == std::string_view::npos) { return error("usage: /dial <ip> <port>"); }
        host = args[0].substr(0, colon);
        port = args[0].substr(colon + 1);
    } else if (args.size() == 2) {
        host = args[0];
        port = args[1];
    } else {
        return error("usage: /dial <ip> <port>");
    }
    const auto p = parse_port(port);
    if (host.empty() || !p) { return error("usage: /dial <ip> <port> (port 1-65535)"); }
    return msg::DialReq{0, wire::Endpoint{std::string(host), *p}};
}

}  // namespace

std::expected<Command, CommandError> parse_command(std::string_view line, ConvId active) {
    if (trim(line).empty()) { return Blank{}; }

    if (!line.starts_with('/') || line.starts_with("//")) {
        const std::string_view text = line.starts_with("//") ? line.substr(1) : line;
        if (active == kNoId) { return error("no active conversation: use /send <conv> <text>, or /open one first"); }
        return msg::SendText{0, active, std::string(text)};
    }

    std::string_view rest = line.substr(1);
    const std::string_view name = next_word(rest);
    const auto args = words(rest);

    if (name == "dial") { return parse_dial(args); }

    if (name == "send") {
        std::string_view text = rest;
        const std::string_view conv_word = next_word(text);
        const auto conv = conv_or_active(conv_word, kNoId);
        if (conv_word.empty() || !conv) { return error("usage: /send <conv> <text>"); }
        if (text.empty()) { return error("usage: /send <conv> <text>"); }
        return msg::SendText{0, *conv, std::string(text)};
    }

    if (name == "open") {
        if (args.size() != 1) { return error("usage: /open <conv>"); }
        const auto conv = conv_or_active(args[0], kNoId);
        if (!conv) { return std::unexpected(conv.error()); }
        return OpenConv{*conv};
    }

    if (name == "reconnect" || name == "close") {
        if (args.size() > 1) { return error("usage: /" + std::string(name) + " [<conv>]"); }
        const auto conv = conv_or_active(args.empty() ? std::string_view{} : args[0], active);
        if (!conv) { return std::unexpected(conv.error()); }
        if (name == "reconnect") { return msg::ReconnectReq{0, *conv}; }
        return msg::CloseReq{0, *conv};
    }

    if (name == "echo") {
        if (args.empty() || args.size() > 2 || (args[0] != "on" && args[0] != "off")) {
            return error("usage: /echo on|off [<conv>]");
        }
        const auto conv = conv_or_active(args.size() == 2 ? args[1] : std::string_view{}, active);
        if (!conv) { return std::unexpected(conv.error()); }
        return EchoMode{*conv, args[0] == "on"};
    }

    if (name == "list" || name == "quit" || name == "help") {
        if (!args.empty()) { return error("/" + std::string(name) + " takes no arguments"); }
        if (name == "list") { return msg::ListReq{}; }
        if (name == "quit") { return msg::QuitReq{}; }
        return Help{};
    }

    return error("unknown command /" + std::string(name) + " (try /help)");
}

std::string_view help_text() noexcept {
    return "/dial <ip> <port>  /send <conv> <text>  <text>  //<text>  /open <conv>  /list  "
           "/reconnect [<conv>]  /close [<conv>]  /echo on|off [<conv>]  /help  /quit";
}

}  // namespace cmd
