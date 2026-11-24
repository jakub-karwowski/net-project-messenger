#include "CommandProcessor.hpp"

#include <type_traits>
#include <utility>

CommandProcessor::CommandProcessor(ThreadQueue<msg::StoreMsg>& store, ProcessorMode mode)
    : store_(store), mode_(mode) {}

bool CommandProcessor::push(msg::StoreMsg m) {
    return store_.push(std::move(m));
}

std::expected<CommandProcessor::Local, cmd::CommandError> CommandProcessor::submit(std::string_view line, ReqId req) {
    auto parsed = cmd::parse_command(line, active_);
    if (!parsed) { return std::unexpected(std::move(parsed.error())); }

    const auto fail = [] { return std::expected<Local, cmd::CommandError>(std::unexpect, "shutting down"); };

    return std::visit([&](auto& c) -> std::expected<Local, cmd::CommandError> {
        using C = std::decay_t<decltype(c)>;
        if constexpr (std::is_same_v<C, cmd::Blank>) {
            return Local::Blank;
        } else if constexpr (std::is_same_v<C, cmd::Help>) {
            return Local::Help;
        } else if constexpr (std::is_same_v<C, cmd::OpenConv>) {
            if (mode_.previous && active_ != kNoId && active_ != c.conv) {
                if (!push(msg::Subscribe{0, active_, *mode_.previous})) { return fail(); }
            }
            if (!push(msg::Subscribe{req, c.conv, mode_.active})) { return fail(); }
            active_ = c.conv;
            return Local::None;
        } else if constexpr (std::is_same_v<C, cmd::EchoMode>) {
            msg::Subscription sub = c.conv == active_ || !mode_.previous ? mode_.active : *mode_.previous;
            sub.outbound = c.on ? msg::Subscription::Outbound::Echo : mode_.active.outbound;
            if (!push(msg::Subscribe{req, c.conv, sub})) { return fail(); }
            return Local::None;
        } else {
            // A Store request as it stands: stamp the request id and send it.
            c.req = req;
            if (!push(std::move(c))) { return fail(); }
            return std::is_same_v<C, msg::QuitReq> ? Local::Quit : Local::None;
        }
    }, *parsed);
}
