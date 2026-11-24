#include "CliFormat.hpp"

#include "Json.hpp"

#include <chrono>
#include <ctime>
#include <format>

namespace cli {

namespace {

std::string iso_time(msg::WallClock::time_point at) {
    using namespace std::chrono;
    const auto ms = duration_cast<milliseconds>(at.time_since_epoch()).count();
    const std::time_t seconds = static_cast<std::time_t>(ms / 1000);
    std::tm utc{};
    gmtime_r(&seconds, &utc);
    return std::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
                       utc.tm_hour, utc.tm_min, utc.tm_sec, ms % 1000);
}

std::string endpoint_text(const wire::Endpoint& ep) {
    return std::format("{}:{}", ep.address, ep.port);
}

// The fields of one stored message, shared by "msg" and the entries of "snapshot".
json::Object& message_fields(json::Object& o, MsgId id, const msg::StoredMessage& m) {
    return o.num("id", id)
        .str("from", m.sender == kNoId ? "me" : "peer")
        .str("body", m.body)
        .str("state", msg::to_string(m.state))
        .str("at", iso_time(m.at));
}

}  // namespace

std::string ready(const wire::Endpoint& listening, std::size_t pts) {
    return json::Object{}.str("ev", "ready").str("listen", endpoint_text(listening)).num("pts", pts).done();
}

std::string reply(const msg::Reply& r) {
    json::Object o;
    o.str("ev", "reply").num("req", r.req).boolean("ok", r.ok);
    if (r.conv != kNoId) { o.id("conv", r.conv); }
    if (!r.ok) { o.str("error", r.error); }
    return std::move(o).done();
}

std::string reply_error(ReqId req, std::string_view error) {
    return reply(msg::Reply{req, false, kNoId, std::string(error)});
}

std::string reply_ok(ReqId req) {
    return reply(msg::Reply{req, true, kNoId, {}});
}

std::string conv_state(const msg::ConvState& s) {
    json::Object o;
    o.str("ev", "conv_state").id("conv", s.conv).str("status", msg::to_string(s.status));
    if (!s.detail.empty()) { o.str("detail", s.detail); }
    return std::move(o).done();
}

std::string message(const msg::MsgAppended& m) {
    json::Object o;
    o.str("ev", "msg").id("conv", m.conv);
    message_fields(o, m.id, m.msg);
    return std::move(o).done();
}

std::string msg_status(const msg::MsgStatus& s) {
    return json::Object{}.str("ev", "msg_status").id("conv", s.conv).num("id", s.id).str("state", msg::to_string(s.state)).done();
}

std::string snapshot(const msg::ConvSnapshot& s) {
    json::Array msgs;
    for (const auto& m : s.msgs) {
        json::Object entry;
        message_fields(entry, m.id, m);
        msgs.raw(std::move(entry).done());
    }
    return json::Object{}
        .str("ev", "snapshot")
        .id("conv", s.conv)
        .str("status", msg::to_string(s.status))
        .raw("msgs", std::move(msgs).done())
        .done();
}

std::string convs(const msg::ConvList& l) {
    json::Array list;
    for (const auto& c : l.convs) {
        list.raw(json::Object{}
                     .id("conv", c.conv)
                     .str("status", msg::to_string(c.status))
                     .str("remote", endpoint_text(c.remote))
                     .num("unread", c.unread)
                     .done());
    }
    return json::Object{}.str("ev", "convs").raw("convs", std::move(list).done()).done();
}

std::string notice(std::string_view text) {
    return json::Object{}.str("ev", "notice").str("text", text).done();
}

std::string shutdown_forced(const Escalation& e) {
    json::Array stuck;
    for (const auto& name : e.stuck) { stuck.str(name); }
    return json::Object{}
        .str("ev", "shutdown_forced")
        .str("phase", to_string(e.phase))
        .str("reason", e.reason)
        .raw("stuck", std::move(stuck).done())
        .done();
}

std::string shutdown_done() {
    return json::Object{}.str("ev", "shutdown_done").done();
}

std::string fatal(std::string_view error) {
    return json::Object{}.str("ev", "fatal").str("error", error).done();
}

}  // namespace cli
