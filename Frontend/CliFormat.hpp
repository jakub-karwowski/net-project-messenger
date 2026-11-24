#pragma once

// The CLI's output events, one JSON object per line. Every object has "ev".
//
//   ready           {"ev":"ready","listen":"127.0.0.1:40123","pts":2}        always first
//   reply           {"ev":"reply","req":3,"ok":true,"conv":"…"}               one per request
//                   {"ev":"reply","req":4,"ok":false,"error":"…"}
//   conv_state      {"ev":"conv_state","conv":"…","status":"connected","detail":"…"}
//   msg             {"ev":"msg","conv":"…","id":0,"from":"me","body":"…","state":"pending","at":"…"}
//   msg_status      {"ev":"msg_status","conv":"…","id":0,"state":"sent"}
//   snapshot        {"ev":"snapshot","conv":"…","status":"…","msgs":[{"id":…,"from":…,"body":…,"state":…,"at":…}]}
//   convs           {"ev":"convs","convs":[{"conv":"…","status":"…","remote":"ip:port","unread":0}]}
//   notice          {"ev":"notice","text":"…"}
//   shutdown_forced {"ev":"shutdown_forced","phase":"emergency","reason":"…","stuck":["…"]}
//   shutdown_done   {"ev":"shutdown_done"}                                      always last
//   fatal           {"ev":"fatal","error":"…"}                                  start-up failed
//
// Adding a field is a compatible change; renaming or removing one is not.

#include "Messages.hpp"
#include "Supervisor.hpp"

#include <cstddef>
#include <string>
#include <string_view>

namespace cli {

std::string ready(const wire::Endpoint& listening, std::size_t pts);
std::string reply(const msg::Reply& r);
std::string reply_error(ReqId req, std::string_view error);
std::string reply_ok(ReqId req);
std::string conv_state(const msg::ConvState& s);
std::string message(const msg::MsgAppended& m);
std::string msg_status(const msg::MsgStatus& s);
std::string snapshot(const msg::ConvSnapshot& s);
std::string convs(const msg::ConvList& l);
std::string notice(std::string_view text);
std::string shutdown_forced(const Escalation& e);
std::string shutdown_done();
std::string fatal(std::string_view error);

}  // namespace cli
