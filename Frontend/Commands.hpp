#pragma once

// Parsing of the command language both frontends share. Pure: no I/O, no
// state beyond the active conversation passed in.
//
//   /dial <ip> <port> | /dial <ip>:<port>    connect to a new or known peer
//   /send <conv> <text>                      send to a conversation
//   <text>                                   send to the active conversation
//   //<text>                                 send text that starts with '/'
//   /open <conv>                             make <conv> active (and get its history)
//   /list                                    list conversations
//   /reconnect [<conv>]                      reconnect (default: the active one)
//   /close [<conv>]                          disconnect (default: the active one)
//   /echo on|off [<conv>]                    bounce received messages back (stress mode)
//   /help                                    list the commands
//   /quit                                    shut down
//
// <conv> is a conversation id as printed (hex).

#include "Ids.hpp"
#include "Messages.hpp"

#include <expected>
#include <string>
#include <string_view>
#include <variant>

namespace cmd {

struct Blank {};                          // an empty or all-blank line: nothing to do
struct OpenConv { ConvId conv = kNoId; };
struct EchoMode { ConvId conv = kNoId; bool on = false; };
struct Help {};

// Store requests carry req = 0 here; the CommandProcessor stamps the real one.
using Command = std::variant<Blank, msg::SendText, msg::DialReq, msg::ReconnectReq, msg::CloseReq, msg::ListReq,
                             msg::QuitReq, OpenConv, EchoMode, Help>;

struct CommandError {
    std::string text;
};

std::expected<Command, CommandError> parse_command(std::string_view line, ConvId active);

// The command list shown by /help.
std::string_view help_text() noexcept;

}  // namespace cmd
