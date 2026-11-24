#pragma once

// Turning terminal input bytes into keys. Pure: the keyboard thread does the
// reading (and the waiting for the rest of a sequence) and asks these.

#include "Messages.hpp"

#include <optional>
#include <string_view>

namespace keys {

enum class ByteKind {
    Key,        // a key by itself (`key` is set)
    Escape,     // ESC: the start of a sequence, or a lone Escape key
    Utf8Lead,   // the first byte of a multi-byte character
    Ignore,     // a control byte with no meaning here
};

struct ByteResult {
    ByteKind kind = ByteKind::Ignore;
    msg::Key key{};
};

// Printable ASCII is Text; Enter is CR or LF; Backspace is DEL or BS; Tab;
// Ctrl+C (0x03) is Exit. Other control bytes are ignored.
ByteResult classify(unsigned char byte);

// The key for a CSI or SS3 sequence given without its "ESC [" or "ESC O":
// "A" Up, "B" Down, "C" Right, "D" Left, "5~" PageUp, "6~" PageDown. Anything
// else (and modifiers, as in "1;5C") is not a key this UI uses.
std::optional<msg::Key> sequence(std::string_view body);

// True for the byte that ends a CSI sequence (0x40-0x7E).
constexpr bool is_final(unsigned char byte) noexcept { return byte >= 0x40 && byte <= 0x7e; }

}  // namespace keys
