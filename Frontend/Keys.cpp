#include "Keys.hpp"

#include <string>

namespace keys {

namespace {

msg::Key key(msg::Key::Type type, std::string text = {}) {
    return msg::Key{type, std::move(text)};
}

}  // namespace

ByteResult classify(unsigned char byte) {
    switch (byte) {
        case 0x03: return {ByteKind::Key, key(msg::Key::Exit)};
        case '\r':
        case '\n': return {ByteKind::Key, key(msg::Key::Enter)};
        case 0x7f:
        case 0x08: return {ByteKind::Key, key(msg::Key::Backspace)};
        case '\t': return {ByteKind::Key, key(msg::Key::Tab)};
        case 0x1b: return {ByteKind::Escape, {}};
        default: break;
    }
    if (byte < 0x20) { return {ByteKind::Ignore, {}}; }
    if (byte >= 0x80) { return {ByteKind::Utf8Lead, {}}; }
    return {ByteKind::Key, key(msg::Key::Text, std::string(1, static_cast<char>(byte)))};
}

std::optional<msg::Key> sequence(std::string_view body) {
    if (body == "A") { return key(msg::Key::Up); }
    if (body == "B") { return key(msg::Key::Down); }
    if (body == "C") { return key(msg::Key::Right); }
    if (body == "D") { return key(msg::Key::Left); }
    if (body == "5~") { return key(msg::Key::PageUp); }
    if (body == "6~") { return key(msg::Key::PageDown); }
    return std::nullopt;
}

}  // namespace keys
