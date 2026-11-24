#pragma once

// The little JSON the CLI writes: objects and arrays of strings, integers and
// booleans, built in place as one line.
//
// Strings are escaped so the output is always valid JSON in valid UTF-8, and so
// nothing a peer sends can act on a terminal that shows it: C0 controls, DEL
// and the C1 range become \uXXXX, as do U+2028/U+2029 (line breaks to
// JavaScript), and invalid UTF-8 becomes U+FFFD.
//
// Ids are 64-bit and lose precision as JSON numbers, so they are written as
// 16-digit lowercase hex strings.

#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace json {

namespace detail {

constexpr char kHex[] = "0123456789abcdef";

inline void append_u_escape(std::string& out, std::uint32_t cp) {
    out += "\\u";
    for (int shift = 12; shift >= 0; shift -= 4) { out += kHex[(cp >> shift) & 0xf]; }
}

// Decodes one UTF-8 sequence at `pos`; on success returns the code point and
// sets `len`. Rejects overlong forms, surrogates and values above U+10FFFF.
inline std::optional<std::uint32_t> decode_utf8(std::string_view s, std::size_t pos, std::size_t& len) {
    const auto b0 = static_cast<unsigned char>(s[pos]);
    std::uint32_t cp = 0;
    std::uint32_t min = 0;
    if (b0 < 0x80) { len = 1; return b0; }
    if ((b0 & 0xe0) == 0xc0) { len = 2; cp = b0 & 0x1f; min = 0x80; }
    else if ((b0 & 0xf0) == 0xe0) { len = 3; cp = b0 & 0x0f; min = 0x800; }
    else if ((b0 & 0xf8) == 0xf0) { len = 4; cp = b0 & 0x07; min = 0x10000; }
    else { return std::nullopt; }
    if (pos + len > s.size()) { return std::nullopt; }
    for (std::size_t i = 1; i < len; ++i) {
        const auto b = static_cast<unsigned char>(s[pos + i]);
        if ((b & 0xc0) != 0x80) { return std::nullopt; }
        cp = (cp << 6) | (b & 0x3f);
    }
    if (cp < min || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) { return std::nullopt; }
    return cp;
}

}  // namespace detail

// Appends `text` as a quoted JSON string.
inline void append_string(std::string& out, std::string_view text) {
    out += '"';
    for (std::size_t pos = 0; pos < text.size();) {
        std::size_t len = 1;
        const auto cp = detail::decode_utf8(text, pos, len);
        if (!cp) {
            out += "\\ufffd";
            pos += 1;   // resynchronise on the next byte
            continue;
        }
        switch (*cp) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
                if (*cp < 0x20 || (*cp >= 0x7f && *cp <= 0x9f) || *cp == 0x2028 || *cp == 0x2029) {
                    detail::append_u_escape(out, *cp);
                } else {
                    out.append(text.substr(pos, len));
                }
        }
        pos += len;
    }
    out += '"';
}

inline std::string hex_id(std::uint64_t id) {
    std::string out(16, '0');
    for (int i = 15; i >= 0; --i) {
        out[static_cast<std::size_t>(i)] = detail::kHex[id & 0xf];
        id >>= 4;
    }
    return out;
}

// Accepts 1 to 16 hex digits, either case. 0 is not an id.
inline std::optional<std::uint64_t> parse_hex_id(std::string_view text) {
    if (text.empty() || text.size() > 16) { return std::nullopt; }
    std::uint64_t value = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value, 16);
    if (ec != std::errc{} || end != text.data() + text.size() || value == 0) { return std::nullopt; }
    return value;
}

// Builds one JSON object, fields in the order they are added.
class Object {
public:
    Object& str(std::string_view key, std::string_view value) {
        add_key(key);
        append_string(out_, value);
        return *this;
    }
    Object& id(std::string_view key, std::uint64_t value) { return str(key, hex_id(value)); }
    Object& num(std::string_view key, std::uint64_t value) {
        add_key(key);
        out_ += std::to_string(value);
        return *this;
    }
    Object& boolean(std::string_view key, bool value) {
        add_key(key);
        out_ += value ? "true" : "false";
        return *this;
    }
    // `value` must already be valid JSON (a nested object or array).
    Object& raw(std::string_view key, std::string_view value) {
        add_key(key);
        out_ += value;
        return *this;
    }

    // Finishes the text and moves it out; the builder is spent afterwards.
    std::string done() {
        out_ += '}';
        return std::move(out_);
    }

private:
    void add_key(std::string_view key) {
        out_ += first_ ? "" : ",";
        first_ = false;
        append_string(out_, key);
        out_ += ':';
    }

    std::string out_ = "{";
    bool first_ = true;
};

// Builds one JSON array of already-encoded values.
class Array {
public:
    Array& raw(std::string_view value) {
        out_ += first_ ? "" : ",";
        first_ = false;
        out_ += value;
        return *this;
    }
    Array& str(std::string_view value) {
        out_ += first_ ? "" : ",";
        first_ = false;
        append_string(out_, value);
        return *this;
    }

    // Finishes the text and moves it out; the builder is spent afterwards.
    std::string done() {
        out_ += ']';
        return std::move(out_);
    }

private:
    std::string out_ = "[";
    bool first_ = true;
};

}  // namespace json
