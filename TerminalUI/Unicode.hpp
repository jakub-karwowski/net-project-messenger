#pragma once

// Unicode.hpp - the one place that decodes UTF-8 for display.
//
// Text enters the UI as UTF-8 bytes. next_glyph() turns it into Glyphs: one
// user-perceived character each (a base codepoint plus the combining marks after
// it) that knows its display width. Everything after that works in glyphs and
// cells, never in bytes.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>

namespace unicode {

inline constexpr char32_t kReplacement = U'�';

// --- codepoint classes ----------------------------------------------------------

// C0 controls, DEL and C1 controls. ESC is one of them, so any escape sequence
// in untrusted text loses its introducer.
constexpr bool is_control(char32_t cp) noexcept {
    return cp < 0x20 || (cp >= 0x7F && cp < 0xA0);
}

constexpr bool is_combining(char32_t cp) noexcept {
    return (cp >= 0x0300 && cp <= 0x036F)
        || (cp >= 0x1AB0 && cp <= 0x1AFF)
        || (cp >= 0x1DC0 && cp <= 0x1DFF)
        || (cp >= 0x20D0 && cp <= 0x20FF)
        || (cp >= 0xFE20 && cp <= 0xFE2F);
}

// East-Asian Wide/Fullwidth approximation (common ranges).
constexpr bool is_wide(char32_t cp) noexcept {
    return (cp >= 0x1100  && cp <= 0x115F)
        || (cp >= 0x2E80  && cp <= 0x303E)
        || (cp >= 0x3041  && cp <= 0x33FF)
        || (cp >= 0x3400  && cp <= 0x4DBF)
        || (cp >= 0x4E00  && cp <= 0x9FFF)
        || (cp >= 0xA000  && cp <= 0xA4CF)
        || (cp >= 0xAC00  && cp <= 0xD7A3)
        || (cp >= 0xF900  && cp <= 0xFAFF)
        || (cp >= 0xFE30  && cp <= 0xFE4F)
        || (cp >= 0xFF00  && cp <= 0xFF60)
        || (cp >= 0xFFE0  && cp <= 0xFFE6)
        || (cp >= 0x1F300 && cp <= 0x1FAFF)
        || (cp >= 0x20000 && cp <= 0x3FFFD);
}

// Display columns: 0 for controls and combining marks, 2 for wide, else 1.
constexpr int width(char32_t cp) noexcept {
    if (is_control(cp) || is_combining(cp)) { return 0; }
    return is_wide(cp) ? 2 : 1;
}

// --- UTF-8 ------------------------------------------------------------------------

struct Decoded {
    char32_t codepoint;
    size_t length;   // bytes consumed, always >= 1 when pos < size
    bool valid;      // false: malformed, codepoint is kReplacement, length is 1
};

// Decodes the codepoint at `pos`. A malformed sequence (bad lead, missing or bad
// continuation byte, overlong form, surrogate, > U+10FFFF) yields kReplacement
// and consumes one byte, so decoding always resynchronizes.
constexpr Decoded decode(std::string_view s, size_t pos) noexcept {
    if (pos >= s.size()) { return {kReplacement, 0, false}; }
    const auto byte = [&](size_t i) { return static_cast<unsigned char>(s[i]); };
    const unsigned char lead = byte(pos);
    if (lead < 0x80) { return {lead, 1, true}; }

    size_t length = 0;
    char32_t cp = 0;
    char32_t minimum = 0;
    if      ((lead & 0xE0) == 0xC0) { length = 2; cp = lead & 0x1F; minimum = 0x80; }
    else if ((lead & 0xF0) == 0xE0) { length = 3; cp = lead & 0x0F; minimum = 0x800; }
    else if ((lead & 0xF8) == 0xF0) { length = 4; cp = lead & 0x07; minimum = 0x10000; }
    else { return {kReplacement, 1, false}; }

    if (pos + length > s.size()) { return {kReplacement, 1, false}; }
    for (size_t k = 1; k < length; ++k) {
        const unsigned char next = byte(pos + k);
        if ((next & 0xC0) != 0x80) { return {kReplacement, 1, false}; }
        cp = (cp << 6) | (next & 0x3F);
    }
    if (cp < minimum || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
        return {kReplacement, 1, false};
    }
    return {cp, length, true};
}

inline void append_utf8(std::string& out, char32_t cp) {
    if (cp < 0x80) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

// --- Glyph ------------------------------------------------------------------------

// One user-perceived character, stored inline as UTF-8: a base codepoint plus
// the combining marks that follow it. Fixed size, never allocates. A default
// Glyph is blank (drawn as a space).
class Glyph {
    public:
        static constexpr size_t kCapacity = 15;

        constexpr Glyph() = default;

        // `cp` should be printable (not a control); its width comes from width(cp),
        // except that a lone combining mark gets width 0 so the caller can attach it.
        static constexpr Glyph of(char32_t cp) noexcept {
            Glyph glyph;
            glyph.push(cp);
            glyph.width_ = static_cast<uint8_t>(unicode::width(cp) == 2 ? 2 : (is_combining(cp) ? 0 : 1));
            return glyph;
        }

        // Adds a combining mark. False (and unchanged) when it would not fit.
        constexpr bool append(char32_t mark) noexcept { return push(mark); }

        std::string_view text() const noexcept { return {bytes_.data(), size_}; }
        constexpr int width() const noexcept { return width_; }
        constexpr bool empty() const noexcept { return size_ == 0; }

        constexpr bool operator==(const Glyph&) const = default;

    private:
        constexpr bool push(char32_t cp) noexcept {
            char encoded[4] = {};
            size_t n = 0;
            if (cp < 0x80) { encoded[n++] = static_cast<char>(cp); }
            else if (cp < 0x800) {
                encoded[n++] = static_cast<char>(0xC0 | (cp >> 6));
                encoded[n++] = static_cast<char>(0x80 | (cp & 0x3F));
            } else if (cp < 0x10000) {
                encoded[n++] = static_cast<char>(0xE0 | (cp >> 12));
                encoded[n++] = static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                encoded[n++] = static_cast<char>(0x80 | (cp & 0x3F));
            } else {
                encoded[n++] = static_cast<char>(0xF0 | (cp >> 18));
                encoded[n++] = static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
                encoded[n++] = static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                encoded[n++] = static_cast<char>(0x80 | (cp & 0x3F));
            }
            if (size_ + n > kCapacity) { return false; }
            for (size_t i = 0; i < n; ++i) { bytes_[size_ + i] = encoded[i]; }
            size_ = static_cast<uint8_t>(size_ + n);
            return true;
        }

        // Bytes past size_ stay zero, so the defaulted == compares only content.
        std::array<char, kCapacity> bytes_{};
        uint8_t size_ = 0;
        uint8_t width_ = 1;
};

struct GlyphRead {
    Glyph glyph;
    size_t length;   // bytes of the source consumed
};

// Reads the glyph at `pos`: one codepoint plus the combining marks after it
// (marks that no longer fit in a Glyph are dropped, still consumed). Control
// characters and malformed bytes come back as U+FFFD, so nothing read here can
// reach the terminal as a control sequence. A combining mark with no base before
// it comes back on its own, with width 0.
inline GlyphRead next_glyph(std::string_view s, size_t pos) noexcept {
    const Decoded base = decode(s, pos);
    if (base.length == 0) { return {Glyph{}, 0}; }
    const char32_t cp = (!base.valid || is_control(base.codepoint)) ? kReplacement : base.codepoint;
    Glyph glyph = Glyph::of(cp);
    size_t length = base.length;
    if (glyph.width() == 0) { return {glyph, length}; }   // a lone mark

    while (pos + length < s.size()) {
        const Decoded mark = decode(s, pos + length);
        if (!mark.valid || !is_combining(mark.codepoint)) { break; }
        std::ignore = glyph.append(mark.codepoint);
        length += mark.length;
    }
    return {glyph, length};
}

// Display columns of `s`, glyph by glyph.
inline int width(std::string_view s) noexcept {
    int columns = 0;
    for (size_t pos = 0; pos < s.size();) {
        const GlyphRead read = next_glyph(s, pos);
        columns += read.glyph.width();
        pos += read.length;
    }
    return columns;
}

// Makes untrusted text safe to display: every control character except '\n'
// and every malformed byte becomes U+FFFD, and a tab becomes a space.
inline std::string sanitize(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t pos = 0; pos < s.size();) {
        const Decoded d = decode(s, pos);
        pos += d.length;
        if (d.valid && d.codepoint == '\n') { out.push_back('\n'); continue; }
        if (d.valid && d.codepoint == '\t') { out.push_back(' '); continue; }
        append_utf8(out, (!d.valid || is_control(d.codepoint)) ? kReplacement : d.codepoint);
    }
    return out;
}

}  // namespace unicode
