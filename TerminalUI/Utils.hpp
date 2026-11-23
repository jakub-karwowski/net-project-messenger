#pragma once

// Utils.hpp - byte-level UTF-8 helpers for *editing* text (InputModel, the key
// reader): moving between codepoint boundaries in a byte string. Display work
// (glyphs, widths, wrapping) lives in Unicode.hpp and TextLayout.hpp.

#include <algorithm>
#include <cstddef>
#include <string_view>

namespace utils {

namespace utf8 {

    inline bool is_continuation(unsigned char byte) noexcept {
        return (byte & 0xC0) == 0x80;
    }

    // Number of bytes in the UTF-8 sequence introduced by `lead` (1 if invalid).
    inline size_t sequence_length(unsigned char lead) noexcept {
        if (lead < 0x80)           { return 1; }
        if ((lead & 0xE0) == 0xC0) { return 2; }
        if ((lead & 0xF0) == 0xE0) { return 3; }
        if ((lead & 0xF8) == 0xF0) { return 4; }
        return 1;
    }

    // First codepoint boundary strictly after `i` (clamped to s.size()).
    inline size_t next(std::string_view s, size_t i) noexcept {
        if (i >= s.size()) { return s.size(); }
        return std::min(i + sequence_length(static_cast<unsigned char>(s[i])), s.size());
    }

    // First codepoint boundary strictly before `i`.
    inline size_t prev(std::string_view s, size_t i) noexcept {
        if (i == 0) { return 0; }
        size_t j = std::min(i, s.size()) - 1;
        while (j > 0 && is_continuation(static_cast<unsigned char>(s[j]))) { --j; }
        return j;
    }

    // Rounds `i` down to the nearest codepoint boundary.
    inline size_t align(std::string_view s, size_t i) noexcept {
        if (i >= s.size()) { return s.size(); }
        while (i > 0 && is_continuation(static_cast<unsigned char>(s[i]))) { --i; }
        return i;
    }

} // namespace utf8

} // namespace utils
