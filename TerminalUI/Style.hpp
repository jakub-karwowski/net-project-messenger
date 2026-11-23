#pragma once

// Style.hpp - how a cell looks, as data. Escape sequences exist only at the
// edges: append_sgr() writes one for the terminal, and apply_sgr() reads one
// (only for text that still carries colour codes, until the widgets set
// Style directly).

#include <cstdint>
#include <string>
#include <string_view>

struct Color {
    enum class Kind : uint8_t { Default, Ansi16, Ansi256, Rgb };
    Kind kind = Kind::Default;
    // Ansi16 / Ansi256: the palette index is in r.
    uint8_t r = 0, g = 0, b = 0;

    static constexpr Color ansi(uint8_t index) noexcept { return {Kind::Ansi16, static_cast<uint8_t>(index & 15), 0, 0}; }
    static constexpr Color ansi256(uint8_t index) noexcept { return {Kind::Ansi256, index, 0, 0}; }
    static constexpr Color rgb(uint8_t red, uint8_t green, uint8_t blue) noexcept { return {Kind::Rgb, red, green, blue}; }

    constexpr bool operator==(const Color&) const = default;
};

struct Style {
    enum Attr : uint8_t { Bold = 1, Dim = 2, Italic = 4, Underline = 8, Reverse = 16 };
    Color fg;
    Color bg;
    uint8_t attrs = 0;

    constexpr bool operator==(const Style&) const = default;
};

// Appends the SGR sequence that sets exactly `style`, starting from a reset
// ("\033[0;...m"; "\033[0m" for the default style), so it never depends on the
// terminal's previous state.
void append_sgr(std::string& out, const Style& style);

// Applies the parameters of one SGR sequence (the text between "\033[" and "m",
// e.g. "1;31" or "38;5;200") to `style`. Unknown parameters are ignored.
void apply_sgr(Style& style, std::string_view parameters) noexcept;
