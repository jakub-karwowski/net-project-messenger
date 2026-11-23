#include "Style.hpp"

#include <array>
#include <cstddef>
#include <string>
#include <utility>

namespace {

void append_number(std::string& out, unsigned value) {
    out += std::to_string(value);
}

// `base` is 30 for the foreground and 40 for the background.
void append_color(std::string& out, const Color& color, unsigned base) {
    switch (color.kind) {
        case Color::Kind::Default:
            return;
        case Color::Kind::Ansi16:
            out.push_back(';');
            append_number(out, color.r < 8 ? base + color.r : base + 60 + (color.r - 8u));
            return;
        case Color::Kind::Ansi256:
            out.push_back(';');
            append_number(out, base + 8);
            out += ";5;";
            append_number(out, color.r);
            return;
        case Color::Kind::Rgb:
            out.push_back(';');
            append_number(out, base + 8);
            out += ";2;";
            append_number(out, color.r);
            out.push_back(';');
            append_number(out, color.g);
            out.push_back(';');
            append_number(out, color.b);
            return;
    }
}

}  // namespace

void append_sgr(std::string& out, const Style& style) {
    out += "\033[0";
    static constexpr std::array<std::pair<uint8_t, unsigned>, 5> kAttrs{{
        {Style::Bold, 1}, {Style::Dim, 2}, {Style::Italic, 3}, {Style::Underline, 4}, {Style::Reverse, 7},
    }};
    for (const auto& [flag, code] : kAttrs) {
        if (style.attrs & flag) {
            out.push_back(';');
            append_number(out, code);
        }
    }
    append_color(out, style.fg, 30);
    append_color(out, style.bg, 40);
    out.push_back('m');
}
