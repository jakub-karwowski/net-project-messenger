#include "Style.hpp"

#include <array>
#include <cstddef>
#include <optional>
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

// SGR parameters, split on ';'. An empty field counts as 0 ("\033[m" = reset).
class Parameters {
    public:
        explicit Parameters(std::string_view text) noexcept : text_(text) {}

        // The next parameter, or nullopt once the list is used up.
        std::optional<unsigned> next() noexcept {
            if (done_) { return std::nullopt; }
            unsigned value = 0;
            while (pos_ < text_.size() && text_[pos_] != ';') {
                const char c = text_[pos_++];
                if (c >= '0' && c <= '9' && value < 100000) { value = value * 10 + static_cast<unsigned>(c - '0'); }
            }
            if (pos_ < text_.size()) { ++pos_; } else { done_ = true; }
            return value;
        }

    private:
        std::string_view text_;
        size_t pos_ = 0;
        bool done_ = false;
};

// After 38 or 48: "5;n" (palette) or "2;r;g;b" (true colour).
std::optional<Color> read_extended_color(Parameters& params) noexcept {
    const auto mode = params.next();
    if (mode == 5u) {
        if (const auto index = params.next()) { return Color::ansi256(static_cast<uint8_t>(*index)); }
    } else if (mode == 2u) {
        const auto r = params.next(), g = params.next(), b = params.next();
        if (r && g && b) {
            return Color::rgb(static_cast<uint8_t>(*r), static_cast<uint8_t>(*g), static_cast<uint8_t>(*b));
        }
    }
    return std::nullopt;
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

void apply_sgr(Style& style, std::string_view parameters) noexcept {
    Parameters params(parameters);
    while (const auto p = params.next()) {
        const unsigned code = *p;
        if (code == 0) { style = Style{}; }
        else if (code == 1) { style.attrs |= Style::Bold; }
        else if (code == 2) { style.attrs |= Style::Dim; }
        else if (code == 3) { style.attrs |= Style::Italic; }
        else if (code == 4) { style.attrs |= Style::Underline; }
        else if (code == 7) { style.attrs |= Style::Reverse; }
        else if (code == 22) { style.attrs &= static_cast<uint8_t>(~(Style::Bold | Style::Dim)); }
        else if (code == 23) { style.attrs &= static_cast<uint8_t>(~Style::Italic); }
        else if (code == 24) { style.attrs &= static_cast<uint8_t>(~Style::Underline); }
        else if (code == 27) { style.attrs &= static_cast<uint8_t>(~Style::Reverse); }
        else if (code >= 30 && code <= 37) { style.fg = Color::ansi(static_cast<uint8_t>(code - 30)); }
        else if (code == 38) { if (auto c = read_extended_color(params)) { style.fg = *c; } }
        else if (code == 39) { style.fg = Color{}; }
        else if (code >= 40 && code <= 47) { style.bg = Color::ansi(static_cast<uint8_t>(code - 40)); }
        else if (code == 48) { if (auto c = read_extended_color(params)) { style.bg = *c; } }
        else if (code == 49) { style.bg = Color{}; }
        else if (code >= 90 && code <= 97) { style.fg = Color::ansi(static_cast<uint8_t>(code - 90 + 8)); }
        else if (code >= 100 && code <= 107) { style.bg = Color::ansi(static_cast<uint8_t>(code - 100 + 8)); }
    }
}
