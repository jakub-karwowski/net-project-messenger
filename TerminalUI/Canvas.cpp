#include "Canvas.hpp"

#include <algorithm>

Canvas::Canvas(FrameBuffer& frame, std::optional<Point>& cursor) noexcept
    : Canvas(&frame, &cursor, 0, 0, frame.width, frame.height, 0, 0, frame.width, frame.height) {}

Canvas::Canvas(FrameBuffer* frame, std::optional<Point>* cursor, int origin_x, int origin_y,
               int width, int height, int clip_left, int clip_top, int clip_right, int clip_bottom) noexcept
    : frame_(frame), cursor_(cursor), origin_x_(origin_x), origin_y_(origin_y),
      width_(width), height_(height),
      clip_left_(clip_left), clip_top_(clip_top), clip_right_(clip_right), clip_bottom_(clip_bottom) {}

Canvas Canvas::sub(int x, int y, int width, int height) const noexcept {
    width = std::max(width, 0);
    height = std::max(height, 0);
    const int left = origin_x_ + x;
    const int top = origin_y_ + y;
    // Intersect with this clip; an empty result keeps right >= left.
    const int clip_left = std::clamp(left, clip_left_, clip_right_);
    const int clip_top = std::clamp(top, clip_top_, clip_bottom_);
    const int clip_right = std::clamp(left + width, clip_left, clip_right_);
    const int clip_bottom = std::clamp(top + height, clip_top, clip_bottom_);
    return Canvas(frame_, cursor_, left, top, width, height, clip_left, clip_top, clip_right, clip_bottom);
}

void Canvas::put(int x, int y, const unicode::Glyph& glyph, const Style& style) noexcept {
    const int ax = origin_x_ + x;
    const int ay = origin_y_ + y;
    const bool wide = glyph.width() == 2;
    if (!visible(ax, ay)) {
        // Only the right half of a wide glyph is inside: show a space there.
        if (wide && visible(ax + 1, ay)) { frame_->put(ay, ax + 1, unicode::Glyph{}, style); }
        return;
    }
    if (wide && !visible(ax + 1, ay)) {
        frame_->put(ay, ax, unicode::Glyph{}, style);   // the right half would be clipped
        return;
    }
    frame_->put(ay, ax, glyph, style);
}

int Canvas::text(int x, int y, std::span<const ShapedGlyph> glyphs) noexcept {
    int column = x;
    for (const ShapedGlyph& g : glyphs) {
        put(column, y, g.glyph, g.style);
        column += g.glyph.width();
    }
    return column - x;
}

int Canvas::text(int x, int y, std::string_view plain, const Style& style) noexcept {
    int column = x;
    for (size_t pos = 0; pos < plain.size();) {
        const unicode::GlyphRead read = unicode::next_glyph(plain, pos);
        pos += read.length;
        put(column, y, read.glyph, style);
        column += read.glyph.width();
    }
    return column - x;
}

void Canvas::fill(int x, int y, int width, int height, const unicode::Glyph& glyph, const Style& style) noexcept {
    const int step = std::max(glyph.width(), 1);
    for (int row = y; row < y + height; ++row) {
        for (int column = x; column < x + width; column += step) { put(column, row, glyph, style); }
    }
}

void Canvas::set_cursor(int x, int y) noexcept {
    if (clip_left_ >= clip_right_ || clip_top_ >= clip_bottom_) { return; }
    *cursor_ = Point{std::clamp(origin_x_ + x, clip_left_, clip_right_ - 1),
                     std::clamp(origin_y_ + y, clip_top_, clip_bottom_ - 1)};
}

void Canvas::write_legacy(int x, int y, std::string_view text) noexcept {
    Style style{};
    int column = x;
    for (size_t i = 0; i < text.size();) {
        if (text[i] == '\n') {
            ++y;
            column = x;
            ++i;
            continue;
        }
        if (text[i] == '\r') {
            column = x;
            ++i;
            continue;
        }
        if (text[i] == '\033' && i + 1 < text.size() && text[i + 1] == '[') {
            // CSI: parameters, then a final byte in 0x40..0x7E. Only SGR is kept.
            const size_t parameters = i + 2;
            size_t end = parameters;
            while (end < text.size()
                   && !(static_cast<unsigned char>(text[end]) >= 0x40
                        && static_cast<unsigned char>(text[end]) <= 0x7E)) { ++end; }
            if (end < text.size() && text[end] == 'm') {
                apply_sgr(style, text.substr(parameters, end - parameters));
            }
            i = std::min(end + 1, text.size());
            continue;
        }
        const unicode::GlyphRead read = unicode::next_glyph(text, i);
        i += read.length;
        put(column, y, read.glyph, style);
        column += read.glyph.width();
    }
}
