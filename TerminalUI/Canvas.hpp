#pragma once

// Canvas.hpp - where elements draw (design.md §13.2): a view of a FrameBuffer
// with its own origin and a clip rectangle. Coordinates are local to the canvas.
// Nothing drawn through a canvas can land outside its clip, and sub() can only
// shrink the clip, so an element can never paint over its neighbours.

#include "TerminalUI.hpp"
#include "TextLayout.hpp"

#include <optional>
#include <span>
#include <string_view>

class Canvas {
    public:
        struct Point {
            int x;
            int y;
            bool operator==(const Point&) const = default;
        };

        // The whole frame. Cursor requests from any sub-canvas go to `cursor`, in
        // frame coordinates (0-based); both must outlive the canvas.
        Canvas(FrameBuffer& frame, std::optional<Point>& cursor) noexcept;

        // The size this canvas was given (its visible part may be smaller).
        int width() const noexcept { return width_; }
        int height() const noexcept { return height_; }

        // A child area at local (x, y). It is clipped to this canvas, so it can
        // never draw outside it; a negative size counts as 0.
        Canvas sub(int x, int y, int width, int height) const noexcept;

        // Draws one glyph, through FrameBuffer::put (wide glyphs stay whole). A wide
        // glyph cut by the clip edge is shown as a space where its visible half
        // would be.
        void put(int x, int y, const unicode::Glyph& glyph, const Style& style) noexcept;

        // Draws laid-out glyphs from (x, y) along the row; returns the columns used.
        int text(int x, int y, std::span<const ShapedGlyph> glyphs) noexcept;
        // Draws plain text in one style (for labels); returns the columns used.
        // Controls show as U+FFFD, like everywhere else.
        int text(int x, int y, std::string_view plain, const Style& style = {}) noexcept;

        // Fills a rectangle with one glyph.
        void fill(int x, int y, int width, int height, const unicode::Glyph& glyph, const Style& style) noexcept;

        // Asks for the terminal cursor at local (x, y), clamped into the visible
        // area. The last request in a frame wins; none at all for an empty area.
        void set_cursor(int x, int y) noexcept;

    private:
        Canvas(FrameBuffer* frame, std::optional<Point>* cursor, int origin_x, int origin_y,
               int width, int height, int clip_left, int clip_top, int clip_right, int clip_bottom) noexcept;

        // Absolute (frame) coordinates.
        bool visible(int x, int y) const noexcept {
            return x >= clip_left_ && x < clip_right_ && y >= clip_top_ && y < clip_bottom_;
        }

        FrameBuffer* frame_;
        std::optional<Point>* cursor_;
        int origin_x_, origin_y_;   // frame position of local (0, 0)
        int width_, height_;
        // The clip in frame coordinates, right and bottom exclusive.
        int clip_left_, clip_top_, clip_right_, clip_bottom_;
};
