#pragma once

// TextLayout.hpp - text is decoded and wrapped once, here (design.md §13.2).
//
// StyledText is what widgets produce: runs of UTF-8 text, each with a Style,
// never escape codes. TextLayout::wrap() turns it into glyphs (each knowing its
// width, its style and the source bytes it came from) and splits them into lines
// of a given width. Drawing then copies glyphs into cells; nothing downstream
// decodes UTF-8 or measures text again.

#include "Style.hpp"
#include "Unicode.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

// Text with styles. Byte offsets used by TextLayout count through the runs in
// order, as if they were one string. The text is kept as given; TextLayout
// shows control characters and malformed bytes as U+FFFD, so nothing here can
// reach the terminal as a control sequence.
class StyledText {
    public:
        struct Run {
            Style style;
            std::string text;
        };

        StyledText() = default;
        explicit StyledText(std::string_view text, Style style = {}) { add(text, style); }

        // Appends text; joins the previous run when the style is the same.
        StyledText& add(std::string_view text, Style style = {});

        std::span<const Run> runs() const noexcept { return runs_; }
        size_t size() const noexcept { return size_; }   // total bytes
        bool empty() const noexcept { return size_ == 0; }

    private:
        std::vector<Run> runs_;
        size_t size_ = 0;
};

// One glyph of laid-out text.
struct ShapedGlyph {
    unicode::Glyph glyph;
    Style style;
    uint32_t offset;   // first source byte
    uint32_t length;   // source bytes it covers (several for a glyph with marks)
};

class TextLayout {
    public:
        struct Line {
            uint32_t first = 0;          // glyph index range [first, last)
            uint32_t last = 0;
            int columns = 0;             // display width
            uint32_t source_begin = 0;   // source offset where the line starts
        };
        struct Position {
            size_t line;
            int column;
        };

        // Wraps `text` into lines of at most `width` columns (width < 1 counts
        // as 1). Rules:
        //   - '\n' ends a line; text ending in '\n' ends with an empty line, and
        //     empty text is one empty line, so there is always a line for a cursor;
        //   - a line breaks at its last space when the next glyph does not fit; the
        //     space is dropped (it still belongs to the line it ended, for locate());
        //   - a word longer than the line is broken where it overflows;
        //   - a glyph wider than the whole line gets a line of its own;
        //   - a tab is shown as one space; controls and bad bytes as U+FFFD.
        static TextLayout wrap(const StyledText& text, int width);

        std::span<const ShapedGlyph> glyphs() const noexcept { return glyphs_; }
        std::span<const Line> lines() const noexcept { return lines_; }
        std::span<const ShapedGlyph> line_glyphs(size_t line) const noexcept;

        // Where the source byte `offset` is drawn: the line that contains it and
        // the column of its glyph. An offset inside a multi-byte glyph snaps to that
        // glyph's start; an offset past the end is the end of the last line.
        Position locate(size_t offset) const noexcept;

    private:
        std::vector<ShapedGlyph> glyphs_;
        std::vector<Line> lines_;
};
