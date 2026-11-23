#include "TextLayout.hpp"

#include <algorithm>
#include <limits>

StyledText& StyledText::add(std::string_view text, Style style) {
    if (text.empty()) { return *this; }
    if (!runs_.empty() && runs_.back().style == style) {
        runs_.back().text.append(text);
    } else {
        runs_.push_back(Run{style, std::string(text)});
    }
    size_ += text.size();
    return *this;
}

std::span<const ShapedGlyph> TextLayout::line_glyphs(size_t line) const noexcept {
    if (line >= lines_.size()) { return {}; }
    const Line& l = lines_[line];
    return std::span<const ShapedGlyph>(glyphs_).subspan(l.first, l.last - l.first);
}

namespace {

// Builds lines while glyphs are added one at a time. Every glyph goes into
// TextLayout's glyph list; a line is a range of it, so a space dropped at a
// break simply lies between two ranges.
class LineBuilder {
    public:
        LineBuilder(std::vector<ShapedGlyph>& glyphs, std::vector<TextLayout::Line>& lines, int width)
            : glyphs_(glyphs), lines_(lines), width_(std::max(width, 1)) {}

        void add(const ShapedGlyph& shaped) {
            const uint32_t index = static_cast<uint32_t>(glyphs_.size());
            glyphs_.push_back(shaped);
            const int w = shaped.glyph.width();
            const bool is_space = shaped.glyph.text() == " ";
            if (is_space) { last_space_ = index; }

            if (columns_ == 0 || columns_ + w <= width_) {
                columns_ += w;
                open_ = false;
                return;
            }

            if (is_space) {
                // The space itself overflows: break here and drop it.
                end_line(index, shaped.offset + shaped.length, index + 1);
                return;
            }
            if (last_space_ != kNone && last_space_ > first_) {
                // Break at the last space; the word in progress moves down.
                const ShapedGlyph& space = glyphs_[last_space_];
                const uint32_t resume = last_space_ + 1;
                end_line(last_space_, space.offset + space.length, resume);
                for (uint32_t i = resume; i <= index; ++i) { columns_ += glyphs_[i].glyph.width(); }
                return;
            }
            // No space to break at: the word is broken here.
            end_line(index, shaped.offset, index);
            columns_ = w;
        }

        // '\n' at source byte `offset`.
        void newline(uint32_t offset) {
            const uint32_t here = static_cast<uint32_t>(glyphs_.size());
            end_line(here, offset + 1, here);
            open_ = true;
        }

        // The last line: always after a '\n' or for empty text, otherwise only
        // when it has glyphs (a break that ends the text adds no empty line).
        void finish() {
            const uint32_t here = static_cast<uint32_t>(glyphs_.size());
            if (open_ || first_ < here) { push(here); }
        }

    private:
        static constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();

        void push(uint32_t last) {
            int columns = 0;
            for (uint32_t i = first_; i < last; ++i) { columns += glyphs_[i].glyph.width(); }
            lines_.push_back(TextLayout::Line{first_, last, columns, source_begin_});
        }

        // Ends the current line before glyph `last`; the next one starts at glyph
        // `next_first`, source byte `next_source`.
        void end_line(uint32_t last, uint32_t next_source, uint32_t next_first) {
            push(last);
            first_ = next_first;
            source_begin_ = next_source;
            columns_ = 0;
            last_space_ = kNone;
            open_ = false;
        }

        std::vector<ShapedGlyph>& glyphs_;
        std::vector<TextLayout::Line>& lines_;
        int width_;
        uint32_t first_ = 0;
        uint32_t source_begin_ = 0;
        int columns_ = 0;
        uint32_t last_space_ = kNone;
        bool open_ = true;   // a line has been started that must be emitted even if empty
};

}  // namespace

TextLayout TextLayout::wrap(const StyledText& text, int width) {
    TextLayout layout;
    layout.glyphs_.reserve(text.size());
    LineBuilder builder(layout.glyphs_, layout.lines_, width);

    uint32_t base = 0;              // source offset of the current run
    bool can_attach = false;        // a glyph precedes, on the same line, that a mark may join
    for (const StyledText::Run& run : text.runs()) {
        const std::string_view s = run.text;
        for (size_t pos = 0; pos < s.size();) {
            const uint32_t offset = base + static_cast<uint32_t>(pos);
            if (s[pos] == '\n') {
                builder.newline(offset);
                can_attach = false;
                ++pos;
                continue;
            }
            if (s[pos] == '\t') {
                builder.add(ShapedGlyph{unicode::Glyph::of(U' '), run.style, offset, 1});
                can_attach = true;
                ++pos;
                continue;
            }

            const unicode::GlyphRead read = unicode::next_glyph(s, pos);
            pos += read.length;
            const uint32_t length = static_cast<uint32_t>(read.length);
            if (read.glyph.width() == 0) {
                // A combining mark with no base in this run: it joins the glyph
                // before it (e.g. a base in the previous run), or a blank.
                if (can_attach && !layout.glyphs_.empty()) {
                    ShapedGlyph& previous = layout.glyphs_.back();
                    std::ignore = previous.glyph.append(unicode::decode(read.glyph.text(), 0).codepoint);
                    previous.length += length;
                    continue;
                }
                unicode::Glyph blank = unicode::Glyph::of(U' ');
                std::ignore = blank.append(unicode::decode(read.glyph.text(), 0).codepoint);
                builder.add(ShapedGlyph{blank, run.style, offset, length});
                can_attach = true;
                continue;
            }
            builder.add(ShapedGlyph{read.glyph, run.style, offset, length});
            can_attach = true;
        }
        base += static_cast<uint32_t>(s.size());
    }
    builder.finish();
    return layout;
}

TextLayout::Position TextLayout::locate(size_t offset) const noexcept {
    if (lines_.empty()) { return {0, 0}; }
    // The last line starting at or before `offset`.
    size_t line = 0;
    while (line + 1 < lines_.size() && lines_[line + 1].source_begin <= offset) { ++line; }

    int column = 0;
    for (const ShapedGlyph& g : line_glyphs(line)) {
        if (g.offset + g.length > offset) { break; }   // `offset` is at or inside this glyph
        column += g.glyph.width();
    }
    return {line, column};
}
