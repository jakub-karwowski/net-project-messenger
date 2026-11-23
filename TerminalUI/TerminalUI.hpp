#pragma once

#include "Style.hpp"
#include "Unicode.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// A screen-sized grid of display cells. Renderables fill this instead of a
// string of escape sequences: two frames can then be compared cell by cell, so
// TerminalUI only redraws the rows that actually changed.
struct FrameBuffer {
    struct Cell {
        // What is drawn here; a blank glyph is drawn as a space.
        unicode::Glyph glyph;
        // How it is drawn. Every cell states its full style, so a row never
        // depends on the colour state left by the cells before it.
        Style style;
        // This column is the right half of a wide glyph starting on its left;
        // it emits nothing of its own.
        bool continuation{false};

        bool operator==(const Cell& other) const = default;
    };

    int width{0};
    int height{0};
    std::vector<Cell> cells;   // row-major, width * height cells

    FrameBuffer() = default;
    FrameBuffer(int columns, int rows);

    // Sizes to `columns` x `rows` and blanks every cell, so a renderable always
    // produces a complete frame instead of patching the previous one.
    void resize(int columns, int rows);

    void clear() noexcept;

    bool contains(int row, int column) const noexcept {
        return row >= 0 && row < height && column >= 0 && column < width;
    }

    Cell& at(int row, int column) noexcept {
        return cells[static_cast<size_t>(row) * static_cast<size_t>(width)
                   + static_cast<size_t>(column)];
    }

    const Cell& at(int row, int column) const noexcept {
        return cells[static_cast<size_t>(row) * static_cast<size_t>(width)
                   + static_cast<size_t>(column)];
    }

    // Draws `glyph` at (row, column), keeping wide glyphs whole:
    //   - a wide glyph partly covered (either half) is blanked entirely, so no
    //     half of a character is ever left behind;
    //   - a wide glyph whose right half would fall outside the frame is drawn as
    //     a space instead.
    // Outside the frame: nothing happens. A glyph of width 0 (a lone combining
    // mark) is added to the glyph on its left instead, if there is one.
    void put(int row, int column, const unicode::Glyph& glyph, const Style& style) noexcept;

    // True when `row` renders identically in both frames. Frames of different
    // sizes never match, so a resize forces a full repaint.
    bool row_equal(const FrameBuffer& other, int row) const noexcept;

    // The bytes that reproduce `row`: an SGR sequence wherever the style changes
    // from the previous cell (and a reset at the end if needed), each glyph once,
    // blanks as spaces.
    std::string row_text(int row) const;

    bool operator==(const FrameBuffer& other) const = default;

private:
    // Blanks whatever occupies `column`; when that is half of a wide glyph, the
    // other half too.
    void detach(int row, int column) noexcept;
};

struct Renderable {
    struct Cursor {
        int x{1};
        int y{1};
        enum struct State {
            NOT_VISIBLE = 0,
            VISIBLE = 1,
        } state {State::VISIBLE};

        bool operator==(const Cursor& other) const = default;
    };
    virtual ~Renderable() = default;
    // Fills `frame` with the whole frame (cells, not escape sequences) and
    // returns where the cursor should end up once it has been drawn, if the
    // renderable asks for one at all.
    virtual std::optional<Cursor> write_frame(FrameBuffer& frame) = 0;
    virtual void resize(int width, int height) = 0;
};

class TerminalUI {
public:
    // Outcome of a timed read of a single input byte.
    enum class ReadResult { Byte, Timeout, Eof };

    TerminalUI();
    TerminalUI(const TerminalUI&) = delete;
    TerminalUI& operator=(const TerminalUI&) = delete;
    TerminalUI(TerminalUI&& other) = delete;
    TerminalUI& operator=(TerminalUI&& other) = delete;
    ~TerminalUI();

    bool render(Renderable& renderable);
    void clear() const noexcept;

    // Waits up to `timeout_ms` for one byte on stdin (negative waits forever).
    // The constructor disables ISIG, so Ctrl+C is delivered here as the byte
    // 0x03 rather than as SIGINT.
    ReadResult read_byte(unsigned char& byte, int timeout_ms = -1) const noexcept;

    // Completes the UTF-8 sequence introduced by `lead`, reading its continuation
    // bytes with `timeout_ms`. Returns `lead` alone when it is a single-byte or
    // invalid lead, or when the rest of the sequence does not arrive in time.
    std::string read_utf8_char(unsigned char lead, int timeout_ms = 50) const noexcept;

private:
    struct TerminalSize {
        int width;
        int height;
    };
    // The last frame that was actually put on screen, diffed against the one the
    // renderable writes; both must have identical cells for a row to be skipped.
    FrameBuffer previous_frame;
    FrameBuffer frame_buffer;
    std::optional<Renderable::Cursor> previous_cursor;
    struct TermImpl;
    std::optional<TerminalSize> get_terminal_size() const noexcept;
    std::unique_ptr<TermImpl> originalTermState;
};