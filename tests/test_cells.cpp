// test_cells.cpp - the building blocks of cell-based rendering (design.md §13):
// UTF-8 decoding and glyphs (Unicode.hpp), styles and SGR (Style.hpp), and the
// FrameBuffer cell operations (put, row_text).
//
// Plain assertions, no framework.

#include "Canvas.hpp"
#include "Style.hpp"
#include "TextLayout.hpp"
#include "TerminalUI.hpp"
#include "Unicode.hpp"

#include <cstdlib>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

void run(const char* title, const std::function<void()>& test) {
    std::cout << title << '\n';
    test();
}

std::string sgr(const Style& style) {
    std::string out;
    append_sgr(out, style);
    return out;
}

Style parsed(std::string_view parameters, Style start = {}) {
    apply_sgr(start, parameters);
    return start;
}

unicode::Glyph glyph(std::string_view text) {
    return unicode::next_glyph(text, 0).glyph;
}

// --- Unicode ----------------------------------------------------------------------

void test_decode() {
    using unicode::decode;
    check(decode("A", 0).codepoint == U'A' && decode("A", 0).length == 1, "ASCII");
    check(decode("é", 0).codepoint == U'é' && decode("é", 0).length == 2, "2-byte sequence");
    check(decode("日", 0).codepoint == U'日' && decode("日", 0).length == 3, "3-byte sequence");
    check(decode("😀", 0).codepoint == U'😀' && decode("😀", 0).length == 4, "4-byte sequence");

    const auto bad = [](std::string_view s) {
        const auto d = decode(s, 0);
        return !d.valid && d.codepoint == unicode::kReplacement && d.length == 1;
    };
    check(bad("\x80"), "a stray continuation byte is malformed, one byte consumed");
    check(bad("\xC3"), "a truncated sequence is malformed");
    check(bad("\xC3\x41"), "a bad continuation byte is malformed");
    check(bad("\xC0\x80"), "an overlong form is malformed");
    check(bad("\xED\xA0\x80"), "a surrogate is malformed");
    check(bad("\xF4\x90\x80\x80"), "a codepoint above U+10FFFF is malformed");
}

void test_widths() {
    check(unicode::width(U'a') == 1, "narrow");
    check(unicode::width(U'日') == 2, "wide");
    check(unicode::width(U'́') == 0, "combining mark");
    check(unicode::width(U'\033') == 0 && unicode::width(U'\x9B') == 0, "C0 and C1 controls");
    check(unicode::width(std::string_view("a日é")) == 4, "string width counts glyphs");
}

void test_next_glyph() {
    const auto read = unicode::next_glyph("é̂x", 0);
    check(read.glyph.text() == "é̂" && read.glyph.width() == 1 && read.length == 5,
          "a base and its combining marks form one glyph");

    const auto control = unicode::next_glyph("\033[2J", 0);
    check(control.glyph.text() == "�" && control.glyph.width() == 1 && control.length == 1,
          "a control character becomes U+FFFD");
    check(glyph("\x80").text() == "�", "a malformed byte becomes U+FFFD");

    const auto lone = unicode::next_glyph("́", 0);
    check(lone.glyph.width() == 0 && lone.glyph.text() == "́", "a lone combining mark has width 0");

    std::string many = "e";
    for (int i = 0; i < 20; ++i) { many += "́"; }
    const auto capped = unicode::next_glyph(many, 0);
    check(capped.length == many.size(), "marks that do not fit are still consumed");
    check(capped.glyph.text().size() <= unicode::Glyph::kCapacity, "and the glyph stays within its capacity");
}

void test_glyph_equality() {
    check(glyph("a") == glyph("a") && !(glyph("a") == glyph("b")), "glyphs compare by content");
    check(unicode::Glyph{}.empty() && unicode::Glyph{}.width() == 1, "a default glyph is a blank of width 1");
}

void test_sanitize() {
    check(unicode::sanitize("a\033[31mb") == "a�[31mb", "ESC loses its power");
    check(unicode::sanitize("x\a\x7F\xC2\x9By") == "x���y", "BEL, DEL and a C1 control are replaced");
    check(unicode::sanitize("a\tb\nc") == "a b\nc", "tab becomes a space, newline stays");
    check(unicode::sanitize("\xC3(") == "�(", "malformed bytes are replaced");
    check(unicode::sanitize("héllo 日本") == "héllo 日本", "ordinary text is unchanged");
}

// --- Style / SGR ------------------------------------------------------------------

void test_append_sgr() {
    check(sgr(Style{}) == "\033[0m", "default style is a plain reset");
    check(sgr(Style{.fg = Color::ansi(1)}) == "\033[0;31m", "red foreground");
    check(sgr(Style{.fg = Color::ansi(9)}) == "\033[0;91m", "bright red foreground");
    check(sgr(Style{.bg = Color::ansi(4)}) == "\033[0;44m", "blue background");
    check(sgr(Style{.fg = Color::ansi256(200)}) == "\033[0;38;5;200m", "palette colour");
    check(sgr(Style{.bg = Color::rgb(1, 2, 3)}) == "\033[0;48;2;1;2;3m", "true colour");
    check(sgr(Style{.attrs = Style::Bold | Style::Underline}) == "\033[0;1;4m", "attributes");
}

void test_apply_sgr() {
    check(parsed("31").fg == Color::ansi(1), "31: red");
    check(parsed("1;32") == Style{.fg = Color::ansi(2), .attrs = Style::Bold}, "1;32: bold green");
    check(parsed("94").fg == Color::ansi(12), "94: bright blue");
    check(parsed("38;5;200").fg == Color::ansi256(200), "38;5;n: palette");
    check(parsed("48;2;10;20;30").bg == Color::rgb(10, 20, 30), "48;2;r;g;b: true colour");
    const Style red_bold{.fg = Color::ansi(1), .attrs = Style::Bold};
    check(parsed("0", red_bold) == Style{}, "0: reset");
    check(parsed("", red_bold) == Style{}, "empty parameters: reset");
    check(parsed("39", red_bold) == Style{.attrs = Style::Bold}, "39: default foreground only");
    check(parsed("22", red_bold) == Style{.fg = Color::ansi(1)}, "22: normal intensity only");
    check(parsed("999;31").fg == Color::ansi(1), "unknown parameters are skipped");
    check(parsed("38;5").fg == Color{}, "an incomplete extended colour is ignored");
    for (const Style& s : {Style{}, red_bold, Style{.fg = Color::ansi256(7), .bg = Color::rgb(1, 2, 3)}}) {
        const std::string out = sgr(s);
        check(parsed(std::string_view(out).substr(2, out.size() - 3)) == s, "append_sgr round-trips through apply_sgr");
    }
}

// --- FrameBuffer ----------------------------------------------------------------

const Style kRed{.fg = Color::ansi(1)};

void test_put_basic() {
    FrameBuffer f(4, 1);
    f.put(0, 1, glyph("a"), kRed);
    check(f.at(0, 1).glyph.text() == "a" && f.at(0, 1).style == kRed, "a glyph and its style");
    f.put(0, 9, glyph("b"), {});
    f.put(3, 0, glyph("b"), {});
    f.put(0, -1, glyph("b"), {});
    check(f.row_text(0) == " \033[0;31ma\033[0m  ", "outside the frame: nothing happens");
}

void test_put_wide() {
    FrameBuffer f(4, 1);
    f.put(0, 0, glyph("日"), kRed);
    check(!f.at(0, 0).continuation && f.at(0, 1).continuation && f.at(0, 1).style == kRed,
          "a wide glyph takes two cells; the right half is a continuation in the same style");

    f.put(0, 1, glyph("x"), {});
    check(f.at(0, 0).glyph.empty() && !f.at(0, 1).continuation && f.at(0, 1).glyph.text() == "x",
          "covering the right half blanks the left half");

    f.put(0, 2, glyph("本"), {});
    f.put(0, 1, glyph("y"), {});
    check(f.at(0, 2).glyph.text() == "本" && f.at(0, 3).continuation,
          "a narrow glyph leaves the wide glyph to its right alone");

    f.put(0, 2, glyph("z"), {});
    check(f.at(0, 2).glyph.text() == "z" && !f.at(0, 3).continuation && f.at(0, 3).glyph.empty(),
          "covering the left half blanks the right half");

    f.put(0, 3, glyph("日"), {});
    check(f.at(0, 3).glyph.empty(), "a wide glyph cut by the frame edge becomes a space");
}

void test_put_wide_over_wide() {
    FrameBuffer f(5, 1);
    f.put(0, 0, glyph("日"), {});
    f.put(0, 2, glyph("本"), {});
    f.put(0, 1, glyph("語"), {});   // covers the right half of 日 and the left half of 本
    check(f.at(0, 0).glyph.empty() && f.at(0, 1).glyph.text() == "語" && f.at(0, 2).continuation
              && f.at(0, 3).glyph.empty() && !f.at(0, 3).continuation,
          "a wide glyph straddling two others blanks both");
}

void test_put_lone_mark() {
    FrameBuffer f(3, 1);
    f.put(0, 0, glyph("e"), {});
    f.put(0, 1, glyph("́"), {});
    check(f.at(0, 0).glyph.text() == "é" && f.at(0, 1).glyph.empty(), "a lone mark joins the glyph on its left");
    f.put(0, 2, glyph("́"), {});
    check(f.at(0, 2).glyph.empty(), "with a blank on its left it is dropped");
    FrameBuffer w(3, 1);
    w.put(0, 0, glyph("日"), {});
    w.put(0, 2, glyph("́"), {});
    check(w.at(0, 0).glyph.text() == "日́", "after a wide glyph it joins the wide glyph");
}

void test_row_text_emits_style_changes_only() {
    FrameBuffer f(5, 1);
    f.put(0, 0, glyph("a"), kRed);
    f.put(0, 1, glyph("b"), kRed);
    f.put(0, 2, glyph("c"), {});
    f.put(0, 3, glyph("日"), kRed);
    check(f.row_text(0) == "\033[0;31mab\033[0mc\033[0;31m日\033[0m",
          "one SGR per style change, wide glyph once, reset at the end");
    FrameBuffer plain(3, 1);
    plain.put(0, 0, glyph("x"), {});
    check(plain.row_text(0) == "x  ", "a row in the default style has no escapes at all");
}

// --- Canvas -----------------------------------------------------------------------

std::string row_glyphs(const FrameBuffer& f, int row) {
    std::string out;
    for (int column = 0; column < f.width; ++column) {
        const auto& cell = f.at(row, column);
        if (cell.continuation) { continue; }
        out += cell.glyph.empty() ? std::string(" ") : std::string(cell.glyph.text());
    }
    return out;
}

void test_canvas_clipping() {
    FrameBuffer f(6, 3);
    std::optional<Canvas::Point> cursor;
    Canvas screen(f, cursor);
    Canvas a = screen.sub(1, 1, 3, 1);
    check(a.text(0, 0, "abcdef") == 6, "text reports the columns it covered, clipped or not");
    check(row_glyphs(f, 1) == " abc  ", "drawing stops at the clip");
    a.text(-2, 0, "xyz");
    check(row_glyphs(f, 1) == " zbc  ", "negative local coordinates are clipped too");
    a.put(0, 1, glyph("q"), {});
    check(row_glyphs(f, 2) == "      ", "rows outside the clip are not touched");

    Canvas b = a.sub(2, 0, 10, 10);
    check(b.width() == 10 && b.height() == 10, "a sub-canvas keeps the size it was given");
    b.text(0, 0, "12345");
    check(row_glyphs(f, 1) == " zb1  ", "but its clip is the intersection with its parent");
    Canvas outside = a.sub(5, 0, 2, 1);
    outside.text(0, 0, "!!");
    check(row_glyphs(f, 1) == " zb1  ", "a sub-canvas entirely outside its parent draws nothing");
}

void test_canvas_wide_at_clip_edges() {
    FrameBuffer f(6, 1);
    std::optional<Canvas::Point> cursor;
    Canvas screen(f, cursor);
    screen.put(4, 0, glyph("z"), {});
    Canvas a = screen.sub(1, 0, 3, 1);   // columns 1..3
    a.put(2, 0, glyph("日"), kRed);       // right half would be column 4
    check(f.at(0, 3).glyph.empty() && f.at(0, 3).style == kRed && f.at(0, 4).glyph.text() == "z",
          "cut by the right edge: a space inside, nothing outside");
    a.put(-1, 0, glyph("日"), kRed);      // left half would be column 0
    check(f.at(0, 1).glyph.empty() && f.at(0, 1).style == kRed && f.at(0, 0).glyph.empty() && f.at(0, 0).style == Style{},
          "cut by the left edge: a space where the right half would be");
}

void test_canvas_fill_and_text() {
    FrameBuffer f(5, 2);
    std::optional<Canvas::Point> cursor;
    Canvas screen(f, cursor);
    screen.sub(1, 0, 3, 2).fill(0, 0, 9, 9, glyph("#"), kRed);
    check(row_glyphs(f, 0) == " ### " && row_glyphs(f, 1) == " ### " && f.at(1, 2).style == kRed,
          "fill covers the clipped rectangle");
    FrameBuffer w(5, 1);
    Canvas wide_screen(w, cursor);
    wide_screen.fill(0, 0, 5, 1, glyph("日"), {});
    check(row_glyphs(w, 0) == "日日 ", "a wide fill steps by two, the cut-off last glyph is a space");

    StyledText t;
    t.add("ab", kRed).add("c", Style{});
    const auto layout = TextLayout::wrap(t, 10);
    FrameBuffer g(4, 1);
    Canvas gs(g, cursor);
    check(gs.text(1, 0, layout.line_glyphs(0)) == 3, "laid-out glyphs report their columns");
    check(row_glyphs(g, 0) == " abc" && g.at(0, 1).style == kRed && g.at(0, 3).style == Style{},
          "and keep their styles");
}

void test_canvas_cursor() {
    FrameBuffer f(6, 3);
    std::optional<Canvas::Point> cursor;
    Canvas screen(f, cursor);
    Canvas a = screen.sub(1, 1, 3, 1);
    a.set_cursor(1, 0);
    check(cursor == Canvas::Point{2, 1}, "frame coordinates");
    a.set_cursor(10, 5);
    check(cursor == Canvas::Point{3, 1}, "clamped into the visible area");
    Canvas empty = a.sub(9, 9, 2, 2);
    empty.set_cursor(0, 0);
    check(cursor == Canvas::Point{3, 1}, "an invisible area cannot place the cursor");
}

}  // namespace

int main() {
    run("unicode: decode", test_decode);
    run("unicode: widths", test_widths);
    run("unicode: next_glyph", test_next_glyph);
    run("unicode: glyph equality", test_glyph_equality);
    run("unicode: sanitize", test_sanitize);
    run("style: append_sgr", test_append_sgr);
    run("style: apply_sgr", test_apply_sgr);
    run("frame: put", test_put_basic);
    run("frame: wide glyphs", test_put_wide);
    run("frame: wide over wide", test_put_wide_over_wide);
    run("frame: lone combining mark", test_put_lone_mark);
    run("frame: row_text", test_row_text_emits_style_changes_only);
    run("canvas: clipping", test_canvas_clipping);
    run("canvas: wide glyphs at the clip edges", test_canvas_wide_at_clip_edges);
    run("canvas: fill and text", test_canvas_fill_and_text);
    run("canvas: cursor", test_canvas_cursor);

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
