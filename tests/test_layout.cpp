// test_layout.cpp - StyledText and TextLayout (design.md §13.2, step 2):
// shaping text into glyphs once, wrapping it, and mapping source offsets to
// (line, column).
//
// Plain assertions, no framework.

#include "TextLayout.hpp"

#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

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

const Style kRed{.fg = Color::ansi(1)};
const Style kGreen{.fg = Color::ansi(2)};

std::string line_text(const TextLayout& layout, size_t line) {
    std::string out;
    for (const ShapedGlyph& g : layout.line_glyphs(line)) { out += g.glyph.text(); }
    return out;
}

std::vector<std::string> lines_of(const TextLayout& layout) {
    std::vector<std::string> out;
    for (size_t i = 0; i < layout.lines().size(); ++i) { out.push_back(line_text(layout, i)); }
    return out;
}

std::vector<std::string> wrap(std::string_view text, int width) {
    return lines_of(TextLayout::wrap(StyledText(text), width));
}

std::string show(const std::vector<std::string>& lines) {
    std::string out = "[";
    for (size_t i = 0; i < lines.size(); ++i) { out += (i ? "|" : "") + lines[i]; }
    return out + "]";
}

void expect_lines(std::string_view text, int width, const std::vector<std::string>& expected, const std::string& what) {
    const auto got = wrap(text, width);
    check(got == expected, what + ": got " + show(got) + ", want " + show(expected));
}

bool at(const TextLayout& layout, size_t offset, size_t line, int column) {
    const auto p = layout.locate(offset);
    return p.line == line && p.column == column;
}

// --- StyledText -------------------------------------------------------------------

void test_styled_text() {
    StyledText t;
    t.add("ab", kRed).add("cd", kRed).add("", kGreen).add("ef", kGreen);
    check(t.runs().size() == 2 && t.runs()[0].text == "abcd" && t.runs()[1].text == "ef",
          "equal styles join; empty text adds nothing");
    check(t.size() == 6, "size counts bytes");
}

// --- wrapping ---------------------------------------------------------------------

void test_wrap_basics() {
    expect_lines("", 10, {""}, "empty text is one empty line");
    expect_lines("hello", 10, {"hello"}, "text that fits");
    expect_lines("hello world foo", 11, {"hello world", "foo"}, "breaks at the last space");
    expect_lines("aaaa bbbb", 4, {"aaaa", "bbbb"}, "an overflowing space is dropped");
    expect_lines("abcdefghij", 4, {"abcd", "efgh", "ij"}, "a long word is broken");
    expect_lines("ab cdefghij", 4, {"ab", "cdef", "ghij"}, "a word moves down, then breaks");
    expect_lines("aaaa ", 4, {"aaaa"}, "a break at the very end adds no empty line");
    expect_lines("abc", 0, {"a", "b", "c"}, "width < 1 counts as 1");
}

void test_wrap_newlines() {
    expect_lines("ab\ncd", 10, {"ab", "cd"}, "newline ends a line");
    expect_lines("ab\n", 10, {"ab", ""}, "a trailing newline leaves an empty last line");
    expect_lines("\n\n", 10, {"", "", ""}, "only newlines");
    expect_lines("aaaa bbbb\ncc", 4, {"aaaa", "bbbb", "cc"}, "wrapping and newlines together");
}

void test_wrap_wide_glyphs() {
    const auto layout = TextLayout::wrap(StyledText("日本語"), 4);
    check(lines_of(layout) == std::vector<std::string>{"日本", "語"}, "wide glyphs wrap by columns");
    check(layout.lines()[0].columns == 4 && layout.lines()[1].columns == 2, "line columns");
    expect_lines("a日本", 2, {"a", "日", "本"}, "a wide glyph that does not fit moves down");
    expect_lines("日x", 1, {"日", "x"}, "a glyph wider than the line gets a line of its own");
}

void test_wrap_keeps_styles() {
    StyledText t;
    t.add("aaaa ", kRed).add("bbbb", kGreen);
    const auto layout = TextLayout::wrap(t, 4);
    bool first_red = true, second_green = true;
    for (const auto& g : layout.line_glyphs(0)) { first_red = first_red && g.style == kRed; }
    for (const auto& g : layout.line_glyphs(1)) { second_green = second_green && g.style == kGreen; }
    check(first_red && second_green, "each glyph keeps its run's style across the break");

    const auto long_red = TextLayout::wrap(StyledText("aaaa bbbb cccc", kRed), 4);
    check(long_red.lines().size() == 3 && long_red.line_glyphs(2)[0].style == kRed,
          "a continuation line keeps the colour of the text it continues");
}

void test_shaping() {
    const auto layout = TextLayout::wrap(StyledText("aé日"), 10);
    const auto g = layout.glyphs();
    check(g.size() == 3 && g[0].offset == 0 && g[1].offset == 1 && g[2].offset == 3
              && g[1].length == 2 && g[2].length == 3,
          "glyphs record their source offset and length");

    const auto marks = TextLayout::wrap(StyledText("éx"), 10);
    check(marks.glyphs().size() == 2 && marks.glyphs()[0].glyph.text() == "é"
              && marks.glyphs()[0].length == 3,
          "a combining mark is part of its base glyph");

    StyledText split;
    split.add("e", kRed).add("́", kGreen);
    const auto across = TextLayout::wrap(split, 10);
    check(across.glyphs().size() == 1 && across.glyphs()[0].glyph.text() == "é"
              && across.glyphs()[0].style == kRed && across.glyphs()[0].length == 3,
          "a mark in the next run joins the base before it");

    const auto lone = TextLayout::wrap(StyledText("́a"), 10);
    check(lone.glyphs().size() == 2 && lone.glyphs()[0].glyph.text() == " ́" && lone.glyphs()[0].glyph.width() == 1,
          "a mark with no base is shown on a blank");

    const auto controls = TextLayout::wrap(StyledText("a\033[31mb\tc"), 20);
    check(line_text(controls, 0) == "a�[31mb c", "controls show as U+FFFD, a tab as a space");
    check(controls.glyphs()[1].offset == 1 && controls.glyphs()[1].length == 1,
          "a replaced control still covers its one source byte");
}

// --- locate -----------------------------------------------------------------------

void test_locate() {
    const auto layout = TextLayout::wrap(StyledText("hello world"), 5);
    check(at(layout, 0, 0, 0), "start");
    check(at(layout, 3, 0, 3), "inside the first line");
    check(at(layout, 5, 0, 5), "the dropped space belongs to the line it ended");
    check(at(layout, 6, 1, 0), "the next line starts after it");
    check(at(layout, 11, 1, 5), "the end of the text");
    check(at(layout, 99, 1, 5), "past the end: end of the last line");

    const auto wide = TextLayout::wrap(StyledText("日本"), 10);
    check(at(wide, 3, 0, 2), "after a wide glyph: two columns");
    check(at(wide, 4, 0, 2), "inside a glyph: snaps to its start");
    check(at(wide, 6, 0, 4), "after both");

    const auto nl = TextLayout::wrap(StyledText("ab\ncd\n"), 10);
    check(at(nl, 2, 0, 2), "on the newline: end of its line");
    check(at(nl, 3, 1, 0), "after the newline: start of the next line");
    check(at(nl, 6, 2, 0), "after a trailing newline: the empty last line");

    const auto empty = TextLayout::wrap(StyledText(), 10);
    check(at(empty, 0, 0, 0), "empty text: line 0, column 0");

    const auto hard = TextLayout::wrap(StyledText("abcdef"), 3);
    check(at(hard, 3, 1, 0) && at(hard, 2, 0, 2), "at a hard break the offset moves to the next line");
}

}  // namespace

int main() {
    run("styled text", test_styled_text);
    run("wrap: basics", test_wrap_basics);
    run("wrap: newlines", test_wrap_newlines);
    run("wrap: wide glyphs", test_wrap_wide_glyphs);
    run("wrap: styles", test_wrap_keeps_styles);
    run("shaping", test_shaping);
    run("locate", test_locate);

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
