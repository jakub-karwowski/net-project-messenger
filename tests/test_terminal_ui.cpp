// test_terminal_ui.cpp - TerminalUI rendering, captured before the cell-based
// rewrite (design.md §13).
//
// Two kinds of assertion:
//   check(...)      current behaviour that the rewrite must keep;
//   known_bug(...)  the behaviour we want, which the current code gets wrong.
//                   It reports XFAIL while the bug is there and XPASS once it is
//                   fixed (turn it into a check() then). Neither fails the run.
//
// Everything renders into a FrameBuffer through AbsoluteLayout::write_frame(),
// so no terminal is needed.

#include "Chat.hpp"
#include "Element.hpp"
#include "TerminalUI.hpp"

#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

int g_failures = 0;
int g_checks = 0;
int g_known_bugs = 0;
int g_fixed_bugs = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

// Unused while no bug is open; kept for the next one found.
[[maybe_unused]] void known_bug(bool wanted_behaviour_holds, const std::string& what) {
    if (wanted_behaviour_holds) {
        ++g_fixed_bugs;
        std::cout << "  XPASS (fixed, make it a check): " << what << '\n';
    } else {
        ++g_known_bugs;
        std::cout << "  XFAIL (known bug): " << what << '\n';
    }
}

// --- helpers -------------------------------------------------------------------

// A fixed-text element: line i of `lines` is inner row i.
struct Label : Element {
    std::vector<std::string> lines;
    Label(Position position, int width, int height, std::vector<std::string> text)
        : Element(position, width, height), lines(std::move(text)) {}
    void draw_content(Canvas& inner) override {
        for (size_t row = 0; row < lines.size(); ++row) { inner.text(0, static_cast<int>(row), lines[row]); }
    }
};

struct Rendered {
    FrameBuffer frame;
    std::optional<Renderable::Cursor> cursor;
};

Rendered draw(AbsoluteLayout& root) {
    Rendered r;
    r.cursor = root.write_frame(r.frame);
    return r;
}

// What the terminal would emit for `row`, escape sequences included.
std::string raw_row(const FrameBuffer& frame, int row) {
    return frame.row_text(row);
}

// The row as the user would read it: CSI sequences removed.
std::string visible_row(const FrameBuffer& frame, int row) {
    const std::string raw = frame.row_text(row);
    std::string out;
    for (size_t i = 0; i < raw.size();) {
        if (raw[i] == '\033' && i + 1 < raw.size() && raw[i + 1] == '[') {
            i += 2;
            while (i < raw.size() && !(raw[i] >= 0x40 && raw[i] <= 0x7E)) { ++i; }
            if (i < raw.size()) { ++i; }
            continue;
        }
        out.push_back(raw[i++]);
    }
    return out;
}

std::string repeat(std::string_view piece, int times) {
    std::string out;
    for (int i = 0; i < times; ++i) { out += piece; }
    return out;
}

const Color kGreen = Color::ansi(2);

// 2026-11-23 14:05 UTC, rendered by ChatWindow as "2026-11-23[14:05]".
std::chrono::system_clock::time_point fixed_time() {
    using namespace std::chrono;
    return sys_days{year{2026} / 11 / 23} + 14h + 5min;
}

// "p@2026-11-23[14:05]::> " is 23 columns.
const std::string kHeader = "p@2026-11-23[14:05]::> ";

struct ChatSetup {
    std::shared_ptr<ChatModel> model = std::make_shared<ChatModel>();
    std::shared_ptr<ChatWindow> window;
    AbsoluteLayout root;
    ChatSetup(int width, int height) : window(std::make_shared<ChatWindow>(Element::Position{0, 0}, width, height)),
                                       root({0, 0}, width, height) {
        window->set_model(model);
        root.add_element(window);
    }
    void add(std::string body) { model->add_message(fixed_time(), "p", std::move(body)); }
};

struct InputSetup {
    std::shared_ptr<InputModel> model = std::make_shared<InputModel>();
    std::shared_ptr<InputBox> box;
    AbsoluteLayout root;
    InputSetup(int width, int height) : box(std::make_shared<InputBox>(Element::Position{0, 0}, width, height)),
                                        root({0, 0}, width, height) {
        box->set_model(model);
        root.add_element(box);
    }
    void set(std::string_view text, size_t cursor) {
        const auto [old, unused] = model->get_content();
        (void)unused;
        const InputModel::Edit edits[] = {Input::remove(0, old.size()), Input::put(0, text), Input::set_cursor(cursor)};
        model->apply(edits);
    }
};

void run(const char* title, const std::function<void()>& test) {
    std::cout << title << '\n';
    test();
}

// --- layout: borders, padding, margins, positions --------------------------------

void test_border() {
    AbsoluteLayout root({0, 0}, 6, 3);
    auto label = std::make_shared<Label>(Element::Position{0, 0}, 6, 3, std::vector<std::string>{"hi"});
    label->set_border(Element::Border(Element::Border::BorderType::Hash));
    root.add_element(label);
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == "######", "top border");
    check(visible_row(r.frame, 1) == "#hi  #", "content row padded to the inner width");
    check(visible_row(r.frame, 2) == "######", "bottom border");
}

void test_padding_and_margin() {
    AbsoluteLayout root({0, 0}, 8, 5);
    auto label = std::make_shared<Label>(Element::Position{0, 0}, 8, 5, std::vector<std::string>{"hi"});
    label->set_border(Element::Border(Element::Border::BorderType::Hash));
    label->set_margin({.top = 1, .bottom = 0, .left = 1, .right = 0});
    label->set_padding({.top = 0, .bottom = 0, .left = 1, .right = 0});
    root.add_element(label);
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == "        ", "margin row is blank");
    check(visible_row(r.frame, 1) == " #######", "border starts after the left margin");
    check(visible_row(r.frame, 2) == " # hi  #", "padding before the content");
    check(visible_row(r.frame, 4) == " #######", "bottom border");
}

void test_positions_and_nesting() {
    AbsoluteLayout root({0, 0}, 10, 4);
    auto inner = std::make_shared<AbsoluteLayout>(Element::Position{1, 1}, 8, 3);
    inner->add_element(std::make_shared<Label>(Element::Position{1, 1}, 3, 1, std::vector<std::string>{"abc"}));
    root.add_element(inner);
    const auto r = draw(root);
    check(visible_row(r.frame, 2) == "  abc     ", "a child's position adds to its parent's");
    check(visible_row(r.frame, 1) == "          ", "nothing else is drawn");
}

// --- drawing into canvases (step 3) ------------------------------------------------

// 10. Found while porting: AbsoluteLayout::set_size() rebuilt each child's
// position without z, so a resize reset every z to 0.
void test_z_survives_resize() {
    AbsoluteLayout root({0, 0}, 3, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0, 1}, 3, 1, std::vector<std::string>{"AAA"}));
    root.add_element(std::make_shared<Label>(Element::Position{0, 0, 0}, 3, 1, std::vector<std::string>{"BBB"}));
    root.resize(6, 2);
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == "AAA   ", "10 (fixed in step 3): z survives a resize (before: \"BBB   \")");
}

void test_margin_shows_what_is_underneath() {
    AbsoluteLayout root({0, 0}, 6, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0}, 6, 1, std::vector<std::string>{"xxxxxx"}));
    auto top = std::make_shared<Label>(Element::Position{0, 0}, 6, 1, std::vector<std::string>{"ab"});
    top->set_margin({.top = 0, .bottom = 0, .left = 2, .right = 0});
    root.add_element(top);
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == "xxab  ", "the margin is not drawn; the box is cleared");
}

void test_children_are_clipped_to_their_parent() {
    AbsoluteLayout root({0, 0}, 6, 1);
    auto inner = std::make_shared<AbsoluteLayout>(Element::Position{0, 0}, 3, 1);
    inner->add_element(std::make_shared<Label>(Element::Position{1, 0}, 10, 1, std::vector<std::string>{"abcdefg"}));
    root.add_element(inner);
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == " ab   ", "a child wider than its parent stops at the parent's edge");
}

void test_border_in_element_style() {
    AbsoluteLayout root({0, 0}, 5, 3);   // inner area: columns 1..3, "hi" in 1..2
    auto label = std::make_shared<Label>(Element::Position{0, 0}, 5, 3, std::vector<std::string>{"hi"});
    label->set_border(Element::Border(Element::Border::BorderType::Hash));
    label->set_style(Style{.fg = Color::ansi(4)});
    root.add_element(label);
    const auto r = draw(root);
    check(r.frame.at(0, 0).style.fg == Color::ansi(4) && r.frame.at(1, 4).style.fg == Color::ansi(4)
              && r.frame.at(1, 3).style.fg == Color::ansi(4),
          "border and cleared inner cells take the element's style");
    check(r.frame.at(1, 1).style == Style{}, "content drawn without a style stays plain");
}

void test_cursor_in_nested_layouts() {
    AbsoluteLayout root({0, 0}, 10, 3);
    auto inner = std::make_shared<AbsoluteLayout>(Element::Position{2, 1}, 8, 2);
    auto model = std::make_shared<InputModel>();
    auto box = std::make_shared<InputBox>(Element::Position{1, 0}, 5, 1);
    box->set_model(model);
    const InputModel::Edit edits[] = {Input::put(0, "hi"), Input::set_cursor(2)};
    model->apply(edits);
    inner->add_element(box);
    root.add_element(inner);
    const auto r = draw(root);
    check(r.cursor && r.cursor->x == 6 && r.cursor->y == 2, "cursor offsets add up through nested layouts (1-based)");
}

// --- glyphs ------------------------------------------------------------------------

void test_wide_glyphs() {
    AbsoluteLayout root({0, 0}, 6, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0}, 6, 1, std::vector<std::string>{"日本x"}));
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == "日本x ", "wide glyphs take two columns");
    check(r.frame.at(0, 1).continuation && r.frame.at(0, 3).continuation && !r.frame.at(0, 4).continuation,
          "their right halves are continuation cells");
}

void test_overwriting_half_a_wide_glyph() {
    AbsoluteLayout root({0, 0}, 6, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0}, 4, 1, std::vector<std::string>{"日本"}));
    root.add_element(std::make_shared<Label>(Element::Position{1, 0}, 1, 1, std::vector<std::string>{"x"}));
    const auto r = draw(root);
    check(visible_row(r.frame, 0).starts_with(" x本"), "covering the right half blanks the whole glyph");
}

void test_combining_marks() {
    AbsoluteLayout root({0, 0}, 3, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0}, 3, 1, std::vector<std::string>{"éx"}));
    const auto r = draw(root);
    check(r.frame.at(0, 0).glyph.text() == "é" && r.frame.at(0, 1).glyph.text() == "x",
          "a combining mark joins the glyph on its left");
}

// --- ChatWindow -------------------------------------------------------------------

void test_chat_bottom_aligned() {
    ChatSetup chat(30, 3);
    chat.add("abcdefg");
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 0) == repeat(" ", 30), "empty rows on top");
    check(visible_row(r.frame, 2) == kHeader + "abcdefg", "the newest message on the bottom row, exactly 30 columns");
}

void test_chat_several_messages() {
    ChatSetup chat(30, 3);
    chat.add("one");
    chat.add("two");
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 1).starts_with(kHeader + "one"), "older message above");
    check(visible_row(r.frame, 2).starts_with(kHeader + "two"), "newer message below");
}

void test_chat_word_wrap() {
    ChatSetup chat(30, 3);
    chat.add("hello world foo bar baz");
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 1) == (kHeader + "hello").append(2, ' '), "wraps before the word that does not fit");
    check(visible_row(r.frame, 2) == std::string("world foo bar baz").append(13, ' '), "the rest on the next row");
}

void test_chat_truncation_marker() {
    ChatSetup chat(30, 1);
    chat.add("hello world foo bar baz");
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 0).starts_with("...ld foo bar baz"), "a cut-off top line starts with ...");
}

void test_chat_styles() {
    ChatSetup chat(30, 1);
    chat.window->set_separator_style(Style{.fg = Color::ansi(1)});
    chat.window->set_message_style(Style{.fg = kGreen});
    chat.add("abc");
    const auto r = draw(chat.root);
    const int body = static_cast<int>(kHeader.size());
    check(r.frame.at(0, 0).style.fg == Color::ansi(1) && r.frame.at(0, body - 1).style.fg == Color::ansi(1),
          "the header is in the separator style");
    check(r.frame.at(0, body).style.fg == kGreen && r.frame.at(0, body + 2).style.fg == kGreen,
          "the text is in the message style");
}

void test_chat_multiline_message() {
    ChatSetup chat(30, 3);
    chat.add("one\ntwo");
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 1).starts_with(kHeader + "one") && visible_row(r.frame, 2).starts_with("two "),
          "a newline in a message starts a new row");
}

void test_chat_marker_when_older_messages_are_hidden() {
    ChatSetup chat(30, 1);
    chat.add("old");
    chat.add("new");
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 0).starts_with("...") && visible_row(r.frame, 0).find("new") != std::string::npos,
          "older messages above the window are marked too");
}

void test_chat_marker_over_wide_glyphs() {
    ChatSetup chat(30, 1);
    chat.add(repeat("日", 30));   // the visible line is all wide glyphs
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 0).starts_with("... 日"), "the marker blanks the wide glyph it half covers");
    check(!r.frame.at(0, 3).continuation && r.frame.at(0, 4).glyph.text() == "日" && r.frame.at(0, 5).continuation,
          "and no half glyph is left");
}

// --- InputBox -------------------------------------------------------------------

void test_input_cursor() {
    InputSetup input(10, 3);
    input.set("hello", 5);
    const auto r = draw(input.root);
    check(visible_row(r.frame, 0) == "hello     ", "content");
    check(r.cursor && r.cursor->x == 6 && r.cursor->y == 1, "cursor after the last character (1-based)");
}

void test_input_wrap_and_cursor() {
    InputSetup input(10, 3);
    input.set("aaaa bbbb cccc", 14);
    const auto r = draw(input.root);
    check(visible_row(r.frame, 0) == "aaaa bbbb ", "first wrapped line");
    check(visible_row(r.frame, 1) == "cccc      ", "second wrapped line");
    check(r.cursor && r.cursor->x == 5 && r.cursor->y == 2, "cursor at the end of the second line");
}

void test_input_cursor_after_wide_glyphs() {
    InputSetup input(10, 1);
    input.set("日本", 6);
    auto r = draw(input.root);
    check(r.cursor && r.cursor->x == 5, "cursor after two wide glyphs is at column 5");
    input.set("日本", 4);   // inside 本: snapped back to its start
    r = draw(input.root);
    check(r.cursor && r.cursor->x == 3, "a cursor inside a glyph snaps to its start");
}

void test_input_scrolls_with_cursor() {
    InputSetup input(5, 1);
    input.set("aaaa bbbb cccc", 14);
    auto r = draw(input.root);
    check(visible_row(r.frame, 0) == "cccc ", "the cursor's line is shown");
    const InputModel::Edit home[] = {Input::set_cursor(0)};
    input.model->apply(home);
    r = draw(input.root);
    check(visible_row(r.frame, 0) == "aaaa ", "the view follows the cursor back");
    check(r.cursor && r.cursor->x == 1 && r.cursor->y == 1, "cursor at the start");
}

void test_input_newline_moves_the_cursor_down() {
    InputSetup input(10, 3);
    input.set("ab\n", 3);
    const auto r = draw(input.root);
    check(r.cursor && r.cursor->x == 1 && r.cursor->y == 2,
          "after a trailing newline the cursor is at the start of the next line (new in step 4)");
}

void test_input_shows_controls_and_keeps_the_cursor() {
    InputSetup input(10, 1);
    input.set("a\x01" "b", 3);   // split: "\x01b" would be the one byte 0x1B
    const auto r = draw(input.root);
    check(visible_row(r.frame, 0).starts_with("a\uFFFDb"), "a control character shows as U+FFFD");
    check(r.cursor && r.cursor->x == 4, "and the cursor after it is still in the right column");
}

void test_input_uses_the_element_style() {
    InputSetup input(10, 1);
    input.box->set_style(Style{.fg = kGreen});
    input.set("hi", 2);
    const auto r = draw(input.root);
    check(r.frame.at(0, 0).style.fg == kGreen && r.frame.at(0, 5).style.fg == kGreen, "text and background in the box's style");
}

void test_input_model_edits_respect_glyphs() {
    InputModel model;
    const InputModel::Edit put_e[] = {Input::put(0, "é")};
    model.apply(put_e);
    const InputModel::Edit put_mid[] = {Input::put(1, "x")};   // byte 1 is inside é
    model.apply(put_mid);
    check(std::get<0>(model.get_content()) == "xé", "an insert inside a glyph moves to its start");
    const InputModel::Edit remove_half[] = {Input::remove(1, 1)};
    model.apply(remove_half);
    check(std::get<0>(model.get_content()) == "xé", "removing half a glyph removes nothing");
    const InputModel::Edit remove_whole[] = {Input::remove(1, 2)};
    model.apply(remove_whole);
    check(std::get<0>(model.get_content()) == "x", "removing all its bytes removes it");
}

// --- known bugs (design.md §13.1) --------------------------------------------------

// 1. Wrapping counts colour escape bytes as visible columns.
void bug_wrap_counts_escapes() {
    ChatSetup chat(30, 3);
    chat.window->set_separator_style(Style{.fg = Color::ansi(1)});
    chat.window->set_message_style(Style{.fg = kGreen});
    chat.add("abcdefg");   // header + body = exactly 30 columns
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 2) == kHeader + "abcdefg", "1 (fixed in step 4): coloured text that fits one row stays on one row");
}

// 2. Control sequences in a peer's message reach the terminal.
void bug_peer_text_reaches_terminal() {
    ChatSetup chat(40, 3);
    chat.add("hi\033]52;c;ZXZpbA==\a\033[2J");   // OSC 52 = write the clipboard; CSI 2J = clear screen
    const auto r = draw(chat.root);
    bool clean = true;
    for (int row = 0; row < r.frame.height; ++row) {
        const std::string raw = raw_row(r.frame, row);
        clean = clean && raw.find('\033') == std::string::npos && raw.find('\a') == std::string::npos;
    }
    check(clean, "2 (fixed in step 1): escape and control characters from a message never reach the terminal");
}

// 2b. A peer can still colour its own text: the transitional write_text()
// turns SGR codes in any text into styles. Fixed once text is cleaned at the
// input boundary (step 5).
void bug_peer_can_set_colours() {
    ChatSetup chat(40, 3);
    chat.add("\033[31mred");
    const auto r = draw(chat.root);
    const int body = static_cast<int>(kHeader.size());
    check(r.frame.at(2, body).style == Style{}, "2b (fixed in step 4): colour codes in a message do not change its style");
}

// 3. A wrapped line loses the colour of the text it continues. The body is
// longer than a row, so its second row is a continuation line.
void bug_wrapped_line_loses_colour() {
    ChatSetup chat(30, 4);
    chat.window->set_message_style(Style{.fg = kGreen});
    chat.add("aaaa bbbb cccc dddd eeee ffff gggg hhhh");
    const auto r = draw(chat.root);
    check(visible_row(r.frame, 3).starts_with("hhhh") && r.frame.at(3, 0).style.fg == kGreen,
              "3 (fixed in step 4): a continuation line is still drawn in the message colour");
}

// 4. Content colour leaks into the padding and right border.
void bug_colour_leaks_into_border() {
    ChatSetup chat(40, 3);
    chat.window->set_border(Element::Border(Element::Border::BorderType::Hash));
    chat.window->set_message_style(Style{.fg = kGreen});
    chat.add("hi");
    const auto r = draw(chat.root);
    const bool body_green = r.frame.at(1, 1 + static_cast<int>(kHeader.size())).style.fg == kGreen;
    const bool border_plain = r.frame.at(1, 39).style == Style{};
    check(body_green && border_plain, "4 (fixed in step 3): the message is green, the right border is not");
}

// 5. The truncation marker overwrites bytes, not glyphs.
void bug_truncation_splits_a_glyph() {
    ChatSetup chat(30, 1);
    chat.add(repeat("é", 40));   // the visible (last) line is all 2-byte é
    const auto r = draw(chat.root);
    const std::string row = visible_row(r.frame, 0);
    bool only_e = row.starts_with("...");
    for (size_t i = 3; only_e && i < row.size();) {
        if (row.compare(i, 2, "é") == 0) { i += 2; }
        else if (row[i] == ' ') { ++i; }
        else { only_e = false; }
    }
    check(only_e, "5 (fixed in step 4): after the ... marker every glyph is intact");
}

// 6. An element can draw outside its own box.
void bug_no_clipping() {
    AbsoluteLayout root({0, 0}, 8, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0}, 3, 1, std::vector<std::string>{"abcdef"}));
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == "abc     ", "6 (fixed in step 3): text wider than its element is clipped to the element");
}

// 7. Position::z does not affect paint order.
void bug_z_ignored() {
    AbsoluteLayout root({0, 0}, 3, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0, 1}, 3, 1, std::vector<std::string>{"AAA"}));
    root.add_element(std::make_shared<Label>(Element::Position{0, 0, 0}, 3, 1, std::vector<std::string>{"BBB"}));
    const auto r = draw(root);
    check(visible_row(r.frame, 0) == "AAA", "7 (fixed in step 3): the element with the higher z is drawn on top");
}

// 8. A wide glyph that does not fit at the frame's edge is half-drawn.
void bug_wide_glyph_at_edge() {
    AbsoluteLayout root({0, 0}, 3, 1);
    root.add_element(std::make_shared<Label>(Element::Position{2, 0}, 2, 1, std::vector<std::string>{"日"}));
    const auto r = draw(root);
    check(r.frame.at(0, 2).glyph.text().find("日") == std::string::npos,
          "8 (fixed in step 1): a wide glyph that would stick out of the frame is not drawn");
}

// 9. Drawing a narrow glyph breaks the wide glyph to its right: write_text()
// clears the next cell's continuation flag even for a 1-column glyph, so the
// row is emitted one column too wide.
void bug_narrow_glyph_breaks_right_neighbour() {
    AbsoluteLayout root({0, 0}, 6, 1);
    root.add_element(std::make_shared<Label>(Element::Position{0, 0}, 4, 1, std::vector<std::string>{"日本"}));
    root.add_element(std::make_shared<Label>(Element::Position{1, 0}, 1, 1, std::vector<std::string>{"x"}));
    const auto r = draw(root);
    check(r.frame.at(0, 3).continuation && visible_row(r.frame, 0) == " x本  ",
          "9 (fixed in step 1): the wide glyph right of an overwritten cell keeps its right half");
}

}  // namespace

int main() {
    run("border", test_border);
    run("padding and margin", test_padding_and_margin);
    run("positions and nesting", test_positions_and_nesting);
    run("z survives a resize", test_z_survives_resize);
    run("margin shows what is underneath", test_margin_shows_what_is_underneath);
    run("children are clipped to their parent", test_children_are_clipped_to_their_parent);
    run("border in the element's style", test_border_in_element_style);
    run("cursor in nested layouts", test_cursor_in_nested_layouts);
    run("wide glyphs", test_wide_glyphs);
    run("overwriting half a wide glyph", test_overwriting_half_a_wide_glyph);
    run("combining marks", test_combining_marks);
    run("chat: bottom aligned", test_chat_bottom_aligned);
    run("chat: several messages", test_chat_several_messages);
    run("chat: word wrap", test_chat_word_wrap);
    run("chat: truncation marker", test_chat_truncation_marker);
    run("chat: styles", test_chat_styles);
    run("chat: multi-line message", test_chat_multiline_message);
    run("chat: marker when older messages are hidden", test_chat_marker_when_older_messages_are_hidden);
    run("chat: marker over wide glyphs", test_chat_marker_over_wide_glyphs);
    run("input: cursor", test_input_cursor);
    run("input: wrap and cursor", test_input_wrap_and_cursor);
    run("input: cursor after wide glyphs", test_input_cursor_after_wide_glyphs);
    run("input: scrolls with the cursor", test_input_scrolls_with_cursor);
    run("input: a newline moves the cursor down", test_input_newline_moves_the_cursor_down);
    run("input: controls shown, cursor kept", test_input_shows_controls_and_keeps_the_cursor);
    run("input: element style", test_input_uses_the_element_style);
    run("input model: edits respect glyphs", test_input_model_edits_respect_glyphs);

    run("known bugs", [] {
        bug_wrap_counts_escapes();
        bug_peer_text_reaches_terminal();
        bug_peer_can_set_colours();
        bug_wrapped_line_loses_colour();
        bug_colour_leaks_into_border();
        bug_truncation_splits_a_glyph();
        bug_no_clipping();
        bug_z_ignored();
        bug_wide_glyph_at_edge();
        bug_narrow_glyph_breaks_right_neighbour();
    });

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed; "
              << g_known_bugs << " known bugs, " << g_fixed_bugs << " fixed\n";
    return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
