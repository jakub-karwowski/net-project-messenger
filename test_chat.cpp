// test_chat.cpp - interactive TerminalUI demo.
//
//   AbsoluteLayout (root, 80x24 design size; scaled to the real terminal)
//     |- ChatWindow  (top,    shows ChatModel messages)
//     '- InputBox    (bottom, edits InputModel)
//
// Threads:
//   reader - blocks in TerminalUI::read_byte, decodes keys, pushes InputEvents
//            into a ThreadQueue. It owns no model state and does no output.
//   main   - pops events, mutates the models, renders. All output stays here.
//   `ui` outlives the reader (joined before main returns), so the terminal stays
//   in raw mode for the whole reader lifetime.
//
// Controls:
//   printable text -> inserted at the cursor
//   Backspace      -> deletes the character before the cursor
//   Enter          -> sends the input box content to the ChatModel and clears it
//   Left / Right   -> move the cursor
//   ESC / Ctrl+C (or EOF) -> quit
//
// Must be run on a real terminal (TerminalUI puts stdin in raw mode).

#include "Chat.hpp"
#include "Element.hpp"
#include "TerminalUI.hpp"
#include "ThreadQueue.hpp"
#include "Utils.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

    enum class Event { None, Char, Backspace, Enter, Left, Right, Escape };

    struct InputEvent {
        Event kind{Event::None};
        std::string text{};   // UTF-8 bytes, only for Event::Char
    };

    // Returns Event::None when no key arrived within `idle_timeout_ms`, so the
    // reader thread can poll for shutdown between keys.
    InputEvent read_event(const TerminalUI& ui, int idle_timeout_ms) {
        unsigned char lead = 0;
        switch (ui.read_byte(lead, idle_timeout_ms)) {
            case TerminalUI::ReadResult::Timeout: return {Event::None, {}};
            case TerminalUI::ReadResult::Eof:     return {Event::Escape, {}};   // EOF quits
            case TerminalUI::ReadResult::Byte:    break;
        }

        if (lead == 0x1B) {                                 // ESC or ANSI CSI sequence
            unsigned char next = 0;
            if (ui.read_byte(next, 50) != TerminalUI::ReadResult::Byte || next != '[') {
                return {Event::Escape, {}};
            }
            unsigned char code = 0;
            if (ui.read_byte(code, 50) == TerminalUI::ReadResult::Byte) {
                if (code == 'C') { return {Event::Right, {}}; }
                if (code == 'D') { return {Event::Left, {}}; }
            }
            return {Event::None, {}};
        }
        if (lead == 0x03) { return {Event::Escape, {}}; }    // Ctrl+C (ISIG off) quits
        if (lead == 0x7F || lead == 0x08) { return {Event::Backspace, {}}; }
        if (lead == '\r' || lead == '\n') { return {Event::Enter, {}}; }
        if (lead < 0x20) { return {Event::None, {}}; }       // other control bytes

        // TerminalUI gathers the remaining UTF-8 bytes of the character.
        return {Event::Char, ui.read_utf8_char(lead, 50)};
    }

} // namespace

int main() {
    TerminalUI ui;

    auto chat_model = std::make_shared<ChatModel>();
    auto input_model = std::make_shared<InputModel>();

    constexpr int kWidth = 50;
    constexpr int kHeight = 24;
    constexpr int kInputHeight = 8;

    auto chat_window = std::make_shared<ChatWindow>(
        Element::Position{0, 0}, kWidth, kHeight - kInputHeight);
    auto input_box = std::make_shared<InputBox>(
        Element::Position{0, kHeight - kInputHeight}, kWidth, kInputHeight);
    input_box->set_border({Element::Border::BorderType::Asterisk, 1, 1, 0 , 0});
    input_box->set_margin({1, 1, 0, 0});   // top, bottom, left, right
    chat_window->set_model(chat_model);
    input_box->set_model(input_model);

    AbsoluteLayout layout(Element::Position{0, 0}, kWidth, kHeight);
    layout.add_element(chat_window);
    layout.add_element(input_box);

    ThreadQueue<InputEvent> events;

    // Reads keys off the terminal and hands them to the main thread. It touches
    // no model state and does no output; it learns the consumer is gone when
    // `events.push` returns false (queue shut down).
    std::thread reader([&ui, &events] {
        constexpr int kIdleTimeoutMs = 100;
        while (true) {
            InputEvent event = read_event(ui, kIdleTimeoutMs);
            if (event.kind == Event::None) { continue; }     // idle: re-check shutdown
            const bool quit = (event.kind == Event::Escape);
            if (!events.push(std::move(event))) { break; }   // consumer finished
            if (quit) { break; }
        }
        events.graceful_shutdown();
    });

    ui.clear();
    ui.render(layout);   // initial (empty) frame

    bool running = true;
    while (running) {
        const std::optional<InputEvent> event = events.pop();
        if (!event) { break; }        // reader finished and the queue is drained

        switch (event->kind) {
            case Event::Char: {
                const size_t cursor = input_model->get_cursor();
                input_model->apply(std::vector<InputModel::Edit>{
                    Input::put(cursor, event->text),
                    Input::set_cursor(cursor + event->text.size())
                });
                break;
            }
            case Event::Backspace: {
                const size_t cursor = input_model->get_cursor();
                if (cursor > 0) {
                    const std::string_view text = std::get<0>(input_model->get_content());
                    const size_t previous = utils::utf8::prev(text, cursor);
                    input_model->apply(std::vector<InputModel::Edit>{
                        Input::remove(previous, cursor - previous),
                        Input::set_cursor(previous)
                    });
                }
                break;
            }
            case Event::Left: {
                const std::string_view text = std::get<0>(input_model->get_content());
                input_model->apply(std::vector<InputModel::Edit>{
                    Input::set_cursor(utils::utf8::prev(text, input_model->get_cursor()))
                });
                break;
            }
            case Event::Right: {
                const std::string_view text = std::get<0>(input_model->get_content());
                input_model->apply(std::vector<InputModel::Edit>{
                    Input::set_cursor(utils::utf8::next(text, input_model->get_cursor()))
                });
                break;
            }
            case Event::Enter: {
                const std::string message{std::get<0>(input_model->get_content())};
                if (!message.empty()) {
                    chat_model->add_message(std::chrono::system_clock::now(), "me", message);
                    input_model->apply(std::vector<InputModel::Edit>{
                        Input::remove(0, message.size()),
                        Input::set_cursor(0)
                    });
                }
                break;
            }
            case Event::Escape:
                running = false;
                break;
            case Event::None:
                break;
        }

        if (running) { ui.render(layout); }
    }

    reader.join();       // `ui` is destroyed after this, so raw mode stays valid
    return 0;
}
