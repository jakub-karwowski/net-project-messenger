#include "KeyboardWorker.hpp"

#include "Keys.hpp"

#include <string>
#include <tuple>
#include <utility>

namespace {

// The rest of a sequence normally arrives together with its ESC; anything
// slower is a lone Escape key followed by unrelated input.
constexpr int kSequenceTimeoutMs = 50;
constexpr std::size_t kMaxSequence = 16;

}  // namespace

KeyboardWorker::KeyboardWorker(const TerminalUI& terminal, ThreadQueue<msg::FrontendMsg>& out, Logger log,
                               std::chrono::milliseconds poll_timeout) noexcept
    : terminal_(terminal),
      out_(out),
      log_(log.with_component("keyboard")),
      poll_ms_(static_cast<int>(poll_timeout.count())) {}

void KeyboardWorker::emit(msg::Key key) {
    std::ignore = out_.push(std::move(key));   // false: the frontend has stopped
}

void KeyboardWorker::run() {
    while (!stopping_.load(std::memory_order_acquire)) {
        unsigned char byte = 0;
        const auto read = terminal_.read_byte(byte, poll_ms_);
        if (read == TerminalUI::ReadResult::Timeout) { continue; }
        if (read == TerminalUI::ReadResult::Eof) {
            log_.info("terminal input closed");
            emit(msg::Key{msg::Key::Exit, {}});
            return;
        }
        const auto c = keys::classify(byte);
        switch (c.kind) {
            case keys::ByteKind::Key: emit(c.key); break;
            case keys::ByteKind::Utf8Lead: emit(msg::Key{msg::Key::Text, terminal_.read_utf8_char(byte, kSequenceTimeoutMs)}); break;
            case keys::ByteKind::Escape: read_sequence(); break;
            case keys::ByteKind::Ignore: break;
        }
    }
}

void KeyboardWorker::read_sequence() {
    unsigned char intro = 0;
    if (terminal_.read_byte(intro, kSequenceTimeoutMs) != TerminalUI::ReadResult::Byte) { return; }   // lone ESC
    if (intro != '[' && intro != 'O') { return; }   // Alt+key and the like: not used
    std::string body;
    while (body.size() < kMaxSequence) {
        unsigned char b = 0;
        if (terminal_.read_byte(b, kSequenceTimeoutMs) != TerminalUI::ReadResult::Byte) { return; }
        body += static_cast<char>(b);
        if (keys::is_final(b)) { break; }
    }
    if (auto key = keys::sequence(body)) { emit(std::move(*key)); }
}
