#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <string_view>
#include <tuple>
#include <vector>


namespace utils {

namespace utf8 {

    inline bool is_continuation(unsigned char byte) noexcept {
        return (byte & 0xC0) == 0x80;
    }

    // Number of bytes in the UTF-8 sequence introduced by `lead` (1 if invalid).
    inline size_t sequence_length(unsigned char lead) noexcept {
        if (lead < 0x80)           { return 1; }
        if ((lead & 0xE0) == 0xC0) { return 2; }
        if ((lead & 0xF0) == 0xE0) { return 3; }
        if ((lead & 0xF8) == 0xF0) { return 4; }
        return 1;
    }

    // First codepoint boundary strictly after `i` (clamped to s.size()).
    inline size_t next(std::string_view s, size_t i) noexcept {
        if (i >= s.size()) { return s.size(); }
        return std::min(i + sequence_length(static_cast<unsigned char>(s[i])), s.size());
    }

    // First codepoint boundary strictly before `i`.
    inline size_t prev(std::string_view s, size_t i) noexcept {
        if (i == 0) { return 0; }
        size_t j = std::min(i, s.size()) - 1;
        while (j > 0 && is_continuation(static_cast<unsigned char>(s[j]))) { --j; }
        return j;
    }

    // Rounds `i` down to the nearest codepoint boundary.
    inline size_t align(std::string_view s, size_t i) noexcept {
        if (i >= s.size()) { return s.size(); }
        while (i > 0 && is_continuation(static_cast<unsigned char>(s[i]))) { --i; }
        return i;
    }

    inline size_t count(std::string_view s) noexcept {
        size_t n = 0;
        for (size_t i = 0; i < s.size(); i = next(s, i)) { ++n; }
        return n;
    }

    // Decodes the codepoint at `i` and advances `i` past it.
    inline char32_t decode(std::string_view s, size_t& i) noexcept {
        if (i >= s.size()) { return U'\0'; }
        static constexpr char32_t lead_mask[] = {0x7F, 0x1F, 0x0F, 0x07};
        const unsigned char lead = static_cast<unsigned char>(s[i]);
        const size_t length = std::min(sequence_length(lead), s.size() - i);
        char32_t codepoint = 0;
        if (length == 1) {
            codepoint = lead;
        } else {
            codepoint = lead & lead_mask[length - 1];
            for (size_t k = 1; k < length; ++k) {
                codepoint = (codepoint << 6)
                    | (static_cast<unsigned char>(s[i + k]) & 0x3F);
            }
        }
        i += length;
        return codepoint;
    }

    inline bool is_combining(char32_t codepoint) noexcept {
        return (codepoint >= 0x0300 && codepoint <= 0x036F)
            || (codepoint >= 0x1AB0 && codepoint <= 0x1AFF)
            || (codepoint >= 0x1DC0 && codepoint <= 0x1DFF)
            || (codepoint >= 0x20D0 && codepoint <= 0x20FF)
            || (codepoint >= 0xFE20 && codepoint <= 0xFE2F);
    }

    // East-Asian Wide/Fullwidth approximation (common ranges).
    inline bool is_wide(char32_t codepoint) noexcept {
        return (codepoint >= 0x1100  && codepoint <= 0x115F)
            || (codepoint >= 0x2E80  && codepoint <= 0x303E)
            || (codepoint >= 0x3041  && codepoint <= 0x33FF)
            || (codepoint >= 0x3400  && codepoint <= 0x4DBF)
            || (codepoint >= 0x4E00  && codepoint <= 0x9FFF)
            || (codepoint >= 0xA000  && codepoint <= 0xA4CF)
            || (codepoint >= 0xAC00  && codepoint <= 0xD7A3)
            || (codepoint >= 0xF900  && codepoint <= 0xFAFF)
            || (codepoint >= 0xFE30  && codepoint <= 0xFE4F)
            || (codepoint >= 0xFF00  && codepoint <= 0xFF60)
            || (codepoint >= 0xFFE0  && codepoint <= 0xFFE6)
            || (codepoint >= 0x1F300 && codepoint <= 0x1FAFF)
            || (codepoint >= 0x20000 && codepoint <= 0x3FFFD);
    }

    inline int codepoint_width(char32_t codepoint) noexcept {
        if (codepoint < 0x20 || (codepoint >= 0x7F && codepoint < 0xA0)) { return 0; }
        if (is_combining(codepoint)) { return 0; }
        if (is_wide(codepoint)) { return 2; }
        return 1;
    }

    // Display columns of s[0, up_to), ignoring CSI/SGR escape sequences and
    // stopping at the first newline.
    inline int width(std::string_view s, size_t up_to) noexcept {
        const size_t end = std::min(up_to, s.size());
        int columns = 0;
        for (size_t i = 0; i < end;) {
            const unsigned char byte = static_cast<unsigned char>(s[i]);
            if (byte == '\033' && i + 1 < end && s[i + 1] == '[') {
                i += 2;
                while (i < end && !(static_cast<unsigned char>(s[i]) >= 0x40
                                 && static_cast<unsigned char>(s[i]) <= 0x7E)) { ++i; }
                if (i < end) { ++i; }
                continue;
            }
            if (byte == '\n' || byte == '\r') { break; }
            columns += codepoint_width(decode(s, i));
        }
        return columns;
    }

    inline int width(std::string_view s) noexcept {
        return width(s, s.size());
    }

} // namespace utf8

    template<std::contiguous_iterator Itr>
    std::tuple<size_t, bool> line_split(
        std::vector<std::string_view>& line_buffer, 
        Itr begin, 
        Itr end,
        int line_length, 
        size_t max_n_lines = std::numeric_limits<size_t>::max()) 
    {  
        if (max_n_lines == 0) { return {0, begin == end}; }
        const size_t initial_buffer_size = line_buffer.size();
        
        Itr line_start_itr = begin;
        Itr previous_word_end_itr = end;
        
        line_length = std::max(line_length, 1);
        int columns = 0;
        Itr next_codepoint = begin;
        for (Itr itr = begin; itr < end; itr = next_codepoint) {
            const size_t available = static_cast<size_t>(std::distance(itr, end));
            const size_t sequence = std::min(utf8::sequence_length(static_cast<unsigned char>(*itr)), available);
            next_codepoint = std::next(itr, static_cast<std::ptrdiff_t>(sequence));
            size_t decode_pos = 0;
            const int codepoint_columns = utf8::codepoint_width(utf8::decode(std::string_view(&*itr, sequence), decode_pos));
            if (*itr == '\n') {
                line_buffer.emplace_back(line_start_itr, itr);
                if (line_buffer.size() - initial_buffer_size >= max_n_lines) { 
                    return {max_n_lines, std::next(itr) >= end}; 
                }
                
                line_start_itr = next_codepoint;
                previous_word_end_itr = end;
                columns = 0;
                continue;
            } 
            else if (*itr == ' ') {
                previous_word_end_itr = itr;
            }

            if (columns > 0 && columns + codepoint_columns > line_length) {
                Itr resume_itr;
                if (previous_word_end_itr != end && previous_word_end_itr > line_start_itr) {
                    line_buffer.emplace_back(line_start_itr, previous_word_end_itr);
                    resume_itr = std::next(previous_word_end_itr);
                } else {
                    line_buffer.emplace_back(line_start_itr, itr);
                    resume_itr = itr;
                }
                if (line_buffer.size() - initial_buffer_size >= max_n_lines) {
                    return {max_n_lines, resume_itr >= end};
                }
                line_start_itr = resume_itr;
                previous_word_end_itr = end;
                if (resume_itr > itr) {
                    // The codepoint that overflowed the line is the space itself,
                    // so the break resumes after it (`resume_itr == next(itr)`) and
                    // `distance(resume_itr, itr)` would be -1 -> SIZE_MAX. The space
                    // is dropped with the break like any other trailing space, so the
                    // new line starts empty and this codepoint must not be counted.
                    columns = 0;
                    continue;
                }
                columns = utf8::width(std::string_view(&*resume_itr, static_cast<size_t>(std::distance(resume_itr, itr))));
            }
            columns += codepoint_columns;
        }
        
        if (line_start_itr < end) {
            line_buffer.emplace_back(line_start_itr, end);
        }
        
        size_t total_added = line_buffer.size() - initial_buffer_size;
        return {total_added, true};
    }

    inline std::tuple<size_t, bool> line_split(
        std::vector<std::string_view>& line_buffer, 
        std::string_view text,
        int line_length, 
        size_t max_n_lines = std::numeric_limits<size_t>::max()) 
    {
        return line_split(line_buffer, text.begin(), text.end(), line_length, max_n_lines);
    }

    inline std::tuple<size_t, bool> line_split_tail(
        std::vector<std::string_view>& line_buffer,
        std::string_view text,
        int line_length,
        size_t max_n_lines)
    {
        if (max_n_lines == 0) { return {0, text.empty()}; }

        const size_t initial_size = line_buffer.size();

        // Skip a prefix so the whole buffer is not wrapped. The true per-column
        // cost is at most 4 bytes (a 4-byte codepoint of width 1); 7 is a
        // deliberate safety margin for text that adds bytes without columns
        // (combining marks, ANSI escapes). A larger stride can only make the
        // wrap start earlier, never later, so it trades extra work for fewer
        // fallbacks. Under-skip is still detected and corrected below.
        const size_t stride = static_cast<size_t>(std::max(line_length, 0)) * 7;
        size_t safe_char_count = text.size();
        if (stride != 0 && stride <= text.size() / max_n_lines) {
            safe_char_count = stride * max_n_lines;
        }

        size_t start_idx = 0;
        if (text.size() > safe_char_count) {
            const size_t search_pos = text.size() - safe_char_count;
            const size_t pos = text.rfind('\n', search_pos);
            if (pos != std::string_view::npos) {
                start_idx = pos + 1;
            }
        }

        size_t generated_lines = 0;
        bool fully_consumed = false;
        std::tie(generated_lines, fully_consumed) =
            line_split(line_buffer, text.substr(start_idx), line_length);

        if (generated_lines < max_n_lines && start_idx != 0) {
            // The guess started too late; redo from the beginning so the tail is
            // complete.
            line_buffer.resize(initial_size);
            start_idx = 0;
            std::tie(generated_lines, fully_consumed) = line_split(line_buffer, text, line_length);
        }

        if (generated_lines > max_n_lines) {
            const size_t excess = generated_lines - max_n_lines;
            line_buffer.erase(
                line_buffer.begin() + initial_size,
                line_buffer.begin() + initial_size + excess
            );
            fully_consumed = false;
        }

        return {line_buffer.size() - initial_size, fully_consumed && (start_idx == 0)};
    }
}