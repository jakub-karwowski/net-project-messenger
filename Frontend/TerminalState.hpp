#pragma once

// A copy of the terminal's settings taken before the TUI changes them, and a
// way to put them back that is safe at any moment (the Supervisor's hard exit
// calls it with other threads possibly holding any lock).

namespace terminal_state {

// Saves stdin's terminal settings. Call before TerminalUI is constructed.
// Returns false when stdin is not a terminal.
bool save() noexcept;

// Restores the saved settings, resets colours, shows the cursor and clears the
// screen. Only async-signal-safe calls (tcsetattr, write). No-op before save().
void restore() noexcept;

}  // namespace terminal_state
