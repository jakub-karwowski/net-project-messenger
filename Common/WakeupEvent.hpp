#pragma once

// A wake-up signal (an eventfd). Any thread raises it with signal(); the thread
// that waits watches fd() in poll()/epoll next to its other descriptors, and
// calls clear() once it has woken. Signals raised before clear() collapse into one wake-up.
// It carries no meaning of its own: keep the reason for waking in a flag next to it.
class WakeupEvent {
    public:
        WakeupEvent();   // throws std::system_error when the eventfd cannot be created
        ~WakeupEvent();
        WakeupEvent(const WakeupEvent&) = delete;
        WakeupEvent& operator=(const WakeupEvent&) = delete;
        WakeupEvent(WakeupEvent&& other) noexcept;
        WakeupEvent& operator=(WakeupEvent&& other) noexcept;

        void signal() const noexcept;
        void clear() const noexcept;
        int fd() const noexcept;

    private:
        int fd_;
};
