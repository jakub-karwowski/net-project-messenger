#pragma once

// Wire.hpp - application-layer framing and (de)serialization for the p2p chat.
//
// This header is the single source of truth for "what bytes go on the socket".
// It is transport agnostic: it only produces/consumes `std::byte` buffers, so it
// plugs straight into `TcpConnection::send` / `TcpConnection::receive`.
//
// Wire format
// -----------
// A stream is a sequence of length-prefixed frames:
//
//   +--------+---------+--------+----------+-------------------+
//   | magic  | version | type   | length   | payload (length)  |
//   | u16 BE | u8      | u8     | u32 BE   | length bytes      |
//   +--------+---------+--------+----------+-------------------+
//
// Every integer on the wire is big-endian (network byte order). `length` counts
// only the payload, so the full frame is `kFrameHeaderSize + length` bytes.
//
// Message types
// -------------
//   * Connect (1) - `ConnectInfo`: sent after a connection is established. A node
//     names each peer it talks to: `peer_id` is the id the *sender* generated for
//     the *receiver* ("I will know you as ..."), `assigned_peer_id` is the id the
//     *receiver* generated for the *sender* the last time they met ("you know me
//     as ...", 0 when it has never been told), and `endpoints` are where the
//     sender accepts inbound connections. Re-presenting `assigned_peer_id` is how
//     a peer that moved (new ip) is recognised instead of being treated as new.
//   * ConnectAck (3) - `ConnectInfoAck`: the reply the peer sends after it has
//     accepted a Connect. `peer_id` is the id the responder generated for the
//     initiator (the Ack's receiver) and `ack_peer_id` echoes the id the
//     initiator generated for the responder, so both sides learn the names the
//     other side uses for them. The `peer_id` here is the very value the
//     initiator later re-presents as `ConnectInfo::assigned_peer_id`.
//   * Message (2) - `ChatMessage`: a user message routed through the overlay.
//
// Usage
// -----
//   Sending:
//       auto buffer = wire::encode(wire::make_chat_message(conv_id, peer_id, "hi"));
//       conn.send(*buffer);
//
//   Receiving (resumable, tolerant of partial reads and coalesced frames):
//       wire::Decoder decoder;
//       for (;;) {
//           auto chunk = conn.receive(scratch);
//           decoder.feed(*chunk);
//           for (;;) {
//               wire::Decoded next = decoder.next();
//               if (std::holds_alternative<wire::NeedMore>(next)) break;
//               if (auto* err = std::get_if<wire::WireError>(&next)) { report(*err); continue; }
//               handle(std::get<wire::Message>(next));
//           }
//       }

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace wire {

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

constexpr std::uint16_t kMagic = 0x4E50;  // 'N' 'P'
constexpr std::uint8_t kVersion = 1;

// magic(2) + version(1) + type(1) + length(4)
constexpr std::size_t kFrameHeaderSize = 8;

// Hard ceiling for a single payload. Guards the decoder against hostile or
// corrupt length fields that would otherwise trigger huge allocations.
constexpr std::uint32_t kMaxPayloadSize = 16u * 1024u * 1024u;  // 16 MiB

// The longest ChatMessage body that still fits in one frame: the payload also
// holds conversation_id(8), sender_id(8) and the body length(4).
constexpr std::size_t kMaxChatBodySize = kMaxPayloadSize - 20;

enum class MessageType : std::uint8_t {
    Connect = 1,
    Message = 2,
    ConnectAck = 3,
};

// Recoverable protocol problems. All of them leave the decoder in a defined
// state so the caller may keep feeding bytes (the stream resynchronizes on the
// next magic marker).
enum class WireError {
    BadMagic,
    UnsupportedVersion,
    UnknownType,
    FrameTooLarge,
    MalformedPayload,
};

constexpr std::string_view to_string(WireError err) noexcept {
    switch (err) {
        case WireError::BadMagic:          return "bad frame magic";
        case WireError::UnsupportedVersion:return "unsupported protocol version";
        case WireError::UnknownType:       return "unknown message type";
        case WireError::FrameTooLarge:     return "frame exceeds maximum payload size";
        case WireError::MalformedPayload:  return "malformed message payload";
    }
    return "unknown wire error";
}

// ---------------------------------------------------------------------------
// Message model
// ---------------------------------------------------------------------------

// An address a peer can be reached at. `address` is a textual IPv4 address
// (dotted quad, e.g. "127.0.0.1") so it feeds straight into TcpListener /
// TcpConnection::connect without extra conversion.
//
// Port 0 means "this side accepts no connections: never dial it". Port 0 is
// reserved and can never be connected to, so it is the natural sentinel; a duplex
// peer that only dials (stressgen --duplex) advertises it. Receivers must not dial
// it: on Linux a connect() to port 0 is not refused but blocks for the whole SYN
// retry budget (~130 s). TcpConnection::connect() rejects port 0 up front.
struct Endpoint {
    std::string address;
    std::uint16_t port = 0;
    friend bool operator==(const Endpoint&, const Endpoint&) = default;
};

// Type 1: connection handshake. `peer_id` is the id the sender generated for the
// receiver ("I will know you as ..."); `assigned_peer_id` is the id the receiver
// generated for the sender the last time they talked ("you know me as ..."), or 0
// while the sender has never been told. `endpoints` lists every endpoint the
// sender accepts inbound connections on.
struct ConnectInfo {
    std::uint64_t peer_id = 0;
    std::uint64_t assigned_peer_id = 0;
    std::vector<Endpoint> endpoints;
    friend bool operator==(const ConnectInfo&, const ConnectInfo&) = default;
};

// Type 2: a user message. `conversation_id` routes it to the right conversation
// and `sender_id` identifies its author.
struct ChatMessage {
    std::uint64_t conversation_id = 0;
    std::uint64_t sender_id = 0;
    std::string body;
    friend bool operator==(const ChatMessage&, const ChatMessage&) = default;
};

// Type 3: the reply to a ConnectInfo. `peer_id` is the id the responder generated
// for the initiator (the Ack's receiver), and `ack_peer_id` echoes the id the
// initiator generated for the responder (the Ack's sender), so both sides learn
// the names the other uses for them. `peer_id` is the value the initiator stores
// as "the name the peer gave me" and re-presents as ConnectInfo::assigned_peer_id.
struct ConnectInfoAck {
    std::uint64_t peer_id = 0;
    std::uint64_t ack_peer_id = 0;
    friend bool operator==(const ConnectInfoAck&, const ConnectInfoAck&) = default;
};

// A decoded/encodable message. The active alternative selects the wire type
// (see `message_type`).
using Message = std::variant<ConnectInfo, ConnectInfoAck, ChatMessage>;

inline MessageType message_type(const Message& msg) noexcept {
    if (std::holds_alternative<ConnectInfo>(msg)) { return MessageType::Connect; }
    if (std::holds_alternative<ConnectInfoAck>(msg)) { return MessageType::ConnectAck; }
    return MessageType::Message;
}

inline const ConnectInfo* as_connect(const Message& msg) noexcept {
    return std::get_if<ConnectInfo>(&msg);
}
inline const ConnectInfoAck* as_connect_ack(const Message& msg) noexcept {
    return std::get_if<ConnectInfoAck>(&msg);
}
inline const ChatMessage* as_message(const Message& msg) noexcept {
    return std::get_if<ChatMessage>(&msg);
}

inline Message make_connect(std::uint64_t peer_id,
                            std::uint64_t assigned_peer_id,
                            std::vector<Endpoint> endpoints) {
    return ConnectInfo{peer_id, assigned_peer_id, std::move(endpoints)};
}
inline Message make_connect_ack(std::uint64_t peer_id, std::uint64_t ack_peer_id) {
    return ConnectInfoAck{peer_id, ack_peer_id};
}
inline Message make_chat_message(std::uint64_t conversation_id,
                                 std::uint64_t sender_id,
                                 std::string body) {
    return ChatMessage{conversation_id, sender_id, std::move(body)};
}

// ---------------------------------------------------------------------------
// Byte helpers (big-endian)
// ---------------------------------------------------------------------------

namespace detail {

inline std::uint8_t to_u8(std::byte b) noexcept {
    return std::to_integer<std::uint8_t>(b);
}

inline void append_u8(std::vector<std::byte>& out, std::uint8_t v) {
    out.push_back(static_cast<std::byte>(v));
}

inline void append_u16(std::vector<std::byte>& out, std::uint16_t v) {
    out.push_back(static_cast<std::byte>((v >> 8) & 0xFF));
    out.push_back(static_cast<std::byte>(v & 0xFF));
}

inline void append_u32(std::vector<std::byte>& out, std::uint32_t v) {
    out.push_back(static_cast<std::byte>((v >> 24) & 0xFF));
    out.push_back(static_cast<std::byte>((v >> 16) & 0xFF));
    out.push_back(static_cast<std::byte>((v >> 8) & 0xFF));
    out.push_back(static_cast<std::byte>(v & 0xFF));
}

inline void append_u64(std::vector<std::byte>& out, std::uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::byte>((v >> shift) & 0xFF));
    }
}

inline void append_string(std::vector<std::byte>& out, std::string_view text) {
    append_u32(out, static_cast<std::uint32_t>(text.size()));
    const auto* first = reinterpret_cast<const std::byte*>(text.data());
    out.insert(out.end(), first, first + text.size());
}

inline std::uint16_t load_u16(const std::byte* p) noexcept {
    return static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(to_u8(p[0])) << 8) |
        static_cast<std::uint16_t>(to_u8(p[1])));
}

inline std::uint32_t load_u32(const std::byte* p) noexcept {
    return (static_cast<std::uint32_t>(to_u8(p[0])) << 24) |
           (static_cast<std::uint32_t>(to_u8(p[1])) << 16) |
           (static_cast<std::uint32_t>(to_u8(p[2])) << 8) |
           static_cast<std::uint32_t>(to_u8(p[3]));
}

// Bounds-checked cursor over a payload. Returns false the moment the buffer runs
// out so callers can bail out without reading past the end.
class Reader {
    public:
        explicit Reader(std::span<const std::byte> data) noexcept : data_(data) {}

        bool read_u8(std::uint8_t& out) noexcept {
            if (remaining() < 1) { return false; }
            out = to_u8(data_[pos_++]);
            return true;
        }

        bool read_u16(std::uint16_t& out) noexcept {
            if (remaining() < 2) { return false; }
            out = load_u16(data_.data() + pos_);
            pos_ += 2;
            return true;
        }

        bool read_u32(std::uint32_t& out) noexcept {
            if (remaining() < 4) { return false; }
            out = load_u32(data_.data() + pos_);
            pos_ += 4;
            return true;
        }

        bool read_u64(std::uint64_t& out) noexcept {
            if (remaining() < 8) { return false; }
            const std::byte* p = data_.data() + pos_;
            std::uint64_t v = 0;
            for (int i = 0; i < 8; ++i) { v = (v << 8) | to_u8(p[i]); }
            out = v;
            pos_ += 8;
            return true;
        }

        bool read_string(std::string& out) {
            std::uint32_t n = 0;
            if (!read_u32(n)) { return false; }
            if (n > remaining()) { return false; }
            out.assign(reinterpret_cast<const char*>(data_.data() + pos_), n);
            pos_ += n;
            return true;
        }

        std::size_t remaining() const noexcept { return data_.size() - pos_; }

    private:
        std::span<const std::byte> data_;
        std::size_t pos_ = 0;
};

// --- payload serialization --------------------------------------------------

inline void serialize(std::vector<std::byte>& out, const ConnectInfo& info) {
    append_u64(out, info.peer_id);
    append_u64(out, info.assigned_peer_id);
    append_u32(out, static_cast<std::uint32_t>(info.endpoints.size()));
    for (const Endpoint& endpoint : info.endpoints) {
        append_string(out, endpoint.address);
        append_u16(out, endpoint.port);
    }
}

inline void serialize(std::vector<std::byte>& out, const ConnectInfoAck& ack) {
    append_u64(out, ack.peer_id);
    append_u64(out, ack.ack_peer_id);
}

inline void serialize(std::vector<std::byte>& out, const ChatMessage& msg) {
    append_u64(out, msg.conversation_id);
    append_u64(out, msg.sender_id);
    append_string(out, msg.body);
}

// --- payload deserialization ------------------------------------------------

inline bool deserialize(Reader& reader, ConnectInfo& out) {
    if (!reader.read_u64(out.peer_id)) { return false; }
    if (!reader.read_u64(out.assigned_peer_id)) { return false; }

    std::uint32_t count = 0;
    if (!reader.read_u32(count)) { return false; }
    // Each endpoint needs at least a 4-byte string length plus a 2-byte port;
    // reject counts that could not possibly fit in the remaining bytes.
    constexpr std::size_t kMinEndpointBytes = 6;
    if (count > reader.remaining() / kMinEndpointBytes) { return false; }

    out.endpoints.clear();
    out.endpoints.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        Endpoint endpoint;
        if (!reader.read_string(endpoint.address)) { return false; }
        if (!reader.read_u16(endpoint.port)) { return false; }
        out.endpoints.push_back(std::move(endpoint));
    }
    return true;
}

inline bool deserialize(Reader& reader, ChatMessage& out) {
    return reader.read_u64(out.conversation_id) &&
           reader.read_u64(out.sender_id) &&
           reader.read_string(out.body);
}

inline bool deserialize(Reader& reader, ConnectInfoAck& out) {
    return reader.read_u64(out.peer_id) && reader.read_u64(out.ack_peer_id);
}

// Maps a wire type byte + payload span onto a concrete `Message`.
inline std::expected<Message, WireError> decode_payload(MessageType type,
                                                        std::span<const std::byte> payload) {
    Reader reader{payload};
    switch (type) {
        case MessageType::Connect: {
            ConnectInfo info;
            if (!deserialize(reader, info)) {
                return std::unexpected(WireError::MalformedPayload);
            }
            return Message{std::move(info)};
        }
        case MessageType::Message: {
            ChatMessage msg;
            if (!deserialize(reader, msg)) {
                return std::unexpected(WireError::MalformedPayload);
            }
            return Message{std::move(msg)};
        }
        case MessageType::ConnectAck: {
            ConnectInfoAck ack;
            if (!deserialize(reader, ack)) {
                return std::unexpected(WireError::MalformedPayload);
            }
            return Message{std::move(ack)};
        }
    }
    return std::unexpected(WireError::UnknownType);
}

}  // namespace detail

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

// Appends one complete frame for `msg` to `out`. Returns false (leaving `out`
// unchanged) when the payload would exceed `kMaxPayloadSize`.
inline bool encode_into(std::vector<std::byte>& out, const Message& msg) {
    std::vector<std::byte> payload;
    std::visit([&payload](const auto& body) { detail::serialize(payload, body); }, msg);
    if (payload.size() > kMaxPayloadSize) { return false; }

    detail::append_u16(out, kMagic);
    detail::append_u8(out, kVersion);
    detail::append_u8(out, static_cast<std::uint8_t>(message_type(msg)));
    detail::append_u32(out, static_cast<std::uint32_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end());
    return true;
}

// Builds a ready-to-send buffer containing exactly one framed message.
inline std::expected<std::vector<std::byte>, WireError> encode(const Message& msg) {
    std::vector<std::byte> out;
    if (!encode_into(out, msg)) {
        return std::unexpected(WireError::FrameTooLarge);
    }
    return out;
}

// Decodes a buffer that is expected to hold exactly one complete frame.
inline std::expected<Message, WireError> decode(std::span<const std::byte> frame) {
    if (frame.size() < kFrameHeaderSize) {
        return std::unexpected(WireError::MalformedPayload);
    }
    if (detail::load_u16(frame.data()) != kMagic) {
        return std::unexpected(WireError::BadMagic);
    }
    if (detail::to_u8(frame[2]) != kVersion) {
        return std::unexpected(WireError::UnsupportedVersion);
    }
    const std::uint32_t length = detail::load_u32(frame.data() + 4);
    if (length > kMaxPayloadSize) {
        return std::unexpected(WireError::FrameTooLarge);
    }
    if (frame.size() < kFrameHeaderSize + length) {
        return std::unexpected(WireError::MalformedPayload);
    }
    const auto type = static_cast<MessageType>(detail::to_u8(frame[3]));
    return detail::decode_payload(type, frame.subspan(kFrameHeaderSize, length));
}

// ---------------------------------------------------------------------------
// Streaming decoder
// ---------------------------------------------------------------------------

// Decoder::next() has not got a complete frame yet: feed it more bytes.
struct NeedMore {
    friend bool operator==(const NeedMore&, const NeedMore&) = default;
};

// One step of decoding: a complete message, a request for more bytes, or a
// malformed frame (already skipped; the decoder carries on with what follows).
using Decoded = std::variant<Message, NeedMore, WireError>;

// Resumable parser for a received byte stream. Feed it whatever `receive()`
// returned (a partial frame, several frames, or a frame split across calls) and
// pull completed messages out with `next()`.
class Decoder {
    public:
        // Appends a chunk of received bytes to the internal reassembly buffer.
        void feed(std::span<const std::byte> chunk) {
            buffer_.insert(buffer_.end(), chunk.begin(), chunk.end());
        }

        // Returns the next complete message, NeedMore when more bytes are
        // needed, or the WireError of a malformed frame. After an error the
        // decoder stays usable: it resynchronizes on the next magic marker.
        [[nodiscard]] Decoded next() {
            for (;;) {
                compact();
                if (available() < kFrameHeaderSize) {
                    return NeedMore{};
                }

                const std::byte* base = buffer_.data() + offset_;
                if (detail::load_u16(base) != kMagic) {
                    if (!resync_magic()) {
                        return WireError::BadMagic;
                    }
                    continue;
                }
                if (detail::to_u8(base[2]) != kVersion) {
                    drop_to(offset_ + 2);  // skip this magic, resync on the next
                    return WireError::UnsupportedVersion;
                }

                const std::uint32_t length = detail::load_u32(base + 4);
                if (length > kMaxPayloadSize) {
                    drop_to(offset_ + kFrameHeaderSize);
                    return WireError::FrameTooLarge;
                }
                if (available() < kFrameHeaderSize + length) {
                    return NeedMore{};  // wait for the rest
                }

                const auto type = static_cast<MessageType>(detail::to_u8(base[3]));
                const std::span<const std::byte> payload{
                    buffer_.data() + offset_ + kFrameHeaderSize, length};

                auto msg = detail::decode_payload(type, payload);
                drop_to(offset_ + kFrameHeaderSize + length);
                if (!msg) {
                    return msg.error();
                }
                return Decoded{std::move(*msg)};
            }
        }

        [[nodiscard]] std::size_t buffered() const noexcept { return available(); }

        void reset() noexcept {
            buffer_.clear();
            offset_ = 0;
        }

    private:
        // Erase consumed bytes once they are worth reclaiming, so feeding many
        // small frames does not grow the buffer without bound.
        static constexpr std::size_t kCompactThreshold = 4096;

        std::size_t available() const noexcept { return buffer_.size() - offset_; }

        void compact() {
            if (offset_ == 0) { return; }
            if (offset_ == buffer_.size()) {
                buffer_.clear();
                offset_ = 0;
            } else if (offset_ >= kCompactThreshold) {
                buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(offset_));
                offset_ = 0;
            }
        }

        void drop_to(std::size_t position) noexcept { offset_ = position; }

        // Scans for the next magic marker after the current position. On success
        // the read position is moved onto it. On failure everything is discarded
        // (keeping a trailing 'N' in case it starts a split magic marker).
        bool resync_magic() {
            const std::size_t n = buffer_.size();
            for (std::size_t i = offset_ + 1; i + 1 < n; ++i) {
                if (detail::to_u8(buffer_[i]) == 0x4E &&
                    detail::to_u8(buffer_[i + 1]) == 0x50) {
                    offset_ = i;
                    return true;
                }
            }
            offset_ = (n > 0 && detail::to_u8(buffer_[n - 1]) == 0x4E) ? n - 1 : n;
            return false;
        }

        std::vector<std::byte> buffer_;
        std::size_t offset_ = 0;
};

}  // namespace wire
