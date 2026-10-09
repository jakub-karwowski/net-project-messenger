// test_wire.cpp - unit tests for the Wire.hpp application-layer protocol.
//
// Plain assertions (no framework): prints every failed check and exits non-zero
// if anything failed, so it plugs into the usual "build target + run" flow.

#include "Wire.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "FAIL: " << what << '\n';
    }
}

std::span<const std::byte> as_span(const std::vector<std::byte>& buffer) {
    return {buffer.data(), buffer.size()};
}

// Drains every currently-complete message. Sets `ok` to false on a protocol
// error so tests can distinguish "needs more data" from "decoder broke".
std::vector<wire::Message> drain(wire::Decoder& decoder, bool& ok) {
    std::vector<wire::Message> out;
    ok = true;
    while (true) {
        auto item = decoder.next();
        if (!item) {
            ok = false;
            return out;
        }
        if (!*item) {
            return out;
        }
        out.push_back(std::move(**item));
    }
}

// Builds a raw frame header so tests can forge malformed/edge-case frames.
std::vector<std::byte> raw_header(std::uint16_t magic, std::uint8_t version,
                                  std::uint8_t type, std::uint32_t length) {
    std::vector<std::byte> out;
    auto push = [&out](std::uint8_t b) { out.push_back(static_cast<std::byte>(b)); };
    push(static_cast<std::uint8_t>((magic >> 8) & 0xFF));
    push(static_cast<std::uint8_t>(magic & 0xFF));
    push(version);
    push(type);
    push(static_cast<std::uint8_t>((length >> 24) & 0xFF));
    push(static_cast<std::uint8_t>((length >> 16) & 0xFF));
    push(static_cast<std::uint8_t>((length >> 8) & 0xFF));
    push(static_cast<std::uint8_t>(length & 0xFF));
    return out;
}

// ---------------------------------------------------------------------------

void test_connect_round_trip() {
    const wire::Message sent = wire::make_connect(
        42, 77, {{"10.0.0.1", 9000}, {"192.168.0.5", 9001}});

    auto buffer = wire::encode(sent);
    check(buffer.has_value(), "connect: encode succeeds");

    auto decoded = wire::decode(as_span(*buffer));
    check(decoded.has_value(), "connect: decode succeeds");
    check(decoded && *decoded == sent, "connect: round trip is byte-exact");

    const wire::ConnectInfo* info = decoded ? wire::as_connect(*decoded) : nullptr;
    check(info != nullptr, "connect: variant holds ConnectInfo");
    check(info && info->peer_id == 42, "connect: peer id preserved");
    check(info && info->assigned_peer_id == 77, "connect: re-presented id preserved");
    check(info && info->endpoints.size() == 2, "connect: endpoint count");
    check(info && info->endpoints[0] == wire::Endpoint{"10.0.0.1", 9000},
          "connect: first endpoint");
    check(info && wire::as_message(*decoded) == nullptr,
          "connect: not mistaken for a chat message");
}

void test_connect_empty_endpoints() {
    const wire::Message sent = wire::make_connect(7, 0, {});
    auto buffer = wire::encode(sent);
    auto decoded = buffer ? wire::decode(as_span(*buffer))
                          : std::expected<wire::Message, wire::WireError>{
                                std::unexpected(wire::WireError::MalformedPayload)};
    check(decoded && *decoded == sent, "connect: empty endpoint list round trips");

    const wire::ConnectInfo* info = decoded ? wire::as_connect(*decoded) : nullptr;
    check(info && info->assigned_peer_id == 0,
          "connect: never-assigned id decodes as 0");
}

void test_connect_ack_round_trip() {
    const wire::Message sent = wire::make_connect_ack(101, 202);
    auto buffer = wire::encode(sent);
    check(buffer.has_value(), "connect_ack: encode succeeds");

    auto decoded = wire::decode(as_span(*buffer));
    check(decoded.has_value(), "connect_ack: decode succeeds");
    check(decoded && *decoded == sent, "connect_ack: round trip is byte-exact");

    const wire::ConnectInfoAck* ack = decoded ? wire::as_connect_ack(*decoded) : nullptr;
    check(ack != nullptr, "connect_ack: variant holds ConnectInfoAck");
    check(ack && ack->peer_id == 101, "connect_ack: responder id preserved");
    check(ack && ack->ack_peer_id == 202, "connect_ack: acked id preserved");
    check(decoded && wire::as_connect(*decoded) == nullptr,
          "connect_ack: not mistaken for ConnectInfo");
    check(decoded && wire::as_message(*decoded) == nullptr,
          "connect_ack: not mistaken for ChatMessage");
}

void test_chat_message_round_trip() {
    std::string body = "héllo \xF0\x9F\x91\x8B with embedded nul";
    body.push_back('\0');
    body += "and more";

    const wire::Message sent = wire::make_chat_message(7, 9, body);
    auto buffer = wire::encode(sent);
    check(buffer.has_value(), "message: encode succeeds");

    auto decoded = wire::decode(as_span(*buffer));
    check(decoded.has_value(), "message: decode succeeds");
    check(decoded && *decoded == sent, "message: round trip is byte-exact");

    const wire::ChatMessage* msg = decoded ? wire::as_message(*decoded) : nullptr;
    check(msg && msg->conversation_id == 7, "message: conversation id preserved");
    check(msg && msg->sender_id == 9, "message: sender id preserved");
    check(msg && msg->body == body, "message: utf-8 and embedded nul preserved");
}

void test_header_layout() {
    const wire::Message sent = wire::make_chat_message(1, 2, "");
    auto buffer = wire::encode(sent);
    check(buffer.has_value(), "header: encode succeeds");
    if (!buffer) { return; }

    const std::vector<std::byte>& b = *buffer;
    check(b.size() == wire::kFrameHeaderSize + 20, "header: total frame size");
    check(std::to_integer<std::uint8_t>(b[0]) == 0x4E &&
              std::to_integer<std::uint8_t>(b[1]) == 0x50,
          "header: magic bytes");
    check(std::to_integer<std::uint8_t>(b[2]) == wire::kVersion, "header: version");
    check(std::to_integer<std::uint8_t>(b[3]) ==
              static_cast<std::uint8_t>(wire::MessageType::Message),
          "header: type byte is Message");
    check(std::to_integer<std::uint8_t>(b[4]) == 0 &&
              std::to_integer<std::uint8_t>(b[5]) == 0 &&
              std::to_integer<std::uint8_t>(b[6]) == 0 &&
              std::to_integer<std::uint8_t>(b[7]) == 20,
          "header: big-endian payload length");

    auto connect = wire::encode(wire::make_connect(0, 0, {}));
    check(connect && std::to_integer<std::uint8_t>((*connect)[3]) ==
                         static_cast<std::uint8_t>(wire::MessageType::Connect),
          "header: type byte is Connect");
    check(connect && connect->size() == wire::kFrameHeaderSize + 20,
          "header: connect frame carries two ids and an endpoint count");

    auto ack = wire::encode(wire::make_connect_ack(1, 2));
    check(ack && std::to_integer<std::uint8_t>((*ack)[3]) ==
                     static_cast<std::uint8_t>(wire::MessageType::ConnectAck),
          "header: type byte is ConnectAck");
}

void test_encode_into_appends() {
    std::vector<std::byte> buffer;
    const wire::Message first = wire::make_connect(1, 0, {{"127.0.0.1", 4000}});
    const wire::Message second = wire::make_chat_message(3, 4, "payload");

    check(wire::encode_into(buffer, first), "encode_into: first frame appended");
    check(wire::encode_into(buffer, second), "encode_into: second frame appended");

    wire::Decoder decoder;
    decoder.feed(as_span(buffer));
    bool ok = false;
    auto messages = drain(decoder, ok);

    check(ok, "encode_into: no protocol errors");
    check(messages.size() == 2, "encode_into: both frames decoded");
    check(messages.size() == 2 && messages[0] == first, "encode_into: first matches");
    check(messages.size() == 2 && messages[1] == second, "encode_into: second matches");
}

void test_streaming_byte_by_byte() {
    const wire::Message a = wire::make_connect(11, 0, {{"1.2.3.4", 5555}});
    const wire::Message b = wire::make_chat_message(1, 2, "split me across reads");

    std::vector<std::byte> first_frame;
    wire::encode_into(first_frame, a);
    std::vector<std::byte> stream = first_frame;
    wire::encode_into(stream, b);

    wire::Decoder decoder;
    std::vector<wire::Message> received;
    bool errored = false;

    for (std::size_t i = 0; i < stream.size(); ++i) {
        decoder.feed(std::span<const std::byte>(stream).subspan(i, 1));
        auto item = decoder.next();
        if (!item) {
            errored = true;
            break;
        }
        if (*item) {
            received.push_back(std::move(**item));
        }

        // The first frame must not surface until its final byte has arrived.
        if (i + 1 < first_frame.size()) {
            check(received.empty(), "streaming: no message before frame completes");
        }
        if (i + 1 == first_frame.size()) {
            check(received.size() == 1 && received[0] == a,
                  "streaming: first frame completes exactly on its last byte");
        }
    }

    check(!errored, "streaming: byte-by-byte feed never errors");
    check(received.size() == 2, "streaming: both messages recovered");
    check(received.size() == 2 && received[0] == a, "streaming: first matches");
    check(received.size() == 2 && received[1] == b, "streaming: second matches");
    check(decoder.buffered() == 0, "streaming: buffer drained");
}

void test_partial_then_complete() {
    auto buffer = wire::encode(wire::make_chat_message(5, 6, "hello"));
    check(buffer.has_value(), "partial: encode succeeds");
    if (!buffer) { return; }

    wire::Decoder decoder;
    const std::span<const std::byte> all = as_span(*buffer);

    decoder.feed(all.first(4));
    auto early = decoder.next();
    check(early.has_value(), "partial: header-only read is not an error");
    check(early && !*early, "partial: header-only read yields no message");

    decoder.feed(all.subspan(4));
    auto late = decoder.next();
    check(late && *late, "partial: remaining bytes complete the frame");
    check(late && *late && **late == wire::make_chat_message(5, 6, "hello"),
          "partial: recovered message matches");
}

void test_resync_on_leading_garbage() {
    std::vector<std::byte> stream{std::byte{0x00}, std::byte{0x11}, std::byte{0x22}};
    const wire::Message sent = wire::make_chat_message(1, 1, "after garbage");
    wire::encode_into(stream, sent);

    wire::Decoder decoder;
    decoder.feed(as_span(stream));
    bool ok = false;
    auto messages = drain(decoder, ok);

    check(ok, "resync: garbage prefix is skipped without an error");
    check(messages.size() == 1, "resync: exactly one message recovered");
    check(messages.size() == 1 && messages[0] == sent, "resync: message intact");
}

void test_bad_magic_without_resync() {
    // Eight bytes with no magic marker at all: nothing to resync onto.
    std::vector<std::byte> junk(wire::kFrameHeaderSize, std::byte{0x5A});
    wire::Decoder decoder;
    decoder.feed(as_span(junk));

    auto item = decoder.next();
    check(!item, "bad magic: reported as an error");
    check(!item && item.error() == wire::WireError::BadMagic,
          "bad magic: correct error code");
}

void test_unsupported_version_recovers() {
    std::vector<std::byte> stream = raw_header(wire::kMagic, wire::kVersion + 1,
                                               static_cast<std::uint8_t>(wire::MessageType::Message), 0);
    const wire::Message sent = wire::make_chat_message(2, 2, "still here");
    wire::encode_into(stream, sent);

    wire::Decoder decoder;
    decoder.feed(as_span(stream));

    auto first = decoder.next();
    check(!first, "version: unsupported version is an error");
    check(!first && first.error() == wire::WireError::UnsupportedVersion,
          "version: correct error code");

    auto second = decoder.next();
    check(second && *second, "version: recovers on the following frame");
    check(second && *second && **second == sent, "version: following frame intact");
}

void test_unknown_type() {
    std::vector<std::byte> frame = raw_header(wire::kMagic, wire::kVersion, 99, 0);
    wire::Decoder decoder;
    decoder.feed(as_span(frame));

    auto item = decoder.next();
    check(!item && item.error() == wire::WireError::UnknownType,
          "unknown type: reported");
}

void test_frame_too_large() {
    std::vector<std::byte> frame =
        raw_header(wire::kMagic, wire::kVersion,
                   static_cast<std::uint8_t>(wire::MessageType::Message),
                   wire::kMaxPayloadSize + 1);
    wire::Decoder decoder;
    decoder.feed(as_span(frame));

    auto item = decoder.next();
    check(!item && item.error() == wire::WireError::FrameTooLarge,
          "oversized: reported as FrameTooLarge");
}

void test_malformed_payload() {
    // A Connect frame whose payload is too short to hold a peer id.
    std::vector<std::byte> frame = raw_header(
        wire::kMagic, wire::kVersion,
        static_cast<std::uint8_t>(wire::MessageType::Connect), 2);
    frame.push_back(std::byte{0x00});
    frame.push_back(std::byte{0x01});

    wire::Decoder decoder;
    decoder.feed(as_span(frame));

    auto item = decoder.next();
    check(!item && item.error() == wire::WireError::MalformedPayload,
          "malformed: short payload reported");

    // A Connect frame with a peer id but no room for the re-presented id.
    std::vector<std::byte> truncated = raw_header(
        wire::kMagic, wire::kVersion,
        static_cast<std::uint8_t>(wire::MessageType::Connect), 8);
    truncated.insert(truncated.end(), 8, std::byte{0x01});

    wire::Decoder second;
    second.feed(as_span(truncated));

    auto result = second.next();
    check(!result && result.error() == wire::WireError::MalformedPayload,
          "malformed: missing re-presented id reported");
}

void test_large_payload_round_trip() {
    std::string body(200'000, 'x');
    for (std::size_t i = 0; i < body.size(); i += 97) {
        body[i] = static_cast<char>('a' + (i % 26));
    }
    const wire::Message sent = wire::make_chat_message(1, 2, body);

    auto buffer = wire::encode(sent);
    check(buffer.has_value(), "large: encode succeeds");

    wire::Decoder decoder;
    if (buffer) {
        // Feed in awkward chunks to exercise reassembly of a big frame.
        const std::span<const std::byte> all = as_span(*buffer);
        std::size_t offset = 0;
        while (offset < all.size()) {
            const std::size_t n = std::min<std::size_t>(777, all.size() - offset);
            decoder.feed(all.subspan(offset, n));
            offset += n;
        }
    }

    auto item = decoder.next();
    check(item.has_value(), "large: chunked feed never errors");
    check(item && *item && **item == sent, "large: payload recovered exactly");
    check(decoder.buffered() == 0, "large: no trailing bytes");
}

void test_many_small_frames() {
    constexpr int kCount = 2000;
    std::vector<std::byte> stream;
    for (int i = 0; i < kCount; ++i) {
        wire::encode_into(stream, wire::make_chat_message(1, static_cast<std::uint64_t>(i),
                                                          "m" + std::to_string(i)));
    }

    wire::Decoder decoder;
    std::vector<wire::Message> received;
    const std::span<const std::byte> all = as_span(stream);

    // Feed in 64-byte chunks, draining after each feed.
    bool ok = true;
    for (std::size_t offset = 0; offset < all.size() && ok; offset += 64) {
        const std::size_t n = std::min<std::size_t>(64, all.size() - offset);
        decoder.feed(all.subspan(offset, n));
        auto batch = drain(decoder, ok);
        for (auto& m : batch) {
            received.push_back(std::move(m));
        }
    }

    check(ok, "many: no protocol errors");
    check(received.size() == static_cast<std::size_t>(kCount), "many: all frames decoded");
    bool ordered = received.size() == static_cast<std::size_t>(kCount);
    for (int i = 0; ordered && i < kCount; ++i) {
        const wire::ChatMessage* msg = wire::as_message(received[static_cast<std::size_t>(i)]);
        ordered = msg && msg->sender_id == static_cast<std::uint64_t>(i);
    }
    check(ordered, "many: frames stay in order");
    check(decoder.buffered() == 0, "many: buffer drained (compaction works)");
}

}  // namespace

int main() {
    test_connect_round_trip();
    test_connect_empty_endpoints();
    test_connect_ack_round_trip();
    test_chat_message_round_trip();
    test_header_layout();
    test_encode_into_appends();
    test_streaming_byte_by_byte();
    test_partial_then_complete();
    test_resync_on_leading_garbage();
    test_bad_magic_without_resync();
    test_unsupported_version_recovers();
    test_unknown_type();
    test_frame_too_large();
    test_malformed_payload();
    test_large_payload_round_trip();
    test_many_small_frames();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    if (g_failures != 0) {
        std::cout << g_failures << " check(s) FAILED\n";
        return 1;
    }
    std::cout << "all wire tests passed\n";
    return 0;
}
