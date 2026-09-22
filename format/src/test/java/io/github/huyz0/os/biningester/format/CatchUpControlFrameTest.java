// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CatchUpControlFrameTest {

    private static final UUID REQUEST = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final RunKey KEY = new RunKey(
            UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), 7);

    private static CatchUpEventFrame eventFrame() {
        return new CatchUpEventFrame(REQUEST, new SubscriptionEvent("session", 4, 9, KEY,
                "segment-41", 41, 2, FetchMode.INLINE, new byte[] {1, 2, 3}));
    }

    private static CatchUpEndFrame endFrame() {
        return new CatchUpEndFrame(REQUEST);
    }

    @Test
    void requestRoundTripsItsNodeScopedStreamsAndResumeOffsets() throws Exception {
        var request = new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(KEY, 41),
                new CatchUpRequestFrame.Stream(
                        new RunKey(UUID.fromString("00000000-0000-0000-0000-000000000001"), 0),
                        99)));

        assertThat(CatchUpRequestFrame.decode(request.encode())).isEqualTo(request);
    }

    @Test
    void responseFramesRoundTripTheExistingEventShapeAndCompletion() throws Exception {
        var catchUpEvent = eventFrame();

        assertThat(CatchUpEventFrame.decode(catchUpEvent.encode())).isEqualTo(catchUpEvent);
        assertThat(CatchUpEndFrame.decode(endFrame().encode())).isEqualTo(endFrame());
    }

    @Test
    void eachControlFrameCarriesAnExplicitKindTag() {
        var request = new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(KEY, 41))).encode();
        assertThat(request[8])
                .isEqualTo((byte) 1);
        assertThat(eventFrame().encode()[8]).isEqualTo((byte) 2);
        assertThat(endFrame().encode()[8]).isEqualTo((byte) 3);
        request[8] = 2;
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpRequestFrame.decode(request))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("frame kind");
    }

    @Test
    void wireRequestIdsAreDecodedAndBadMagicIsRejected() throws Exception {
        var otherRequest = UUID.fromString("99999999-8888-7777-6666-555555555555");
        var request = new CatchUpRequestFrame(otherRequest, List.of(
                new CatchUpRequestFrame.Stream(KEY, 41))).encode();
        assertThat(CatchUpRequestFrame.decode(request).requestId()).isEqualTo(otherRequest);
        var encoded = new CatchUpEndFrame(otherRequest).encode();
        assertThat(CatchUpEndFrame.decode(encoded).requestId()).isEqualTo(otherRequest);
        var event = new CatchUpEventFrame(otherRequest, eventFrame().event()).encode();
        assertThat(CatchUpEventFrame.decode(event).requestId()).isEqualTo(otherRequest);

        encoded[0] = 0;
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpEndFrame.decode(encoded))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void unknownVersionsRefuseBeforeDecodingAnyCatchUpFrame() {
        var request = new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(KEY, 41))).encode();
        request[7] = 2;

        var event = eventFrame().encode();
        event[7] = 2;
        var end = endFrame().encode();
        end[7] = 2;

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> CatchUpRequestFrame.decode(request))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("version");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> CatchUpEventFrame.decode(event))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("version");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> CatchUpEndFrame.decode(end))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    void trailingBytesAndOversizedRequestsRefuse() throws Exception {
        var request = new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(KEY, 41))).encode();
        var event = eventFrame().encode();
        var end = endFrame().encode();

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpRequestFrame.decode(withTrailingByte(request)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("trailing");
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpEventFrame.decode(withTrailingByte(event)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("trailing");
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpEndFrame.decode(withTrailingByte(end)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("trailing");

        var oversized = new byte[8 + 1 + 16 + 2];
        System.arraycopy(request, 0, oversized, 0, 8 + 1 + 16);
        oversized[8] = 1;
        oversized[25] = (byte) 0x81;
        oversized[26] = 0x08;
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpRequestFrame.decode(oversized))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("maximum");

        var tooManyStreams = java.util.stream.IntStream.range(0, 1025)
                .mapToObj(partition -> new CatchUpRequestFrame.Stream(
                        new RunKey(KEY.indexId(), partition), 0))
                .toList();
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new CatchUpRequestFrame(REQUEST, tooManyStreams))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
        var maximumStreams = java.util.stream.IntStream.range(0, 1024)
                .mapToObj(partition -> new CatchUpRequestFrame.Stream(
                        new RunKey(KEY.indexId(), partition), 0))
                .toList();
        assertThat(CatchUpRequestFrame.decode(
                new CatchUpRequestFrame(REQUEST, maximumStreams).encode()).streams())
                .hasSize(1024);

        var partitionOverflow = new byte[request.length + 9];
        System.arraycopy(request, 0, partitionOverflow, 0, 42);
        java.util.Arrays.fill(partitionOverflow, 42, 51, (byte) 0x80);
        partitionOverflow[51] = 1;
        System.arraycopy(request, 43, partitionOverflow, 52, request.length - 43);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpRequestFrame.decode(partitionOverflow))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("partition");

        var batchStartOverflow = new byte[request.length + 9];
        System.arraycopy(request, 0, batchStartOverflow, 0, 43);
        java.util.Arrays.fill(batchStartOverflow, 43, 52, (byte) 0x80);
        batchStartOverflow[52] = 1;
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> CatchUpRequestFrame.decode(batchStartOverflow))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("batch_start");
    }

    @Test
    void requestGoldenBytesStayStable() throws Exception {
        var request = new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(KEY, 41)));

        try (var golden = CatchUpControlFrameTest.class
                .getResourceAsStream("/golden/catch-up-request-v1.hex")) {
            assertThat(golden).isNotNull();
            assertThat(java.util.HexFormat.of().formatHex(request.encode()))
                    .isEqualTo(new String(golden.readAllBytes(), StandardCharsets.UTF_8).trim());
        }
    }

    @Test
    void responseGoldenBytesStayStable() throws Exception {
        assertThat(java.util.HexFormat.of().formatHex(eventFrame().encode()))
                .isEqualTo(readGolden("/golden/catch-up-event-v1.hex"));
        assertThat(java.util.HexFormat.of().formatHex(endFrame().encode()))
                .isEqualTo(readGolden("/golden/catch-up-end-v1.hex"));
    }

    private static byte[] withTrailingByte(byte[] bytes) {
        return java.util.Arrays.copyOf(bytes, bytes.length + 1);
    }

    private static String readGolden(String name) throws Exception {
        try (var golden = CatchUpControlFrameTest.class.getResourceAsStream(name)) {
            assertThat(golden).isNotNull();
            return new String(golden.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
    }
}
