// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Turns a node-scoped catch-up request into durable segment-backed event frames
 * (M8.24e, ADR-0065).
 *
 * <p>The source supplies committed run boundaries while the store supplies the
 * bytes. Segment bytes are cached for the lifetime of one response, so several
 * streams sharing a segment cost one object-store GET. The responder emits an
 * end marker only after every requested stream has been exhausted.
 */
public final class DurableCatchUpResponder {

    /** Receives one complete versioned response frame at a time. */
    @FunctionalInterface
    public interface FrameSink {
        void write(byte[] frame) throws IOException;
    }

    /** Four bytes prefix every response frame carries on the wire. */
    static final int FRAME_PREFIX_BYTES = 4;

    /** Matches the HTTP adapter's default answer budget. */
    static final long DEFAULT_MAX_RESPONSE_BYTES = 8L << 20;

    private final BinStore store;
    private final CommittedDeltaSource source;
    private final LongSupplier epoch;
    private final long maxResponseBytes;

    public DurableCatchUpResponder(BinStore store, CommittedDeltaSource source,
            LongSupplier epoch) {
        this(store, source, epoch, DEFAULT_MAX_RESPONSE_BYTES);
    }

    public DurableCatchUpResponder(BinStore store, CommittedDeltaSource source,
            LongSupplier epoch, long maxResponseBytes) {
        this.store = Objects.requireNonNull(store, "store");
        this.source = Objects.requireNonNull(source, "source");
        this.epoch = Objects.requireNonNull(epoch, "epoch");
        if (maxResponseBytes <= FRAME_PREFIX_BYTES
                || maxResponseBytes > Integer.MAX_VALUE - 1L) {
            throw new IllegalArgumentException("response budget is invalid: " + maxResponseBytes);
        }
        this.maxResponseBytes = maxResponseBytes;
    }

    /**
     * Replays every run after each request stream's {@code batch_start}.
     *
     * @throws IOException when a durable segment cannot be read or exceeds the
     *         configured segment bound
     */
    public List<byte[]> respond(CatchUpRequestFrame request) throws IOException {
        List<byte[]> frames = new java.util.ArrayList<>();
        long[] responseBytes = {0};
        respond(request, frame -> {
            long framed = FRAME_PREFIX_BYTES + (long) frame.length;
            if (framed > maxResponseBytes || responseBytes[0] > maxResponseBytes - framed) {
                throw new ResponseTooLargeException("catch-up response exceeds "
                        + maxResponseBytes + " bytes");
            }
            responseBytes[0] += framed;
            frames.add(frame);
        });
        return List.copyOf(frames);
    }

    /** Streams frames as each committed run is resolved, without a response-sized frame list. */
    public void respond(CatchUpRequestFrame request, FrameSink sink) throws IOException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sink, "sink");
        List<CommittedDeltaSource.ReplayRequest> positions = request.streams().stream()
                .map(stream -> new CommittedDeltaSource.ReplayRequest(stream.key(),
                        exclusiveOffset(stream.batchStart())))
                .toList();
        CommittedDeltaSource.SegmentReplayCursor cursor = source.openSegments(positions);
        while (true) {
            var next = cursor.next();
            if (next.isEmpty()) {
                break;
            }
            CommittedDeltaSource.ReplaySegment replaySegment = next.get();
            byte[] segment = readSegment(replaySegment.segmentKey());
            for (CommittedDeltaSource.CommittedRun run : replaySegment.runs()) {
                SubscriptionEvent event = new SubscriptionEvent(
                        request.requestId().toString(), epoch.getAsLong(), 1,
                        run.key(), run.segmentKey(), run.firstOffset(), run.recordCount(),
                        FetchMode.INLINE, segment);
                byte[] encoded = new CatchUpEventFrame(request.requestId(), event).encode();
                requireFrameWithinBudget(encoded);
                sink.write(encoded);
            }
        }
        byte[] end = new CatchUpEndFrame(request.requestId()).encode();
        requireFrameWithinBudget(end);
        sink.write(end);
    }

    private static long exclusiveOffset(long batchStart) {
        if (batchStart == 0) {
            return -1;
        }
        return batchStart - 1;
    }

    private byte[] readSegment(String key) throws IOException {
        try (InputStream in = store.get(key)) {
            byte[] bytes = in.readNBytes((int) maxResponseBytes + 1);
            if (bytes.length > maxResponseBytes) {
                throw new ResponseTooLargeException("segment " + key + " cannot fit in the "
                        + maxResponseBytes + " byte catch-up response budget");
            }
            return bytes;
        }
    }

    private void requireFrameWithinBudget(byte[] frame) throws ResponseTooLargeException {
        long framed = FRAME_PREFIX_BYTES + (long) frame.length;
        if (framed > maxResponseBytes) {
            throw new ResponseTooLargeException("catch-up response frame exceeds "
                    + maxResponseBytes + " bytes");
        }
    }

    /** A deterministic size refusal, distinct from a temporarily unavailable store. */
    public static final class ResponseTooLargeException extends IOException {
        public ResponseTooLargeException(String message) {
            super(message);
        }
    }

}
