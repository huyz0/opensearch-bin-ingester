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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        Objects.requireNonNull(request, "request");
        Map<String, byte[]> segments = new HashMap<>();
        List<byte[]> frames = new ArrayList<>();
        long responseBytes = 0;
        for (CatchUpRequestFrame.Stream stream : request.streams()) {
            CommittedDeltaSource.ReplayCursor cursor = source.open(stream.key(),
                    exclusiveOffset(stream.batchStart()));
            while (true) {
                var next = cursor.next();
                if (next.isEmpty()) {
                    break;
                }
                CommittedDeltaSource.CommittedRun run = next.get();
                byte[] segment;
                try {
                    segment = segments.computeIfAbsent(run.segmentKey(), key -> {
                        try {
                            return readSegment(key);
                        } catch (IOException failed) {
                            throw new ReplayReadFailure(failed);
                        }
                    });
                } catch (ReplayReadFailure failed) {
                    throw (IOException) failed.getCause();
                }
                SubscriptionEvent event = new SubscriptionEvent(
                        request.requestId().toString(), epoch.getAsLong(), 1,
                        run.key(), run.segmentKey(), run.firstOffset(), run.recordCount(),
                        FetchMode.INLINE, segment);
                byte[] encoded = new CatchUpEventFrame(request.requestId(), event).encode();
                responseBytes = addFrame(responseBytes, encoded);
                frames.add(encoded);
            }
        }
        byte[] end = new CatchUpEndFrame(request.requestId()).encode();
        addFrame(responseBytes, end);
        frames.add(end);
        return List.copyOf(frames);
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

    private long addFrame(long currentBytes, byte[] frame) throws IOException {
        long framed = FRAME_PREFIX_BYTES + (long) frame.length;
        if (framed > maxResponseBytes || currentBytes > maxResponseBytes - framed) {
            throw new ResponseTooLargeException("catch-up response exceeds "
                    + maxResponseBytes + " bytes");
        }
        return currentBytes + framed;
    }

    /** A deterministic size refusal, distinct from a temporarily unavailable store. */
    public static final class ResponseTooLargeException extends IOException {
        public ResponseTooLargeException(String message) {
            super(message);
        }
    }

    /** Keeps a checked store failure from being hidden by Map.computeIfAbsent. */
    private static final class ReplayReadFailure extends RuntimeException {
        ReplayReadFailure(IOException cause) {
            super(cause);
        }
    }
}
