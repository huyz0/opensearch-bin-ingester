// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The takeover's frames under {@link FastFrame}'s header (ADR-0082 §2;
 * M13.33a): FENCE, successor to member; COLLECTED, member to successor; and
 * RELEASE, leader to holder.
 *
 * <p>FENCE is a u32 count of the unclosed terms' epochs, each an i64,
 * strictly ascending and each above {@code closedThrough}, then
 * {@code closedThrough} i64, then the page of COLLECTED it asks for u32
 * (M13.33b): the successor asks for each page by number, so a lost answer
 * is asked for again and never skips one. COLLECTED is the page number u32, {@code last}
 * u8, a u32 count of entries, each its byte length u32 then the whole journal
 * record of ADR-0082 §4, as a REPLICA carries one. RELEASE is a u32 count of
 * streams, each a RunKey then its release-below offset i64, no stream twice.
 *
 * <p>⚠️ A MEMBER HOLDING NOTHING ANSWERS ONE EMPTY PAGE MARKED LAST: the
 * successor counts a member answered only on the page marked last with every
 * earlier page received (M13.22e review round 3, P3), so an empty answer is
 * still a page.
 */
public final class FastTakeoverFrame {

    public static final int KIND_FENCE = 9;
    public static final int KIND_COLLECTED = 10;
    public static final int KIND_RELEASE = 11;

    private FastTakeoverFrame() {
    }

    /**
     * FENCE, successor to member: the unclosed terms it takes over, the
     * highest closed, and the page of the member's COLLECTED it asks for.
     */
    public record Fence(List<Long> unclosedEpochs, long closedThrough, long page)
            implements FastFrame.Body {
        public Fence {
            unclosedEpochs = List.copyOf(unclosedEpochs);
            if (closedThrough < 0) {
                throw new IllegalArgumentException("closedThrough is never negative");
            }
            if (page < 0 || page > FastFrame.MAX_U32) {
                throw new IllegalArgumentException("a page number is a u32: " + page);
            }
            if (unclosedEpochs.isEmpty()) {
                throw new IllegalArgumentException("a FENCE names at least one unclosed term");
            }
            long previous = closedThrough;
            for (long epoch : unclosedEpochs) {
                if (epoch <= previous) {
                    throw new IllegalArgumentException("a FENCE's epochs ascend strictly above "
                            + "closedThrough " + closedThrough + ": " + unclosedEpochs);
                }
                previous = epoch;
            }
        }

        /** The first page. */
        public Fence(List<Long> unclosedEpochs, long closedThrough) {
            this(unclosedEpochs, closedThrough, 0);
        }

        @Override
        public int kind() {
            return KIND_FENCE;
        }
    }

    /** COLLECTED, member to successor: one page of the entries it holds of the fenced terms. */
    public record Collected(long page, boolean last, List<FastJournalRecord.Entry> entries)
            implements FastFrame.Body {
        public Collected {
            if (page < 0 || page > FastFrame.MAX_U32) {
                throw new IllegalArgumentException("a page number is a u32: " + page);
            }
            entries = List.copyOf(entries);
        }

        @Override
        public int kind() {
            return KIND_COLLECTED;
        }
    }

    /** One stream a RELEASE names: its entries below {@code below} go. */
    public record ReleaseAt(RunKey stream, long below) {
        public ReleaseAt {
            Objects.requireNonNull(stream, "stream");
            if (below < 0) {
                throw new IllegalArgumentException("a release offset is never negative");
            }
        }
    }

    /** RELEASE, leader to holder: per stream, the offset below which it drops. */
    public record Release(List<ReleaseAt> streams) implements FastFrame.Body {
        public Release {
            streams = List.copyOf(streams);
            Set<RunKey> seen = new HashSet<>();
            for (ReleaseAt at : streams) {
                if (!seen.add(at.stream())) {
                    throw new IllegalArgumentException("a RELEASE names stream " + at.stream()
                            + " twice");
                }
            }
        }

        @Override
        public int kind() {
            return KIND_RELEASE;
        }
    }

    static void encode(ByteArrayOutputStream out, FastFrame.Body body) {
        switch (body) {
            case Fence f -> {
                FastFrame.u32(out, f.unclosedEpochs().size());
                for (long epoch : f.unclosedEpochs()) {
                    FastFrame.i64(out, epoch);
                }
                FastFrame.i64(out, f.closedThrough());
                FastFrame.u32(out, f.page());
            }
            case Collected c -> {
                FastFrame.u32(out, c.page());
                out.write(c.last() ? 1 : 0);
                FastFrame.u32(out, c.entries().size());
                for (FastJournalRecord.Entry e : c.entries()) {
                    byte[] bytes = e.encode();
                    FastFrame.u32(out, bytes.length);
                    out.writeBytes(bytes);
                }
            }
            case Release r -> {
                FastFrame.u32(out, r.streams().size());
                for (ReleaseAt at : r.streams()) {
                    FastFrame.runKey(out, at.stream());
                    FastFrame.i64(out, at.below());
                }
            }
            default -> throw new IllegalArgumentException("not a takeover frame: " + body.kind());
        }
    }

    static FastFrame.Body decode(int kind, Cursor c) throws IOException {
        try {
            return switch (kind) {
                case KIND_FENCE -> {
                    long n = FastFrame.count(c, 8);
                    List<Long> epochs = new ArrayList<>();
                    for (long i = 0; i < n; i++) {
                        epochs.add(FastFrame.i64(c));
                    }
                    long closedThrough = FastFrame.i64(c);
                    yield new Fence(epochs, closedThrough, FastFrame.u32(c));
                }
                case KIND_COLLECTED -> {
                    long page = FastFrame.u32(c);
                    int last = c.bytes(1)[0] & 0xFF;
                    if (last > 1) {
                        throw new IOException("a flag is 0 or 1, not " + last);
                    }
                    long n = FastFrame.count(c, 4 + FastJournalRecord.HEADER_BYTES);
                    List<FastJournalRecord.Entry> entries = new ArrayList<>();
                    for (long i = 0; i < n; i++) {
                        long length = FastFrame.u32(c);
                        if (length > c.remaining()) {
                            throw new IOException("a COLLECTED entry overruns the frame");
                        }
                        entries.add(FastWriteFrame.entry(c.bytes((int) length)));
                    }
                    yield new Collected(page, last == 1, entries);
                }
                case KIND_RELEASE -> {
                    long n = FastFrame.count(c, 28);
                    List<ReleaseAt> streams = new ArrayList<>();
                    for (long i = 0; i < n; i++) {
                        streams.add(new ReleaseAt(FastFrame.runKey(c), FastFrame.i64(c)));
                    }
                    yield new Release(streams);
                }
                default -> throw new IOException("fast frame kind " + kind
                        + " is not readable by this build");
            };
        } catch (IllegalArgumentException invalid) {
            // ⚠️ A FIELD THE RECORD REFUSES IS A MALFORMED FRAME, never a crash
            // of the route that read it.
            throw new IOException("a malformed takeover frame: " + invalid.getMessage(),
                    invalid);
        }
    }
}
