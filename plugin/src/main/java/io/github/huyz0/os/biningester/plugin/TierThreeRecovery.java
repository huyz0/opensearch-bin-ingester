// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;

/** Node-scoped, bounded Tier 3 gap recovery. */
final class TierThreeRecovery {

    static final int MAX_GETS = 30;
    static final long MAX_EPISODE_BYTES = 64L << 20;

    @FunctionalInterface
    interface RecoverySink {
        boolean accept(SubscriptionEvent event);
    }

    record Gap(long expectedOffset, long receivedOffset) { }

    private record PendingEvent(long epoch, long sequence, RunCommit run, String segmentKey,
            byte[] segment) {
        SubscriptionEvent toEvent() {
            return new SubscriptionEvent(UUID.randomUUID().toString(), epoch, 1, run.key(),
                    segmentKey, run.firstOffset(), run.recordCount(), FetchMode.INLINE, segment,
                    null, SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT,
                    sequence);
        }
    }

    interface Reader {
        OptionalLong stat(String bucket, String prefix, String key) throws IOException;
        Optional<InputStream> get(String bucket, String prefix, String key) throws IOException;
    }

    private final String bucket;
    private final String prefix;
    private final Reader reader;

    TierThreeRecovery(String bucket, String prefix, Reader reader) {
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.reader = Objects.requireNonNull(reader, "reader");
        if (bucket.isBlank() || prefix.isBlank() || prefix.startsWith("/")
                || prefix.endsWith("/")) {
            throw new IllegalArgumentException("recovery namespace is invalid");
        }
    }

    boolean recover(long epoch, long throughSequence, Map<RunKey, Gap> gaps,
            Consumer<SubscriptionEvent> sink) {
        Objects.requireNonNull(sink, "sink");
        return recoverWithSink(epoch, throughSequence, gaps, event -> {
            sink.accept(event);
            return true;
        });
    }

    boolean recoverWithSink(long epoch, long throughSequence, Map<RunKey, Gap> gaps,
            RecoverySink sink) {
        return recoverWithSink(epoch, throughSequence, gaps, sink, () -> true);
    }

    boolean recoverWithSink(long epoch, long throughSequence, Map<RunKey, Gap> gaps,
            RecoverySink sink, BooleanSupplier recoveryAllowed) {
        Objects.requireNonNull(gaps, "gaps");
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(recoveryAllowed, "recoveryAllowed");
        if (epoch < 0 || throughSequence < 0 || !validGaps(gaps)) {
            return false;
        }
        try {
            Budget budget = new Budget();
            Optional<Checkpoint> checkpoint = checkpoint(epoch, throughSequence, budget,
                    recoveryAllowed);
            if (checkpoint.isEmpty()) {
                return false;
            }
            Optional<List<PendingEvent>> replay = replay(epoch, throughSequence, gaps,
                    checkpoint.orElseThrow(), budget, recoveryAllowed);
            if (replay.isEmpty()) {
                return false;
            }
            // No consumer is unparked until every required chain and segment object is present.
            for (PendingEvent event : replay.orElseThrow()) {
                if (!recoveryAllowed.getAsBoolean() || !sink.accept(event.toEvent())) {
                    return false;
                }
            }
            return true;
        } catch (IOException | ArithmeticException malformedOrUnavailable) {
            return false;
        }
    }

    private static boolean validGaps(Map<RunKey, Gap> gaps) {
        return !gaps.isEmpty() && gaps.values().stream().allMatch(gap -> gap != null
                && gap.expectedOffset() >= 0 && gap.receivedOffset() > gap.expectedOffset());
    }

    private Optional<Checkpoint> checkpoint(long epoch, long throughSequence, Budget budget,
            BooleanSupplier recoveryAllowed) throws IOException {
        String pointer = String.format(java.util.Locale.ROOT,
                "%s/ctl/log/0/%016x/ckpt/LATEST", prefix, epoch);
        if (!recoveryAllowed.getAsBoolean()) {
            return Optional.empty();
        }
        OptionalLong size = reader.stat(bucket, prefix, pointer);
        if (size.isEmpty() || size.getAsLong() < 1 || size.getAsLong() > (64L << 20)) {
            return Optional.empty();
        }
        Optional<byte[]> bytes = get(pointer, budget, recoveryAllowed);
        if (bytes.isEmpty()) {
            return Optional.empty();
        }
        Checkpoint checkpoint = Checkpoint.decode(bytes.orElseThrow());
        return checkpoint.sequence() <= throughSequence
                ? Optional.of(checkpoint) : Optional.empty();
    }

    private Optional<List<PendingEvent>> replay(long epoch, long throughSequence,
            Map<RunKey, Gap> gaps, Checkpoint checkpoint, Budget budget,
            BooleanSupplier recoveryAllowed) throws IOException {
        Map<String, byte[]> segments = new HashMap<>();
        List<PendingEvent> events = new ArrayList<>();
        Map<RunKey, Long> covered = new HashMap<>();
        gaps.forEach((key, gap) -> covered.put(key, gap.expectedOffset()));
        for (long sequence = checkpoint.sequence(); sequence <= throughSequence; sequence++) {
            Optional<List<SegmentCommit>> commits = commits(epoch, sequence, budget,
                    recoveryAllowed);
            if (commits.isEmpty()) {
                return Optional.empty();
            }
            for (SegmentCommit segment : commits.orElseThrow()) {
                if (!appendRelevantRuns(epoch, sequence, segment, gaps, covered, segments,
                        events, budget, recoveryAllowed)) {
                    return Optional.empty();
                }
            }
            if (sequence == Long.MAX_VALUE) {
                break;
            }
        }
        boolean complete = covered.entrySet().stream().allMatch(entry ->
                entry.getValue() == gaps.get(entry.getKey()).receivedOffset());
        return complete ? Optional.of(events) : Optional.empty();
    }

    /**
     * The segments committed at {@code sequence}, or empty if the slot cannot
     * be used for the repair.
     *
     * <p>⚠️ A RECOVERY THAT ONLY VOIDS COMMITS NO SEGMENTS, AND THAT IS NOT
     * MISSING (M13.25 review round 2, P1): answered as empty, it abandoned the
     * whole repair, so no gap on any stream with such an entry in its window
     * could ever be repaired.
     */
    private Optional<List<SegmentCommit>> commits(long epoch, long sequence, Budget budget,
            BooleanSupplier recoveryAllowed) throws IOException {
        String key = String.format(java.util.Locale.ROOT,
                "%s/ctl/log/0/%016x/%016x.delta", prefix, epoch, sequence);
        Optional<byte[]> bytes = get(key, budget, recoveryAllowed);
        if (bytes.isEmpty()) {
            return Optional.empty();
        }
        // ⚠️ EXHAUSTIVE (M13.25): a recovered run sits in a recovery entry, and
        // a gap over it is repaired from its commits as from a delta's.
        return switch (ChainEntry.decode(bytes.orElseThrow())) {
            case CommitDelta d -> d.sequence() == sequence
                    ? Optional.of(d.segments()) : Optional.empty();
            case io.github.huyz0.os.biningester.format.Recovery recovery ->
                    recovery.sequence() == sequence
                            ? Optional.of(recovery.segments()) : Optional.empty();
            case io.github.huyz0.os.biningester.format.Seal ignored -> Optional.empty();
            case io.github.huyz0.os.biningester.format.Continue ignored -> Optional.empty();
        };
    }

    private boolean appendRelevantRuns(long epoch, long sequence, SegmentCommit segment,
            Map<RunKey, Gap> gaps, Map<RunKey, Long> covered, Map<String, byte[]> segments,
            List<PendingEvent> events, Budget budget, BooleanSupplier recoveryAllowed)
            throws IOException {
        List<RunCommit> relevant = segment.runs().stream()
                .filter(run -> gaps.containsKey(run.key()))
                .filter(run -> overlaps(run, gaps.get(run.key())))
                .toList();
        if (relevant.isEmpty()) {
            return true;
        }
        Optional<SegmentReader> parsed = segment(segment.segmentKey(), segments, budget,
                recoveryAllowed);
        if (parsed.isEmpty()) {
            return false;
        }
        for (RunCommit run : relevant) {
            var entry = parsed.orElseThrow().find(run.key());
            if (entry.isEmpty() || entry.orElseThrow().recordCount() != run.recordCount()) {
                return false;
            }
            parsed.orElseThrow().read(entry.orElseThrow());
            long next = covered.get(run.key());
            long end = Math.addExact(run.firstOffset(), run.recordCount());
            if (run.firstOffset() != next || end > gaps.get(run.key()).receivedOffset()) {
                return false;
            }
            events.add(new PendingEvent(epoch, sequence, run, segment.segmentKey(),
                    segments.get(segment.segmentKey())));
            if (!budget.reserveRetained(Math.multiplyExact(
                    (long) segments.get(segment.segmentKey()).length, 2L))) {
                return false;
            }
            covered.put(run.key(), end);
        }
        return true;
    }

    private Optional<SegmentReader> segment(String key, Map<String, byte[]> segments,
            Budget budget, BooleanSupplier recoveryAllowed) throws IOException {
        byte[] bytes = segments.get(key);
        if (bytes == null) {
            Optional<byte[]> loaded = get(key, budget, recoveryAllowed);
            if (loaded.isEmpty()) {
                return Optional.empty();
            }
            bytes = loaded.orElseThrow();
            segments.put(key, bytes);
        }
        return Optional.of(SegmentReader.open(bytes));
    }

    private static boolean overlaps(RunCommit run, Gap gap) {
        long end = Math.addExact(run.firstOffset(), run.recordCount());
        return run.firstOffset() < gap.receivedOffset() && end > gap.expectedOffset();
    }

    private Optional<byte[]> get(String key, Budget budget, BooleanSupplier recoveryAllowed)
            throws IOException {
        if (!recoveryAllowed.getAsBoolean()) {
            return Optional.empty();
        }
        if (!budget.reserve()) {
            return Optional.empty();
        }
        Optional<InputStream> result = reader.get(bucket, prefix, key);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        try (InputStream body = result.orElseThrow()) {
            long remaining = budget.remainingBytes();
            int limit = Math.toIntExact(Math.min(Integer.MAX_VALUE, remaining + 1));
            byte[] bytes = body.readNBytes(limit);
            if (bytes.length > remaining || !budget.reserveRetained(bytes.length)) {
                return Optional.empty();
            }
            return Optional.of(bytes);
        }
    }

    static final class Budget {
        private int gets;
        private long retainedBytes;

        boolean reserve() {
            if (gets == MAX_GETS) {
                return false;
            }
            gets++;
            return true;
        }

        long remainingBytes() {
            return MAX_EPISODE_BYTES - retainedBytes;
        }

        boolean reserveRetained(long bytes) {
            if (bytes < 0 || bytes > remainingBytes()) {
                return false;
            }
            retainedBytes += bytes;
            return true;
        }
    }
}
