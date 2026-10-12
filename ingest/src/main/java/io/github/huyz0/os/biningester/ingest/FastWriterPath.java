// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.FastWriter;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The fast path over {@link FastWriter} (M13.29c; ADR-0081 §2): a fast
 * append is one COMMIT under this pod's own key, sent to the leader of the
 * newest term this pod joined, and answered at the offsets assigned once
 * EXPOSED.
 *
 * <p>⚠️ A RETRY REUSES THE KEY: the leader answers a held batch from its
 * offsets once exposed, so a batch resent under a new key would be assigned
 * twice. Only {@code attempts} are made; then the producer is refused and
 * retries the whole request, its duplicates removed by {@code _id}.
 *
 * <p>⚠️ THE DEFAULT PATH WHERE NO FAST WRITE CAN BE MADE: no term joined, a
 * leader that takes none (NOT_FAST), or an append longer than {@code B} --
 * one run takes one first offset, and a split one would not be answered as
 * one range. A fenced or unrostered writer refuses instead: the producer's
 * retry finds the newer term, or this pod's re-join.
 */
public final class FastWriterPath implements FastPath {

    /** {@link FastWriter#write}: a seam, so the outcomes are tested one by one. */
    @FunctionalInterface
    public interface Writer {
        FastWriter.Outcome write(long epoch, String leaderUid, String leaderEndpoint,
                FastWriteFrame.Commit commit) throws IOException;
    }

    private final String podId;
    private final UUID incarnationId;
    private final JoinedTerms joined;
    private final Writer writer;
    private final int attempts;
    private final long bound;
    private final AtomicLong fastSeq = new AtomicLong();

    /**
     * @param incarnationId this pod's incarnation: a restart's keys never
     *     collide with the last one's
     * @param bound {@code B}: an append longer than it goes the default way
     */
    public FastWriterPath(String podId, UUID incarnationId, JoinedTerms joined, Writer writer,
            int attempts, long bound) {
        this.podId = Objects.requireNonNull(podId, "podId");
        this.incarnationId = Objects.requireNonNull(incarnationId, "incarnationId");
        this.joined = Objects.requireNonNull(joined, "joined");
        this.writer = Objects.requireNonNull(writer, "writer");
        if (attempts < 1 || bound < 1) {
            throw new IllegalArgumentException("at least one attempt, and a bound of one");
        }
        this.attempts = attempts;
        this.bound = bound;
    }

    @Override
    public Optional<AppendResult> append(RunKey stream, List<SegmentRecord> records)
            throws IOException {
        Optional<JoinedTerms.Leader> leader = joined.latest();
        if (leader.isEmpty() || records.size() > bound) {
            return Optional.empty();
        }
        FastWriteFrame.Commit commit = new FastWriteFrame.Commit(
                new FastJournalRecord.IdempotencyKey(podId, incarnationId,
                        fastSeq.getAndIncrement()),
                List.of(new FastWriteFrame.CommitRun(stream, records)));
        JoinedTerms.Leader to = leader.get();
        String why = "";
        for (int attempt = 0; attempt < attempts; attempt++) {
            switch (writer.write(to.epoch(), to.uid(), to.endpoint(), commit)) {
                case FastWriter.Written written -> {
                    long first = written.assigned().runs().get(0).firstOffset();
                    return Optional.of(new AppendResult(records.size(), first,
                            first + records.size() - 1));
                }
                case FastWriter.NotFast notFast -> {
                    return Optional.empty();
                }
                case FastWriter.Retry retry -> why = retry.why();
                case FastWriter.Rejoin rejoin -> throw new IOException(
                        "this pod is to join term " + to.epoch() + " again: " + rejoin.why());
                case FastWriter.Fenced fenced -> throw new IOException(
                        "term " + to.epoch() + " is fenced at " + fenced.fence());
            }
        }
        throw new IOException("the batch was not exposed after " + attempts
                + " attempts: " + why);
    }
}
