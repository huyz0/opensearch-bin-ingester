// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The chain BELOW what recovery replayed, read once per takeover (M8.42,
 * FR-9, ADR-0033).
 *
 * <p>⚠️ **A REPLAY STOPS AT A CHECKPOINT, AND A CHECKPOINT IS NOT THE SEGMENT
 * INDEX.** So a new term's keep list lacked every delta below it: a segment
 * named only there was never judged by retention, and the orphan sweep had to
 * stay out of every hour before the term began -- widening it would have
 * listed a committed segment, found it in no delta, and deleted it.
 *
 * <p>⚠️ **WHAT SURVIVES BELOW THE BOUNDARY IS EXACTLY WHAT MATTERS.**
 * {@link ChainGc} deletes a delta only once every segment it names is gone, so
 * the deltas still in the bucket name every committed segment still in it. A
 * gap is chain GC's work, not the end of the chain, which is why this LISTs
 * rather than walking back by GET until a 404.
 *
 * <p>⚠️ **THE COST IS PER TAKEOVER, NEVER PER PASS** (cost.md): one LIST per
 * page of {@code ctl/log/0/} and one GET per surviving chain entry below the
 * boundary -- a CONTINUE or a SEAL costs one too, since no key says which it
 * is, and there is one of each per epoch. Nothing at or after the boundary is
 * read, and no checkpoint is.
 */
public final class ChainBackfill {

    /** {@code <prefix>/ctl/log/0/<epoch>/<sequence>.delta}, both in 16 hex digits. */
    private static final Pattern DELTA = Pattern.compile("/ctl/log/0/([0-9a-f]{16})/"
            + "([0-9a-f]{16})\\.delta$");

    private static final int PAGE = 1000;

    private static final System.Logger LOG =
            System.getLogger(ChainBackfill.class.getName());

    private ChainBackfill() {
    }

    /**
     * Backfills {@code chain} below where its replay began, on a virtual
     * thread, so a takeover does not wait on it (M8.42).
     *
     * <p>⚠️ **OFF THE ELECTION's PATH**: a retention window of deltas is tens
     * of thousands of GETs, and a term that could not commit until they
     * finished would turn a takeover into an outage. Until it lands the chain
     * does not reach the floor, which keeps the sweep out of the hours before
     * the term -- the behaviour before this class existed.
     *
     * <p>⚠️ **A FAILURE IS LOGGED AND LEFT**: the chain stays short of the
     * floor, the safe direction, and the next takeover tries again.
     */
    public static Thread inBackground(BinStore store, String prefix, ChainMemory chain) {
        ChainMemory.Snapshot at = chain.snapshot();
        return Thread.ofVirtual().name("chain-backfill").start(() -> {
            try {
                chain.backfill(below(store, prefix, at.firstEpoch(), at.firstSequence()));
            } catch (IOException | RuntimeException failed) {
                LOG.log(System.Logger.Level.WARNING, () -> "the chain below this term's "
                        + "replay could not be read; the orphan sweep stays out of the hours "
                        + "before the term: " + failed);
            }
        });
    }

    /**
     * Every surviving commit delta strictly before {@code (epoch, sequence)},
     * oldest chain first and ascending within each.
     *
     * @param epoch the chain of the first delta recovery already holds
     * @param sequence that delta's sequence, or the checkpoint it started at
     */
    public static List<ChainGc.DeltaAt> below(BinStore store, String prefix, long epoch,
            long sequence) throws IOException {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(prefix, "prefix");
        List<long[]> keys = new ArrayList<>();
        String listed = prefix + "/ctl/log/0/";
        String after = null;
        do {
            ListPage page = store.list(listed, after, PAGE);
            for (ObjectStat object : page.objects()) {
                Matcher m = DELTA.matcher(object.key());
                if (!m.find()) {
                    continue;
                }
                long e = Long.parseUnsignedLong(m.group(1), 16);
                long s = Long.parseUnsignedLong(m.group(2), 16);
                if (e < epoch || (e == epoch && s < sequence)) {
                    keys.add(new long[] {e, s});
                }
            }
            after = page.nextStartAfter().orElse(null);
        } while (after != null);
        keys.sort(Comparator.<long[]>comparingLong(k -> k[0]).thenComparingLong(k -> k[1]));

        List<ChainGc.DeltaAt> found = new ArrayList<>();
        for (long[] key : keys) {
            read(store, prefix, key[0], key[1]).ifPresent(found::add);
        }
        return found;
    }

    /**
     * The commit at {@code (epoch, sequence)}, or empty for a CONTINUE or a SEAL.
     *
     * <p>⚠️ **A FAILED GET FAILS THE WHOLE WALK**, and that is the safe answer:
     * the caller then leaves the chain short of the floor, so the sweep stays
     * out of the hours before the term. Nothing can delete between the LIST and
     * the GET -- the predecessor is fenced, and this term's chain GC collects
     * only what it already holds.
     */
    private static Optional<ChainGc.DeltaAt> read(BinStore store, String prefix, long epoch,
            long sequence) throws IOException {
        byte[] bytes;
        try (InputStream in = store.get(new LogKeys(prefix, epoch).keyFor(sequence))) {
            bytes = in.readAllBytes();
        }
        return ChainEntry.decode(bytes) instanceof CommitDelta delta
                ? Optional.of(new ChainGc.DeltaAt(epoch, sequence, delta))
                : Optional.empty();
    }
}
