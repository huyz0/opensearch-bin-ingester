// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Continue;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The commit chain as the bucket holds it, read back after the processes
 * that wrote it are gone, and judged against architecture.md's I1-I5 (M8.11).
 *
 * <p>⚠️ **READ FROM THE BUCKET, NOT FROM ANY NODE.** A node killed mid-commit
 * cannot be asked what it committed, and a survivor would answer from its own
 * replay, which is the code under test. The layout is {@code
 * <prefix>/ctl/log/0/<epoch:%016x>/<seq:%016x>.delta}, with SEAL and
 * CONTINUE as ordinary entries (the sequencer's {@code LogKeys}).
 *
 * <p>⚠️ **WHAT THIS CAN AND CANNOT SEE.** It sees every entry every writer
 * landed, including a fenced writer's suffix, which is exactly what I1, I5's
 * store half and the SEAL/CONTINUE link are about. It does not see acks; the
 * ack half of I5 and I4 are judged by the caller, against what its producers
 * were told, using {@link #committedSegments()}.
 */
public final class ChainAudit {

    /** One epoch's entries, in sequence order. */
    public record EpochChain(long epoch, List<ChainEntry> entries) {

        /** The first SEAL in this chain, or {@code null}: the one that fences it. */
        Seal firstSeal() {
            for (ChainEntry entry : entries) {
                if (entry instanceof Seal seal) {
                    return seal;
                }
            }
            return null;
        }
    }

    private final TreeMap<Long, EpochChain> epochs;
    private final List<String> violations = new ArrayList<>();
    private final Set<String> committedSegments = new LinkedHashSet<>();
    private final Map<RunKey, Long> nextOffsets = new HashMap<>();
    private int seals;

    private ChainAudit(TreeMap<Long, EpochChain> epochs) {
        this.epochs = epochs;
        judge();
    }

    /** Reads every epoch's chain out of {@code bucket}, and judges it. */
    public static ChainAudit of(ChainBucketReader bucket) throws IOException {
        TreeMap<Long, EpochChain> epochs = new TreeMap<>();
        String root = ChaosBucket.PREFIX + "/ctl/log/0/";
        Map<Long, TreeMap<Long, ChainEntry>> raw = new HashMap<>();
        List<String> misfiled = new ArrayList<>();
        for (String key : bucket.keys(root)) {
            String rest = key.substring(root.length());
            int slash = rest.indexOf('/');
            if (slash < 0 || !rest.endsWith(".delta") || rest.contains("ckpt/")) {
                continue;
            }
            long epoch = Long.parseUnsignedLong(rest.substring(0, slash), 16);
            long slot = Long.parseUnsignedLong(
                    rest.substring(slash + 1, rest.length() - ".delta".length()), 16);
            ChainEntry entry = ChainEntry.decode(bucket.get(key));
            if (entry.sequence() != slot) {
                // ⚠️ I1 AT THE STORE: one key per slot makes a second write
                // of a slot a CAS failure, so what can be seen is an entry
                // claiming a sequence other than the slot it occupies.
                misfiled.add("I1: " + key + " holds sequence " + entry.sequence());
            }
            raw.computeIfAbsent(epoch, e -> new TreeMap<>()).put(slot, entry);
        }
        raw.forEach((epoch, entries) ->
                epochs.put(epoch, new EpochChain(epoch, List.copyOf(entries.values()))));
        ChainAudit audit = new ChainAudit(epochs);
        audit.violations.addAll(0, misfiled);
        return audit;
    }

    /** What the audit needs from a bucket. */
    public interface ChainBucketReader {
        List<String> keys(String prefix) throws IOException;

        byte[] get(String key) throws IOException;
    }

    /**
     * One epoch's chain, one line per entry: what each delta committed, for
     * whom, and at which offsets. Printed by a row whose audit failed, so a
     * chaos failure arrives with its evidence.
     */
    public String describe(long epoch) {
        EpochChain chain = epochs.get(epoch);
        if (chain == null) {
            return "epoch " + epoch + ": no chain";
        }
        StringBuilder out = new StringBuilder("epoch ").append(epoch).append(":\n");
        for (ChainEntry entry : chain.entries()) {
            out.append("  ").append(entry.sequence()).append(' ');
            if (entry instanceof CommitDelta delta) {
                for (SegmentCommit segment : delta.segments()) {
                    out.append(segment.segmentKey()).append(' ').append(segment.attribution())
                            .append(' ');
                    for (RunCommit run : segment.runs()) {
                        out.append('[').append(run.firstOffset()).append(',')
                                .append(run.firstOffset() + run.recordCount()).append(") ");
                    }
                }
            } else {
                out.append(entry);
            }
            out.append('\n');
        }
        return out.toString();
    }

    public List<String> violations() {
        return List.copyOf(violations);
    }

    /** One epoch's entries, SEAL and CONTINUE included, in slot order. */
    public List<ChainEntry> entries(long epoch) {
        EpochChain chain = epochs.get(epoch);
        return chain == null ? List.of() : chain.entries();
    }

    /** Every epoch that has a chain. */
    public Set<Long> epochs() {
        return epochs.keySet();
    }

    /**
     * How many takeovers on the effective history found their predecessor
     * sealed exactly where they continued from.
     */
    public int seals() {
        return seals;
    }

    /**
     * Every segment a delta in the EFFECTIVE history names: each epoch's
     * entries up to the point its successor continued from. A segment named
     * only past that point was committed by a fenced writer and is not
     * committed at all (I3).
     */
    public Set<String> committedSegments() {
        return Set.copyOf(committedSegments);
    }

    /** Where each stream's committed offsets end, exclusive. */
    public Map<RunKey, Long> nextOffsets() {
        return Map.copyOf(nextOffsets);
    }

    private void judge() {
        for (EpochChain chain : epochs.values()) {
            // The chain is contiguous from 0: a skipped slot is a gap a
            // reader would stop at, and whatever was committed past it lost.
            List<ChainEntry> entries = chain.entries();
            for (int i = 0; i < entries.size(); i++) {
                if (entries.get(i).sequence() != i) {
                    violations.add("chain: epoch " + chain.epoch() + " slot " + i
                            + " holds sequence " + entries.get(i).sequence());
                    break;
                }
            }
            // I5, store half: nothing after the first SEAL in its own chain.
            Seal seal = chain.firstSeal();
            if (seal != null) {
                for (ChainEntry entry : entries) {
                    if (entry instanceof CommitDelta && entry.sequence() > seal.sequence()) {
                        violations.add("I5: epoch " + chain.epoch() + " has delta "
                                + entry.sequence() + " after its SEAL at " + seal.sequence());
                    }
                }
            }
        }
        // The effective history, newest epoch backwards along the CONTINUE
        // links, then replayed forwards.
        List<long[]> path = new ArrayList<>();
        Set<Long> onPath = new java.util.HashSet<>();
        Long epoch = epochs.isEmpty() ? null : epochs.lastKey();
        if (epoch != null) {
            onPath.add(epoch);
        }
        long upTo = Long.MAX_VALUE;
        while (epoch != null) {
            EpochChain chain = epochs.get(epoch);
            path.add(0, new long[] {epoch, upTo});
            ChainEntry first = chain.entries().isEmpty() ? null : chain.entries().get(0);
            if (!(first instanceof Continue link)) {
                break;
            }
            EpochChain previous = epochs.get(link.prevEpoch());
            if (previous == null) {
                // ⚠️ THE FIRST TERM continues from an epoch nobody wrote, at
                // slot 0 -- that is genesis. From anywhere else it is a
                // successor claiming a history that is not in the bucket.
                if (link.prevSeq() != 0) {
                    violations.add("link: epoch " + epoch + " continues from "
                            + link.prevEpoch() + "@" + link.prevSeq() + ", which has no chain");
                }
                break;
            }
            Seal fence = previous.firstSeal();
            // ⚠️ THE SEAL SUCCEEDED: the predecessor is fenced exactly where
            // this epoch says it continued from. ⚠️ BY THIS EPOCH, OR BY ONE
            // BETWEEN THAT WAS NEVER OPENED (ADR-0037): a leader that sealed
            // its predecessor and died before writing its CONTINUE leaves the
            // SEAL naming itself, and the next leader continues past it.
            boolean byThisOrABurnedOne = fence != null && fence.continuedAt() <= epoch
                    && fence.continuedAt() > link.prevEpoch()
                    && burnedBetween(fence.continuedAt(), epoch);
            if (!byThisOrABurnedOne || fence.sequence() != link.prevSeq()) {
                violations.add("link: epoch " + epoch + " continues from " + link.prevEpoch()
                        + "@" + link.prevSeq() + " but that chain's first SEAL is " + fence);
            } else {
                seals++;
            }
            onPath.add(link.prevEpoch());
            upTo = link.prevSeq();
            epoch = link.prevEpoch();
        }
        // ⚠️ A HISTORY THAT WAS DROPPED: an epoch holding deltas that the
        // newest epoch's links never reach. A successor that started afresh
        // instead of continuing would otherwise pass every check above.
        for (EpochChain chain : epochs.values()) {
            if (!onPath.contains(chain.epoch())
                    && chain.entries().stream().anyMatch(e -> e instanceof CommitDelta)) {
                violations.add("history: epoch " + chain.epoch()
                        + " holds deltas no later epoch continues from");
            }
        }
        for (long[] hop : path) {
            for (ChainEntry entry : epochs.get(hop[0]).entries()) {
                if (entry.sequence() >= hop[1] || entry instanceof Seal) {
                    break;
                }
                if (entry instanceof CommitDelta delta) {
                    apply(hop[0], delta);
                }
            }
        }
    }

    /**
     * Whether every epoch in {@code [from, to)} was burned: acquired and
     * never opened, so it holds no CONTINUE and no delta.
     */
    private boolean burnedBetween(long from, long to) {
        for (long e = from; e < to; e++) {
            EpochChain chain = epochs.get(e);
            if (chain != null && chain.entries().stream()
                    .anyMatch(x -> x instanceof Continue || x instanceof CommitDelta)) {
                return false;
            }
        }
        return true;
    }

    /** I2: every run starts exactly where its stream's last one ended. */
    private void apply(long epoch, CommitDelta delta) {
        for (SegmentCommit segment : delta.segments()) {
            committedSegments.add(segment.segmentKey());
        }
        for (RunCommit run : delta.allRuns()) {
            long mark = nextOffsets.getOrDefault(run.key(), 0L);
            if (run.firstOffset() < mark) {
                violations.add("I2: epoch " + epoch + " delta " + delta.sequence()
                        + " reassigns " + run.key() + " from " + run.firstOffset()
                        + ", below " + mark);
            } else if (run.firstOffset() > mark) {
                violations.add("gap: epoch " + epoch + " delta " + delta.sequence() + " starts "
                        + run.key() + " at " + run.firstOffset() + ", past " + mark);
            }
            nextOffsets.put(run.key(), run.firstOffset() + run.recordCount());
        }
    }
}
