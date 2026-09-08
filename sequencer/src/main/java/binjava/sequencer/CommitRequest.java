// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.RunKey;
import java.util.Map;
import java.util.Objects;

/**
 * One node's request to commit one segment's runs.
 *
 * <p>⚠️ SHAPED FOR A COMMIT THAT ARRIVES FROM ANOTHER NODE, from the very first
 * one. M4 ships only the local {@link Sequencer}: the process holding the lease
 * commits directly, and {@code podId} is always its own. But M5 adds commit
 * forwarding — a non-leaseholder node sending its commits to the one node that
 * holds the lease — and a forwarded commit is meaningless without the identity
 * of the node that produced it and that node's own flush sequence. Carrying
 * both now is what stops the record shape changing when the remote
 * implementation lands (M4 SPEC § <i>Deployment constraint</i>), and
 * {@code (podId, incarnationId, flushSeq)} is the idempotency key M4.10d will
 * use to ignore a replay. ⚠️ THE SHAPE DID CHANGE ONCE, at M4.10c: ADR-0036
 * added {@code incarnationId} because the pair could not tell a restart from a
 * replay.
 *
 * <p>⚠️ At the end of M4 a multi-node deployment is NOT yet correct, because the
 * transport does not exist. That is stated rather than hidden; the simulation
 * drives many logical nodes through this seam so the ordering and idempotency
 * arguments are proven at M4 and only the transport waits for M5.
 *
 * @param podId which node produced this commit. ⚠️ Refuses {@code -} and
 *     {@code /} so this is the SAME identity as {@code SegmentPublisher}'s
 *     {@code podShortId}, which carries that restriction because the segment
 *     key's own grammar is parsed on those separators. No SEQUENCER key
 *     contains podId — M4.0 settled the chain path as {@code ctl/log/<slot>/…}
 *     — so the constraint is inherited, not intrinsic. ⚠️ Consequence for a
 *     deployment: a Kubernetes {@code POD_NAME} is hyphenated in every
 *     StatefulSet, so a caller must pass the short id, not the pod name
 * @param incarnationId minted ONCE per ingester process (ADR-0036). ⚠️ It is
 *     what tells a RESTART from a REPLAY: {@code flushSeq} restarts at 0 on
 *     every process start while {@code podId} is stable, so the pair alone
 *     would suppress a restarted pod's genuine commits. Minted per FLUSH
 *     instead, dedup becomes a no-op in production while every sequencer-seam
 *     suite stays green.
 * @param flushSeq that node's own monotonic flush counter. ⚠️ AN EARLIER
 *     VERSION SAID IT IDENTIFIES THIS COMMIT UNIQUELY "together with podId",
 *     which ADR-0036 measured false across a restart. The three fields
 *     together do.
 * @param segmentKey the segment already durable in the store — ordering is
 *     assigned here, not at write time (ADR-0001)
 * @param recordCounts how many records each stream contributed. ⚠️ COPIED, not
 *     aliased: this record crosses a seam and, from M5, a network
 */
public record CommitRequest(String podId, String incarnationId, long flushSeq,
        String segmentKey, Map<RunKey, Integer> recordCounts) {

    public CommitRequest {
        Objects.requireNonNull(podId, "podId");
        Objects.requireNonNull(incarnationId, "incarnationId");
        if (incarnationId.isBlank()) {
            // ⚠️ ADR-0036: `flushSeq` restarts at 0 on every process start while
            // `podId` is stable, so the pair cannot tell a RESTART from a
            // REPLAY. A blank incarnation collapses every incarnation of a pod
            // into one identity and reinstates exactly that.
            throw new IllegalArgumentException("incarnationId is never blank");
        }
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(recordCounts, "recordCounts");
        if (podId.isBlank()) {
            // ⚠️ Not cosmetic: podId is half the idempotency key, so a blank one
            // collapses every node's commits into one identity and makes a
            // replay from node A indistinguishable from a first commit by B.
            throw new IllegalArgumentException("podId is never blank");
        }
        if (podId.indexOf('-') >= 0 || podId.indexOf('/') >= 0) {
            throw new IllegalArgumentException("podId may not contain '-' or '/': " + podId);
        }
        if (flushSeq < 0) {
            throw new IllegalArgumentException("flushSeq must not be negative: " + flushSeq);
        }
        if (segmentKey.isBlank()) {
            throw new IllegalArgumentException("segmentKey is never blank");
        }
        // ⚠️ COPY FIRST, then validate the copy -- every check below reads the
        // immutable copy, never the caller's map. Round-2 review found the
        // empty-check still reading the caller's map ten lines above the copy,
        // under a comment claiming otherwise.
        // ⚠️ Map.copyOf itself rejects a null key or value, which is why there
        // is no explicit null check in the loop: adding one back would be
        // unreachable code asserting something already guaranteed.
        recordCounts = Map.copyOf(recordCounts);
        if (recordCounts.isEmpty()) {
            // ⚠️ CommitDelta refuses an empty run list too, with a stated
            // reason: an empty delta would consume a sequence number and commit
            // nothing, so a replay would see a gap it cannot explain. Refusing
            // it HERE means the caller learns at the seam rather than several
            // layers down inside the delta's constructor.
            throw new IllegalArgumentException("recordCounts is never empty");
        }
        for (Map.Entry<RunKey, Integer> e : recordCounts.entrySet()) {
            if (e.getValue() <= 0) {
                throw new IllegalArgumentException(
                        "recordCounts must be positive: " + e.getKey() + " -> " + e.getValue());
            }
        }
    }
}
