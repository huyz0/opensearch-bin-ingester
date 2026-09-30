// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.sequencer.AmbiguousAppendException;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link Sequencer} that resends a commit ONCE if the reply is lost (M5.52a).
 *
 * <p>⚠️ A DECORATOR RATHER THAN A METHOD ON {@code DefaultIngest} (M5.69).
 * That file sat at exactly its 700-line cap and the resend policy is the part
 * of it least about accumulating and flushing -- it is about the sequencer
 * seam, so it lives beside it. {@code DefaultIngest} wraps its sequencer once,
 * in its constructor, so no flush path can reach the bare seam and skip the
 * resend.
 *
 * <p>⚠️ THE SPLIT RELOCATES THE CODE AND NOT THE LOCK, which M5.69's row says
 * in as many words. Both attempts still run under the pod-wide append lock,
 * because the caller blocks on the commit either way -- no decorator may return
 * before durability. A brownout still holds that lock across two round trips
 * per flush, and no row owns that yet.
 *
 * <p>⚠️ IT DECORATES {@code commit}, NOT {@code commitAll}. {@code commit} is
 * the single-request form a flush uses, and one ambiguous reply names one slot
 * to reconcile. A BATCH's ambiguity is several, each possibly at a different
 * slot, so resending the whole batch would resend requests that were never in
 * doubt. {@code commitAll} passes straight through, and whoever gives a
 * batching caller a resend policy owns that question separately.
 */
final class ResendOnceSequencer implements Sequencer {

    private final Sequencer delegate;

    ResendOnceSequencer(Sequencer delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * Commits {@code request}, resending it ONCE if the reply is lost.
     *
     * <p>⚠️ ONLY {@link AmbiguousAppendException}, NEVER {@code IOException}. An
     * ambiguous append NAMES THE SLOT it was attempted at, which is what lets a
     * sequencer reconcile against the chain and answer the repeat instead of
     * applying it twice; a bare {@code IOException} names no slot, and {@code
     * CommitRetryTripleTest} says what retrying one costs. The narrowing is
     * pinned by {@code anABANDONEDFlushBURNSItsNumberRatherThanWedgingThePod}:
     * widening this catch reds it, and nothing else in the module.
     *
     * <p>⚠️ THE SAME REQUEST OBJECT, WHICH IS WHY M5.2 HOISTED IT -- the window
     * answers only when the triple AND the segment key match. The caller builds
     * it once -- {@code BatchFlusher.flush} constructs the
     * {@code CommitRequest} before calling here, and {@code flushSeq} advances
     * at THAT construction, once per flush, whatever happens in this method.
     *
     * <p>⚠️ ONCE, AND THE BOUND IS A COST CHOICE RATHER THAN A CORRECTNESS ONE.
     * A second resend would also be answerable -- {@code
     * LocalSequencer.applyFresh} records the NEW slot before rethrowing, so
     * attempt n+1 reconciles attempt n's slot exactly as attempt 2 reconciles
     * attempt 1's. What argues for one is that every attempt runs under the
     * single per-pod APPEND LOCK, the pod-wide chokepoint, so a loop holds it
     * for as long as the store stays sick and nothing bounds that. A second
     * ambiguous reply abandons the flush and burns its number: the bytes have
     * left the accumulator either way.
     *
     * <p>⚠️ THE LIMITS ARE NOT RESTATED HERE, AND NOT COUNTED --
     * {@link io.github.huyz0.os.biningester.sequencer.Sequencer} enumerates them under headings and
     * says why no count appears beside them. What this buys is only: a reply
     * lost between this pod and a sequencer that REMAINS the leaseholder is
     * answered rather than committed twice. OTHER limits under its DUPLICATED
     * heading reach pods that never restarted, so no new incarnation is minted
     * and M5.52b's incarnation limit does not answer those.
     *
     * <p>⚠️ AND THE RESEND MAY NOT REACH THE INSTANCE THAT MADE THE APPEND:
     * {@code LocalSequencer} throws {@link io.github.huyz0.os.biningester.sequencer.FencedException}
     * BEFORE reconciling, and {@code FleetSequencer} catches that, retires the
     * leadership the mark lives on, and forwards the identical request to
     * another pod. Correlated, not independent: ADR-0027's self-fence fires on
     * an ambiguous lease RENEW. ⚠️ THAT IS ONE ROUTE, NOT THE ENUMERATION --
     * {@link io.github.huyz0.os.biningester.sequencer.Sequencer} holds it. A resend reaching a pod
     * that never made the append is NOT by itself a duplicate: review measured
     * one delta, because that pod seeds its window from the CHAIN.
     */
    @Override
    public CommitDelta commit(CommitRequest request) throws IOException {
        try {
            return delegate.commit(request);
        } catch (AmbiguousAppendException lost) {
            return delegate.commit(request);
        }
    }

    /** ⚠️ Straight through, for the reason the class javadoc gives. */
    @Override
    public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
        return delegate.commitAll(requests);
    }

    /** ⚠️ Straight through: a decorator writes no chain of its own. */
    @Override
    public long epoch() {
        return delegate.epoch();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
