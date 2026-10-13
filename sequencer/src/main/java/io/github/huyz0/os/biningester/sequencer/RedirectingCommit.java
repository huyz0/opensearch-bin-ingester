// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A default-path commit answered with ADR-0082 §7's commit answer at a fast
 * term's leader (ADR-0081 §8; M13.34c), split out of {@link LocalSequencer}
 * at its ceiling: each run of a stream the leader's catalog has in fast mode
 * REDIRECTED -- the writer re-sends those records by COMMIT -- and the rest
 * committed, the answer listing every delta that committed one of them.
 *
 * <p>⚠️ A RETRY IS ANSWERED PER RUN: a committed run from the deltas the
 * idempotency window names (M13.34b); a run never committed -- redirected
 * then, its stream turned back to the default path since -- committed NOW
 * from the already-uploaded segment; a run of a stream fast now redirected
 * again, never committed over fast offsets.
 */
final class RedirectingCommit {

    private RedirectingCommit() {
    }

    /**
     * Whether {@code request} must be answered here rather than batched
     * (M13.34k): a run of a stream fast at the leader, a run a switch holds
     * (M13.34s: its stream paused here first), or a retry the window answers
     * per delta.
     */
    static boolean alone(LocalSequencer sequencer, CommitRequest request) {
        Optional<FastLeaderTerm> fast = sequencer.fastTerm();
        if (fast.isPresent()) {
            request.recordCounts().keySet().forEach(fast.get()::switchPause);
            if (request.recordCounts().keySet().stream().anyMatch(
                    s -> fast.get().fastHere(s) || fast.get().holdsDefault(s))) {
                return true;
            }
        }
        synchronized (sequencer) {
            return sequencer.window.runsOf(request).isPresent();
        }
    }

    static CommitAnswer answer(LocalSequencer sequencer, CommitRequest request)
            throws IOException {
        Optional<FastLeaderTerm> fast = sequencer.fastTerm();
        Map<RunKey, Integer> kept = new TreeMap<>();
        List<RunKey> redirected = new ArrayList<>();
        CommitRequest committing;
        synchronized (sequencer) {
            sequencer.reconcileAmbiguousAppend();
            // ⚠️ THE WINDOW FIRST (its review round 1, P1): a run already
            // committed is answered from its delta though its stream turned fast
            // since -- redirected, its records would be written twice.
            Map<RunKey, IdempotencyWindow.At> tracked =
                    sequencer.window.runsOf(request).orElse(Map.of());
            request.recordCounts().forEach((stream, count) -> {
                if (!tracked.containsKey(stream) && fast.isPresent()
                        && fast.get().fastHere(stream)) {
                    redirected.add(stream);
                } else {
                    kept.put(stream, count);
                }
            });
            redirected.sort(null);
            if (kept.isEmpty()) {
                return new CommitAnswer(List.of(), redirected);
            }
            committing = new CommitRequest(request.podId(), request.incarnationId(),
                    request.flushSeq(), request.segmentKey(), kept);
            if (sequencer.window.runsOf(committing).isPresent()) {
                Map<RunKey, Integer> missing = new TreeMap<>(kept);
                missing.keySet().removeAll(tracked.keySet());
                if (!missing.isEmpty()) {
                    CommitRequest now = new CommitRequest(request.podId(),
                            request.incarnationId(), request.flushSeq(), request.segmentKey(),
                            missing);
                    // ⚠️ refused at once, not waited on: a retry's hold is its retry
                    DefaultHold.refuseHeld(fast.orElse(null), List.of(now));
                    sequencer.applyFresh(List.of(now));
                }
                return new CommitAnswer(sequencer.window.answerByDelta(committing), redirected);
            }
        }
        // ⚠️ NO OTHER STREAM's ACKS WAIT (ADR-0081 §8; M13.34s): a run a switch
        // holds waits alone -- the request's others committed now, the held
        // ones after the switch, in a later delta of the same segment, answered
        // with both by the window's per-run answer below.
        if (fast.isPresent()) {
            kept.keySet().forEach(fast.get()::switchPause);
            Map<RunKey, Integer> held = new TreeMap<>(kept);
            held.keySet().removeIf(s -> !fast.get().holdsDefault(s));
            if (!held.isEmpty() && held.size() < kept.size()) {
                Map<RunKey, Integer> now = new TreeMap<>(kept);
                now.keySet().removeAll(held.keySet());
                sequencer.commitAll(List.of(new CommitRequest(request.podId(),
                        request.incarnationId(), request.flushSeq(), request.segmentKey(), now)));
                DefaultHold.await(fast.get(), List.of(new CommitRequest(request.podId(),
                        request.incarnationId(), request.flushSeq(), request.segmentKey(),
                        held)));
                return answer(sequencer, request);
            }
        }
        CommitDelta delta = sequencer.commitAll(List.of(committing));
        List<SegmentCommit> mine = delta.segments().stream()
                .filter(s -> s.segmentKey().equals(request.segmentKey())).toList();
        return new CommitAnswer(List.of(new CommitDelta(delta.sequence(), mine)), redirected);
    }
}
