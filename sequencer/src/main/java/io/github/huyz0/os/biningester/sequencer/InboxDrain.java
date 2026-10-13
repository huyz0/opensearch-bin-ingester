// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.GovernorScope;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.LongConsumer;

/**
 * The leaseholder applies the inbox's intents and deletes them (M8.14a,
 * ADR-0058).
 *
 * <p>⚠️ **ON HEAL AND ON TAKEOVER, NEVER ON A LEADER's TIMER**: a pod that
 * deferred asks for a drain the next time it can reach the leaseholder, and
 * again every renew interval until one succeeds (M13.78); a new term drains
 * once for the pods that died deferring. A leader's timer would LIST the inbox
 * per interval on every idle leader, for ever (cost.md rule 2).
 *
 * <p>⚠️ **EACH POD's INTENTS IN ORDER, AND A POD STOPS AT ITS FIRST FAILURE**:
 * the dedupe window keeps one high mark per incarnation, and an intent at or
 * below it is DELETED as applied (M13.73) -- so applying flush N+1 after N
 * failed would have N deleted unapplied, an acked write silently lost. Applied
 * intents are
 * deleted; an intent batch that failed stays, and so does everything after it.
 * The ordered intents are committed in one batch per pod, so commit-log PUT
 * cost scales with pods rather than with the number of intents accumulated
 * during a partition (M9.12); the inbox's one GET per intent remains bounded
 * recovery work.
 *
 * <p>⚠️ **AN INTENT's RUNS OF A STREAM THE LEADER HOLDS FAST ARE WRITTEN FAST**
 * (M13.34m; ADR-0081 §8): deferred while its writer's registration was stale,
 * the producer answered 202, a default commit of them is refused -- and was,
 * by every drain, stalling that pod. They are written through the leader's
 * own fast path FIRST ({@link FastRedirect}), then the rest committed under
 * the intent's triple, so a failed write leaves the intent unapplied. Each
 * such intent costs its own commit, its pod's plain intents around it still
 * batched; an intent applied is deleted even if a later one of its pod fails.
 * ⚠️ AT LEAST ONCE, NEVER LOST: an intent whose fast write landed stays if
 * a later step fails -- a second fast run, its rest's commit -- or if the
 * drain dies before its delete, and every later drain writes those records
 * again while that lasts. One the window has applied is never written again.
 */
public final class InboxDrain {

    private static final System.Logger LOG = System.getLogger(InboxDrain.class.getName());

    /** An intent's runs of streams the leader holds fast, written through its fast path. */
    @FunctionalInterface
    public interface FastRedirect {

        /** None set: the intent stays, as before M13.34m. */
        FastRedirect NONE = (intent, runs) -> {
            throw new IOException("no fast path to write " + runs + " of an intent through");
        };

        /**
         * Writes {@code runs}' records, read from {@code intent}'s segment, fast.
         *
         * @throws IOException not all were written: the intent stays
         */
        void write(CommitRequest intent, Set<RunKey> runs) throws IOException;
    }

    private InboxDrain() {
    }

    /** {@code term}'s drains write fast runs through {@code redirect} (M13.34m). */
    public static void redirect(Sequencer term, FastRedirect redirect) {
        Objects.requireNonNull(redirect, "redirect");
        LocalSequencer.underneath(term).flatMap(LocalSequencer::fastTerm)
                .ifPresent(fast -> fast.gate.redirect = redirect);
    }

    /**
     * Applies every intent it can through {@code term} and deletes those.
     *
     * @return how many intents were applied
     * @throws IOException if any pod's intents could not all be applied -- ⚠️ so
     *     the pod that asked stays deferring rather than forwarding past them
     */
    public static int drain(BinStore store, String prefix, Sequencer term) throws IOException {
        return drain(store, prefix, term, null, ignored -> { });
    }

    /**
     * The same, failing only if {@code requester}'s own intents could not all
     * be applied.
     *
     * <p>⚠️ **ONE POD's STUCK INTENT MUST NOT HOLD EVERY OTHER POD IN THE INBOX**
     * (review R2): a pod stops deferring once ITS intents are in the chain,
     * whatever another pod's are doing. {@code null} fails on any pod's.
     *
     * <p>⚠️ **SERIALISED PER TERM** (review R4): the takeover drain, the route
     * and two deferring pods asking at once would otherwise apply one intent
     * in two concurrent batches, where the dedupe window counts both fresh.
     * One at a time, the second finds the first's work deleted -- which also
     * bounds the cost at heal to the intents plus one LIST per page. Within
     * one pod, the ordered intents are one {@link Sequencer#commitAll} batch,
     * so the commit-log PUT is per pod rather than per intent; reading the
     * intent bodies still costs one GET per intent, as the inbox format requires.
     * The lock is the inner {@link LocalSequencer}'s dedicated drain monitor
     * when {@code term} is the elected {@link BatchingSequencer}; takeover
     * passes that inner term while the route passes the wrapper, so both entry
     * points must canonicalize to one monitor. It is separate from the local
     * sequencer monitor because a batching drain calls back into that monitor.
     */
    public static int drain(BinStore store, String prefix, Sequencer term, String requester)
            throws IOException {
        return drain(store, prefix, term, requester, ignored -> { });
    }

    /** The same drain, reporting the size of each failed per-pod intent batch in memory. */
    public static int drain(BinStore store, String prefix, Sequencer term, String requester,
            LongConsumer failedBatchSize) throws IOException {
        Objects.requireNonNull(term, "term");
        Objects.requireNonNull(failedBatchSize, "failedBatchSize");
        Object lock = LocalSequencer.underneath(term)
                .map(LocalSequencer::drainLock)
                .orElse(term);
        synchronized (lock) {
            return drainLocked(store, prefix, term, requester, failedBatchSize);
        }
    }

    private static int drainLocked(BinStore store, String prefix, Sequencer term,
            String requester, LongConsumer failedBatchSize) throws IOException {
        // ⚠️ DECLARED RECOVERY (ADR-0075), HERE AND NOT IN `Inbox.pending`: the
        // orphan sweep reads the same inbox for its keep list, and that read is
        // discretionary and governed. A refused DRAIN strands acked intents;
        // it is bounded by its trigger: a heal, a takeover, or a deferring
        // pod's retry every renew interval (ADR-0058, M13.78), which backs off
        // to one ask per 64 intervals while that pod's own intent keeps
        // failing (M13.80): a LIST page per 1,000 keys and a GET per pending
        // intent per ask. ⚠️
        // Bound on the thread that lists: `inBackground` calls this on its own.
        List<Inbox.Pending> pending = GovernorScope.recovery(
                () -> Inbox.pending(store, prefix));
        Map<String, List<Inbox.Pending>> byPod = new LinkedHashMap<>();
        for (Inbox.Pending intent : pending) {
            String who = intent.request().podId() + "/" + intent.request().incarnationId();
            byPod.computeIfAbsent(who, ignored -> new ArrayList<>()).add(intent);
        }
        List<String> applied = new ArrayList<>();
        IOException first = null;
        int stalled = 0;
        Optional<FastLeaderTerm> fast = LocalSequencer.underneath(term)
                .flatMap(LocalSequencer::fastTerm);
        for (List<Inbox.Pending> batch : byPod.values()) {
            Inbox.Pending firstIntent = batch.getFirst();
            try {
                List<CommitRequest> requests = batch.stream().map(Inbox.Pending::request)
                        .toList();
                // ⚠️ ONLY WHAT THE TERM HAS NOT APPLIED (M13.73): the rest landed
                // under an earlier drain that died before its deletes, and is
                // deleted below with the batch.
                List<CommitRequest> unapplied = LocalSequencer.underneath(term).isPresent()
                        ? LocalSequencer.underneath(term).get().unapplied(requests)
                        : requests;
                applyInOrder(term, fast, batch, unapplied, applied);
            } catch (FencedException fenced) {
                deleteApplied(store, applied);
                throw fenced;
            } catch (IOException failed) {
                stalled++;
                try {
                    failedBatchSize.accept(batch.size());
                } catch (RuntimeException metricsFailure) {
                    LOG.log(System.Logger.Level.WARNING,
                            "could not record failed inbox intent attempts", metricsFailure);
                }
                if (requester == null || requester.equals(firstIntent.request().podId())) {
                    first = first == null ? failed : first;
                }
                LOG.log(System.Logger.Level.WARNING, () -> "an inbox intent could not be "
                        + "applied; its pod's ordered batch stays: " + firstIntent.key()
                        + " (" + batch.size() + " intents): " + failed);
            }
        }
        deleteApplied(store, applied);
        if (first != null) {
            throw new IOException(stalled + " pod(s) still have intents in the inbox",
                    first);
        }
        return applied.size();
    }

    /**
     * {@code unapplied} committed in {@code batch}'s order, an intent with a
     * fast run split around its fast write (M13.34m); each intent's key added
     * to {@code applied} once it is, an already applied one's with its neighbours.
     */
    private static void applyInOrder(Sequencer term, Optional<FastLeaderTerm> fast,
            List<Inbox.Pending> batch, List<CommitRequest> unapplied, List<String> applied)
            throws IOException {
        Set<CommitRequest> pending = new java.util.HashSet<>(unapplied);
        List<CommitRequest> plain = new ArrayList<>();
        List<String> plainKeys = new ArrayList<>();
        for (Inbox.Pending intent : batch) {
            CommitRequest request = intent.request();
            Set<RunKey> runs = new TreeSet<>();
            if (pending.contains(request) && fast.isPresent()) {
                request.recordCounts().keySet().stream().filter(fast.get()::fastHere)
                        .forEach(runs::add);
            }
            if (runs.isEmpty()) {
                if (pending.contains(request)) {
                    plain.add(request);
                }
                plainKeys.add(intent.key());
                continue;
            }
            commitPlain(term, plain, plainKeys, applied);
            fast.get().gate.redirect.write(request, runs);
            Map<RunKey, Integer> rest = new TreeMap<>(request.recordCounts());
            rest.keySet().removeAll(runs);
            if (!rest.isEmpty()) {
                term.commitAll(List.of(new CommitRequest(request.podId(),
                        request.incarnationId(), request.flushSeq(), request.segmentKey(), rest)));
            }
            applied.add(intent.key());
        }
        commitPlain(term, plain, plainKeys, applied);
    }

    /** The plain intents so far, in one commit, then counted applied. */
    private static void commitPlain(Sequencer term, List<CommitRequest> plain,
            List<String> keys, List<String> applied) throws IOException {
        if (!plain.isEmpty()) {
            term.commitAll(List.copyOf(plain));
        }
        applied.addAll(keys);
        plain.clear();
        keys.clear();
    }

    private static void deleteApplied(BinStore store, List<String> applied) throws IOException {
        for (int from = 0; from < applied.size(); from += 1000) {
            store.delete(applied.subList(from, Math.min(from + 1000, applied.size())));
        }
    }

    /**
     * Drains once on a virtual thread, for a term just taken (M8.14a): the
     * intents of pods that died deferring have nobody else to ask.
     */
    public static Thread inBackground(BinStore store, String prefix, Sequencer term) {
        return inBackground(store, prefix, term, ignored -> { });
    }

    /** The same one-shot takeover drain, with a process-local failed-attempt counter. */
    public static Thread inBackground(BinStore store, String prefix, Sequencer term,
            LongConsumer failedBatchSize) {
        return Thread.ofVirtual().name("inbox-drain").start(() -> {
            try {
                drain(store, prefix, term, null, failedBatchSize);
            } catch (IOException | RuntimeException failed) {
                LOG.log(System.Logger.Level.WARNING, () -> "the inbox drain at takeover did "
                        + "not finish; a deferring pod's next request tries again: " + failed);
            }
        });
    }
}
