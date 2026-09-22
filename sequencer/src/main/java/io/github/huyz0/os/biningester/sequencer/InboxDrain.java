// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The leaseholder applies the inbox's intents and deletes them (M8.14a,
 * ADR-0058).
 *
 * <p>⚠️ **ON HEAL AND ON TAKEOVER, NEVER ON A TIMER**: a pod that deferred asks
 * for a drain the next time it can reach the leaseholder, and a new term
 * drains once for the pods that died deferring. A timer would LIST the inbox
 * per interval on every idle leader, for ever (cost.md rule 2).
 *
 * <p>⚠️ **EACH POD's INTENTS IN ORDER, AND A POD STOPS AT ITS FIRST FAILURE**:
 * the dedupe window keeps one high mark per incarnation, so applying flush N+1
 * after N failed would turn N into a refused replay. Applied intents are
 * deleted; an intent batch that failed stays, and so does everything after it.
 * The ordered intents are committed in one batch per pod, so commit-log PUT
 * cost scales with pods rather than with the number of intents accumulated
 * during a partition (M9.12); the inbox's one GET per intent remains bounded
 * recovery work.
 */
public final class InboxDrain {

    private static final System.Logger LOG = System.getLogger(InboxDrain.class.getName());

    private InboxDrain() {
    }

    /**
     * Applies every intent it can through {@code term} and deletes those.
     *
     * @return how many intents were applied
     * @throws IOException if any pod's intents could not all be applied -- ⚠️ so
     *     the pod that asked stays deferring rather than forwarding past them
     */
    public static int drain(BinStore store, String prefix, Sequencer term) throws IOException {
        return drain(store, prefix, term, null);
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
        Objects.requireNonNull(term, "term");
        Object lock = LocalSequencer.underneath(term)
                .map(LocalSequencer::drainLock)
                .orElse(term);
        synchronized (lock) {
            return drainLocked(store, prefix, term, requester);
        }
    }

    private static int drainLocked(BinStore store, String prefix, Sequencer term,
            String requester) throws IOException {
        List<Inbox.Pending> pending = Inbox.pending(store, prefix);
        Map<String, List<Inbox.Pending>> byPod = new LinkedHashMap<>();
        for (Inbox.Pending intent : pending) {
            String who = intent.request().podId() + "/" + intent.request().incarnationId();
            byPod.computeIfAbsent(who, ignored -> new ArrayList<>()).add(intent);
        }
        List<String> applied = new ArrayList<>();
        IOException first = null;
        int stalled = 0;
        for (List<Inbox.Pending> batch : byPod.values()) {
            Inbox.Pending firstIntent = batch.getFirst();
            try {
                term.commitAll(batch.stream().map(Inbox.Pending::request).toList());
                applied.addAll(batch.stream().map(Inbox.Pending::key).toList());
            } catch (FencedException fenced) {
                deleteApplied(store, applied);
                throw fenced;
            } catch (IOException failed) {
                stalled++;
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
        return Thread.ofVirtual().name("inbox-drain").start(() -> {
            try {
                drain(store, prefix, term);
            } catch (IOException | RuntimeException failed) {
                LOG.log(System.Logger.Level.WARNING, () -> "the inbox drain at takeover did "
                        + "not finish; a deferring pod's next request tries again: " + failed);
            }
        });
    }
}
