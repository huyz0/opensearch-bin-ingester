// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.util.Optional;
import java.util.TreeMap;

/**
 * The terms a pod has joined (ADR-0081 §1; M13.26d): it holds no entry and
 * sends no batch of a term it has not been told, by JOINED, that it joined --
 * so the incarnations that can hold a term's copies are its roster's members.
 *
 * <p>⚠️ A CLOSED TERM IS HELD BY NOBODY: every JOINED carries
 * {@code closedThrough}, and a term at or below it is forgotten here as its
 * entries are dropped from the journal.
 */
public final class JoinedTerms {

    /** A joined term's leader, where a writer sends its batches (M13.29c). */
    public record Leader(long epoch, String uid, String endpoint) {
    }

    /** Each joined term, by epoch, and its leader's UID: empty where it was not recorded. */
    private final TreeMap<Long, String> joined = new TreeMap<>();
    /** Each joined term's leader endpoint, where its join recorded one. */
    private final TreeMap<Long, String> endpoints = new TreeMap<>();
    private long closedThrough;

    /** A JOINED for {@code epoch} arrived, naming the highest closed epoch. */
    public synchronized void joined(long epoch, long closedThrough) {
        joined(epoch, closedThrough, "");
    }

    /**
     * The same, from {@code leaderUid}, the term's leader (M13.28a): a holder
     * journals a REPLICA only from it.
     */
    public synchronized void joined(long epoch, long closedThrough, String leaderUid) {
        joined(epoch, closedThrough, leaderUid, "");
    }

    /**
     * The same, at {@code leaderEndpoint}, where the join reached the leader
     * (M13.29c): a writer's batches of the term go there.
     */
    public synchronized void joined(long epoch, long closedThrough, String leaderUid,
            String leaderEndpoint) {
        java.util.Objects.requireNonNull(leaderEndpoint, "leaderEndpoint");
        if (epoch < 1 || closedThrough < 0) {
            throw new IllegalArgumentException("a joined term is at least 1, a closed one "
                    + "never negative");
        }
        java.util.Objects.requireNonNull(leaderUid, "leaderUid");
        this.closedThrough = Math.max(this.closedThrough, closedThrough);
        if (epoch > this.closedThrough) {
            joined.put(epoch, leaderUid);
            endpoints.put(epoch, leaderEndpoint);
        }
        joined.headMap(this.closedThrough, true).clear();
        endpoints.headMap(this.closedThrough, true).clear();
    }

    /**
     * The newest joined term's leader, where its join recorded the leader and
     * its endpoint (M13.29c): empty where it did not, or no term is held.
     */
    public synchronized Optional<Leader> latest() {
        if (joined.isEmpty()) {
            return Optional.empty();
        }
        long epoch = joined.lastKey();
        String uid = joined.get(epoch);
        String endpoint = endpoints.getOrDefault(epoch, "");
        return uid.isEmpty() || endpoint.isEmpty() ? Optional.empty()
                : Optional.of(new Leader(epoch, uid, endpoint));
    }

    /**
     * The leader of joined, unclosed term {@code epoch} and its endpoint
     * (M13.30p): where a fast key of that term is read before its upload.
     */
    public synchronized Optional<Leader> leader(long epoch) {
        if (!mayHold(epoch)) {
            return Optional.empty();
        }
        String uid = joined.get(epoch);
        String endpoint = endpoints.getOrDefault(epoch, "");
        return uid.isEmpty() || endpoint.isEmpty() ? Optional.empty()
                : Optional.of(new Leader(epoch, uid, endpoint));
    }

    /** Whether this pod may journal, or send, an entry of {@code epoch}. */
    public synchronized boolean mayHold(long epoch) {
        return epoch > closedThrough && joined.containsKey(epoch);
    }

    /** The leader of joined term {@code epoch}, where its join recorded one (M13.28a). */
    public synchronized Optional<String> leaderOf(long epoch) {
        String leader = mayHold(epoch) ? joined.get(epoch) : null;
        return leader == null || leader.isEmpty() ? Optional.empty() : Optional.of(leader);
    }
}
