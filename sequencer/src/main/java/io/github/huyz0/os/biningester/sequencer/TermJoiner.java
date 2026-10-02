// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The pod side of JOIN (ADR-0081 §1; M13.26g): every ready ingester node joins
 * each term as soon as it learns the term's leader -- at startup and at every
 * leader change -- whether or not it writes or subscribes, so holders exist in
 * every AZ that has a pod. A {@code q >= 2} index whose writers all sit in the
 * leader's AZ still finds holders elsewhere.
 *
 * <p>⚠️ THE ANSWER IS FENCED BEFORE IT IS READ: its header must be addressed
 * to this incarnation, and its epoch admitted by the pod's {@link EpochFence}
 * -- a deposed leader's JOINED is refused like any lower frame. ⚠️ ONLY A
 * JOINED OF THE TERM ASKED is recorded in {@link JoinedTerms}; a pod holds
 * nothing of a term it was not told it joined.
 *
 * <p>The frame travels over {@link Transport}, a seam (non-negotiable 7): the
 * HTTP adapter lands with the leader's endpoint (M13.27).
 */
public final class TermJoiner {

    /** One fast frame to a peer and its answer. */
    @FunctionalInterface
    public interface Transport {
        byte[] exchange(String endpoint, byte[] frame) throws IOException;
    }

    /**
     * The term is below this pod's epoch fence: its leader is deposed, and
     * ⚠️ RETRYING CANNOT SUCCEED (M13.26g review round 1, P2) -- the caller
     * joins the newer leader instead. Thrown before anything is sent when the
     * term asked is already below the fence, and for an answer below it.
     */
    public static final class Fenced extends IOException {
        private final long fence;

        Fenced(long fence, String message) {
            super(message);
            this.fence = fence;
        }

        /** The fence's epoch when the join was refused. */
        public long fence() {
            return fence;
        }
    }

    /** How a join ended. */
    public sealed interface Outcome permits Joined, AlreadyJoined, Refused, Superseded {
    }

    /** Rostered in the term; the leader's status of what this pod reported holding. */
    public record Joined(long epoch, long closedThrough, FastFrame.HeldStatus status)
            implements Outcome {
    }

    /** This pod had already joined the term: nothing was sent. */
    public record AlreadyJoined(long epoch) implements Outcome {
    }

    /** The leader refused the join -- this incarnation departed, or it is not the leader. */
    public record Refused(FastFrame.Reason reason, String text) implements Outcome {
    }

    /** The answer came from another term than the one asked: nothing is recorded. */
    public record Superseded(long answeredEpoch) implements Outcome {
    }

    private final Roster.Incarnation self;
    private final Transport transport;
    private final EpochFence fence;
    private final JoinedTerms joined;
    private final Supplier<FastFrame.Held> held;

    /**
     * @param held what this pod's journal holds now, reported in every JOIN
     *     ({@link FastFrame.Held#NONE} for a pod that never journaled)
     */
    public TermJoiner(Roster.Incarnation self, Transport transport, EpochFence fence,
            JoinedTerms joined, Supplier<FastFrame.Held> held) {
        this.self = Objects.requireNonNull(self, "self");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.fence = Objects.requireNonNull(fence, "fence");
        this.joined = Objects.requireNonNull(joined, "joined");
        this.held = Objects.requireNonNull(held, "held");
    }

    /**
     * Joins term {@code epoch}, whose leader this pod just learned.
     *
     * @throws Fenced the term or its answer is below the fence -- final
     * @throws IOException the exchange failed, or the answer was malformed or
     *     addressed elsewhere -- nothing is recorded, and the caller tries
     *     again
     */
    public Outcome join(long epoch, String leaderUid, String leaderEndpoint) throws IOException {
        Objects.requireNonNull(leaderUid, "leaderUid");
        Objects.requireNonNull(leaderEndpoint, "leaderEndpoint");
        // ⚠️ THE FENCE FIRST (M13.26g review round 2, P1): a term joined
        // earlier and deposed since is final too, never "already joined".
        if (epoch < fence.highest()) {
            throw new Fenced(fence.highest(), "term " + epoch + " is below the fence at "
                    + fence.highest() + "; its leader is deposed");
        }
        if (joined.mayHold(epoch)) {
            return new AlreadyJoined(epoch);
        }
        byte[] answer = transport.exchange(leaderEndpoint, FastFrame.encode(epoch,
                self.podUid(), leaderUid, new FastFrame.Join(self, held.get())));
        FastFrame.Header header = FastFrame.header(answer);
        if (!header.targetUid().equals(self.podUid())) {
            throw new IOException("a JOIN answer addressed to " + header.targetUid());
        }
        if (!header.senderUid().equals(leaderUid)) {
            throw new IOException("a JOIN answered by " + header.senderUid() + ", not the leader");
        }
        if (!fence.admit(header.epoch())) {
            throw new Fenced(fence.highest(), "a JOIN answer of epoch " + header.epoch()
                    + ", below the fence at " + fence.highest());
        }
        FastFrame.Body body = FastFrame.decode(answer).body();
        if (header.epoch() != epoch) {
            return new Superseded(header.epoch());
        }
        return switch (body) {
            case FastFrame.Joined j -> {
                joined.joined(epoch, j.closedThrough());
                yield new Joined(epoch, j.closedThrough(), j.status());
            }
            case FastFrame.Refused r -> new Refused(r.reason(), r.text());
            case FastFrame.Join unexpected -> throw new IOException(
                    "a JOIN answered by a JOIN");
            case FastFrame.Depart unexpected -> throw new IOException(
                    "a JOIN answered by a DEPART");
            case FastFrame.HeldReport unexpected -> throw new IOException(
                    "a JOIN answered by a HELD");
            case FastFrame.HeldStatusReport unexpected -> throw new IOException(
                    "a JOIN answered by a HELD_STATUS");
            default -> throw new IOException("a JOIN answered by kind " + body.kind());
        };
    }
}
