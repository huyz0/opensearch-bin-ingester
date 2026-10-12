// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A takeover's RELEASE (ADR-0081 §5 step 7; M13.33i): sent only after the
 * chain entry covering it, one frame to each holder of the unclosed terms,
 * naming every decided stream and the offset below which it is committed.
 *
 * <p>⚠️ ONE FRAME PER HOLDER, NEVER PER STREAM: every stream rides one
 * RELEASE, so the frames scale with pods -- split only past
 * {@link #PER_FRAME} streams, which a peer frame could not carry (M13.33q).
 *
 * <p>⚠️ A HOLDER THAT MISSES IT IS COUNTED, NEVER THROWN: it learns the same
 * from its next HELD report's answer, at the cost of one report -- never a
 * takeover that waits on a RELEASE.
 */
public final class ReleaseSender {

    /**
     * The streams one RELEASE carries (M13.33q): 28 bytes each, so a frame
     * stays under 30 MiB, inside the peer frame's 64 MiB.
     */
    public static final int PER_FRAME = 1 << 20;

    private final String selfUid;
    private final long epoch;
    private final TermJoiner.Transport transport;
    private final int perFrame;

    /** @param epoch the successor's term, E' */
    public ReleaseSender(String selfUid, long epoch, TermJoiner.Transport transport) {
        this(selfUid, epoch, transport, PER_FRAME);
    }

    /** @param perFrame the most streams one RELEASE carries */
    ReleaseSender(String selfUid, long epoch, TermJoiner.Transport transport, int perFrame) {
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.epoch = epoch;
        this.transport = Objects.requireNonNull(transport, "transport");
        if (perFrame < 1) {
            throw new IllegalArgumentException("a RELEASE carries at least one stream");
        }
        this.perFrame = perFrame;
    }

    /**
     * Sends {@code releaseBelow} to every ROSTERED member of {@code unclosed}
     * but this pod, in as few frames as {@link #PER_FRAME} allows: how many
     * acknowledged every frame.
     */
    public int send(List<Roster> unclosed, Map<RunKey, Long> releaseBelow) {
        Objects.requireNonNull(unclosed, "unclosed");
        // ⚠️ NOTHING DECIDED RELEASES NOTHING (its review round 1, P1): no
        // frame, and no holder counted as having answered one.
        if (releaseBelow.isEmpty()) {
            return 0;
        }
        List<FastTakeoverFrame.ReleaseAt> streams = new ArrayList<>();
        new TreeMap<>(releaseBelow).forEach((stream, below) ->
                streams.add(new FastTakeoverFrame.ReleaseAt(stream, below)));
        List<FastTakeoverFrame.Release> frames = new ArrayList<>();
        for (int from = 0; from < streams.size(); from += perFrame) {
            frames.add(new FastTakeoverFrame.Release(
                    streams.subList(from, Math.min(streams.size(), from + perFrame))));
        }
        Map<String, Roster.Incarnation> holders = new LinkedHashMap<>();
        for (Roster r : unclosed) {
            for (Roster.Member m : r.members()) {
                String uid = m.incarnation().podUid();
                if (m.state() == Roster.State.ROSTERED && !uid.equals(selfUid)) {
                    holders.putIfAbsent(uid, m.incarnation());
                }
            }
        }
        int acked = 0;
        for (Roster.Incarnation holder : holders.values()) {
            if (sentEvery(holder, frames)) {
                acked++;
            }
        }
        return acked;
    }

    private boolean sentEvery(Roster.Incarnation holder,
            List<FastTakeoverFrame.Release> frames) {
        boolean every = true;
        for (FastTakeoverFrame.Release release : frames) {
            try {
                FastFrame.Frame answer = FastFrame.decode(transport.exchange(holder.endpoint(),
                        FastFrame.encode(epoch, selfUid, holder.podUid(), release)));
                every &= answer.body() instanceof FastFrame.HeldStatusReport;
            } catch (IOException missed) {
                // learned from its next HELD report's answer
                every = false;
            }
        }
        return every;
    }
}
