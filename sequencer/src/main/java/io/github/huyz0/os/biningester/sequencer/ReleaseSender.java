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
 * RELEASE, so the frames scale with pods.
 *
 * <p>⚠️ A HOLDER THAT MISSES IT IS COUNTED, NEVER THROWN: it learns the same
 * from its next HELD report's answer, at the cost of one report -- never a
 * takeover that waits on a RELEASE.
 */
public final class ReleaseSender {

    private final String selfUid;
    private final long epoch;
    private final TermJoiner.Transport transport;

    /** @param epoch the successor's term, E' */
    public ReleaseSender(String selfUid, long epoch, TermJoiner.Transport transport) {
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.epoch = epoch;
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    /**
     * Sends {@code releaseBelow} to every ROSTERED member of {@code unclosed}
     * but this pod: how many acknowledged it.
     */
    public int send(List<Roster> unclosed, Map<RunKey, Long> releaseBelow) {
        Objects.requireNonNull(unclosed, "unclosed");
        List<FastTakeoverFrame.ReleaseAt> streams = new ArrayList<>();
        new TreeMap<>(releaseBelow).forEach((stream, below) ->
                streams.add(new FastTakeoverFrame.ReleaseAt(stream, below)));
        FastTakeoverFrame.Release release = new FastTakeoverFrame.Release(streams);
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
            try {
                FastFrame.Frame answer = FastFrame.decode(transport.exchange(holder.endpoint(),
                        FastFrame.encode(epoch, selfUid, holder.podUid(), release)));
                if (answer.body() instanceof FastFrame.HeldStatusReport) {
                    acked++;
                }
            } catch (IOException missed) {
                // learned from its next HELD report's answer
            }
        }
        return acked;
    }
}
