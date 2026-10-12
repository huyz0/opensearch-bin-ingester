// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.ReplicaHolder;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * A node's handler of the frames a holder or a writer answers -- REPLICA
 * (M13.28a), and EXPOSED (M13.29a), delivered to the writes waiting in the
 * pod's inbox only from the leader of a term it joined. A REPLICA: its
 * fast journal holds the copy, under the pod's joined terms and holder gate.
 * ⚠️ A POD WITHOUT A FAST JOURNAL HOLDS NO COPY, so refuses {@code NOT_FAST}
 * and the leader places the copy elsewhere.
 */
final class FastHolderFrames {

    /**
     * A COLLECTED page's entry bytes: one peer frame less 1 MiB for the
     * header and the per-entry lengths (ADR-0082 §2: COLLECTED is streamed in
     * pages of the frame's size).
     */
    static final long COLLECTED_PAGE_BYTES =
            io.github.huyz0.os.biningester.http.FastFrameService.MAX_FRAME_BYTES - (1L << 20);

    private FastHolderFrames() {
    }

    static void register(BiConsumer<Integer, FastFrameRouter.Handler> handle, FastDisk disk) {
        register(handle, disk, epoch -> false, "");
    }

    /**
     * The same, on a pod that may lead a term (M13.84): an EXPOSED it sent
     * itself, as the leader of {@code leads}' term, reaches its own inbox.
     *
     * @param leads whether this pod leads the term of an epoch
     * @param selfUid this pod's UID, the sender of its own EXPOSEDs
     */
    static void register(BiConsumer<Integer, FastFrameRouter.Handler> handle, FastDisk disk,
            java.util.function.LongPredicate leads, String selfUid) {
        Optional<ReplicaHolder> holder = disk.journal().map(
                journal -> new ReplicaHolder(journal, disk.joined(), disk.gate(),
                        disk.fence()::highest));
        handle.accept(FastWriteFrame.KIND_REPLICA, (header, body) -> {
            if (holder.isEmpty()) {
                return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                        "this pod holds no fast journal");
            }
            return holder.get().answer(header, (FastWriteFrame.Replica) body);
        });
        // ⚠️ M13.33b: a successor's FENCE, answered from this pod's journal --
        // a diskless pod holds nothing, so answers one empty page marked last.
        Optional<io.github.huyz0.os.biningester.sequencer.FenceAnswer> fences = disk.journal().map(
                journal -> new io.github.huyz0.os.biningester.sequencer.FenceAnswer(journal,
                        COLLECTED_PAGE_BYTES, disk.gate()));
        handle.accept(io.github.huyz0.os.biningester.format.FastTakeoverFrame.KIND_FENCE,
                (header, body) -> {
                    var fence = (io.github.huyz0.os.biningester.format.FastTakeoverFrame.Fence) body;
                    if (fences.isEmpty()) {
                        return new io.github.huyz0.os.biningester.format.FastTakeoverFrame
                                .Collected(fence.page(), true, java.util.List.of());
                    }
                    return fences.get().answer(header, fence);
                });
        // ⚠️ M13.28b: a RELEASE, from the leader of a term this pod joined; a
        // diskless pod holds nothing to release.
        Optional<io.github.huyz0.os.biningester.sequencer.ReleaseAnswer> releases =
                disk.journal().map(journal ->
                        new io.github.huyz0.os.biningester.sequencer.ReleaseAnswer(journal,
                                disk.joined()));
        handle.accept(io.github.huyz0.os.biningester.format.FastTakeoverFrame.KIND_RELEASE,
                (header, body) -> releases.isEmpty()
                        ? new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE)
                        : releases.get().answer(header,
                                (io.github.huyz0.os.biningester.format.FastTakeoverFrame.Release) body));
        handle.accept(FastWriteFrame.KIND_EXPOSED, (header, body) -> {
            // ⚠️ THE LEADER IS A WRITER OF ITS OWN TERM (M13.84; ADR-0081 §2)
            // and joins no term of its own: its EXPOSEDs to itself are
            // admitted for the term it leads, from its own UID only.
            // ⚠️ NEVER EPOCH 0 (its review round 1, P1): a pod leading no
            // serving term reports its own term as 0.
            boolean own = !selfUid.isEmpty() && header.senderUid().equals(selfUid)
                    && header.epoch() > 0 && leads.test(header.epoch());
            Optional<String> leader = disk.joined().leaderOf(header.epoch());
            if (!own && (leader.isEmpty() || !leader.get().equals(header.senderUid()))) {
                return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                        header.senderUid() + " is not the leader of a term this pod joined");
            }
            disk.exposed().deliver(header.epoch(), (FastWriteFrame.Exposed) body);
            return new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
        });
    }
}
