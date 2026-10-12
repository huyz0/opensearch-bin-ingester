// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.ReplicaHolder;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * A node's handler of the frames a holder answers -- REPLICA (M13.28a): its
 * fast journal holds the copy, under the pod's joined terms and holder gate.
 * ⚠️ A POD WITHOUT A FAST JOURNAL HOLDS NO COPY, so refuses {@code NOT_FAST}
 * and the leader places the copy elsewhere.
 */
final class FastHolderFrames {

    private FastHolderFrames() {
    }

    static void register(BiConsumer<Integer, FastFrameRouter.Handler> handle, FastDisk disk) {
        Optional<ReplicaHolder> holder = disk.journal().map(
                journal -> new ReplicaHolder(journal, disk.joined(), disk.gate()));
        handle.accept(FastWriteFrame.KIND_REPLICA, (header, body) -> {
            if (holder.isEmpty()) {
                return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                        "this pod holds no fast journal");
            }
            return holder.get().answer(header, (FastWriteFrame.Replica) body);
        });
    }
}
