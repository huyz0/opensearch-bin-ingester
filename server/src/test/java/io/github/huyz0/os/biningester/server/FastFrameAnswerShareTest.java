// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.EpochFence;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A fast frame answered is counted under the share of the kind it answers
 * (M13.66): a COMMIT's answer as data, a JOIN's as control.
 */
class FastFrameAnswerShareTest {

    @Test
    void aCOMMITsAnswerIsCountedAsData() throws Exception {
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        FastFrameRouter router = FastPeer.router("uid-l",
                EpochFence.start(new MemoryBinStore(), "lease", Optional.empty()), crossAz,
                new PeerZones());
        router.handle(FastWriteFrame.KIND_COMMIT, (header, body) -> new FastFrame.Refused(
                FastFrame.Reason.BACKPRESSURE, Optional.empty(), "held"));

        router.answer(FastFrame.encode(4, "uid-w", "uid-l", new FastWriteFrame.Commit(
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 1),
                List.of(new FastWriteFrame.CommitRun(new RunKey(new UUID(1, 1), 0),
                        List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                new byte[] {1})))))));

        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_DATA)).isPositive();
        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL)).isZero();
    }
}
