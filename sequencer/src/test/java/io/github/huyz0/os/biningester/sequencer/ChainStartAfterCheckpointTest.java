// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * A chain a checkpoint collapsed says it starts AT THE CHECKPOINT, not at
 * (0, 0) (M8.38, NFR-3).
 *
 * <p>⚠️ **(0, 0) SAYS "NOTHING HAS EVER BEEN COLLECTED"** about a chain that a
 * checkpoint has already collapsed. A replay that stopped at a checkpoint at
 * the chain's tail reads zero deltas, and an empty chain reports its boundary,
 * which {@code reset()} had cleared to (0, 0).
 */
class ChainStartAfterCheckpointTest {

    private static final String PREFIX = "chain-start/";
    private static final RunKey STREAM =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0);
    private static final CheckpointWriter.Ticker FROZEN = () -> new CountDownLatch(1).await();

    private static LeaseManager manager(MemoryBinStore store, String pod) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, pod, "",
                Duration.ofSeconds(10), Duration.ofSeconds(3)), Clock.systemUTC());
    }

    /** A term of two commits with K = 1, so its newest checkpoint is at its tail. */
    private static long checkpointedTerm(MemoryBinStore store) throws Exception {
        LocalSequencer term = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                LocalSequencer.sleepFor(Duration.ofSeconds(3)), 1, FROZEN).orElseThrow();
        term.commit(new CommitRequest("poda", "i1", 0, "seg/0", Map.of(STREAM, 1)));
        term.commit(new CommitRequest("poda", "i1", 1, "seg/1", Map.of(STREAM, 1)));
        long epoch = term.epoch();
        term.close();
        return epoch;
    }

    @Test
    void aRECOVERYThatStoppedAtATailCheckpointStartsTHERE() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            long epoch = checkpointedTerm(store);
            long checkpointed = Checkpoints.newest(store, PREFIX, epoch).orElseThrow().sequence();

            CommitLog log = new CommitLog(store, PREFIX, epoch);
            log.recover();
            ChainMemory.Snapshot chain = log.chain().snapshot();

            assertThat(chain.deltas()).as("the premise: the replay read nothing").isEmpty();
            assertThat(chain.firstEpoch())
                    .as("⚠️ THE CHECKPOINT's CHAIN, not epoch 0").isEqualTo(epoch);
            assertThat(chain.firstSequence())
                    .as("⚠️ AND ITS POSITION: what the checkpoint collapsed is behind it")
                    .isEqualTo(checkpointed);
        }
    }

    @Test
    void aSUCCESSORWhoseInheritedWalkStoppedAtACheckpointStartsTHERE() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            long predecessor = checkpointedTerm(store);
            long checkpointed =
                    Checkpoints.newest(store, PREFIX, predecessor).orElseThrow().sequence();

            LocalSequencer successor = LocalSequencer.start(store, PREFIX,
                    manager(store, "podb"), 8, LocalSequencer.sleepFor(Duration.ofSeconds(3)),
                    1_000, FROZEN).orElseThrow();
            try {
                ChainMemory.Snapshot chain = successor.chain().snapshot();

                assertThat(chain.deltas()).as("the premise: nothing crossed").isEmpty();
                assertThat(chain.firstEpoch()).isEqualTo(predecessor);
                assertThat(chain.firstSequence()).isEqualTo(checkpointed);
            } finally {
                successor.close();
            }
        }
    }
}
