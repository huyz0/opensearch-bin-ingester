// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The relay's read: one GET when healthy, a stat only when the GET fails (M10.20a). */
class DeltaReadsTest {

    private static final String PREFIX = "bins/cluster-a";

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, "bins/cluster-a/data/s.bseg",
                List.of(new RunCommit(new RunKey(UUID.randomUUID(), 0), 2, 0)));
    }

    @Test
    void aWrittenDeltaCostsOneGetAndNoStat() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(new LogKeys(PREFIX, 3).keyFor(7), Body.ofBytes(delta(7).encode()));
        long gets = store.counts().gets();
        long stats = store.counts().stats();

        assertThat(DeltaReads.read(store, PREFIX, 3, 7)).map(CommitDelta::sequence).contains(7L);
        assertThat(store.counts().gets() - gets).isEqualTo(1);
        assertThat(store.counts().stats() - stats).as("healthy: no stat").isZero();
    }

    @Test
    void aMissingDeltaIsEmptyAfterTheFailedGetsStat() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());

        assertThat(DeltaReads.read(store, PREFIX, 3, 7)).isEmpty();
        assertThat(store.counts().stats()).as("the stat that tells missing from failing")
                .isEqualTo(1);
    }

    @Test
    void aStoreThatFailsOnAnExistingDeltaThrowsSoTheRelayRetries() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        backing.put(new LogKeys(PREFIX, 3).keyFor(7), Body.ofBytes(delta(7).encode()));
        ForwardingBinStore failingGets = new ForwardingBinStore(backing) {
            @Override
            public InputStream get(String key) throws IOException {
                throw new IOException("the store answered 503");
            }
        };

        assertThatThrownBy(() -> DeltaReads.read(failingGets, PREFIX, 3, 7))
                .isInstanceOf(IOException.class).hasMessageContaining("503");
    }
}
