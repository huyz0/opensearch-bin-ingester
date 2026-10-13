// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The inbox drain's fast write (M13.34m; ADR-0081 §8): each redirected run's
 * records read back from the intent's uploaded segment and appended through
 * the leader's own fast path -- exactly that run, never another's.
 */
class FastDrainRedirectTest {

    private static final String KEY = "bins/c/data/deferred.bseg";
    private static final RunKey FAST = new RunKey(new UUID(1, 1), 0);
    private static final RunKey PLAIN = new RunKey(new UUID(2, 2), 0);

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(), new byte[] {1});
    }

    private static MemoryBinStore withSegment() throws IOException {
        SegmentWriter writer = new SegmentWriter();
        writer.add(FAST, record("f0"), 0);
        writer.add(FAST, record("f1"), 0);
        writer.add(PLAIN, record("p0"), 0);
        byte[] segment = writer.toByteArray(0);
        MemoryBinStore store = new MemoryBinStore();
        store.put(KEY, Body.ofBytes(segment));
        return store;
    }

    private static CommitRequest intent() {
        return new CommitRequest("podb", "i1", 0, KEY, Map.of(FAST, 2, PLAIN, 1));
    }

    @Test
    void aREDIRECTEDRunsRecordsAreAppendedFastExactlyThatRun() throws Exception {
        List<RunKey> streams = new ArrayList<>();
        List<List<String>> ids = new ArrayList<>();
        FastPath path = (stream, records) -> {
            streams.add(stream);
            ids.add(records.stream().map(SegmentRecord::id).toList());
            return Optional.of(new AppendResult(records.size(), 0, records.size() - 1));
        };

        new FastDrainRedirect(withSegment(), () -> path).write(intent(), Set.of(FAST));

        assertThat(streams).containsExactly(FAST);
        assertThat(ids).containsExactly(List.of("f0", "f1"));
    }

    @Test
    void aFASTPathThatAnswersNotFastFailsTheWriteAndTheIntentStays() throws Exception {
        FastDrainRedirect redirect = new FastDrainRedirect(withSegment(),
                () -> FastPath.NONE);

        assertThatThrownBy(() -> redirect.write(intent(), Set.of(FAST)))
                .isInstanceOf(IOException.class).hasMessageContaining("NOT_FAST");
    }
}
