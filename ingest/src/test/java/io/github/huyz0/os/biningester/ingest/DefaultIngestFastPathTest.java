// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A {@code wal=true} index's append takes the fast path (M13.29b; ADR-0081
 * §2): sent through its term's leader and answered at the offsets assigned
 * once EXPOSED, buffering nothing -- or, when the leader takes no fast write,
 * the same records the default way. A {@code wal=false} index never reaches it.
 */
class DefaultIngestFastPathTest {

    /** A fast path answering every append at offset 10, or NOT_FAST, recording what it got. */
    private static final class Fake implements FastPath {
        final List<RunKey> streams = new ArrayList<>();
        final List<List<SegmentRecord>> got = new ArrayList<>();
        boolean notFast;

        @Override
        public Optional<AppendResult> append(RunKey stream, List<SegmentRecord> records) {
            streams.add(stream);
            got.add(List.copyOf(records));
            return notFast ? Optional.empty()
                    : Optional.of(new AppendResult(records.size(), 10, 10 + records.size() - 1));
        }
    }

    @Test
    void aFASTIndexsAppendIsAnsweredByTheFastPathAndBuffersNothing() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Fake fake = new Fake();
        AtomicInteger buffered = new AtomicInteger();
        try (DefaultIngest ingest = IngestTestSupport.ingest(store)) {
            ingest.routeFast("logs"::equals, fake);

            AppendResult got = ingest.append(IngestTestSupport.PRINCIPAL, "logs", 2, (byte) 0,
                    IngestTestSupport.docs(3)::forEach, buffered::incrementAndGet);

            assertThat(got).isEqualTo(new AppendResult(3, 10, 12));
            assertThat(fake.streams).containsExactly(new RunKey(IngestTestSupport.LOGS, 2));
            assertThat(fake.got).singleElement().satisfies(records -> assertThat(records)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyElementsOf(IngestTestSupport.docs(3)));
            assertThat(ingest.pendingAppends()).as("nothing buffered").isZero();
            assertThat(buffered).as("its caller's hold released once").hasValue(1);
        }
    }

    /** A producer's body: read once, as a request's is, and refused a second read. */
    private static Ingest.RecordSource once(List<SegmentRecord> records) {
        AtomicInteger reads = new AtomicInteger();
        return sink -> {
            if (reads.incrementAndGet() > 1) {
                throw new IOException("a request body is read once");
            }
            records.forEach(sink);
        };
    }

    @Test
    void aNOTFASTAnswerSendsTheSameRecordsTheDefaultWay() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Fake fake = new Fake();
        fake.notFast = true;
        try (DefaultIngest ingest = IngestTestSupport.ingest(store)) {
            ingest.routeFast("logs"::equals, fake);
            java.util.concurrent.CompletableFuture<AppendResult> appended =
                    java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                        try {
                            return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                                    once(IngestTestSupport.docs(3)));
                        } catch (IOException e) {
                            throw new java.util.concurrent.CompletionException(e);
                        }
                    });
            IngestTestSupport.awaitPending(ingest, 1);
            ingest.flushNow();

            AppendResult got = appended.get(10, java.util.concurrent.TimeUnit.SECONDS);

            assertThat(fake.got).singleElement().satisfies(records -> assertThat(records)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyElementsOf(IngestTestSupport.docs(3)));
            assertThat(got.recordCount()).as("the same three, committed the default way")
                    .isEqualTo(3);
            assertThat(got.firstOffset()).isZero();
        }
    }

    @Test
    void anEMPTYFastAppendIsRefusedAsTheDefaultPathRefusesIt() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Fake fake = new Fake();
        try (DefaultIngest ingest = IngestTestSupport.ingest(store)) {
            ingest.routeFast("logs"::equals, fake);

            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                    sink -> { })).isInstanceOf(IllegalArgumentException.class);
            assertThat(fake.streams).as("no COMMIT of no runs").isEmpty();
        }
    }

    @Test
    void aDEFAULTIndexNeverReachesTheFastPath() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Fake fake = new Fake();
        try (DefaultIngest ingest = IngestTestSupport.ingest(store)) {
            ingest.routeFast("audit"::equals, fake);

            IngestTestSupport.appendOnce(ingest, "logs", 0, 2);

            assertThat(fake.streams).isEmpty();
        }
    }

    @Test
    void aFASTPathThatFailsRefusesTheAppendAndReleasesNothing() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        AtomicInteger buffered = new AtomicInteger();
        try (DefaultIngest ingest = IngestTestSupport.ingest(store)) {
            ingest.routeFast("logs"::equals, (stream, records) -> {
                throw new IOException("the leader did not answer");
            });

            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                    (byte) 0, IngestTestSupport.docs(1)::forEach, buffered::incrementAndGet))
                    .isInstanceOf(IOException.class);
            assertThat(buffered).as("the caller releases what it holds").hasValue(0);
            assertThat(ingest.pendingAppends()).isZero();
        }
    }
}
