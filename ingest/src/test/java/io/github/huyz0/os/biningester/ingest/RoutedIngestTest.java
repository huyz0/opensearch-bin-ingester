// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Placing a record the producer did not place (M6.6, FR-13, FR-19).
 *
 * <p>⚠️ THE TWO REFUSALS HERE ARE OPPOSITE IN KIND, and conflating them is the
 * defect. An EXPLICIT partition outside the registered count is permanently
 * wrong — the producer must be told so it stops — while an UNREGISTERED index
 * is a race the producer cannot see, and refusing it is a 400 storm on every
 * plugin reconnect.
 */
class RoutedIngestTest {

    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs", "logs-000001", "metrics"));
    private static final String UUID_1 = "nVzgup36TLqWp7VBBREj1w";

    /**
     * Records what it was asked to append, and nothing else.
     *
     * <p>⚠️ SYNCHRONISED, AND THE RESULT COUNTS THIS CALL'S RECORDS RATHER THAN
     * EVERY CALL'S. Two concurrent routed writes is a case here (M6.6 round 1),
     * and a fixture returning a running total would have reported the right
     * number for the wrong reason while the ids lists raced.
     */
    private static final class RecordingIngest implements Ingest {
        private final List<String> indices = java.util.Collections.synchronizedList(
                new ArrayList<>());
        private final List<Integer> partitions = java.util.Collections.synchronizedList(
                new ArrayList<>());
        private final List<String> ids = java.util.Collections.synchronizedList(
                new ArrayList<>());
        private final java.util.Map<String, Integer> partitionOf =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) throws IOException {
            List<String> mine = new ArrayList<>();
            records.forEachRecord(r -> mine.add(r.id()));
            indices.add(index);
            partitions.add(partition);
            ids.addAll(mine);
            for (String id : mine) {
                partitionOf.put(id, partition);
            }
            return new AppendResult(mine.size(), 0L, mine.size() - 1L);
        }

        @Override
        public void close() {
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records, Runnable buffered) throws IOException {
            try { // the removed default's behaviour: buffered once the append returns (M12.2)
                return append(principal, index, partition, lane, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
                byte lane, RecordSource records, Runnable buffered) throws IOException {
            try { // the removed default's behaviour: buffered once the append returns (M12.2)
                return appendRouted(principal, indexOrAlias, routing, lane, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public String concreteIndex(String indexOrAlias) {
            return indexOrAlias; // no catalog in this double (M12.2)
        }
    }

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1), new byte[8]);
    }

    private static Ingest.RecordSource source(String... ids) {
        List<SegmentRecord> records = new ArrayList<>();
        for (String id : ids) {
            records.add(record(id));
        }
        return records::forEach;
    }

    private static RoutedIngest routed(RecordingIngest delegate, Duration timeout) {
        IndexCatalog catalog = new IndexCatalog();
        return new RoutedIngest(delegate, catalog,
                new PendingPool(Clock.systemUTC(), timeout, 1 << 20), timeout,
                Clock.systemUTC());
    }

    private static IndexRegistration index(String name, int shards, String... aliases) {
        return new IndexRegistration(UUID_1, name, List.of(aliases), shards, shards, 1, 1);
    }

    @Test
    void aROUTEDWriteLandsInThePartitionTheHASHNames() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofSeconds(5));
        IndexRegistration logs = index("logs-000001", 8, "logs");
        routed.register(logs);

        routed.appendRouted(PRINCIPAL, "logs", "tenant-a", source("doc-1"));

        assertThat(delegate.partitions)
                .as("the ingester computes it, and the producer never learns the shard count "
                        + "-- which is what keeps an endpoint and a credential out of every "
                        + "producer (ADR-0015)")
                .containsExactly(RoutingPartitioner.partitionFor(logs, "tenant-a"));
        assertThat(delegate.indices)
                .as("and the CONCRETE index name, never the alias: stream identity is "
                        + "(indexUUID, partition) per concrete index")
                .containsExactly("logs-000001");
    }

    @Test
    void anEXPLICITPartitionOutsideTheRegisteredCountIsREFUSED() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofSeconds(5));
        routed.register(index("logs-000001", 3, "logs"));

        assertThatThrownBy(() -> routed.append(PRINCIPAL, "logs", 99, source("doc-1")))
                .as("FR-13's defining clause, served for the first time: ?partition=99 on a "
                        + "3-shard index returned 202 before this and landed in a stream no "
                        + "shard polls, with nothing downstream to report it")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("3 shards");
        assertThat(delegate.ids)
                .as("and nothing was written")
                .isEmpty();
    }

    @Test
    void anEXPLICITPartitionINSIDETheCountStillWrites() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofSeconds(5));
        routed.register(index("logs-000001", 3, "logs"));

        routed.append(PRINCIPAL, "logs", 2, source("doc-1"));

        assertThat(delegate.partitions)
                .as("the last valid partition is IN range -- an off-by-one here refuses a "
                        + "third of a 3-shard index's writes")
                .containsExactly(2);
        assertThat(delegate.indices)
                .as("and an explicit write to an ALIAS lands in the same streams a routed one "
                        + "does, or the two modes write to different places for one index")
                .containsExactly("logs-000001");
    }

    @Test
    void anEXPLICITWriteToAnUNKNOWNIndexWAITSForItsShapeRatherThanPassingThrough()
            throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofMillis(50));

        assertThatThrownBy(() -> routed.append(PRINCIPAL, "metrics", 7, source("doc-1")))
                .as("an unknown index is not an invalid partition -- the producer may be "
                        + "ahead of the plugin -- so it waits, and is refused 503-shaped when "
                        + "the shape never comes (M10.30); passed through, the pod could name "
                        + "no stream for it and answered 500")
                .isInstanceOf(RegistrationTimeoutException.class);
        assertThat(delegate.partitions).isEmpty();
    }

    /**
     * A routed write to an unregistered index WAITS and is released by the
     * registration (M6.6, ADR-0015 § 3).
     */
    @Test
    void aROUTEDWriteToAnUNREGISTEREDIndexWAITSAndIsRELEASED() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        // ⚠️ THE POOL WAITS 60 s AND THE TEST 10 s (M11.15, H10; review R1): the
        // margin runs this way round so a registration that never WAKES the
        // write fails here, rather than the pool's own deadline releasing it --
        // after which the write re-resolves, finds the index, and succeeds.
        RoutedIngest routed = routed(delegate, Duration.ofSeconds(60));
        IndexRegistration logs = index("logs-000001", 8, "logs");

        CompletableFuture<AppendResult> write = CompletableFuture.supplyAsync(() -> {
            try {
                return routed.appendRouted(PRINCIPAL, "logs", "tenant-a", source("doc-1"));
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
        // ⚠️ SPUN ON, NOT SLEPT THROUGH: the point is that the record is
        // HELD, and a sleep would assert only that this test is slower than
        // the pool.
        // ⚠️ M10.12: SPUN UNTIL THE WRITE IS OBSERVED WAITING, and no longer
        // until it is done. The old exit condition -- done, or written --
        // never becomes true when the code is RIGHT, so the loop always burned
        // its full 10 s; the write's own registration wait is also 10 s, so
        // `register` below raced the write's deadline and, under full-suite
        // load, lost: RegistrationTimeoutException after 10.009 s, measured on
        // the M10 baseline. Waiting for `pendingBatches() == 1` asserts the
        // hold directly and leaves the whole 10 s for the release.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!write.isDone() && routed.pendingBatches() == 0
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(routed.pendingBatches())
                .as("PREMISE: the write is held in the pool, waiting")
                .isEqualTo(1);
        assertThat(delegate.ids)
                .as("PREMISE: nothing is written while the index is unknown -- a pool that "
                        + "let the write through would place it by a shard count nobody "
                        + "registered")
                .isEmpty();

        routed.register(logs);

        assertThat(write.get(10, TimeUnit.SECONDS).recordCount())
                .as("the registration releases the write -- the producer that started before "
                        + "the plugin connected is not punished for a race it cannot see")
                .isEqualTo(1);
        assertThat(delegate.partitions)
                .containsExactly(RoutingPartitioner.partitionFor(logs, "tenant-a"));
    }

    @Test
    void aROUTEDWriteThatWAITSTooLongIsREFUSEDRatherThanFolded() {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofMillis(50));

        long start = System.nanoTime();
        assertThatThrownBy(() ->
                routed.appendRouted(PRINCIPAL, "logs", "tenant-a", source("doc-1")))
                .as("ADR-0006's reject-never-fold: partition 0 would funnel the whole index "
                        + "into one shard while telling the producer it succeeded")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("was still not registered after")
                .as("and it is the TIMEOUT's refusal, not the pool-full one -- both are "
                        + "IllegalStateException and both said \"not registered\", so a "
                        + "wait that refused instantly for the wrong reason read as green")
                .hasMessageNotContaining("pending pool is full");
        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .as("and it WAITED: a routed write refused before its timeout could elapse "
                        + "returns 4xx to a producer whose plugin was about to register, "
                        + "which is the reconnect storm the pool exists to absorb")
                .isGreaterThanOrEqualTo(Duration.ofMillis(50));
        assertThat(delegate.ids)
                .as("and nothing reached the ingester")
                .isEmpty();
    }

    /**
     * The boundary itself: {@code partition == numShards} does not exist.
     *
     * <p>⚠️ NOTHING ELSE COVERS IT (M6.6 round 1). With only 99-on-3-shards and
     * 2-on-3-shards asserted, relaxing the guard to {@code partition >} left
     * both green -- and shard 3 of a 3-shard index is the partition an
     * off-by-one in a producer's own loop produces, so it is the likeliest
     * wrong value there is rather than an exotic one.
     */
    @Test
    void theFIRSTPartitionPASTTheEndIsREFUSEDToo() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofSeconds(5));
        routed.register(index("logs-000001", 3, "logs"));

        assertThatThrownBy(() -> routed.append(PRINCIPAL, "logs", 3, source("doc-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("3 shards");
        assertThat(delegate.ids).isEmpty();
    }

    /**
     * TWO routed writes waiting on the SAME unregistered index each keep their
     * own records and their own placement (M6.6 round 1's blocking finding).
     *
     * <p>⚠️ MEASURED ON THE INDEX-KEYED POOL: the first waiter to wake took
     * BOTH requests' records and wrote all of them at the partition ITS routing
     * value named, and the second returned a 5xx for a write that had already
     * happened -- which a retry then duplicates. Two producers writing to one
     * index during a plugin reconnect is the ordinary case, not an edge one.
     */
    @Test
    void TWOConcurrentRoutedWritesKeepTheirOWNRecordsAndTheirOWNPartition() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofSeconds(60));
        IndexRegistration logs = index("logs-000001", 8, "logs");
        String left = "tenant-a";
        String right = null;
        for (char c = 'b'; c <= 'z'; c++) {
            String candidate = "tenant-" + c;
            if (RoutingPartitioner.partitionFor(logs, candidate)
                    != RoutingPartitioner.partitionFor(logs, left)) {
                right = candidate;
                break;
            }
        }
        assertThat(right)
                .as("PREMISE: the two routing values must name DIFFERENT partitions, or the "
                        + "case cannot tell a batch that kept its own records from one that "
                        + "took the other's")
                .isNotNull();
        String rightRouting = right;

        CompletableFuture<AppendResult> a = CompletableFuture.supplyAsync(
                () -> write(routed, left, "a-1", "a-2"));
        CompletableFuture<AppendResult> b = CompletableFuture.supplyAsync(
                () -> write(routed, rightRouting, "b-1"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (routed.pendingBatches() < 2 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(routed.pendingBatches())
                .as("PREMISE: both writes are IN FLIGHT before the registration arrives, or "
                        + "they ran one after the other and never raced")
                .isEqualTo(2);

        routed.register(logs);

        assertThat(a.get(10, TimeUnit.SECONDS).recordCount())
                .as("each request is told how many of ITS records were written -- the "
                        + "index-keyed pool reported 3 to whichever woke first and 0 to the "
                        + "other, for a write that had happened")
                .isEqualTo(2);
        assertThat(b.get(10, TimeUnit.SECONDS).recordCount()).isEqualTo(1);
        assertThat(delegate.partitionOf.get("a-1"))
                .as("and each request's records land at the partition ITS OWN routing value "
                        + "names, which is what makes the shard that polls them the shard "
                        + "OpenSearch will route the search to")
                .isEqualTo(RoutingPartitioner.partitionFor(logs, left));
        assertThat(delegate.partitionOf.get("a-2"))
                .isEqualTo(RoutingPartitioner.partitionFor(logs, left));
        assertThat(delegate.partitionOf.get("b-1"))
                .isEqualTo(RoutingPartitioner.partitionFor(logs, rightRouting));
        assertThat(delegate.ids)
                .as("and every record is written exactly ONCE")
                .containsExactlyInAnyOrder("a-1", "a-2", "b-1");
    }

    private static AppendResult write(RoutedIngest routed, String routing, String... ids) {
        try {
            return routed.appendRouted(PRINCIPAL, "logs", routing, source(ids));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Test
    void aROUTEDWriteToAPartitionedIndexIsREFUSEDImmediately() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofSeconds(10));
        routed.register(new IndexRegistration(UUID_1, "tenants", List.of(), 8, 8, 1, 4));

        assertThatThrownBy(() ->
                routed.appendRouted(PRINCIPAL, "tenants", "tenant-a", source("doc-1")))
                .as("a registered index whose shard depends on the _id is refused NOW, not "
                        + "held until a timeout: no registration that could arrive would make "
                        + "it placeable")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing_partition_size");
    }

    @Test
    void anINGESTERWithNoCatalogREFUSESARoutedWriteWithAMessageThatSaysWhy() {
        Ingest plain = new RecordingIngest();

        assertThatThrownBy(() ->
                plain.appendRouted(PRINCIPAL, "logs", "tenant-a", source("doc-1")))
                .as("a deployment with no catalog has no shard count for any index, so every "
                        + "routed write is unplaceable -- and answering with partition 0 is "
                        + "the fold ADR-0006 forbids -- AND AS A PLACEMENT REFUSAL, which the "
                        + "HTTP layer answers with 400; `IllegalStateException` was a 500, "
                        + "retried for ever (M6.19, closed by M8.32)")
                .isInstanceOf(PlacementRefusedException.class)
                .hasMessageContaining("explicit partition");
    }

    @Test
    void aROUTEDWriteOfNOTHINGIsREFUSEDRatherThanPooled() {
        RecordingIngest delegate = new RecordingIngest();
        RoutedIngest routed = routed(delegate, Duration.ofMillis(50));

        assertThatThrownBy(() -> routed.appendRouted(PRINCIPAL, "logs", "tenant-a", sink -> { }))
                .as("an empty write would otherwise wait out the whole timeout and then be "
                        + "refused for the wrong reason -- the index being unregistered rather "
                        + "than the request being empty")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aROUTEDWriteIsREFUSEDWhenTheINDEXSPoolIsFull() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        IndexCatalog catalog = new IndexCatalog();
        PendingPool pool = new PendingPool(Clock.systemUTC(), Duration.ofMillis(50), 64);
        RoutedIngest routed = new RoutedIngest(delegate, catalog, pool,
                Duration.ofMillis(50), Clock.systemUTC());

        assertThatThrownBy(() -> routed.appendRouted(PRINCIPAL, "logs", "tenant-a",
                source("doc-1", "doc-2", "doc-3", "doc-4", "doc-5", "doc-6")))
                .as("the pool is bounded and a flood for an unregistered index is refused "
                        + "rather than held -- and the refusal names the index, because an "
                        + "operator's next question is which one")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pending pool is full");
        assertThat(pool.bytesHeldFor("logs"))
                .as("and the half-offered request RELEASES what it took. Nothing calls "
                        + "expire() in production yet, so a batch abandoned charged is "
                        + "charged forever: the index's bound erodes by one flood per "
                        + "refusal until every routed write to it is refused")
                .isZero();
        assertThat(pool.waitingBatches())
                .as("and the batch is gone from its index's list, not merely emptied")
                .isZero();
    }

    @Test
    void theCATALOGIsREACHABLESoTheTransportCanRegisterIntoIt() {
        RoutedIngest routed = routed(new RecordingIngest(), Duration.ofSeconds(5));
        routed.register(index("logs-000001", 4, "logs"));

        assertThat(routed.catalog().resolve("logs").map(IndexRegistration::numShards))
                .as("M6.7's listener pushes into this, and a catalog it could not reach would "
                        + "make the registration path a second copy of the state")
                .isEqualTo(Optional.of(4));
    }

    /**
     * An index that registers UNPLACEABLE while a write waits refuses the
     * write AND releases its records (M6.6 round 3).
     *
     * <p>⚠️ MEASURED: the refusal came out of the WAIT, past the catch that
     * discarded the batch, and left {@code waitingBatches=1} and
     * {@code bytesHeldFor=42} -- permanently, because nothing calls
     * {@code expire()} in production. Every such request erodes the index's
     * bound until every routed write to it is refused for a pool full of
     * records nobody is waiting for.
     */
    @Test
    void anIndexThatRegistersUNPLACEABLEWhileAWriteWAITSReleasesItsRecords() throws Exception {
        RecordingIngest delegate = new RecordingIngest();
        IndexCatalog catalog = new IndexCatalog();
        PendingPool pool = new PendingPool(Clock.systemUTC(), Duration.ofSeconds(60), 1 << 20);
        RoutedIngest routed = new RoutedIngest(delegate, catalog, pool,
                Duration.ofSeconds(60), Clock.systemUTC());

        CompletableFuture<AppendResult> write = CompletableFuture.supplyAsync(
                () -> write(routed, "tenant-a", "doc-1"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (routed.pendingBatches() < 1 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(routed.pendingBatches())
                .as("PREMISE: the write is WAITING, or the registration below arrives before "
                        + "the batch is ever opened and the case measures nothing")
                .isEqualTo(1);

        routed.register(new IndexRegistration(UUID_1, "logs", List.of(), 8, 8, 1, 4));

        assertThatThrownBy(() -> write.get(10, TimeUnit.SECONDS))
                .as("the producer is refused: no registration that could arrive makes an "
                        + "index whose shard depends on the document _id placeable")
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing_partition_size");
        assertThat(pool.waitingBatches())
                .as("and the batch is RELEASED. Nothing calls expire() in production, so a "
                        + "batch left charged is charged forever and the index's bound erodes "
                        + "by one request per refusal")
                .isZero();
        assertThat(pool.bytesHeldFor("logs")).isZero();
    }

    @Test
    void aWAITThatDisagreesWithThePOOLSOwnTimeoutIsREFUSEDAtConstruction() {
        PendingPool pool = new PendingPool(Clock.systemUTC(), Duration.ofSeconds(5), 1 << 20);

        assertThatThrownBy(() -> new RoutedIngest(new RecordingIngest(), new IndexCatalog(),
                pool, Duration.ofSeconds(30), Clock.systemUTC()))
                .as("they are the two halves of one bound: a wait longer than the pool's "
                        + "refuses with a duration that decided nothing, and a shorter one "
                        + "refuses records the pool is still holding")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
