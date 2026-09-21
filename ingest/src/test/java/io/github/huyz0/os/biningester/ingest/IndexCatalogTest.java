// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What the ingester knows about an index, and what it refuses to guess (M6.3,
 * FR-16, FR-19).
 *
 * <p>⚠️ EVERY CASE HERE IS ABOUT A QUERY THAT MISSES. The catalog decides which
 * concrete index a write lands in and with what shard count; get either wrong
 * and the record is committed, durable, readable — and on a shard no search
 * will look at, because OpenSearch never sets {@code _routing} on an ingested
 * document. There is no exception to catch and no gate that goes red.
 */
class IndexCatalogTest {

    private static final String UUID_1 = "nVzgup36TLqWp7VBBREj1w";
    private static final String UUID_2 = "TLqWp7VBBREj1wnVzgup36";

    private static IndexRegistration index(String uuid, String name, int shards,
            String... aliases) {
        return new IndexRegistration(uuid, name, List.of(aliases), shards, shards, 1, 1);
    }

    @Test
    void anALIASResolvesToItsCONCRETEIndex() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(index(UUID_1, "logs-000001", 3, "logs", "logs-write"));

        assertThat(catalog.resolve("logs").orElseThrow().indexName())
                .as("a producer writes to the alias and the record lands in the index it "
                        + "currently names -- stream identity is (indexUUID, partition), per "
                        + "CONCRETE index, so the alias is resolved here and never stored")
                .isEqualTo("logs-000001");
        assertThat(catalog.resolve("logs-write").orElseThrow().indexName())
                .isEqualTo("logs-000001");
        assertThat(catalog.resolve("logs-000001").orElseThrow().numShards()).isEqualTo(3);
    }

    @Test
    void anUNKNOWNIndexIsEMPTYRatherThanADefault() {
        IndexCatalog catalog = new IndexCatalog();

        assertThat(catalog.resolve("never-registered"))
                .as("empty means WAIT FOR A REGISTRATION, which is what the pending pool is "
                        + "built on -- a default shard count here would place records by a "
                        + "number nobody configured and return 202 for each one")
                .isEmpty();
    }

    @Test
    void aREREGISTRATIONReplacesRatherThanAccumulates() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(index(UUID_1, "logs-000001", 3, "logs"));
        catalog.register(index(UUID_1, "logs-000001", 3));

        assertThat(catalog.size())
                .as("the plugin pushes on connect and on every relevant change, so the same "
                        + "index arrives repeatedly")
                .isEqualTo(1);
        assertThat(catalog.resolve("logs"))
                .as("an index that stopped claiming an alias stops answering for it -- "
                        + "otherwise a rollover leaves BOTH indices claiming `logs` and the "
                        + "winner is whichever registration arrived last")
                .isEmpty();
    }

    /**
     * A rollover moves the alias and leaves the previous index alone (M6.3,
     * ADR-0015 § 4).
     *
     * <p>⚠️ THE NEW INDEX REGISTERS FIRST, which is the order a cluster-state
     * change produces, and the OLD one's later push must not take the alias
     * back. Removing an alias the previous registration listed is therefore
     * conditional on it still pointing THERE — an unconditional remove leaves
     * `logs` unresolvable and every write to it pending until it times out.
     */
    @Test
    void aROLLOVERMovesTheAliasAndLeavesTheOldIndexReadable() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(index(UUID_1, "logs-000001", 3, "logs"));

        catalog.register(index(UUID_2, "logs-000002", 8, "logs"));
        // ⚠️ THE OLD INDEX PUSHES AGAIN, having lost the alias -- the shape a
        // rollover actually produces, since both indices are in the same
        // cluster state and both nodes push.
        catalog.register(index(UUID_1, "logs-000001", 3));

        assertThat(catalog.resolve("logs").orElseThrow().indexName())
                .as("the alias points at the NEW index, and the old index's own push must not "
                        + "take it back")
                .isEqualTo("logs-000002");
        assertThat(catalog.resolve("logs-000001").orElseThrow().numShards())
                .as("and the previous index is still addressable by name, with ITS shard count "
                        + "-- records already committed keep their partitions there and "
                        + "nothing is repartitioned")
                .isEqualTo(3);
    }

    @Test
    void aCONCRETEIndexNameWinsOverAnALIASOfTheSameSpelling() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(index(UUID_1, "shadow", 2));
        catalog.register(index(UUID_2, "other", 8, "shadow"));

        assertThat(catalog.resolve("shadow").orElseThrow().indexName())
                .as("OpenSearch forbids an alias with the name of an existing index, so the "
                        + "two can only collide across a delete-and-recreate -- and a producer "
                        + "writing to a name that IS an index must never be sent elsewhere")
                .isEqualTo("shadow");
    }

    /**
     * A ROUTED write to a tenant-partitioned index is refused; an explicit one
     * is not (M6.3, SPI § 4b).
     */
    @Test
    void aROUTEDWriteToAPartitionedIndexIsREFUSEDAndAnEXPLICITOneIsNot() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration(UUID_1, "tenants", List.of("t"), 8, 8, 1, 4));

        assertThatThrownBy(() -> catalog.resolveForRouting("tenants"))
                .as("with routing_partition_size > 1 the shard depends on the document _id as "
                        + "well as the routing value (OperationRouting:587), which the "
                        + "ingester does not have -- hashing anyway is a document on the wrong "
                        + "shard that no query finds")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing_partition_size");
        assertThat(catalog.resolve("tenants").orElseThrow().numShards())
                .as("the INDEX is not refused: an explicit-partition write needs no hash and "
                        + "is unaffected, so refusing the registration would take away a mode "
                        + "that works")
                .isEqualTo(8);
        assertThatThrownBy(() -> catalog.resolveForRouting("t"))
                .as("and the refusal follows the alias, or a producer routes around it by "
                        + "writing to the alias instead -- naming the setting either way, "
                        + "since a producer told only 'refused' cannot act on it")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing_partition_size");
    }

    @Test
    void aROUTEDWriteToAnORDINARYIndexIsNOTRefused() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(index(UUID_1, "logs-000001", 3, "logs"));

        assertThat(catalog.resolveForRouting("logs").orElseThrow().numShards())
                .as("routing_partition_size of 1 is the ordinary index, and refusing it would "
                        + "make os_routing unusable everywhere")
                .isEqualTo(3);
    }

    @Test
    void aROUTEDWriteToAnUNKNOWNIndexIsEMPTYRatherThanRefused() {
        IndexCatalog catalog = new IndexCatalog();

        assertThat(catalog.resolveForRouting("never-registered"))
                .as("unknown is not the same as unroutable: the first waits for a "
                        + "registration, the second is a 400, and collapsing them turns every "
                        + "plugin reconnect into a refusal storm")
                .isEmpty();
    }

    @Test
    void aSPLITIndexKeepsItsROUTINGShardCountAndFactor() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration(UUID_1, "big", List.of(), 8, 32, 4, 1));

        IndexRegistration found = catalog.resolve("big").orElseThrow();

        assertThat(found.routingNumShards())
                .as("a split index hashes over its ORIGINAL shard count and scales down -- "
                        + "carrying numShards alone would misplace every record in it")
                .isEqualTo(32);
        assertThat(found.routingFactor()).isEqualTo(4);
    }

    /**
     * The refusal boundary is {@code > 1}, not {@code > 2} (M6.3, round 1's
     * test major).
     *
     * <p>⚠️ MEASURED: every refusal case used 4 and every accepting case used
     * 1, so moving the boundary to {@code > 2} left all nine green.
     * {@code routing_partition_size = 2} is legal from three shards up, and
     * under that mutation such an index is routed by murmur3 alone — the
     * silent wrong-shard commit this class exists to prevent, at the one value
     * nothing looked at.
     */
    @Test
    void aPartitionSizeOfTWOIsAlreadyUNROUTABLE() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration(UUID_1, "tenants-2", List.of(), 3, 3, 1, 2));

        assertThatThrownBy(() -> catalog.resolveForRouting("tenants-2"))
                .as("the shard depends on the _id as soon as the partition size exceeds ONE; "
                        + "two is the smallest value that does, and it is the one a boundary "
                        + "defect leaves routed by the hash alone")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing_partition_size");
    }

    /**
     * An ordinary re-push never makes an alias briefly unresolvable (M6.3,
     * round 1's major).
     *
     * <p>⚠️ MEASURED BEFORE THE FIX: {@code register} removed every alias the
     * previous registration listed and put back the ones the new one claims,
     * so a reader between the two saw EMPTY for an alias nothing had changed
     * about — on every push of every index, since the plugin pushes on connect
     * and on each relevant cluster-state change. A write landing in that window
     * goes to the pending pool and is refused when it times out.
     *
     * <p>⚠️ THE CASE READS FROM ANOTHER THREAD WHILE PUSHES RUN, because a
     * single-threaded case cannot see a window that opens and closes inside one
     * call.
     */
    @Test
    void anORDINARYRePushNeverMakesTheAliasUNRESOLVABLE() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        IndexRegistration same = index(UUID_1, "logs-000001", 3, "logs");
        catalog.register(same);
        java.util.concurrent.atomic.AtomicInteger misses =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean running =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread reader = new Thread(() -> {
            while (running.get()) {
                if (catalog.resolve("logs").isEmpty()) {
                    misses.incrementAndGet();
                }
            }
        });
        reader.start();

        for (int i = 0; i < 20_000; i++) {
            catalog.register(same);
        }
        running.set(false);
        reader.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(10));

        assertThat(misses.get())
                .as("20,000 re-pushes of an UNCHANGED registration, and an alias that resolves "
                        + "throughout -- every miss here is a producer's write sent to the "
                        + "pending pool for a change that did not happen")
                .isZero();
    }
}
