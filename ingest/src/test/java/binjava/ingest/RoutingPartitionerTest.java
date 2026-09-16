// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.IndexRegistration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The properties of routed placement that hold without OpenSearch on the
 * classpath (M6.4, FR-19).
 *
 * <p>⚠️ THE PARITY CASE IS THE ONE THAT MATTERS AND IT IS NOT HERE.
 * {@code RoutingPartitionerParityTest} in {@code plugin} compares every answer
 * against {@code OperationRouting} itself, because {@code ingest} must not
 * depend on OpenSearch — the line {@code check-module} holds. What THIS file
 * adds is the shape of the answer: in range, deterministic, refusing what it
 * cannot compute.
 */
class RoutingPartitionerTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    private static IndexRegistration index(int numShards) {
        return IndexRegistration.unsplit(UUID, "logs", numShards);
    }

    @Test
    void everyPartitionIsINRANGEForEveryShardCount() {
        for (int shards : new int[] {1, 2, 3, 5, 16, 1024}) {
            IndexRegistration index = index(shards);
            for (int i = 0; i < 2_000; i++) {
                int partition = RoutingPartitioner.partitionFor(index, "tenant-" + i);

                assertThat(partition)
                        .as("a partition outside [0, %d) is a stream nothing polls -- and a "
                                + "NEGATIVE one is what `%%` gives for the ~half of routing "
                                + "values whose hash is negative", shards)
                        .isBetween(0, shards - 1);
            }
        }
    }

    @Test
    void theSAMERoutingValueAlwaysLandsInTheSAMEPartition() {
        IndexRegistration index = index(8);

        assertThat(RoutingPartitioner.partitionFor(index, "tenant-a"))
                .as("placement is a pure function of the value and the index shape -- anything "
                        + "else spreads one tenant's records across shards, where a routed "
                        + "query finds only the fraction that happened to land right")
                .isEqualTo(RoutingPartitioner.partitionFor(index, "tenant-a"));
    }

    @Test
    void aSPLITIndexScalesTheHashDOWNByItsFactor() {
        IndexRegistration split = new IndexRegistration(UUID, "big", List.of(), 8, 32, 4, 1);

        for (int i = 0; i < 2_000; i++) {
            assertThat(RoutingPartitioner.partitionFor(split, "tenant-" + i))
                    .as("a split index hashes over its ORIGINAL 32 and divides by 4 -- without "
                            + "the division the answer ranges over [0, 32) for an index with 8 "
                            + "shards, and three quarters of the records land in streams "
                            + "nothing polls")
                    .isBetween(0, 7);
        }
    }

    @Test
    void aROUTEDPlacementOnAPartitionedIndexIsREFUSED() {
        IndexRegistration partitioned = new IndexRegistration(UUID, "tenants", List.of(),
                8, 8, 1, 4);

        assertThatThrownBy(() -> RoutingPartitioner.partitionFor(partitioned, "tenant-a"))
                .as("the shard depends on the document _id too (OperationRouting:587), so "
                        + "there is no answer to give -- and this guard is the second of two, "
                        + "the catalog refusing at resolve time, because a placement reached "
                        + "any other way must not quietly hash")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing_partition_size");
    }

    @Test
    void anEMPTYRoutingValueIsPlacedRatherThanRefused() {
        assertThat(RoutingPartitioner.partitionFor(index(4), ""))
                .as("an empty routing value is a legal one -- murmur3 of zero bytes is a "
                        + "number like any other, and refusing it here would reject a write "
                        + "OpenSearch itself would route")
                .isBetween(0, 3);
    }

    /**
     * The refusal boundary is {@code > 1}, not {@code > 2} (M6.4, round 1's
     * test major).
     *
     * <p>⚠️ THE SAME DEFECT M6.3's ROUND ONE FOUND IN {@code IndexCatalog}, at
     * the second of the two guards: every refusal case used 4 and every
     * accepting case used 1, so moving the boundary left both files' suites
     * green while {@code routing_partition_size = 2} — legal from three shards
     * up — was hashed without the {@code _id} offset it needs.
     */
    @Test
    void aPartitionSizeOfTWOIsAlreadyREFUSED() {
        IndexRegistration partitioned = new IndexRegistration(UUID, "tenants-2", List.of(),
                3, 3, 1, 2);

        assertThatThrownBy(() -> RoutingPartitioner.partitionFor(partitioned, "tenant-a"))
                .as("two is the smallest partition size whose shard depends on the _id, and "
                        + "it is the one a boundary defect leaves hashed by the routing value "
                        + "alone")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing_partition_size");
    }

    // ⚠️ THERE IS NO NON-ASCII CASE HERE, AND THAT IS DELIBERATE. Swapping the
    // two bytes of every char still yields an in-range, deterministic
    // partition, so a case in this file could assert nothing the others do not
    // -- and asserting a specific number would compare this code against
    // itself. `RoutingPartitionerParityTest` includes non-ASCII routing values
    // and compares each against OpenSearch's own answer, which is the only
    // thing that can tell the two byte orders apart.
}
