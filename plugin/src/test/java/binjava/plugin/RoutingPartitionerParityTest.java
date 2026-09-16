// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.IndexRegistration;
import binjava.ingest.RoutingPartitioner;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

/**
 * Our placement is OpenSearch's placement (M6.4, FR-19, M6 criterion 2).
 *
 * <p>⚠️ THE EXPECTED VALUE COMES FROM OPENSEARCH'S OWN CLASSES, NEVER FROM A
 * RE-DERIVATION. ADR-0015 says so in as many words — "must be tested against
 * {@code OperationRouting}, not reimplemented from memory" — and the reason is
 * that a test computing the expectation from our own formula agrees with itself
 * and is wrong in exactly the way that cannot be noticed: OpenSearch never sets
 * {@code _routing} on an ingested document, so a routed search for a
 * misplaced record returns nothing, with no exception, no failed gate and no
 * metric.
 *
 * <p>⚠️ IT LIVES IN {@code plugin} BECAUSE THIS IS THE ONE MODULE WHOSE TESTS
 * SEE BOTH — {@code testImplementation(ingest)} and
 * {@code testImplementation(opensearch)}. {@code ingest} must not depend on
 * OpenSearch at all, which is the line {@code check-module} holds and the
 * reason the hash is transcribed there rather than imported.
 *
 * <p>⚠️ FOUR SHARD COUNTS AND A SPLIT INDEX, because the two halves of the
 * expression fail independently: {@code floorMod} vs {@code %} shows up at any
 * shard count for a negative hash, and dropping {@code / routingFactor} shows
 * up only where {@code routingNumShards != numShards}.
 */
class RoutingPartitionerParityTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    /** ⚠️ Fixed seed: a parity failure must be reproducible from the message. */
    private static List<String> routingValues(int n) {
        Random random = new Random(20260917L);
        List<String> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            // ⚠️ MIXED SHAPES AND LENGTHS -- murmur3's tail handles 1, 2 and 3
            // trailing bytes differently, and a transcription error in one of
            // them is invisible to values whose length is a multiple of four.
            values.add(switch (i % 4) {
                case 0 -> "tenant-" + random.nextInt(1_000_000);
                case 1 -> "user@example.com/" + random.nextLong();
                case 2 -> Long.toHexString(random.nextLong());
                default -> "é中" + random.nextInt(1000);
            });
        }
        return values;
    }

    private static IndexMetadata metadata(String name, int numShards, int routingNumShards) {
        Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUID);
        IndexMetadata.Builder builder = IndexMetadata.builder(name).settings(settings);
        if (routingNumShards != numShards) {
            // ⚠️ ON THE BUILDER, not in the settings map: `getRoutingNumShards`
            // reads the builder's own field, and a settings entry alone left it
            // at `numShards` -- the split fixture then silently tested an
            // UNSPLIT index, which the first run reported as "routingNumShards
            // 8" for an index named split-8-from-32.
            builder.setRoutingNumShards(routingNumShards);
        }
        return builder.build();
    }

    private static IndexRegistration registrationOf(IndexMetadata metadata) {
        return new IndexRegistration(metadata.getIndexUUID(), metadata.getIndex().getName(),
                List.of(), metadata.getNumberOfShards(), metadata.getRoutingNumShards(),
                metadata.getRoutingFactor(), 1);
    }

    private static void assertParity(IndexMetadata metadata, int values) {
        IndexRegistration registration = registrationOf(metadata);
        Index index = metadata.getIndex();
        for (String routing : routingValues(values)) {
            int ours = RoutingPartitioner.partitionFor(registration, routing);
            int theirs = OperationRouting.generateShardId(metadata, null, routing);

            assertThat(ours)
                    .as("routing %s on %s (%d shards, routingNumShards %d, factor %d): "
                            + "OpenSearch would search shard %d. A record we place elsewhere "
                            + "is committed, durable, readable -- and invisible to every "
                            + "routed query, with nothing to say so", routing,
                            index.getName(), metadata.getNumberOfShards(),
                            metadata.getRoutingNumShards(), metadata.getRoutingFactor(), theirs)
                    .isEqualTo(theirs);
        }
    }

    @Test
    void aTHOUSANDRoutingValuesLandWhereOPENSEARCHWouldLookAtFOURShardCounts() {
        // ⚠️ THE PREMISE IS PART OF THE CASE: about half of all routing values
        // hash NEGATIVE, and `%` sends every one of them to shard 0 or throws.
        // A value set that happened to hash positive throughout would let the
        // `floorMod` mutation survive every assertion below, so the count is
        // asserted rather than assumed.
        long negatives = routingValues(1_000).stream()
                .filter(v -> Murmur3HashFunction.hash(v) < 0)
                .count();
        assertThat(negatives)
                .as("PREMISE: without negative hashes, floorMod and %% agree and these cases "
                        + "prove nothing about the most likely defect in the file")
                .isGreaterThan(100);

        for (int shards : new int[] {1, 3, 5, 16}) {
            assertParity(metadata("logs-" + shards, shards, shards), 1_000);
        }
    }

    /**
     * A SPLIT index, where {@code routingNumShards != numShards} (M6.4).
     *
     * <p>⚠️ THE HALF THAT AN UNSPLIT FIXTURE CANNOT SEE. With
     * {@code routingFactor == 1} the division is invisible, so dropping it
     * passes every case above and misplaces every record in every index that
     * has ever been split.
     */
    @Test
    void aSPLITIndexAgreesToo() {
        assertParity(metadata("split-8-from-32", 8, 32), 1_000);
        assertParity(metadata("split-2-from-16", 2, 16), 1_000);
    }

    /**
     * ⚠️ THE HASH ITSELF, against {@code Murmur3HashFunction}, so a parity
     * failure says whether the defect is in the hash or in the arithmetic
     * around it. Without this, a broken transcription and a broken
     * {@code floorMod} present identically.
     */
    @Test
    void theHASHIsOPENSEARCHSHash() {
        for (String routing : routingValues(1_000)) {
            assertThat(RoutingPartitioner.murmur3(routing))
                    .as("murmur3_x86_32, seed 0, over the UTF-16 chars as two bytes each -- "
                            + "NOT the UTF-8 bytes, which is what this case caught on its "
                            + "first run and what nothing else would have: %s", routing)
                    .isEqualTo(Murmur3HashFunction.hash(routing));
        }
    }


    /**
     * The hash of ARBITRARY bytes, including ODD lengths (M6.4, round 1's test
     * major).
     *
     * <p>⚠️ THE STRING PATH CANNOT REACH A 1- OR 3-BYTE TAIL.
     * {@code routing.length() * 2} is always even, so {@code data.length & 3}
     * is only ever 0 or 2 — and the fixture's claim to exercise all three tail
     * lengths was false. Measured: replacing the three-byte tail branch with a
     * constant left every parity case green.
     *
     * <p>⚠️ THE ORACLE IS LUCENE'S OWN {@code StringHelper}, which is what
     * {@code Murmur3HashFunction} delegates to for a byte range. Comparing
     * against our own output would be the re-derivation ADR-0015 forbids.
     */
    @Test
    void theHASHOfARBITRARYBytesMatchesLUCENEAtEVERYTailLength() {
        Random random = new Random(20260918L);
        for (int length = 0; length <= 37; length++) {
            byte[] data = new byte[length];
            random.nextBytes(data);

            assertThat(RoutingPartitioner.murmur3(data))
                    .as("length %d leaves a %d-byte tail -- the 1- and 3-byte cases are "
                            + "unreachable through a String, and a transcription error in "
                            + "either places every record of every index whose routing values "
                            + "have that length on the wrong shard", length, length & 3)
                    .isEqualTo(org.apache.lucene.util.StringHelper.murmurhash3_x86_32(
                            data, 0, length, 0));
        }
    }
}
