// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The {@code s3} store kind, as configuration (M8.4).
 *
 * <p>⚠️ **NO ENDPOINT IS CONTACTED HERE AND THAT IS WHY THIS IS T0.** What is
 * asserted is which settings a kind needs and which it refuses — a predicate
 * over a record, decided before a single request. The behaviour of the backend
 * against a real endpoint is M8.2's and M8.22's, against MinIO.
 *
 * <p>⚠️ **BOTH DIRECTIONS, AND THE SECOND IS THE ONE WORTH HAVING.** A missing
 * bucket fails loudly the first time anything is written. A bucket configured
 * against {@code local-fs} does not fail at all: the node starts, writes to a
 * directory, and the operator's evidence that their data is in that bucket is
 * the config file they wrote.
 */
class StoreKindS3Test {

    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path dir;

    private static StoreConfig s3(Optional<String> endpoint, Optional<String> region,
            Optional<String> bucket) {
        return new StoreConfig("s3", Optional.empty(), endpoint, region, bucket, true);
    }

    @Test
    void anS3StoreOPENSFromEndpointRegionAndBucket() throws Exception {
        try (BinStore store = StoreFactory.open(s3(Optional.of("http://localhost:9000"),
                Optional.of("us-east-1"), Optional.of("bin-test")))) {
            assertThat(store).isNotNull();
        }
    }

    @Test
    void anAWSStoreNEEDSNoEndpointBecauseTheREGIONNamesOne() throws Exception {
        // ⚠️ AN ENDPOINT WRITTEN OUT BY HAND FOR AWS pins a deployment to one
        // region's hostname, so absent must be legal rather than merely
        // tolerated -- and `S3Settings` takes a null for it.
        try (BinStore store = StoreFactory.open(s3(Optional.empty(),
                Optional.of("eu-west-1"), Optional.of("bin-test")))) {
            assertThat(store).isNotNull();
        }
    }

    @Test
    void anS3StoreWithNOREGIONIsREFUSEDByNameBecauseSigV4SignsOverIt() {
        assertThatThrownBy(() -> StoreFactory.open(s3(Optional.of("http://localhost:9000"),
                Optional.empty(), Optional.of("bin-test"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.region");
    }

    @Test
    void anS3StoreWithNOBUCKETIsREFUSEDByNameRatherThanDefaulted() {
        // ⚠️ THE FAILURE A DEFAULT WOULD BUY: a node that starts, acks, and
        // writes into a bucket nobody configured.
        assertThatThrownBy(() -> StoreFactory.open(s3(Optional.of("http://localhost:9000"),
                Optional.of("us-east-1"), Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.bucket");
    }

    @Test
    void aROOTGivenToS3IsREFUSEDBecauseItHasNoFilesystem() {
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("s3", Optional.of("/var/lib/x"),
                Optional.of("http://localhost:9000"), Optional.of("us-east-1"),
                Optional.of("bin-test"), true)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("/var/lib/x");
    }

    @Test
    void anS3SettingGivenToAKindThatTalksToNOEndpointIsREFUSED() throws Exception {
        // ⚠️ THE SILENT DIRECTION. Each of these parses, constructs and would
        // be DROPPED by a factory that only checked for what it needs -- and an
        // operator who wrote `store.bucket` believes their data is in it.
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("memory", Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.of("bin-test"), false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.bucket");
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("memory", Optional.empty(),
                Optional.of("http://localhost:9000"), Optional.empty(), Optional.empty(), false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.endpoint");
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("memory", Optional.empty(),
                Optional.empty(), Optional.of("us-east-1"), Optional.empty(), false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.region");
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("memory", Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), true)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.path-style");

        // ⚠️ AND THE KIND STILL OPENS WITHOUT THEM, so the four cases above are
        // about the settings and not about the kind having become unusable.
        assertThatCode(() -> StoreFactory.open(new StoreConfig("memory", Optional.empty())).close())
                .doesNotThrowAnyException();
    }

    @Test
    void anS3SettingGivenToLOCALFSIsREFUSEDToo() throws Exception {
        // ⚠️ **`local-fs` IS THE KIND AN OPERATOR ACTUALLY LEAVES A STRAY
        // `store.bucket` ON**, because it is what a half-finished migration
        // from `s3` looks like. Review MEASURED the gap: with every case above
        // driven through `memory`, deleting the refusal from the `local-fs`
        // branch left the suite green -- and that branch is the one where a
        // node starts, acks, and writes to a directory while the config file
        // says a bucket.
        Path root = dir.resolve("data");
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("local-fs",
                Optional.of(root.toString()), Optional.empty(), Optional.empty(),
                Optional.of("bin-test"), false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.bucket");
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("local-fs",
                Optional.of(root.toString()), Optional.of("http://localhost:9000"),
                Optional.empty(), Optional.empty(), false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.endpoint");
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("local-fs",
                Optional.of(root.toString()), Optional.empty(), Optional.of("us-east-1"),
                Optional.empty(), false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.region");
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("local-fs",
                Optional.of(root.toString()), Optional.empty(), Optional.empty(),
                Optional.empty(), true)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store.path-style");

        // ⚠️ AND THE KIND STILL OPENS WITHOUT THEM.
        assertThatCode(() -> StoreFactory.open(
                new StoreConfig("local-fs", Optional.of(root.toString()))).close())
                .doesNotThrowAnyException();
    }

    @Test
    void theUNKNOWNKindMessageNAMESTheKindsThatExistNow() {
        assertThatThrownBy(() -> StoreFactory.open(new StoreConfig("gcs", Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gcs")
                .hasMessageContaining("s3");
    }
}
