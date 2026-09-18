// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import binjava.binstore.Body;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * The first real object-store backend, against a real endpoint (M8.2, NFR-8,
 * ADR-0052, ADR-0054).
 *
 * <p>⚠️ **T3, AND THE TIER IS THE POINT.** Two backends already pass the
 * conformance suite and neither has a network: {@code MemoryBinStore} is a map
 * in the caller's own heap and {@code LocalFsBinStore} is a directory. What
 * cannot be bought at T1 is the thing this milestone exists to break — a
 * conditional write decided by a remote service, a range served over HTTP, a
 * batch delete that is one request, and the ERROR SHAPES those produce.
 *
 * <p>⚠️ **MinIO IS NOT S3** (testing.md rule 19a). Where the protocol is
 * ambiguous the two differ, so what these cases assert is the shape this
 * project's own code depends on, and {@code VERIFIED.md} records which criteria
 * rest on MinIO alone.
 *
 * <p>⚠️ **SKIPPED, NOT FAILED, WITH NO DOCKER.** `./gradlew test` starts no
 * container by design (build.md); this suite is `integrationTest` and a
 * developer without a daemon gets a skip that says so rather than a red suite
 * they cannot act on.
 */
class S3BinStoreIntegrationTest {

    private static final AwsCredentialsProvider CREDENTIALS = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(MinioFixture.ACCESS_KEY, MinioFixture.SECRET_KEY));

    private static String endpoint;

    private S3BinStore store;
    private String bucket;

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        endpoint = MinioFixture.endpoint();
    }

    @BeforeEach
    void freshBucket() {
        bucket = "m82-" + UUID.randomUUID();
        try (S3Client admin = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(CREDENTIALS)
                .build()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
        store = S3BinStore.open(new S3Settings(endpoint, "us-east-1", bucket, true), CREDENTIALS);
    }

    @AfterEach
    void closeStore() throws IOException {
        if (store != null) {
            store.close();
        }
    }

    @Test
    void aPUTAndAGETRoundTripTheBYTES() throws Exception {
        Version version = store.put("seg/one", bytes("hello object store"));

        assertThat(version.token())
                .as("⚠️ A VERSION TOKEN IS NEVER EMPTY, and the conditional writes this "
                        + "project is built on compare it")
                .isNotBlank();
        try (var in = store.get("seg/one")) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .isEqualTo("hello object store");
        }
    }

    @Test
    void aRANGEGetReturnsONLYThoseBytes() throws Exception {
        store.put("seg/range", bytes("0123456789"));

        try (var in = store.getRange("seg/range", 2, 5)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .as("⚠️ INCLUSIVE AT BOTH ENDS, which is the SPI's contract and HTTP's -- "
                            + "an exclusive end is off by one on every fetch and the bytes "
                            + "still parse, because a record boundary is inside the segment")
                    .isEqualTo("2345");
        }
    }

    @Test
    void aSTATSaysTheSIZEAndTheVERSIONAndAnABSENTKeyIsEMPTY() throws Exception {
        Version written = store.put("seg/stat", bytes("twelve bytes"));

        Optional<ObjectStat> found = store.stat("seg/stat");
        assertThat(found).isPresent();
        assertThat(found.get().size()).isEqualTo(12);
        assertThat(found.get().version())
                .as("⚠️ THE SAME TOKEN THE WRITE ANSWERED. A stat that reported a different "
                        + "one would make every conditional write fail against a store nobody "
                        + "else was touching")
                .isEqualTo(written);

        assertThat(store.stat("seg/not-there"))
                .as("⚠️ EMPTY, NOT AN EXCEPTION: absence is the ordinary answer here")
                .isEmpty();
    }

    @Test
    void aGETOfAnABSENTKeyIsAnIOExceptionAndNOTAVendorType() {
        // ⚠️ THE SDK'S OWN `NoSuchKeyException` IS A `RuntimeException`. If it
        // escaped, every caller in this tree -- all of which catch IOException
        // because the SPI declares it -- would be unwound instead of retrying,
        // and the one place it matters is the commit-log retry loop (ADR-0002).
        assertThatThrownBy(() -> store.get("seg/not-there"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("seg/not-there");
    }

    @Test
    void aPUTIFABSENTThatLosesTheRaceIsEMPTYRatherThanAnError() throws Exception {
        assertThat(store.putIfAbsent("chain/000001", bytes("first"))).isPresent();

        assertThat(store.putIfAbsent("chain/000001", bytes("second")))
                .as("⚠️ EMPTY MEANS ALREADY EXISTS, and it is a NORMAL outcome: the commit "
                        + "log is a chain of these (ADR-0002), and a loser re-reads and "
                        + "retries at the next sequence number. A throw here would turn every "
                        + "contended commit into an error")
                .isEmpty();
        try (var in = store.get("chain/000001")) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .as("⚠️ AND THE LOSER WROTE NOTHING")
                    .isEqualTo("first");
        }
    }

    @Test
    void aPUTIFABSENTOnATAKENKeyIsEMPTYEvenWhenTheBODYIsBAD() throws Exception {
        // ⚠️ THE CONTRACT RANKS THE TWO FAILURES, and the conformance suite has
        // the `putIfMatch` half of this but not the `putIfAbsent` half -- review
        // MEASURED this arm removable with the whole suite green. Unranked, a
        // truncated commit record written at a key that was already taken is
        // reported as an ERROR rather than as a lost race, and the commit log's
        // retry loop (ADR-0002) unwinds where it should have re-read.
        assertThat(store.putIfAbsent("chain/000002", bytes("first"))).isPresent();
        Body lying = new Body(999, () -> new java.io.ByteArrayInputStream(
                "short".getBytes(StandardCharsets.UTF_8)));

        assertThat(store.putIfAbsent("chain/000002", lying))
                .as("⚠️ ALREADY EXISTS OUTRANKS A BAD BODY, and it is an ordinary empty")
                .isEmpty();
        try (var in = store.get("chain/000002")) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("first");
        }
    }

    @Test
    void aPUTIFABSENTWithABADBodyOnAFREEKeyTHROWSRatherThanReportingALoss() {
        // ⚠️ THE OTHER SIDE OF THE RANK, and without it the fix above is
        // "answer empty whenever anything goes wrong": a writer whose body was
        // broken would be told it lost a race nobody else was in, and its record
        // would be missing from the chain with no error anywhere.
        Body lying = new Body(999, () -> new java.io.ByteArrayInputStream(
                "short".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> store.putIfAbsent("chain/never-written", lying))
                .isInstanceOf(IOException.class);
    }

    @Test
    void aPUTIFMATCHAgainstAMOVEDVersionIsEMPTYAndDoesNotWrite() throws Exception {
        Version first = store.put("lease/term", bytes("pod-a"));
        store.put("lease/term", bytes("pod-b"));

        assertThat(store.putIfMatch("lease/term", bytes("pod-c"), first))
                .as("⚠️ EMPTY MEANS THE VERSION MOVED -- the losing side of a lease renewal "
                        + "(ADR-0008) re-reads and decides, it does not fail")
                .isEmpty();
        try (var in = store.get("lease/term")) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("pod-b");
        }
    }

    @Test
    void aPUTIFMATCHAgainstAnABSENTKeyTHROWSRatherThanBeingEmpty() {
        // ⚠️ NOT FOLDED INTO THE EMPTY CASE, and the SPI says so: there is no
        // version to have moved from. A lease renewal that found its own lease
        // GONE and read that as "somebody else has it" would wait out a TTL for
        // a term nobody holds.
        assertThatThrownBy(() -> store.putIfMatch("lease/never", bytes("pod-c"),
                new Version("\"whatever\"")))
                .isInstanceOf(IOException.class)
                // ⚠️ THE BRANCH'S OWN WORDING, because `IOException` plus the
                // key is also what the generic fallthrough produces -- review
                // measured that this case could not tell "there is nothing to
                // match" from "the endpoint is unreachable".
                .hasMessageContaining("there is no object to match against");
    }

    @Test
    void aLISTPageIsLEXICOGRAPHICAndRESUMESStrictlyAfterStartAfter() throws Exception {
        for (String suffix : List.of("a", "b", "c", "d")) {
            store.put("walk/" + suffix, bytes(suffix));
        }
        store.put("other/z", bytes("z"));

        ListPage all = store.list("walk/", null, 100);
        assertThat(all.objects().stream().map(ObjectStat::key))
                .as("⚠️ THE PREFIX BOUNDS IT AND THE ORDER IS LEXICOGRAPHIC, which is what "
                        + "recovery walks and what GC pages through")
                .containsExactly("walk/a", "walk/b", "walk/c", "walk/d");

        ListPage after = store.list("walk/", "walk/b", 100);
        assertThat(after.objects().stream().map(ObjectStat::key))
                .as("⚠️ STRICTLY AFTER: a resume that re-read its own cursor would deliver "
                        + "one object twice on every page boundary")
                .containsExactly("walk/c", "walk/d");
    }

    @Test
    void aTRUNCATEDListSaysWHEREToResumeAndOneCallIsONERequest() throws Exception {
        for (String suffix : List.of("a", "b", "c", "d", "e")) {
            store.put("page/" + suffix, bytes(suffix));
        }

        ListPage first = store.list("page/", null, 2);
        assertThat(first.objects()).hasSize(2);
        assertThat(first.nextStartAfter())
                .as("⚠️ ONE CALL IS ONE REQUEST (cost rule R9). A backend that paged "
                        + "internally would answer all five here, and the counting decorator "
                        + "would see one request where the bill has three")
                .contains("page/b");

        ListPage second = store.list("page/", first.nextStartAfter().orElseThrow(), 2);
        assertThat(second.objects().stream().map(ObjectStat::key)).containsExactly("page/c", "page/d");

        ListPage last = store.list("page/", second.nextStartAfter().orElseThrow(), 2);
        assertThat(last.objects().stream().map(ObjectStat::key)).containsExactly("page/e");
        assertThat(last.nextStartAfter())
                .as("⚠️ AND THE LAST PAGE SAYS SO, or a walk never terminates")
                .isEmpty();
    }

    @Test
    void aBATCHDeleteRemovesThemALLAndAnABSENTKeyIsNotAnError() throws Exception {
        store.put("gc/one", bytes("1"));
        store.put("gc/two", bytes("2"));

        store.delete(List.of("gc/one", "gc/two", "gc/never-existed"));

        assertThat(store.stat("gc/one")).isEmpty();
        assertThat(store.stat("gc/two")).isEmpty();
    }

    @Test
    void aMULTIPARTUploadASSEMBLESByPARTNumberAndNOTByArrival() throws Exception {
        // ⚠️ THE SPI SAYS PARTS MAY ARRIVE OUT OF ORDER, and review MEASURED
        // both halves of getting this wrong against this very endpoint: with
        // arrival-ordered parts, this legal upload was REFUSED (the size check
        // skipped the last-arrived part rather than the highest-numbered one),
        // and a re-uploaded part number was APPENDED rather than replaced --
        // S3 concatenated it twice and answered a silently doubled object that
        // still parses.
        byte[] big = new byte[5 * 1024 * 1024];
        java.util.Arrays.fill(big, (byte) 'x');

        try (var writer = store.multipart("big/out-of-order")) {
            writer.uploadPart(2, bytes("end"));
            writer.uploadPart(1, new Body(big.length,
                    () -> new java.io.ByteArrayInputStream(big)));
            writer.complete();
        }

        assertThat(store.stat("big/out-of-order").orElseThrow().size())
                .as("⚠️ ASSEMBLED 1 THEN 2, and the 3-byte part is legal because it is the "
                        + "HIGHEST-numbered one rather than the last to arrive")
                .isEqualTo(big.length + 3L);
        try (var in = store.getRange("big/out-of-order", big.length, big.length + 2L)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8))
                    .as("⚠️ AND THE ORDER IS THE PART NUMBERS', not the order they were sent")
                    .isEqualTo("end");
        }
    }

    @Test
    void aREUPLOADEDPartNumberREPLACESWhatWasStagedForIt() throws Exception {
        byte[] big = new byte[5 * 1024 * 1024];
        java.util.Arrays.fill(big, (byte) 'x');

        try (var writer = store.multipart("big/replaced")) {
            writer.uploadPart(1, new Body(big.length,
                    () -> new java.io.ByteArrayInputStream(big)));
            writer.uploadPart(1, new Body(big.length,
                    () -> new java.io.ByteArrayInputStream(big)));
            writer.uploadPart(2, bytes("end"));
            writer.complete();
        }

        assertThat(store.stat("big/replaced").orElseThrow().size())
                .as("⚠️ ONE COPY OF PART 1, NOT TWO. Review measured the appending version "
                        + "answering 10,485,763 bytes here -- a segment silently double the "
                        + "size it should be, which still parses, because a record boundary "
                        + "is inside it")
                .isEqualTo(big.length + 3L);
    }

    @Test
    void aSHORTNonFinalPartIsREFUSEDBeforeItIsPaidFor() throws Exception {
        // ⚠️ S3 REFUSES THIS AT COMPLETE, which is after every byte has been
        // sent and billed. Refusing in front of it turns a wasted upload into an
        // argument error -- and review measured the guard removable with the
        // whole T3 suite green, because no case had a short non-final part.
        try (var writer = store.multipart("big/short-part")) {
            writer.uploadPart(1, bytes("far too small to be a non-final part"));
            writer.uploadPart(2, bytes("end"));
            assertThatThrownBy(writer::complete)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("below the minimum non-final part size");
        }
    }

    @Test
    void aBODYThatYieldsFEWERBytesThanItDeclaredIsREFUSED() {
        // ⚠️ THE SPI's BODY CONTRACT AT THE S3 BOUNDARY. A stream that yields
        // less than it promised would otherwise be signed and sent as a SHORT
        // object -- and every reader afterwards parses it as truncated, which is
        // a data-loss path that reports success. Review measured
        // `checkedStream` swappable for the raw stream with the suite green.
        Body lying = new Body(100, () -> new java.io.ByteArrayInputStream(
                "only ten".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> store.put("seg/lying", lying))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> store.stat("seg/lying").orElseThrow())
                .as("⚠️ AND NOTHING WAS WRITTEN")
                .isInstanceOf(java.util.NoSuchElementException.class);
    }

    @Test
    void aMULTIPARTUploadOccupiesTheKeyONLYWhenItCompletes() throws Exception {
        byte[] part = new byte[5 * 1024 * 1024];
        java.util.Arrays.fill(part, (byte) 'x');

        try (var writer = store.multipart("big/object")) {
            writer.uploadPart(1, new Body(part.length,
                    () -> new java.io.ByteArrayInputStream(part)));
            assertThat(store.stat("big/object"))
                    .as("⚠️ NOTHING IS VISIBLE WHILE PARTS ARE STAGED, which the SPI promises "
                            + "and the writer path relies on: a reader that saw a half-written "
                            + "segment would read a truncated one")
                    .isEmpty();
            writer.uploadPart(2, new Body(3, () -> new java.io.ByteArrayInputStream(
                    "end".getBytes(StandardCharsets.UTF_8))));
            Version completed = writer.complete();
            assertThat(completed.token()).isNotBlank();
        }

        assertThat(store.stat("big/object").orElseThrow().size())
                .isEqualTo(part.length + 3L);
    }

    @Test
    void anABORTEDMultipartLeavesNOTHINGBehind() throws Exception {
        try (var writer = store.multipart("big/aborted")) {
            writer.uploadPart(1, bytes("a part that goes nowhere"));
            writer.abort();
        }

        assertThat(store.stat("big/aborted"))
                .as("⚠️ AND THE PARTS ARE NOT LEFT STAGED EITHER: an abandoned upload is "
                        + "billed storage nobody can see, for ever, unless a lifecycle rule "
                        + "nobody configured removes it")
                .isEmpty();
    }

    @Test
    void theCAPABILITIESAreWhatThisBackendACTUALLYDoes() {
        var capabilities = store.capabilities();

        assertThat(capabilities.conditionalWrites())
                .as("⚠️ TRUE, AND MEASURED BY THE CASES ABOVE RATHER THAN CLAIMED. A backend "
                        + "that advertised this and did not have it would take the sequencer "
                        + "lease and the ordinal registry with it (ADR-0008)")
                .isTrue();
        assertThat(capabilities.batchDelete()).isTrue();
        assertThat(capabilities.presignedUrls())
                .as("⚠️ FALSE FOR NOW, AND HONESTLY SO: presigning is M8.18, with M5.42's "
                        + "obligation that a signing failure's own message carries neither a "
                        + "URL nor a credential. Advertising it before it is implemented is "
                        + "how `direct` gets enabled against a backend that cannot sign")
                .isFalse();
        assertThat(capabilities.minPartSize())
                .as("⚠️ 5 MiB, S3's own minimum for a non-final part")
                .isEqualTo(5L * 1024 * 1024);
    }

    private static Body bytes(String text) {
        byte[] raw = text.getBytes(StandardCharsets.UTF_8);
        return new Body(raw.length, () -> new java.io.ByteArrayInputStream(raw));
    }
}
