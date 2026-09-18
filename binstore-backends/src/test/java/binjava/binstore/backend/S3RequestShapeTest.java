// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import binjava.binstore.Body;
import binjava.binstore.Version;
import java.nio.charset.StandardCharsets;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Error;

/**
 * What the S3 backend asks of the endpoint, and what it does with the answer
 * (M8.2, cost rule R9, NFR-2).
 *
 * <p>⚠️ **T0 AGAINST A STUB CLIENT, AND THE TIER IS NOT LAZINESS.** Neither of
 * these two properties is observable at T3: a real endpoint answers a batch
 * delete of nothing without complaint, so "no request at all" looks identical
 * to "one empty request", and MinIO does not produce a per-key failure inside a
 * 200 on demand. Review measured both — removing the empty-batch guard and
 * ignoring the per-key errors each left the whole MinIO suite green.
 *
 * <p>⚠️ **THE STUB IS THE SDK's OWN INTERFACE.** Every method of
 * {@code S3Client} has a default that refuses, so overriding one method says
 * exactly which request this case is about, and any other request the backend
 * made would fail loudly rather than being silently tolerated.
 */
class S3RequestShapeTest {

    /** Records every batch delete and answers whatever it was told to. */
    private static final class RecordingClient implements S3Client {
        private final List<DeleteObjectsRequest> deletes = new ArrayList<>();
        private final List<S3Error> errors;

        RecordingClient(List<S3Error> errors) {
            this.errors = errors;
        }

        @Override
        public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest request) {
            deletes.add(request);
            return DeleteObjectsResponse.builder().errors(errors).build();
        }

        /** Whatever a list should answer, where a case asks for one. */
        private ListObjectsV2Response listAnswer;

        @Override
        public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
            return listAnswer;
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {
        }
    }

    /**
     * A client whose write LANDS and whose response is lost on the way back,
     * with a {@code stat} that then finds the object THIS write left.
     */
    private static final class LostResponseClient implements S3Client {
        @Override
        public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
            throw SdkClientException.create("Unable to execute HTTP request: connection reset");
        }

        @Override
        public HeadObjectResponse headObject(HeadObjectRequest request) {
            return HeadObjectResponse.builder().contentLength(5L).eTag("\"ours\"").build();
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {
        }
    }

    @Test
    void aCONDITIONALWriteWhoseRESPONSEWasLostIsNOTReportedAsALostRace() throws Exception {
        // ⚠️ THE WRITE LANDED AND THE CONNECTION DROPPED. A `stat` then finds
        // the object -- OUR OWN -- and a store that read that as "someone else
        // got there" would tell a writer that WON that it lost. The commit log
        // cannot tell the difference (ADR-0002): the writer re-reads, sees a
        // record at N it believes is another pod's, and appends its batch again
        // at N+1. Review MEASURED this arriving from the broader rank, which
        // answered empty for both verbs here.
        byte[] raw = "hello".getBytes(StandardCharsets.UTF_8);
        Body good = new Body(raw.length, () -> new java.io.ByteArrayInputStream(raw));

        try (var store = new S3BinStore(new LostResponseClient(), "bucket")) {
            assertThatThrownBy(() -> store.putIfAbsent("chain/000001", good))
                    .as("⚠️ AN ERROR, NOT AN EMPTY. The caller must decide, and this store "
                            + "cannot: only a body THIS PROJECT refused is provably uncommitted")
                    .isInstanceOf(IOException.class);
            assertThatThrownBy(() -> store.putIfMatch("lease/term", good, new Version("\"old\"")))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void deletingNOTHINGIssuesNORequestAtAll() throws Exception {
        // ⚠️ NFR-2 IS "ZERO IDLE OBJECT-STORE REQUESTS", and a GC pass with an
        // empty batch is exactly the idle case: one request per pass per prefix,
        // for ever, on a deployment doing nothing. A real endpoint answers an
        // empty delete happily, which is why no T3 case can see this.
        RecordingClient client = new RecordingClient(List.of());
        try (var store = new S3BinStore(client, "bucket")) {
            store.delete(List.of());
        }

        assertThat(client.deletes)
                .as("⚠️ NOT ONE REQUEST. A backend that sent an empty batch would bill a "
                        + "request per pass on an idle deployment, and every cost assertion "
                        + "above it counts calls rather than requests")
                .isEmpty();
    }

    @Test
    void deletingKEYSIssuesONERequestForTheWholeBatch() throws Exception {
        RecordingClient client = new RecordingClient(List.of());
        try (var store = new S3BinStore(client, "bucket")) {
            store.delete(List.of("a", "b", "c"));
        }

        assertThat(client.deletes)
                .as("⚠️ ONE REQUEST, NOT ONE PER KEY. GC deletes in thousands, and a request "
                        + "per key is the shape cost.md exists to refuse")
                .hasSize(1);
        assertThat(client.deletes.get(0).delete().objects()).hasSize(3);
    }

    @Test
    void aBATCHOverTheServiceLimitIsSPLITRatherThanRefused() throws Exception {
        RecordingClient client = new RecordingClient(List.of());
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < S3BinStore.MAX_DELETE_BATCH + 1; i++) {
            keys.add("k" + i);
        }
        try (var store = new S3BinStore(client, "bucket")) {
            store.delete(keys);
        }

        assertThat(client.deletes)
                .as("⚠️ 1,000 IS THE ENDPOINT'S OWN LIMIT and a caller does not know it. A "
                        + "backend that passed 1,001 straight through would fail a GC pass "
                        + "whose size is set by the retention window")
                .hasSize(2);
        assertThat(client.deletes.get(0).delete().objects()).hasSize(S3BinStore.MAX_DELETE_BATCH);
        assertThat(client.deletes.get(1).delete().objects()).hasSize(1);
    }

    @Test
    void aTRUNCATEDPageWithNOObjectsIsREFUSEDRatherThanEndingTheWalk() {
        // ⚠️ AN EMPTY CURSOR HERE WOULD STOP A RECOVERY WALK OR A GC PASS
        // SILENTLY, mid-prefix: everything after that point looks as though it
        // does not exist, and a retention floor moved past it deletes objects
        // nothing has read. No endpoint in the test path produces this answer,
        // which is why it is asserted against a stub rather than against MinIO.
        RecordingClient client = new RecordingClient(List.of());
        client.listAnswer = ListObjectsV2Response.builder()
                .contents(List.of()).isTruncated(true).build();

        assertThatThrownBy(() -> {
            try (var store = new S3BinStore(client, "bucket")) {
                store.list("walk/", null, 10);
            }
        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("no key to resume from");
    }

    @Test
    void aSTOREThatBUILTItsCredentialProviderCLOSESIt() throws Exception {
        // ⚠️ `DefaultCredentialsProvider` HOLDS A BACKGROUND REFRESH, and the
        // SDK does not close a provider it was handed -- so a pod that stopped
        // its store would go on refreshing credentials for the life of the JVM.
        // Review measured the whole close path removable with both suites green.
        var closed = new java.util.concurrent.atomic.AtomicInteger();
        var store = new S3BinStore(new RecordingClient(List.of()), "bucket",
                closed::incrementAndGet, S3Settings.S3_MAX_KEY_BYTES);

        store.close();

        assertThat(closed.get())
                .as("⚠️ CLOSED EXACTLY ONCE, with the client. ⚠️ A CALLER'S OWN PROVIDER IS "
                        + "NOT CLOSED and there is deliberately no case for that here: the "
                        + "only honest assertion would be over a store built by the public "
                        + "`open`, which needs an endpoint -- so it is stated in the javadoc "
                        + "and left unpinned rather than pinned by something that constrains "
                        + "nothing")
                .isEqualTo(1);
    }

    @Test
    void aPERKeyFailureINSIDEA200IsNOTSuccess() {
        // ⚠️ A BATCH DELETE ANSWERS 200 WITH THE FAILURES IN THE BODY, and the
        // SDK does not throw for them. A backend that ignored that body would
        // report a successful GC pass having deleted nothing -- and the
        // retention loop would then move its floor past objects that are still
        // there and still billed.
        RecordingClient client = new RecordingClient(List.of(
                S3Error.builder().key("b").code("AccessDenied").build()));

        assertThatThrownBy(() -> {
            try (var store = new S3BinStore(client, "bucket")) {
                store.delete(List.of("a", "b"));
            }
        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining("delete refused 1 of 2")
                .hasMessageContaining("b")
                .hasMessageContaining("AccessDenied");
    }
}
