// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.client.StreamFraming;
import binjava.format.Checkpoint;
import binjava.format.IndexRegistration;
import binjava.format.RetainedFloor;
import binjava.format.RunKey;
import binjava.sequencer.Checkpoints;
import io.helidon.webclient.api.WebClient;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The composition root serves the floor from the STORE's checkpoint (M8.6,
 * ADR-0056).
 *
 * <p>⚠️ **REVIEW MEASURED THIS UNPINNED**: handing {@code FrontDoor}
 * {@code RetainedFloors.unknown()} instead of {@code Assembly.floors()} left
 * every server test green, and nothing in any module referenced
 * {@code Checkpoints.newest}, the prefix it reads under, or the epoch guard. So
 * `SubscriptionService` could be correct, the transport correct, the client correct, and
 * the node serve a floor nobody ever knew.
 *
 * <p>⚠️ **THE CHECKPOINT IS PUT WHERE THE READER LOOKS, BY THE READER's OWN
 * KEY** ({@code Checkpoints.newestKey}), so this pins the wiring rather than a
 * copy of the key grammar. A real checkpoint writer would put the same bytes
 * there on its own cadence; waiting for it would make this a test of the
 * cadence.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FloorWiringTest {

    private static final String PREFIX = "bins/cluster-a";

    @TempDir
    Path dir;

    private static String indexUuid(UUID uuid) {
        var buffer = java.nio.ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    @Test
    void aNODEServesTheFLOORItsCurrentTermsCheckpointRecords() throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod1",
                "trust.domain=cluster-a",
                "store.prefix=" + PREFIX,
                "store.kind=memory",
                "endpoint=http://pod1:8080",
                "http.port=0",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                "").getBytes(StandardCharsets.UTF_8));

        UUID index = UUID.randomUUID();
        RunKey stream = new RunKey(index, 0);
        try (IngesterNode node = Main.run(file.toString())) {
            node.assembly().catalog().register(new IndexRegistration(
                    indexUuid(index), "logs", List.of(), 4, 4, 1, 1));
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + node.port()).build();
            assertThat(client.post("/logs/_bulk").queryParam("partition", "0")
                    .submit("{\"index\":{\"_id\":\"d\",\"_version\":1}}\n{\"n\":1}\n")
                    .status().code())
                    .as("the premise: a commit, so this node knows the term's epoch")
                    .isEqualTo(202);

            long epoch = node.assembly().heldTerm().epoch();
            Checkpoint checkpoint = new Checkpoint(5,
                    Map.of(stream, new Checkpoint.StreamOffsets(9000, 777)), Map.of());
            node.assembly().store().put(Checkpoints.newestKey(PREFIX, epoch),
                    Body.ofBytes(checkpoint.encode()));

            byte[] body;
            try (var response = client.get("/sub/" + index + "/0")
                    .queryParam("wait", "1").queryParam("sub", "s-1")
                    .queryParam("floor", "1").request();
                    var in = response.inputStream()) {
                body = in.readAllBytes();
            }
            byte[] first = StreamFraming.readFrame(new ByteArrayInputStream(body));

            assertThat(first).as("a floor frame was sent at all").isNotNull();
            assertThat(RetainedFloor.decode(first))
                    .as("⚠️ THE STORE's CHECKPOINT, FOR THE TERM THIS NODE COMMITTED UNDER, "
                            + "UNDER THE CONFIGURED PREFIX -- read through the root, not handed in")
                    .isEqualTo(new RetainedFloor(stream, 777));
        }
    }
}
