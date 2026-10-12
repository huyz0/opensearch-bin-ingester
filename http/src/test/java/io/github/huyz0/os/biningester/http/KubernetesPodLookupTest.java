// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.PodGone;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The pod-UID GONE seam over the Kubernetes API (M13.33c; ADR-0081 §5 step
 * 4): an incarnation is gone only when its pod is deleted -- no pod by its
 * name, or one by its name with another UID. Anything else is not an answer,
 * and the caller waits rather than counting a live copy lost.
 */
class KubernetesPodLookupTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://ingester-2:1");

    private WebServer server;
    private final List<String> asked = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private PodGone lookup(int status, String body) {
        server = WebServer.builder().port(0).routing(HttpRouting.builder().get(
                "/api/v1/namespaces/{ns}/pods/{name}", (req, res) -> {
                    asked.add(req.path().path());
                    authorizations.add(req.headers().first(HeaderNames.AUTHORIZATION)
                            .orElse(""));
                    res.status(Status.create(status)).send(body);
                })).build().start();
        return new KubernetesPodLookup("http://localhost:" + server.port(), "fast",
                () -> Optional.of("t0ken"), List.of());
    }

    private static String pod(String uid) {
        return "{\"kind\":\"Pod\",\"metadata\":{\"name\":\"ingester-2\",\"uid\":\"" + uid
                + "\"}}";
    }

    @Test
    void aPODStillCarryingTheUidIsNotGone() throws Exception {
        PodGone gone = lookup(200, pod("uid-2"));

        assertThat(gone.gone(POD)).isFalse();
        assertThat(asked).containsExactly("/api/v1/namespaces/fast/pods/ingester-2");
        assertThat(authorizations).containsExactly("Bearer t0ken");
    }

    @Test
    void noPODByItsNameIsGone() throws Exception {
        assertThat(lookup(404, "{\"kind\":\"Status\"}").gone(POD)).isTrue();
    }

    @Test
    void aPODByItsNameWithAnotherUidIsGone() throws Exception {
        // ⚠️ A StatefulSet replaced it: the incarnation that held copies is deleted.
        assertThat(lookup(200, pod("uid-9")).gone(POD)).isTrue();
    }

    @Test
    void anyOTHERAnswerIsNoAnswer() {
        PodGone gone = lookup(503, "busy");

        assertThatThrownBy(() -> gone.gone(POD)).isInstanceOf(IOException.class);
    }

    @Test
    void aPODWithoutAUidIsNoAnswer() {
        PodGone gone = lookup(200, "{\"kind\":\"Pod\",\"metadata\":{\"name\":\"ingester-2\"}}");

        assertThatThrownBy(() -> gone.gone(POD)).isInstanceOf(IOException.class);
    }

    @Test
    void outsideKUBERNETESNoPodIsEverGone() throws Exception {
        assertThat(PodGone.NEVER.gone(POD)).isFalse();
    }
}
