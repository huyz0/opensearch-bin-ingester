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
 * The pod lookup's guards (M13.33c review round 1, R1, T1-T2): a name
 * Kubernetes could not give a pod is never asked, an answer about another
 * pod or with a blank or unreadable UID is no answer, and no token sends no
 * Authorization header.
 */
class KubernetesPodLookupGuardsTest {

    private WebServer server;
    private final List<String> asked = new CopyOnWriteArrayList<>();
    private final List<Boolean> authorized = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private PodGone lookup(int status, String body, Optional<String> token) {
        server = WebServer.builder().port(0).routing(HttpRouting.builder().get("/*",
                (req, res) -> {
                    asked.add(req.path().path());
                    authorized.add(req.headers().contains(HeaderNames.AUTHORIZATION));
                    res.status(Status.create(status)).send(body);
                })).build().start();
        return new KubernetesPodLookup("http://localhost:" + server.port(), "fast",
                () -> token, List.of());
    }

    private static Roster.Incarnation named(String name) {
        return new Roster.Incarnation(name, "uid-2", "az-b", "");
    }

    @Test
    void aNAMENoPodCanHaveIsNeverAsked() {
        PodGone gone = lookup(200, "{}", Optional.of("t"));

        for (String name : List.of("../../pods/other", "Ingester-2", "a/b", "a?b", "-a")) {
            assertThatThrownBy(() -> gone.gone(named(name))).as(name)
                    .isInstanceOf(IOException.class);
        }
        assertThat(asked).as("no request left the pod").isEmpty();
    }

    @Test
    void anANSWERAboutAnotherPodIsNoAnswer() {
        PodGone gone = lookup(200,
                "{\"metadata\":{\"name\":\"other\",\"uid\":\"uid-9\"}}", Optional.of("t"));

        assertThatThrownBy(() -> gone.gone(named("ingester-2"))).isInstanceOf(IOException.class);
    }

    @Test
    void aBLANKOrUnreadableUidIsNoAnswer() {
        PodGone blank = lookup(200,
                "{\"metadata\":{\"name\":\"ingester-2\",\"uid\":\"\"}}", Optional.of("t"));
        assertThatThrownBy(() -> blank.gone(named("ingester-2"))).isInstanceOf(IOException.class);
        server.stop();

        PodGone unreadable = lookup(200, "not json", Optional.of("t"));
        assertThatThrownBy(() -> unreadable.gone(named("ingester-2")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void a404IsAskedByThePodsOwnPathAndNoTokenSendsNoHeader() throws Exception {
        PodGone gone = lookup(404, "{}", Optional.empty());

        assertThat(gone.gone(named("ingester-2.x"))).isTrue();
        assertThat(asked).containsExactly("/api/v1/namespaces/fast/pods/ingester-2.x");
        assertThat(authorized).containsExactly(false);
    }

    @Test
    void aNAMESPACEKubernetesCouldNotHaveIsRefused() {
        assertThatThrownBy(() -> new KubernetesPodLookup("http://localhost:1", "../x",
                Optional::empty, List.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
