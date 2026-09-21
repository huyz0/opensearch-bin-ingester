// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.util.Objects;

/**
 * The two probes Kubernetes asks (M8.7, research 08 §7 step 1).
 *
 * <p>⚠️ **READINESS FAILS AND LIVENESS KEEPS PASSING.** They answer different
 * questions. Readiness is "send me work", and a draining node must say no to
 * it. Liveness is "am I still running", and a draining node must say yes, or
 * the kubelet kills it mid-flush. That kill is a {@code SIGKILL}, and it
 * skips the lease release this whole sequence exists to reach.
 */
public final class HealthService implements HttpService {

    public static final String READY_PATH = "/ready";
    public static final String LIVE_PATH = "/live";

    private final java.util.function.BooleanSupplier ready;

    public HealthService(DrainGate gate) {
        this(Objects.requireNonNull(gate, "gate")::ready);
    }

    /**
     * Readiness from any source, such as the drain gate AND the store's health
     * (M8.15).
     */
    public HealthService(java.util.function.BooleanSupplier ready) {
        this.ready = Objects.requireNonNull(ready, "ready");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get(READY_PATH, this::ready).get(LIVE_PATH, this::live);
    }

    private void ready(ServerRequest request, ServerResponse response) {
        boolean ready = this.ready.getAsBoolean();
        response.status(ready ? Status.OK_200 : Status.SERVICE_UNAVAILABLE_503)
                .send(ready ? "ready" : "not ready");
    }

    private void live(ServerRequest request, ServerResponse response) {
        response.status(Status.OK_200).send("live");
    }
}
