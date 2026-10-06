// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A Kubernetes API server that serves one thing: a watch of the ingester
 * Service's {@code EndpointSlice}, whose membership the test sets (M8.13).
 *
 * <p>⚠️ **THE TEST PLAYS THE ENDPOINTS CONTROLLER.** In a cluster, the
 * controller removes a pod's endpoint when the pod dies. Here the test does
 * it, right after the kill, so a row measures the ingester's reaction to the
 * event rather than the controller's latency, which no ingester can change.
 */
public final class FakeKubeApi implements AutoCloseable {

    public static final String NAMESPACE = "ingest";
    public static final String SERVICE = "ingester";

    private record Ready(String uid, String address) {
    }

    private final Map<String, Ready> ready = new ConcurrentHashMap<>();
    private final List<LinkedBlockingQueue<String>> watchers = new CopyOnWriteArrayList<>();
    private final WebServer server;
    private volatile boolean closed;

    public FakeKubeApi() {
        server = WebServer.builder().port(0).routing(HttpRouting.builder().get(
                "/apis/discovery.k8s.io/v1/namespaces/" + NAMESPACE + "/endpointslices",
                (req, res) -> {
                    LinkedBlockingQueue<String> events = new LinkedBlockingQueue<>();
                    // ⚠️ REGISTERED BEFORE THE SNAPSHOT (M13.53 review round
                    // 1, P1): a `ready` between the two reaches this watch as
                    // a broadcast, and the snapshot after it is never older.
                    watchers.add(events);
                    events.add(slice("ADDED"));
                    try (OutputStream out = res.outputStream()) {
                        while (!closed) {
                            String event = events.poll(100, TimeUnit.MILLISECONDS);
                            if (event != null) {
                                out.write((event + "\n").getBytes(StandardCharsets.UTF_8));
                                out.flush();
                            }
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    } catch (java.io.IOException watcherGone) {
                        // the node closed its watch
                    } finally {
                        watchers.remove(events);
                    }
                })).build().start();
    }

    /** The settings a node needs to watch this server. */
    public Map<String, String> nodeSettings() {
        return Map.of("membership.kube-api", "http://localhost:" + server.port(),
                "membership.namespace", NAMESPACE, "membership.service", SERVICE);
    }

    /**
     * Marks {@code podId}, incarnation {@code uid}, ready at {@code address},
     * and tells every watcher.
     *
     * <p>⚠️ THE UID IS REQUIRED (M13.53): since M8.58 the early challenge
     * matches a lease holder by pod UID only, so an endpoint without one is
     * never evidence and every takeover waits out the TTL.
     */
    public void ready(String podId, String uid, String address) {
        ready.put(podId, new Ready(uid, address));
        broadcast();
    }

    /** Removes {@code podId}'s endpoint, as the controller does when a pod dies. */
    public void remove(String podId) {
        ready.remove(podId);
        broadcast();
    }

    /** How many watches are open right now. */
    public int watchers() {
        return watchers.size();
    }

    private void broadcast() {
        String event = slice("MODIFIED");
        for (LinkedBlockingQueue<String> watcher : watchers) {
            watcher.add(event);
        }
    }

    private String slice(String type) {
        StringBuilder endpoints = new StringBuilder();
        ready.forEach((pod, entry) -> {
            if (endpoints.length() > 0) {
                endpoints.append(',');
            }
            endpoints.append("{\"addresses\":[\"").append(entry.address())
                    .append("\"],\"conditions\":{\"ready\":true,\"terminating\":false},")
                    .append("\"targetRef\":{\"kind\":\"Pod\",\"name\":\"").append(pod)
                    .append("\",\"uid\":\"").append(entry.uid()).append("\"}}");
        });
        return "{\"type\":\"" + type + "\",\"object\":{\"kind\":\"EndpointSlice\","
                + "\"metadata\":{\"name\":\"" + SERVICE + "-1\"},\"endpoints\":[" + endpoints
                + "]}}";
    }

    @Override
    public void close() {
        closed = true;
        server.stop();
    }
}
