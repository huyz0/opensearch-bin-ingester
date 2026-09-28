// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.ingest.IndexCostReport;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.util.Objects;

/**
 * {@code GET /admin/cost?by=index&top=N} -- each index's apportioned share of
 * the pod's store requests, top N by estimated cost (M11.4, ADR-0077, research
 * 15 §4).
 *
 * <p>⚠️ THIS OWNS NO DECISION (ADR-0019): it checks the two parameters and
 * hands the report source the bound. The report is the ledger's, built where
 * the ledger lives.
 *
 * <p>⚠️ **THE ANSWER NAMES INDICES**, as observability.md rule 5 allows a log
 * line to, and carries no payload, credential or routing value. It is served
 * on the same unauthenticated listener as every other route here until
 * per-request authentication lands (M1.7c).
 */
public final class AdminCostService implements HttpService {

    public static final String PATH = "/admin/cost";

    /** Builds the report for the {@code top} indices; the only thing this route calls. */
    @FunctionalInterface
    public interface Source {
        String json(int top);
    }

    private final Source source;

    public AdminCostService(Source source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get(PATH, this::cost);
    }

    private void cost(ServerRequest request, ServerResponse response) {
        String by = request.query().first("by").orElse("index");
        if (!by.equals("index")) {
            // ⚠️ ONE DIMENSION TODAY, NAMED SO A CALLER ASKING FOR ANOTHER IS
            // TOLD RATHER THAN HANDED THE INDEX VIEW AS IF IT WERE WHAT IT ASKED.
            response.status(Status.BAD_REQUEST_400).send("'by' is 'index'");
            return;
        }
        int top = IndexCostReport.DEFAULT_TOP;
        var raw = request.query().first("top");
        if (raw.isPresent()) {
            try {
                top = Integer.parseInt(raw.get());
            } catch (NumberFormatException e) {
                top = 0;
            }
            if (top < 1 || top > IndexCostReport.MAX_TOP) {
                // ⚠️ BOUNDED: a report is built per request, and an unbounded
                // one over 10,000 indices is a heap spike a scrape loop repeats.
                response.status(Status.BAD_REQUEST_400)
                        .send("'top' is an integer from 1 to " + IndexCostReport.MAX_TOP);
                return;
            }
        }
        response.header("Content-Type", "application/json").send(source.json(top));
    }
}
