// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.PutPurposeCounts;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

/**
 * What {@code GET /admin/cost?by=index} answers (M11.4, ADR-0077, research 15
 * §4): the pod's request totals, the unattributed requests, and the top N
 * indices by estimated cost, each with its bytes and apportioned requests.
 *
 * <p>⚠️ **ZERO METRIC CARDINALITY, BY CONSTRUCTION.** This is the one place an
 * index's cost leaves the ledger, and it leaves as a document an operator
 * asks for, never as a series (cost.md rule 16).
 *
 * <p>⚠️ **REQUESTS ARE EXACT, DOLLARS ARE AN ESTIMATE**, and the document says
 * so: a share is printed as the ledger's exact micro-requests in decimal, and
 * {@code estimatedUsd} is those shares at the backend's {@link CostTable}.
 */
public final class IndexCostReport {

    /** The largest {@code top} a caller may ask for. */
    public static final int MAX_TOP = 1_000;

    /** What {@code top} is when the caller names none. */
    public static final int DEFAULT_TOP = 50;

    /** The pod's own counts, the denominators the shares sum to. */
    public record PodTotals(StoreCounts counts, PutPurposeCounts puts, long dataSegmentGets) {
        public PodTotals {
            Objects.requireNonNull(counts, "counts");
            Objects.requireNonNull(puts, "puts");
        }
    }

    private IndexCostReport() {
    }

    /**
     * The report as JSON.
     *
     * @param names an index id to its name, or {@code null} when the pod has
     *     none registered -- the id is printed instead
     * @throws IllegalArgumentException if {@code top} is outside 1..{@value #MAX_TOP}
     */
    public static String json(String podId, IndexCostLedger.Snapshot snapshot,
            PodTotals totals, CostTable prices, Function<UUID, String> names, int top) {
        Objects.requireNonNull(podId, "podId");
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(totals, "totals");
        Objects.requireNonNull(prices, "prices");
        Objects.requireNonNull(names, "names");
        if (top < 1 || top > MAX_TOP) {
            throw new IllegalArgumentException("top is 1 to " + MAX_TOP + ": " + top);
        }
        List<IndexCostLedger.IndexCost> ranked = snapshot.indices().stream()
                .sorted(Comparator.comparingDouble(
                                (IndexCostLedger.IndexCost c) -> usd(c.micros(), prices))
                        .reversed()
                        .thenComparing(c -> c.index().toString()))
                .limit(top)
                .toList();
        StringBuilder out = new StringBuilder(256 + 256 * ranked.size());
        out.append("{\"pod\":").append(string(podId))
                .append(",\"by\":\"index\"")
                .append(",\"estimate\":\"dollars are requests at the backend's price table; "
                        + "an index's requests are its apportioned share (ADR-0077)\"")
                .append(",\"prices\":{\"putMicroUsdPerThousand\":").append(prices.putPerThousand())
                .append(",\"getMicroUsdPerThousand\":").append(prices.getPerThousand())
                .append(",\"listMicroUsdPerThousand\":").append(prices.listPerThousand())
                .append("},\"totals\":{\"puts\":").append(totals.counts().puts())
                .append(",\"dataPuts\":").append(totals.puts().dataPuts())
                .append(",\"commitPuts\":").append(totals.puts().commitPuts())
                .append(",\"checkpointPuts\":").append(totals.puts().checkpointPuts())
                .append(",\"leasePuts\":").append(totals.puts().leasePuts())
                .append(",\"otherPuts\":").append(totals.puts().otherPuts())
                .append(",\"gets\":").append(totals.counts().gets())
                .append(",\"dataSegmentGets\":").append(totals.dataSegmentGets())
                .append(",\"lists\":").append(totals.counts().lists())
                .append(",\"stats\":").append(totals.counts().stats())
                .append(",\"deletes\":").append(totals.counts().deletes())
                .append("},\"unattributed\":").append(requests(snapshot.unattributed()))
                .append(",\"indexCount\":").append(snapshot.indices().size())
                .append(",\"indices\":[");
        for (int i = 0; i < ranked.size(); i++) {
            IndexCostLedger.IndexCost cost = ranked.get(i);
            String name = names.apply(cost.index());
            if (i > 0) {
                out.append(',');
            }
            out.append("{\"index\":").append(string(name == null ? cost.index().toString() : name))
                    .append(",\"id\":").append(string(cost.index().toString()))
                    .append(",\"bytes\":").append(cost.bytes())
                    .append(",\"requests\":").append(requests(cost.micros()))
                    .append(",\"estimatedUsd\":")
                    .append(String.format(Locale.ROOT, "%.9f", usd(cost.micros(), prices)))
                    .append('}');
        }
        return out.append("]}").toString();
    }

    /** What a set of apportioned shares costs, in dollars, at {@code prices}. */
    static double usd(Map<Charge, Long> micros, CostTable prices) {
        double microRequestMicroDollarsPerThousand = 0;
        for (Map.Entry<Charge, Long> share : micros.entrySet()) {
            long price = switch (share.getKey()) {
                case DATA_PUT, COMMIT_PUT -> prices.putPerThousand();
                case DATA_GET -> prices.getPerThousand();
            };
            microRequestMicroDollarsPerThousand += (double) share.getValue() * price;
        }
        // micro-requests x micro-dollars per 1,000 requests -> dollars.
        return microRequestMicroDollarsPerThousand
                / IndexCostLedger.MICROS_PER_REQUEST / 1_000 / 1_000_000;
    }

    private static String requests(Map<Charge, Long> micros) {
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (Charge charge : Charge.values()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append('"').append(key(charge)).append("\":")
                    .append(decimal(micros.getOrDefault(charge, 0L)));
        }
        return out.append('}').toString();
    }

    private static String key(Charge charge) {
        return switch (charge) {
            case DATA_PUT -> "dataPut";
            case COMMIT_PUT -> "commitPut";
            case DATA_GET -> "dataGet";
        };
    }

    /** Micro-requests as an exact decimal number of requests. */
    static String decimal(long micros) {
        return (micros / IndexCostLedger.MICROS_PER_REQUEST) + "."
                + String.format(Locale.ROOT, "%06d", micros % IndexCostLedger.MICROS_PER_REQUEST);
    }

    private static String string(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.append('"').toString();
    }
}
