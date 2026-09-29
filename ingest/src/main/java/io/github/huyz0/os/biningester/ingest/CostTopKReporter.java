// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The periodic top-K cost event (M11.5, ADR-0077, research 15 §4-5): once per
 * interval, one line naming the {@value #TOP} indices that cost the most IN
 * THAT INTERVAL, by estimated USD.
 *
 * <p>⚠️ **THE INTERVAL's COST, NOT THE LIFETIME's.** A cumulative ranking is
 * led for ever by whichever index was busiest at startup; the operator
 * reading this line wants what is costing money now. So each line ranks the
 * change since the last one.
 *
 * <p>⚠️ **ONE LINE, ALWAYS, AT EACH INTERVAL** -- "nothing cost anything" is
 * said, not left to silence, which reads the same as a reporter that stopped.
 * And **NEVER A METRIC LABEL** (cost.md rule 16): this and
 * {@code /admin/cost} are how an index's cost reaches an operator.
 *
 * <p>It touches no store; the clock is injected and the caller schedules
 * {@link #tick}.
 */
public final class CostTopKReporter {

    /** The interval a pod uses when its operator names none. */
    public static final Duration DEFAULT_INTERVAL = Duration.ofMinutes(5);

    /**
     * The longest interval a line may cover (M12.9, M11.5 P1b): past it the
     * schedule's {@code toNanos} overflowed, with an error naming no key.
     */
    public static final Duration MAX_INTERVAL = Duration.ofDays(1);

    /** How many indices a line names. */
    public static final int TOP = 3;

    private final IndexCostLedger ledger;
    private final Supplier<Map<UUID, String>> names;
    private final CostTable prices;
    private final Duration interval;
    private final Clock clock;
    private final Consumer<String> sink;
    private final RefusedIndices refused;
    /** One index's cost: estimated dollars and apportioned micro-requests. */
    private record Spent(UUID index, double usd, long micros) {
    }

    private final Map<UUID, Spent> last = new HashMap<>();
    private long nextDueMillis;
    /** When the last line was emitted, or this reporter made: a line's real window starts here. */
    private long lastLineMillis;

    /**
     * @param interval how often a line is emitted; {@link Duration#ZERO} turns
     *     the event off
     * @throws IllegalArgumentException if {@code interval} is negative
     */
    public CostTopKReporter(IndexCostLedger ledger, Supplier<Map<UUID, String>> names,
            CostTable prices, Duration interval, Clock clock, Consumer<String> sink,
            RefusedIndices refused) {
        this.refused = java.util.Objects.requireNonNull(refused, "refused");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.names = Objects.requireNonNull(names, "names");
        this.prices = Objects.requireNonNull(prices, "prices");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sink = Objects.requireNonNull(sink, "sink");
        if (interval.isNegative()) {
            throw new IllegalArgumentException("a negative interval is not a period: "
                    + interval);
        }
        // ⚠️ THE FLOOR IS THE PARSER's (one second, M11.5); a test builds one
        // shorter on purpose. The CEILING is here, where the overflow was.
        if (interval.compareTo(MAX_INTERVAL) > 0) {
            throw new IllegalArgumentException("the top-K cost interval is at most "
                    + MAX_INTERVAL + ": " + interval);
        }
        this.lastLineMillis = clock.millis();
        this.nextDueMillis = lastLineMillis + interval.toMillis();
    }

    /** Whether this reporter emits at all. */
    public boolean enabled() {
        return !interval.isZero();
    }

    /**
     * Emits the line if an interval has passed since the last one.
     *
     * @return whether a line was emitted
     */
    public synchronized boolean tick() {
        if (!enabled()) {
            return false;
        }
        long now = clock.millis();
        if (now < nextDueMillis) {
            return false;
        }
        // ⚠️ ONE LINE HOWEVER LATE THE TICK, and the next is one interval on
        // from NOW: a stalled scheduler does not owe a burst of catch-up lines.
        nextDueMillis = now + interval.toMillis();
        // ⚠️ THE WINDOW THIS LINE COVERS, NOT THE CONFIGURED ONE (M12.9, M11.5 P3): a
        // stalled scheduler's late line spans longer, and says so.
        Duration window = Duration.ofMillis(now - lastLineMillis);
        lastLineMillis = now;
        List<Spent> spent = new ArrayList<>();
        for (IndexCostLedger.IndexCost cost : ledger.snapshot().indices()) {
            Spent total = new Spent(cost.index(), IndexCostReport.usd(cost.micros(), prices),
                    cost.micros().values().stream().mapToLong(Long::longValue).sum());
            Spent before = last.put(cost.index(), total);
            Spent delta = before == null ? total : new Spent(cost.index(),
                    total.usd() - before.usd(), total.micros() - before.micros());
            if (delta.micros() > 0) {
                spent.add(delta);
            }
        }
        // ⚠️ BY DOLLARS, THEN BY REQUESTS: a backend priced at zero (the local
        // and in-memory ones) still has a busiest index, and ranking by a
        // column of zeros would name them in id order.
        spent.sort(Comparator.comparingDouble(Spent::usd).reversed()
                .thenComparing(Comparator.comparingLong(Spent::micros).reversed())
                .thenComparing(s -> s.index().toString()));
        StringBuilder line = new StringBuilder("cost: top ").append(TOP)
                .append(" indices by estimated USD over the last ").append(window)
                .append(':');
        if (spent.isEmpty()) {
            line.append(" none -- no index request was charged");
        } else {
            Map<UUID, String> byId = names.get();
            for (int i = 0; i < Math.min(TOP, spent.size()); i++) {
                Spent entry = spent.get(i);
                String name = byId.get(entry.index());
                line.append(i == 0 ? " " : ", ")
                        .append(name == null ? entry.index().toString() : name)
                        .append(String.format(Locale.ROOT, " $%.9f", entry.usd()))
                        .append(" (").append(IndexCostReport.decimal(entry.micros()))
                        .append(" requests)");
            }
            line.append(" (estimated; see /admin/cost)");
        }
        // ⚠️ THE INDICES REFUSED 429 IN THIS INTERVAL (M12.5): a pod-level counter
        // says how many, and this line -- never a label -- says which.
        RefusedIndices.Drained refusedNow = refused.drain();
        if (!refusedNow.names().isEmpty()) {
            line.append(" -- refused 429: ").append(String.join(", ", refusedNow.names()));
            if (refusedNow.more() > 0) {
                line.append(" and ").append(refusedNow.more()).append(" more");
            }
        }
        sink.accept(line.toString());
        return true;
    }
}
