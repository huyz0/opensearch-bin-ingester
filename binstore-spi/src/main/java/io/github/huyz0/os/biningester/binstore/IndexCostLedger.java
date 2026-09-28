// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Each index's share of the store requests its pod issued, in memory
 * (FR-21, ADR-0077).
 *
 * <p>A request carries a SEGMENT of many indices, so an index's share of one
 * is apportioned rather than counted: its weight over the request's total
 * weight, in integer MICRO-REQUESTS ({@value #MICROS_PER_REQUEST} per
 * request), rounded by LARGEST REMAINDER so the shares of one request sum to
 * exactly {@value #MICROS_PER_REQUEST}. The per-index totals therefore sum
 * exactly to the requests apportioned -- a checkable invariant, not an
 * approximation.
 *
 * <p>⚠️ **A REQUEST THAT CANNOT BE APPORTIONED IS NEVER DROPPED**: it is
 * charged whole to {@link #unattributed}, so the invariant also holds for the
 * requests nobody could split.
 *
 * <p>⚠️ **NEVER EXPORTED AS A METRIC LABEL** (cost.md rule 16): the ledger
 * answers {@code GET /admin/cost} and the top-K log event, and nothing else.
 * It touches no store, no clock and no socket.
 */
public final class IndexCostLedger {

    /** One request, in the ledger's unit. */
    public static final long MICROS_PER_REQUEST = 1_000_000L;

    private static final BigInteger MICROS = BigInteger.valueOf(MICROS_PER_REQUEST);

    /** What a share is a share of: the bounded {@code (op, purpose)} pairs ADR-0077 apportions. */
    public enum Charge {
        /** A data-segment PUT ({@code op} put, {@code purpose} data). */
        DATA_PUT,
        /**
         * A commit-log PUT ({@code op} put_if_absent, {@code purpose} commit),
         * weighted by the delta's record counts (M11.22).
         */
        COMMIT_PUT,
        /** A data-segment GET ({@code op} get, {@code purpose} data). */
        DATA_GET
    }

    private static final class Entry {
        private final EnumMap<Charge, LongAdder> micros = new EnumMap<>(Charge.class);
        private final LongAdder bytes = new LongAdder();

        Entry() {
            for (Charge charge : Charge.values()) {
                micros.put(charge, new LongAdder());
            }
        }
    }

    private final ConcurrentHashMap<UUID, Entry> entries = new ConcurrentHashMap<>();
    private final EnumMap<Charge, LongAdder> unattributed = new EnumMap<>(Charge.class);

    public IndexCostLedger() {
        for (Charge charge : Charge.values()) {
            unattributed.put(charge, new LongAdder());
        }
    }

    /**
     * Apportions ONE request of {@code charge} across {@code weights}' indices.
     *
     * <p>A request whose weights are empty or all zero is charged whole to
     * {@link #unattributed}.
     *
     * @param weights each index's weight in this request -- its run bytes
     *     (ADR-0077); never negative
     */
    public void apportion(Charge charge, Map<UUID, Long> weights) {
        Objects.requireNonNull(charge, "charge");
        Objects.requireNonNull(weights, "weights");
        long total = 0;
        for (Map.Entry<UUID, Long> w : weights.entrySet()) {
            long weight = Objects.requireNonNull(w.getValue(), "weight");
            if (weight < 0) {
                throw new IllegalArgumentException("a weight is never negative: " + w);
            }
            total = Math.addExact(total, weight);
        }
        if (total == 0) {
            unattributed(charge);
            return;
        }
        List<UUID> ids = new ArrayList<>(weights.keySet());
        // ⚠️ SORTED, so the remainder goes to the same index for the same input
        // whatever order the caller's map iterates in.
        ids.sort(Comparator.naturalOrder());
        long[] shares = shares(ids.stream().mapToLong(weights::get).toArray(), total);
        for (int i = 0; i < ids.size(); i++) {
            if (shares[i] > 0) {
                entry(ids.get(i)).micros.get(charge).add(shares[i]);
            }
        }
    }

    /** Charges ONE request of {@code charge} whole to the unattributed bucket. */
    public void unattributed(Charge charge) {
        unattributed.get(Objects.requireNonNull(charge, "charge")).add(MICROS_PER_REQUEST);
    }

    /** Records {@code bytes} written for {@code index}. */
    public void bytesWritten(UUID index, long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes are never negative: " + bytes);
        }
        entry(Objects.requireNonNull(index, "index")).bytes.add(bytes);
    }

    private Entry entry(UUID index) {
        return entries.computeIfAbsent(index, id -> new Entry());
    }

    /**
     * {@value #MICROS_PER_REQUEST} split by {@code weights} over
     * {@code total}, largest remainder first, ties to the lower position.
     */
    static long[] shares(long[] weights, long total) {
        long[] shares = new long[weights.length];
        long[] remainders = new long[weights.length];
        long given = 0;
        for (int i = 0; i < weights.length; i++) {
            // ⚠️ IN 128 BITS, ALWAYS: MICROS_PER_REQUEST × weight overflows a
            // long above ~9.2e12, and one exact division per index per request
            // costs nothing beside the PUT it apportions.
            BigInteger[] qr = BigInteger.valueOf(weights[i]).multiply(MICROS)
                    .divideAndRemainder(BigInteger.valueOf(total));
            shares[i] = qr[0].longValueExact();
            remainders[i] = qr[1].longValueExact();
            given += shares[i];
        }
        Integer[] order = new Integer[weights.length];
        Arrays.setAll(order, i -> i);
        Arrays.sort(order, (a, b) -> remainders[a] != remainders[b]
                ? Long.compare(remainders[b], remainders[a]) : Integer.compare(a, b));
        for (int k = 0; given < MICROS_PER_REQUEST; k++) {
            shares[order[k]]++;
            given++;
        }
        return shares;
    }

    /** One index's apportioned requests and bytes, as read now. */
    public record IndexCost(UUID index, Map<Charge, Long> micros, long bytes) {
        public IndexCost {
            micros = Map.copyOf(micros);
        }
    }

    /** Every index's cost, and the unattributed micro-requests, as read now. */
    public record Snapshot(List<IndexCost> indices, Map<Charge, Long> unattributed) {
        public Snapshot {
            indices = List.copyOf(indices);
            unattributed = Map.copyOf(unattributed);
        }

        /** Σ of every index's share plus the unattributed, for {@code charge}. */
        public long totalMicros(Charge charge) {
            return unattributed.get(charge)
                    + indices.stream().mapToLong(i -> i.micros().get(charge)).sum();
        }
    }

    /** What the ledger holds now; concurrent charges may or may not be included. */
    public Snapshot snapshot() {
        List<IndexCost> indices = new ArrayList<>();
        entries.forEach((id, entry) -> {
            EnumMap<Charge, Long> micros = new EnumMap<>(Charge.class);
            entry.micros.forEach((charge, adder) -> micros.put(charge, adder.sum()));
            indices.add(new IndexCost(id, micros, entry.bytes.sum()));
        });
        EnumMap<Charge, Long> un = new EnumMap<>(Charge.class);
        unattributed.forEach((charge, adder) -> un.put(charge, adder.sum()));
        return new Snapshot(indices, un);
    }
}
