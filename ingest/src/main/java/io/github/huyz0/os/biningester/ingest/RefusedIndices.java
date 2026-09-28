// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The indices refused {@code 429} since the top-K cost line last read them
 * (M12.5, M11 review F5): per-index detail reaches an operator through that
 * line, never through a metric label (cost.md rule 16).
 *
 * <p>⚠️ BOUNDED: an index name reaches this from a producer, so at most
 * {@value #NAMED} names are kept per interval, and refusals of any other index
 * are only counted.
 */
public final class RefusedIndices {

    /** How many refused index names one interval names. */
    public static final int NAMED = 8;

    /** The names refused since the last drain, sorted, and how many refusals of others. */
    public record Drained(List<String> names, long more) {
        public Drained {
            names = List.copyOf(names);
        }
    }

    private final Set<String> names = new LinkedHashSet<>();
    private long more;

    /** Records one refusal of {@code index}. */
    public synchronized void record(String index) {
        Objects.requireNonNull(index, "index");
        if (!names.contains(index)) {
            if (names.size() < NAMED) {
                names.add(index);
            } else {
                more++;
            }
        }
    }

    /** What was refused since the last drain; the tally starts again. */
    public synchronized Drained drain() {
        List<String> sorted = new ArrayList<>(names);
        sorted.sort(null);
        Drained drained = new Drained(sorted, more);
        names.clear();
        more = 0;
        return drained;
    }
}
