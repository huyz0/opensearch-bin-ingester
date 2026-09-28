// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The per-index quota settings (M11.8, ADR-0078 decision 4): a default for
 * every index, an override per index by name, and the per-index in-flight cap.
 * {@code 0} is unlimited and is the default, so a pod configured with nothing
 * refuses nothing by quota.
 */
final class QuotaProperties {

    /** The default bytes/s every index is admitted at; 0 is unlimited. */
    static final String DEFAULT_BYTES = "ingest.quota.default.bytes-per-second";

    /** The default records/s every index is admitted at; 0 is unlimited. */
    static final String DEFAULT_RECORDS = "ingest.quota.default.records-per-second";

    /** How many requests one quota'd index may have admitted at once (ADR-0078 decision 2a). */
    static final String MAX_IN_FLIGHT = "ingest.quota.max-in-flight-per-index";

    /** The fixed keys; the per-index ones match {@link #PER_INDEX}. */
    static final Set<String> KEYS = Set.of(DEFAULT_BYTES, DEFAULT_RECORDS, MAX_IN_FLIGHT);

    /**
     * {@code ingest.quota.index.<name>.bytes-per-second} and
     * {@code ...records-per-second}. ⚠️ The name is everything between the
     * fixed parts, dots included: an index name may contain a dot.
     */
    static final Pattern PER_INDEX =
            Pattern.compile("ingest\\.quota\\.index\\.(.+)\\.(bytes|records)-per-second");

    private QuotaProperties() {
    }

    /** Whether {@code key} is one of these settings. */
    static boolean isQuotaKey(String key) {
        return KEYS.contains(key) || PER_INDEX.matcher(key).matches();
    }

    /** The quotas {@code settings} configure; nothing configured is {@link IndexQuotas.Config#none()}. */
    static IndexQuotas.Config parse(Map<String, String> settings) {
        IndexQuotas.Limit defaults = new IndexQuotas.Limit(rate(settings, DEFAULT_BYTES),
                rate(settings, DEFAULT_RECORDS));
        Map<String, long[]> overrides = new HashMap<>();
        for (Map.Entry<String, String> setting : settings.entrySet()) {
            if (setting.getKey() == null) {
                continue;
            }
            Matcher m = PER_INDEX.matcher(setting.getKey());
            if (m.matches()) {
                long[] limit = overrides.computeIfAbsent(m.group(1), name -> new long[] {
                        defaults.bytesPerSecond(), defaults.recordsPerSecond()});
                limit[m.group(2).equals("bytes") ? 0 : 1] = rate(settings, setting.getKey());
            }
        }
        Map<String, IndexQuotas.Limit> perIndex = new HashMap<>();
        overrides.forEach((name, limit) ->
                perIndex.put(name, new IndexQuotas.Limit(limit[0], limit[1])));
        return new IndexQuotas.Config(defaults, perIndex, maxInFlight(settings));
    }

    private static long rate(Map<String, String> settings, String key) {
        String value = settings.get(key);
        if (value == null) {
            return 0;
        }
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException notANumber) {
            throw new ConfigurationException(key + " is not a whole number per second (0 is "
                    + "unlimited): " + value, notANumber);
        }
        if (parsed < 0) {
            throw new ConfigurationException(key + " is never negative (0 is unlimited): "
                    + value);
        }
        return parsed;
    }

    private static int maxInFlight(Map<String, String> settings) {
        String value = settings.get(MAX_IN_FLIGHT);
        if (value == null) {
            return IndexQuotas.DEFAULT_MAX_IN_FLIGHT_PER_INDEX;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException notANumber) {
            throw new ConfigurationException(MAX_IN_FLIGHT + " is not a whole number of "
                    + "requests: " + value, notANumber);
        }
        if (parsed < 1) {
            throw new ConfigurationException(MAX_IN_FLIGHT + " must be positive: " + value);
        }
        return parsed;
    }
}
