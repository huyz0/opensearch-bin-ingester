// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.common.settings.Settings;

/**
 * Reads the three fast-mode index settings (M13.23, ADR-0082 §1).
 *
 * <p>⚠️ STRICT, BECAUSE NOTHING ELSE IS. OpenSearch registers
 * {@code index.ingestion_source.param.*} as free-form, dynamic strings and
 * validates none of them, so {@code wal: yes} or {@code wal_quorum: 5} reach
 * this class. A lenient read turns a typo into a durability the operator did
 * not choose, invisibly -- ADR-0013 warns against exactly that -- so anything
 * but a well-formed value is refused, and the registrar keeps the index's last
 * accepted shape and counts the refusal.
 */
final class FastSettings {

    static final String FLUSH_TIMER = "index.ingestion_source.param.flush_timer";
    static final String WAL = "index.ingestion_source.param.wal";
    static final String WAL_QUORUM = "index.ingestion_source.param.wal_quorum";

    /**
     * A positive whole number of {@code ms}, {@code s}, {@code m} or {@code h},
     * and nothing else.
     *
     * <p>⚠️ NOT OPENSEARCH'S {@code TimeValue.parseTimeValue}, which accepts
     * {@code 5S} and {@code " 5s "}, truncates {@code 1500micros} to 1 ms
     * without a word and refuses the positive {@code 999micros} as zero
     * (M13.23 review round 3, P4). A deadline is set in milliseconds or
     * coarser; anything else is a typo this class exists to refuse.
     */
    private static final Pattern DURATION = Pattern.compile("([1-9][0-9]{0,17})(ms|s|m|h)");

    private FastSettings() {
    }

    /**
     * {@code base} with the three settings read from {@code settings}.
     *
     * @throws IllegalArgumentException naming the setting, for a malformed value
     */
    static IndexRegistration apply(IndexRegistration base, Settings settings) {
        String timer = settings.get(FLUSH_TIMER);
        long flushTimerMillis = IndexRegistration.DEFAULT_FLUSH_TIMER_MILLIS;
        if (timer != null) {
            Matcher m = DURATION.matcher(timer);
            if (!m.matches()) {
                throw new IllegalArgumentException(FLUSH_TIMER + " is [" + timer
                        + "], which is not a positive duration in ms, s, m or h");
            }
            long amount = Long.parseLong(m.group(1));
            long unit = switch (m.group(2)) {
                case "ms" -> 1L;
                case "s" -> 1_000L;
                case "m" -> 60_000L;
                default -> 3_600_000L;
            };
            if (amount > Long.MAX_VALUE / unit) {
                throw new IllegalArgumentException(FLUSH_TIMER + " is [" + timer
                        + "], which does not fit in milliseconds");
            }
            flushTimerMillis = amount * unit;
        }
        String walText = settings.get(WAL);
        boolean wal = false;
        if (walText != null) {
            // ⚠️ EXACTLY `true` OR `false`: no trimming, no case folding.
            if (!walText.equals("true") && !walText.equals("false")) {
                throw new IllegalArgumentException(WAL + " is [" + walText
                        + "], which is neither true nor false");
            }
            wal = walText.equals("true");
        }
        String quorumText = settings.get(WAL_QUORUM);
        int walQuorum = IndexRegistration.DEFAULT_WAL_QUORUM;
        if (quorumText != null) {
            // ⚠️ CHECKED EVEN WITH wal OFF: a quorum of 5 is a typo whatever
            // wal says, and it becomes live the moment wal is turned on.
            if (!quorumText.equals("1") && !quorumText.equals("2") && !quorumText.equals("3")) {
                throw new IllegalArgumentException(WAL_QUORUM + " is [" + quorumText
                        + "], which is not 1, 2 or 3");
            }
            walQuorum = Integer.parseInt(quorumText);
        }
        return base.withFastSettings(flushTimerMillis, wal, walQuorum);
    }
}
