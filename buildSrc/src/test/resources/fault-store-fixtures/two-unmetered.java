// SPDX-License-Identifier: Apache-2.0
package fixture;

/**
 * TWO unmetered verbs, so the report cannot be truncated to the first.
 *
 * <p>⚠️ EVERY OTHER FIXTURE HAS EXACTLY ONE, which made `missing[:1]` — report
 * only the first offender — invisible to the whole suite. An operator handed a
 * one-line failure fixes that verb, re-runs, and meets the next one; with a
 * dozen verbs that is a dozen commits. The gate lists all of them.
 */
final class TwoUnmetered {
    private void record(String verb) { }

    @Override public String get(String k) {
        record("get");
        return k;
    }

    @Override public String presign(String k, long ttl) {
        return k + ttl;
    }

    @Override public String multipart(String k) {
        return k;
    }
}
