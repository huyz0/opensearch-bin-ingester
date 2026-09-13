// SPDX-License-Identifier: Apache-2.0
package fixture;

/**
 * The unmetered verb is BETWEEN two metered ones, and that position is the whole
 * point of this fixture.
 *
 * <p>⚠️ FIRST AND LAST ARE BOTH BLIND SPOTS, IN OPPOSITE DIRECTIONS. The
 * original `unmetered.java` put its unmetered verb LAST, where nothing follows
 * to lend it a `record(` — so a forward-bleeding window could not be seen.
 * Round 1 moved it FIRST to kill that, and review measured what the move left
 * open: "examine only the first `@Override`" and "skip anything past line 100"
 * both survived every fixture, because every fixture's unmetered verb was the
 * one a truncated scan still reaches. A verb in the middle is reachable from
 * neither truncation.
 */
final class UnmeteredInTheMiddle {
    private void record(String verb) { }

    @Override public String get(String k) {
        record("get");
        return k;
    }

    @Override public String presign(String k, long ttl) {
        return k + ttl;
    }

    @Override public String close() {
        record("close");
        return "";
    }
}
