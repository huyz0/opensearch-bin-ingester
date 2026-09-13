// SPDX-License-Identifier: Apache-2.0
package fixture;

/**
 * A record(...) call exists in the file but NINE lines below the @Override, so
 * it is a later statement of a long verb rather than the first one. The window
 * must not reach it -- otherwise a verb that meters only on one branch passes.
 */
final class FarAway {
    private void record(String verb) { }

    @Override public String get(String k) {
        if (k == null) {
            throw new IllegalStateException("a");
        }
        if (k.isEmpty()) {
            throw new IllegalStateException("b");
        }
        String v = k.trim();
        record("get");
        return v;
    }
}
