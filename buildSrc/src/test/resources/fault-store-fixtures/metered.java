// SPDX-License-Identifier: Apache-2.0
package fixture;

/** Every verb records. The shape the gate accepts. */
final class Metered {
    private void record(String verb) { }

    @Override public String get(String k) {
        record("get");
        return k;
    }

    @Override
    public String putIfMatch(String k, String b)
            throws IllegalStateException {
        record("putIfMatch");
        return b;
    }
}
