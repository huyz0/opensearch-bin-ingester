// SPDX-License-Identifier: Apache-2.0
package fixture;

/**
 * `record(...)` is present, inside the body, and reachable — but an early
 * `return` above it means the verb meters only ONE branch. Presence alone
 * accepts this; "first statement" does not.
 */
final class MeteredOnOneBranchOnly {
    private void record(String verb) { }

    @Override public String get(String k) {
        if (k == null) {
            return "";
        }
        record("get");
        return k;
    }
}
