// SPDX-License-Identifier: Apache-2.0
package fixture;

/** The call is a SUBSTRING in a comment and nothing meters. */
final class CommentedOut {
    private void record(String verb) { }

    @Override public String get(String k) {
        // TODO: record("get") once the meter lands
        return k;
    }
}
