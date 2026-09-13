// SPDX-License-Identifier: Apache-2.0
package fixture;

/**
 * The call is on the FIRST line of the body — and behind a guard on that same
 * line, so it meters only one path.
 *
 * <p>⚠️ THE SHAPE A VERB AUTHOR PLAUSIBLY WRITES, wanting the call above
 * `refuseIfPartitioned()` but only on the counted path. It is
 * `metered-on-one-branch-only`'s defect written one line shorter, and review
 * measured it separating `startswith('record(')` from `'record(' in stmt`:
 * under the looser predicate this file passes with `ok 1 verb(s) metered`.
 */
final class GuardedInline {
    private void record(String verb) { }

    @Override public String stat(String k) {
        if (k != null) record("stat");
        return k;
    }
}
