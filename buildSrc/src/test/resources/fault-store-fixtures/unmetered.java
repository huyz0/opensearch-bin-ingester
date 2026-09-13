// SPDX-License-Identifier: Apache-2.0
package fixture;

/**
 * `presign` is the verb M5.29 records arriving fifteen hours after the row that
 * predicted it: an @Override with no record(...) call.
 *
 * ⚠️ IT IS THE FIRST MEMBER, NOT THE LAST, AND THAT IS THE POINT. An earlier
 * version of this fixture put it last, so nothing followed it to lend it a
 * `record(` — which is exactly the bleed the gate's old line-count window had,
 * and why the fixture could not see its own defect. Review measured five of the
 * real store's twelve verbs passing unmetered under that window.
 */
final class Unmetered {
    private void record(String verb) { }

    @Override public String presign(String k, long ttl) {
        return k + ttl;
    }

    @Override public String get(String k) {
        record("get");
        return k;
    }
}
