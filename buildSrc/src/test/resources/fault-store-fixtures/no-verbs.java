// SPDX-License-Identifier: Apache-2.0
package fixture;

/**
 * No @Override at all. ⚠️ THE GATE MUST REFUSE THIS RATHER THAN REPORT
 * "0 verb(s) metered": `FaultInjectingStore.java` sits at exactly 700 lines, on
 * `check-file-size.sh`'s ceiling, so its next change is a SPLIT — and a split
 * that moves the verbs to a sibling file would leave this gate green over
 * nothing, forever. That is `check-reviewed`'s documented empty-index failure
 * mode, which this gate exists partly to avoid repeating.
 */
final class NoVerbs {
    private void record(String verb) { }
}
