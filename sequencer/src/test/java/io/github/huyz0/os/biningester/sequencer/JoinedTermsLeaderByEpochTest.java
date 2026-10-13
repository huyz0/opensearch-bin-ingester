// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The leader of a joined term by its epoch (M13.30p): where a fast key of
 * that term is read before its upload -- not of a term never joined, closed,
 * or whose leader's endpoint was not recorded.
 */
class JoinedTermsLeaderByEpochTest {

    @Test
    void aJOINEDTermsLeaderIsFoundByItsEpochAndOnlyWhileItMayHold() {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(3, 0, "uid-a", "a:1");
        joined.joined(4, 0, "uid-b", "b:1");
        joined.joined(5, 0, "uid-c");

        assertThat(joined.leader(3)).hasValue(new JoinedTerms.Leader(3, "uid-a", "a:1"));
        assertThat(joined.leader(4)).hasValue(new JoinedTerms.Leader(4, "uid-b", "b:1"));
        assertThat(joined.leader(5)).as("no endpoint recorded").isEmpty();
        assertThat(joined.leader(6)).as("never joined").isEmpty();

        joined.joined(4, 3);

        assertThat(joined.leader(3)).as("closed").isEmpty();
    }
}
