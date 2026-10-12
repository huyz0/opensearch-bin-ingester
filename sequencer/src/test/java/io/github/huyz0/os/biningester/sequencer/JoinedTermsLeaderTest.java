// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Where a writer sends a fast batch (M13.29c): the leader of the newest term
 * it joined, at the endpoint its join reached -- a closed term is held by
 * nobody, and a term whose leader was not recorded is not written through.
 */
class JoinedTermsLeaderTest {

    @Test
    void theNEWESTJoinedTermsLeaderIsWrittenTo() {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(2, 0, "uid-a", "a:1");
        joined.joined(3, 0, "uid-b", "b:1");

        assertThat(joined.latest()).hasValue(new JoinedTerms.Leader(3, "uid-b", "b:1"));
    }

    @Test
    void aCLOSEDTermIsNeverWrittenTo() {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(3, 0, "uid-b", "b:1");
        joined.joined(4, 3, "uid-c", "");

        assertThat(joined.latest()).as("3 closed, 4's leader endpoint unknown").isEmpty();
    }
}
