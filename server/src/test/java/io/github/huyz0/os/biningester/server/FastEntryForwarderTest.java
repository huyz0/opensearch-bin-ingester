// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastKey;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.http.FastEntryService;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A {@code /seg} fast key this pod holds no copy of, read from its term's
 * leader (M13.30p; ADR-0082 §2, §6): locally when this pod leads that term,
 * else from the joined term's leader -- its answer taken only when it names
 * the key's own term, at or above this pod's fence; anything else, a failure
 * included, is no answer.
 */
class FastEntryForwarderTest {

    private static final RunKey S = RunKey.ofIndexUuid("nVzgup36TLqWp7VBBREj1w", 0);
    private static final String KEY = FastKey.of("p", 5, 0, S, 10);

    private final List<String> asked = new ArrayList<>();
    private long fence = 5;

    private static JoinedTerms joined() {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(5, 0, "uid-l", "l:1");
        return joined;
    }

    private FastEntryForwarder forwarder(FastEntryService.Lookup local, long answeredEpoch,
            boolean failing) {
        return new FastEntryForwarder("p", "uid-me", joined(), () -> fence, local,
                (endpoint, key, self) -> {
                    asked.add(endpoint + " " + self);
                    if (failing) {
                        throw new IOException("unreachable");
                    }
                    return Optional.of(new FastEntryService.Served(answeredEpoch,
                            new byte[] {7}));
                });
    }

    @Test
    void aKEYIsReadFromItsTermsLeaderOrLocallyWhenThisPodLeadsIt() {
        Optional<byte[]> remote = forwarder(key -> Optional.empty(), 5, false).read(KEY);
        Optional<byte[]> local = forwarder(key -> Optional.of(new FastEntryService.Served(5,
                new byte[] {3})), 5, false).read(KEY);

        assertThat(remote).hasValueSatisfying(b -> assertThat(b).containsExactly(7));
        assertThat(local).hasValueSatisfying(b -> assertThat(b).containsExactly(3));
        assertThat(asked).as("the leader once, never for the local read")
                .containsExactly("l:1 uid-me");
    }

    @Test
    void anANSWEROfAnotherTermOrBelowTheFenceOrAFailureIsNone() {
        Optional<byte[]> otherTerm = forwarder(key -> Optional.empty(), 6, false).read(KEY);
        fence = 6;
        Optional<byte[]> fenced = forwarder(key -> Optional.empty(), 5, false).read(KEY);
        fence = 5;
        Optional<byte[]> failed = forwarder(key -> Optional.empty(), 5, true).read(KEY);

        assertThat(otherTerm).isEmpty();
        assertThat(fenced).as("a successor fenced this pod").isEmpty();
        assertThat(failed).isEmpty();
    }

    @Test
    void aKEYOfAnotherPrefixOrAnUnjoinedTermAsksNoOne() {
        FastEntryForwarder forwarder = forwarder(key -> Optional.empty(), 5, false);

        assertThat(forwarder.read(FastKey.of("q", 5, 0, S, 10))).isEmpty();
        assertThat(forwarder.read(FastKey.of("p", 4, 0, S, 10))).isEmpty();
        assertThat(forwarder.read("p/data/x")).isEmpty();

        assertThat(asked).isEmpty();
    }
}
