// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.readLatest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * An unchecked failure in a term start gives the term back, and one in the
 * closer starts it anyway (M13.27d review round 1, P2, T4): the election
 * catches only {@code IOException}, so neither may escape with the lease held.
 */
class FastTermOpeningGuardsTest {

    private static FastLeaseFence quietFence() {
        return new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                new FastLeaseFenceHolderTest.Mono());
    }

    /** {@code backing}, whose every roster creation throws unchecked. */
    private static BinStore brokenRosters(MemoryBinStore backing) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("putIfAbsent")
                            && ((String) args[0]).endsWith(".roster")) {
                        throw new IllegalStateException("a bug below the start");
                    }
                    try {
                        return method.invoke(backing, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    void anUNCHECKEDStartFailureGivesTheTermBack() {
        FastTermOpeningTest.Term term = new FastTermOpeningTest.Term();
        BinStore store = brokenRosters(new MemoryBinStore());
        FastTermOpening opening = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close, quietFence(), FastTermStartTest.SELF,
                Map::of);

        assertThatThrownBy(() -> opening.open(1, term)).isInstanceOf(IllegalStateException.class);

        assertThat(term.closed).isTrue();
    }

    @Test
    void anUNCHECKEDCloseFailureStillStartsTheTerm() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermOpeningTest.Term term = new FastTermOpeningTest.Term();
        FastTermOpening opening = new FastTermOpening(new FastTermStart(store, "p"),
                (epoch, unclosed) -> {
                    throw new IllegalStateException("a bug in the closer");
                }, quietFence(), FastTermStartTest.SELF, Map::of);

        assertThat(opening.open(1, term)).isPresent();

        assertThat(term.closed).isFalse();
        assertThat(readLatest(store)).isEqualTo(1);
    }
}
