// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Pod;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The lease-time fence across writes whose answers are lost, and a term lost
 * on a read (M13.26 review round 1, P1 and T1-T4).
 *
 * <p>⚠️ A TAKEOVER SETTLED LATE OWES THE WAIT: told of the replaced lease only
 * when the takeover's write answered, a successor whose answer was lost and
 * whose next read found it landed assigned at once -- while the old leader,
 * paused inside its own validity, could still expose.
 */
class FastLeaseFenceAmbiguityTest {

    private static final Duration TTL = Duration.ofSeconds(10);

    /** Each conditional write of the lease answers by the next scripted outcome. */
    enum Outcome { LANDED_ANSWER_LOST, LOST, OK }

    static final class ScriptedStore implements BinStore {
        private final BinStore delegate;
        private final Deque<Outcome> script = new ArrayDeque<>();

        ScriptedStore(BinStore delegate) {
            this.delegate = delegate;
        }

        void then(Outcome... outcomes) {
            script.addAll(List.of(outcomes));
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected)
                throws IOException {
            Outcome next = script.isEmpty() ? Outcome.OK : script.poll();
            if (next == Outcome.LOST) {
                throw new IOException("lost before it landed");
            }
            Optional<Version> won = delegate.putIfMatch(key, body, expected);
            if (next == Outcome.LANDED_ANSWER_LOST) {
                throw new IOException("landed; its answer was lost");
            }
            return won;
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            return delegate.putIfAbsent(key, body);
        }

        @Override
        public InputStream get(String key) throws IOException {
            return delegate.get(key);
        }

        @Override
        public InputStream getRange(String key, long start, long endIncl) throws IOException {
            return delegate.getRange(key, start, endIncl);
        }

        @Override
        public Optional<ObjectStat> stat(String key) throws IOException {
            return delegate.stat(key);
        }

        @Override
        public Version put(String key, Body body) throws IOException {
            return delegate.put(key, body);
        }

        @Override
        public MultipartWriter multipart(String key) throws IOException {
            return delegate.multipart(key);
        }

        @Override
        public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Test
    void aTAKEOVERWhoseAnswerWasLostStillOwesTheWait() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        Pod a = FastLeaseFenceHolderTest.pod(backing, "a");
        a.leases().tryAcquire();
        ScriptedStore bStore = new ScriptedStore(backing);
        Pod b = FastLeaseFenceHolderTest.pod(bStore, "b");
        b.leases().tryAcquire();
        b.advanceBoth(TTL);
        bStore.then(Outcome.LANDED_ANSWER_LOST);
        assertThatThrownBy(() -> b.leases().tryAcquire()).isInstanceOf(IOException.class);

        assertThat(b.leases().tryAcquire()).as("the re-read finds the takeover landed")
                .isPresent();

        assertThat(b.fence().successorMayAssign()).isFalse();
        assertThat(b.fence().notBeforeWallMillis())
                .as("a's lease expired at +10 s on the shared start; the margin is 1 s")
                .isEqualTo(1_000_000L + 11_000L);
        b.advanceBoth(Duration.ofMillis(999));
        assertThat(b.fence().successorMayAssign()).isFalse();
        b.advanceBoth(Duration.ofMillis(1));
        assertThat(b.fence().successorMayAssign()).isTrue();
    }

    @Test
    void aSETTLEDRenewalKeepsTheLastConfirmedSend() throws Exception {
        ScriptedStore store = new ScriptedStore(new MemoryBinStore());
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        a.mono().advance(Duration.ofSeconds(2));
        store.then(Outcome.LANDED_ANSWER_LOST, Outcome.LOST);
        assertThatThrownBy(() -> a.leases().renew()).isInstanceOf(IOException.class);
        a.mono().advance(Duration.ofSeconds(1));
        assertThatThrownBy(() -> a.leases().renew())
                .as("settles the first renewal by its re-read, then its own write is lost")
                .isInstanceOf(IOException.class);

        a.mono().advance(Duration.ofMillis(5_999));
        assertThat(a.fence().mayExpose()).as("8.999 s after the acquisition's send").isTrue();
        a.mono().advance(Duration.ofMillis(1));

        assertThat(a.fence().mayExpose())
                .as("the settled renewal may be the landed one or the one before it; "
                        + "validity keeps the one known landed")
                .isFalse();
    }

    @Test
    void aTERMFoundLostOnARereadStopsExposing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        Pod b = FastLeaseFenceHolderTest.pod(store, "b");
        b.wall().advance(Duration.ofSeconds(11));
        b.leases().tryAcquire();

        assertThat(a.leases().tryAcquire()).as("a reads b's term, not its own").isEmpty();

        assertThat(a.fence().mayExpose()).isFalse();
    }

    @Test
    void aFIRSTAcquisitionSettledLateOwesNoWait() throws Exception {
        AmbiguousPutStore store = new AmbiguousPutStore(new MemoryBinStore(),
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        assertThatThrownBy(() -> a.leases().tryAcquire()).isInstanceOf(IOException.class);

        assertThat(a.leases().tryAcquire()).isPresent();

        assertThat(a.fence().successorMayAssign()).as("it replaced no lease").isTrue();
        assertThat(a.fence().notBeforeWallMillis()).isEqualTo(Long.MIN_VALUE);
    }
}
