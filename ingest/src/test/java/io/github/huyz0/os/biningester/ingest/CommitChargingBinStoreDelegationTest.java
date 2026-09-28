// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Every PUT kind the counter counts as a commit is charged, the answers are
 * the delegate's, and a body past the read cap is charged without being read
 * (M11.22).
 */
class CommitChargingBinStoreDelegationTest {

    private static final String KEY = "bins/c/ctl/log/1/00000000000000000004.delta";
    private static final UUID INDEX = UUID.fromString("00000000-0000-4000-8000-0000000000aa");

    private static byte[] delta() {
        return new CommitDelta(4, List.of(new SegmentCommit("bins/c/data/a.bseg",
                List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))))).encode();
    }

    private static long charged(IndexCostLedger ledger) {
        return ledger.snapshot().indices().stream()
                .mapToLong(i -> i.micros().get(Charge.COMMIT_PUT)).sum();
    }

    @Test
    void aPutAndAPutIfMatchAreChargedAndAnswerWhatTheStoreAnswered() throws Exception {
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        IndexCostLedger ledger = new IndexCostLedger();
        BinStore store = new CommitChargingBinStore(counting, ledger);

        Version written = store.put(KEY, Body.ofBytes(delta()));
        Optional<Version> matched = store.putIfMatch(KEY, Body.ofBytes(delta()), written);

        assertThat(matched).as("the delegate's answer, not an invented one").isPresent();
        assertThat(charged(ledger)).as("⚠️ BOTH ARE COMMIT PUTS TO THE COUNTER, BOTH CHARGED")
                .isEqualTo(2 * IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(ledger.snapshot().totalMicros(Charge.COMMIT_PUT))
                .isEqualTo(counting.putPurposeCounts().commitPuts()
                        * IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void aBodyAtTheCapIsReadAndOnePastItIsChargedUnattributedUnread() throws Exception {
        IndexCostLedger ledger = new IndexCostLedger();
        byte[] bytes = delta();
        AtomicBoolean openedPastTheCap = new AtomicBoolean();

        // ⚠️ THE DECLARED LENGTH decides, so the store under it reads no body.
        new CommitChargingBinStore(ignoringBodies(null), ledger).putIfAbsent(KEY, new Body(CommitChargingBinStore.MAX_DECODED_BYTES,
                () -> new ByteArrayInputStream(bytes)));
        IndexCostLedger past = new IndexCostLedger();
        new CommitChargingBinStore(ignoringBodies(null), past).putIfAbsent(KEY,
                new Body(CommitChargingBinStore.MAX_DECODED_BYTES + 1, () -> {
                    openedPastTheCap.set(true);
                    return new ByteArrayInputStream(bytes);
                }));

        assertThat(charged(ledger)).as("at the cap: read and split")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(past.snapshot().unattributed().get(Charge.COMMIT_PUT))
                .as("past it: charged whole, never dropped")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(openedPastTheCap).as("and never read to be charged").isFalse();
    }

    @Test
    void aDeleteAndACloseReachTheDelegate() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        memory.put(KEY, Body.ofBytes(delta()));
        AtomicBoolean closed = new AtomicBoolean();
        BinStore store = new CommitChargingBinStore(memory, new IndexCostLedger());

        store.delete(List.of(KEY));

        assertThat(memory.stat(KEY)).as("the delete reached the store").isEmpty();
        new CommitChargingBinStore(ignoringBodies(closed), new IndexCostLedger()).close();
        assertThat(closed).as("and so did the close").isTrue();
    }

    /** A store that accepts every write without reading its body, and says when closed. */
    private static BinStore ignoringBodies(AtomicBoolean closed) {
        return (BinStore) java.lang.reflect.Proxy.newProxyInstance(
                BinStore.class.getClassLoader(), new Class<?>[] {BinStore.class},
                (self, method, args) -> switch (method.getName()) {
                    case "putIfAbsent" -> Optional.of(new Version("v"));
                    case "close" -> {
                        closed.set(true);
                        yield null;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
