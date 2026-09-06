// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Checkpoint;
import binjava.format.Checkpoint.StreamOffsets;
import binjava.format.RunKey;
import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Recovery reads only the keys it wrote, and ignores the rest (M4.8b1).
 *
 * <p>{@code BinStore.list} is flat and undelimited, so everything under
 * {@code CommitLog.logPrefix()} comes back from one LIST — including the
 * {@code ckpt/...} objects M4.8b2 will write there. Both readers took every
 * key as a chain entry: {@link ChainReplay#chainEnd} takes the LAST one, and
 * {@code ckpt/} sorts after every {@code <seq:016x>.delta} because every hex
 * digit is below {@code 'k'}; {@code ChainReplay#applyChain} GETs and decodes
 * each in turn. Either way the first checkpoint ever written makes a takeover
 * throw, {@code LocalSequencer.start} release the lease, and the next node
 * repeat — forever, since nothing deletes the object.
 *
 * <p>⚠️ THE TWO SITES ARE DRIVEN SEPARATELY, against one UNSEALED chain. One
 * fixture cannot cover both: in a takeover {@code start} seals the inheritable
 * ancestor BETWEEN {@code recoverChainEnd} and the crossing, and a seal is
 * written at a {@code .delta} key that sorts BEFORE {@code ckpt/}, so
 * {@code applyChain} returns at that {@code Seal} and never reaches a
 * checkpoint. The takeover that gives one site a fixture destroys the other's.
 *
 * <p>⚠️ TWO INTERLOPING KEYS, NOT ONE, and neither is redundant. A single
 * checkpoint-shaped key at the END leaves three mutants alive, and the
 * {@code .delta.tmp} key kills all three because it sorts BETWEEN two deltas
 * and CONTAINS {@code .delta}: a deny-list on {@code .ckpt} admits it, a
 * {@code contains(".delta")} predicate admits it, and a {@code return} or
 * {@code break} in place of a skip stops the walk THERE — where stopping is
 * observable, unlike at a trailing key.
 *
 * <p>⚠️ THE FILTER IS AN ALLOW-LIST ON THE KEY, NEVER A CAUGHT DECODE FAILURE.
 * {@code CommitLogTest.recoveryStopsOnAnEntryItCannotUnderstandRatherThanSkippingIt}
 * is the test that says so — {@code catch (IOException) { continue; }} in this
 * loop leaves a reader applying a discarded suffix, which is I3 — and it must
 * still pass unchanged. {@link #recoveryDoesNotGetNonDeltaKeysAtAllHoweverManyThereAre}
 * is what forbids the swallowing form here: it holds the GET count CONSTANT in
 * the number of interlopers rather than merely asserting nothing threw.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ChainReplayNonDeltaKeyTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");
    private static final RunKey RA = new RunKey(A, 0);
    private static final RunKey RB = new RunKey(B, 0);

    private static Map<RunKey, Integer> counts(RunKey key, int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, n);
        return m;
    }

    /** Three deltas at slots 0..2, unsealed, so both readers walk the whole chain. */
    private static void seedChain(MemoryBinStore store) throws Exception {
        CommitLog log = new CommitLog(store, "bins", 1);
        log.commit("seg/0", counts(RA, 3));
        log.commit("seg/1", counts(RA, 4));
        log.commit("seg/2", counts(RB, 5));
    }

    private static void put(MemoryBinStore store, String key, byte[] body) throws Exception {
        store.putIfAbsent(key, new Body(body.length, () -> new ByteArrayInputStream(body)));
    }

    /**
     * A checkpoint exactly as M4.8b2 will write one — real bytes under the real
     * key grammar, not a stand-in. Its {@code BCKP} magic is why decoding it as
     * a chain entry throws.
     */
    private static void seedCheckpoint(MemoryBinStore store, String logPrefix, long seq)
            throws Exception {
        Checkpoint ckpt = new Checkpoint(seq,
                Map.of(RA, new StreamOffsets(7, 0)), Map.of("poda", 1L));
        put(store, String.format(java.util.Locale.ROOT, "%sckpt/%016x.ckpt", logPrefix, seq),
                ckpt.encode());
    }

    /**
     * A key that sorts BETWEEN slots 1 and 2 and whose name CONTAINS
     * {@code .delta}. ⚠️ ITS BODY MUST NOT DECODE AS A CHAIN ENTRY: seed valid
     * delta bytes and both predicate mutants survive, because {@code fold}'s
     * {@code Math::max} then yields an identical offsets map either way.
     */
    private static void seedInterposed(MemoryBinStore store, String logPrefix) throws Exception {
        put(store, logPrefix + "0000000000000001.delta.tmp",
                "not a chain entry".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void recoveryIgnoresKeysUnderTheLogPrefixThatAreNotChainEntries() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        seedChain(store);
        String logPrefix = new CommitLog(store, "bins", 1).logPrefix();

        // ⚠️ THE BASELINE IS MEASURED, not written down: what recovery yields
        // over this chain with nothing interposed is what it must still yield.
        CommitLog clean = new CommitLog(store, "bins", 1);
        clean.recover();

        seedInterposed(store, logPrefix);
        seedCheckpoint(store, logPrefix, 3);

        CommitLog after = new CommitLog(store, "bins", 1);
        after.recover();

        // ⚠️ NOT "does not throw". A `return` in place of a skip stops the walk
        // at the interposed key and throws nothing at all; only comparing the
        // recovered STATE sees the truncated chain.
        assertThat(after.nextSequence())
                .as("the chain ends where it ended before a checkpoint was written beside it")
                .isEqualTo(clean.nextSequence());
        assertThat(after.nextOffset(RA)).isEqualTo(clean.nextOffset(RA));
        assertThat(after.nextOffset(RB))
                .as("the delta AFTER the interposed key still applies")
                .isEqualTo(clean.nextOffset(RB));
    }

    @Test
    void recoveryDoesNotGetNonDeltaKeysAtAllHoweverManyThereAre() throws Exception {
        // ⚠️ GETs, NOT LISTs. A LIST-count assertion is invariant under the fix
        // -- one flat LIST returns every key either way -- and passes with no
        // filter at all. The GET count is what separates ignoring a key from
        // reading it and swallowing the failure.
        MemoryBinStore backing = new MemoryBinStore();
        seedChain(backing);
        String logPrefix = new CommitLog(backing, "bins", 1).logPrefix();
        seedInterposed(backing, logPrefix);
        seedCheckpoint(backing, logPrefix, 3);

        CountingBinStore one = new CountingBinStore(backing);
        new CommitLog(one, "bins", 1).recover();

        for (long seq = 4; seq <= 12; seq++) {
            seedCheckpoint(backing, logPrefix, seq);
        }
        CountingBinStore many = new CountingBinStore(backing);
        new CommitLog(many, "bins", 1).recover();

        assertThat(many.counts().gets())
                .as("recovery's GET count is constant in the number of checkpoints beside the chain")
                .isEqualTo(one.counts().gets());
    }

    @Test
    void aValidEntryAtAKeyThisLogNeverWroteIsNotPartOfItsChain() throws Exception {
        // ⚠️ THE FILTER IS ON THE GRAMMAR, NOT ON THE EXTENSION, and this is the
        // test that says which. `endsWith(".delta")` passes every other test
        // here, because every interloper they seed has a different suffix -- but
        // `BinStore.list` is FLAT, so a key one segment deeper still comes back
        // from this LIST, and M4.8b2 is about to put a `ckpt/` segment under
        // exactly this prefix.
        // ⚠️ THE BODY IS A VALID DELTA ON PURPOSE, which is what makes this the
        // worst shape rather than a curiosity: it DECODES, so nothing throws and
        // no operator sees anything. A foreign object simply drives this chain's
        // `nextSequence` past its real end, and the next commit is written into
        // a slot the chain never reached -- I2, silently.
        MemoryBinStore store = new MemoryBinStore();
        seedChain(store);
        String logPrefix = new CommitLog(store, "bins", 1).logPrefix();

        CommitLog clean = new CommitLog(store, "bins", 1);
        clean.recover();
        CommitLog cleanEnd = new CommitLog(store, "bins", 1);
        cleanEnd.recoverChainEnd();

        // Sorts after every entry, decodes as a CommitDelta, and its BODY
        // carries sequence 9 -- past this chain's real end at 2.
        // ⚠️ THE BODY'S SEQUENCE IS WHAT MATTERS, NOT THE KEY'S, and a first
        // draft of this fixture got that wrong: it wrote one delta from a fresh
        // log, so the body said 0 while the key said 9, and `applyChain`'s
        // `Math.max(3, 0 + 1)` absorbed it. Only the chain-end half failed, and
        // the assertion below would have been decoration.
        CommitLog other = new CommitLog(store, "other", 1);
        byte[] alien = null;
        for (int i = 0; i <= 9; i++) {
            alien = other.commit("seg/x", counts(RA, 2)).encode();
        }
        put(store, String.format(java.util.Locale.ROOT, "%sckpt/%016x.delta", logPrefix, 9L),
                alien);

        CommitLog after = new CommitLog(store, "bins", 1);
        after.recover();
        CommitLog afterEnd = new CommitLog(store, "bins", 1);
        afterEnd.recoverChainEnd();

        assertThat(after.nextSequence())
                .as("a decodable entry at a key this log never wrote does not extend its chain")
                .isEqualTo(clean.nextSequence());
        assertThat(after.nextOffset(RA))
                .as("nor do its offsets enter this chain's history")
                .isEqualTo(clean.nextOffset(RA));
        assertThat(afterEnd.nextSequence())
                .as("nor does it move where the chain ends")
                .isEqualTo(cleanEnd.nextSequence());
    }

    @Test
    void findingTheChainEndIgnoresKeysThatAreNotChainEntries() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        seedChain(store);
        String logPrefix = new CommitLog(store, "bins", 1).logPrefix();

        CommitLog clean = new CommitLog(store, "bins", 1);
        clean.recoverChainEnd();

        seedInterposed(store, logPrefix);
        seedCheckpoint(store, logPrefix, 3);

        CommitLog after = new CommitLog(store, "bins", 1);
        after.recoverChainEnd();

        // ⚠️ THE INTERPOSED KEY IS WHAT MAKES THIS ASSERTION BITE. A `break` in
        // place of a skip is EQUIVALENT against a trailing `ckpt/` key -- it
        // sorts last, so stopping there and skipping it leave the same last
        // key -- and leaves `nextSequence` short only when a non-delta key
        // sorts in the MIDDLE. Without it this test passes against the break.
        assertThat(after.nextSequence())
                .as("the last DELTA is the chain end, not the last key under the prefix")
                .isEqualTo(clean.nextSequence());
    }
}
