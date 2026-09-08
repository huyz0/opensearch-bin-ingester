// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.Version;
import binjava.binstore.backend.MemoryBinStore;
import binjava.sequencer.FaultInjectingStore.Faults;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The WITHHELD write: the response was lost and NOTHING landed (M4.13c).
 *
 * <p>⚠️ THE MODEL ALREADY KNEW THIS CASE EXISTED AND THE INJECTOR COULD NOT
 * PRODUCE IT. {@code AmbiguousPutStore.Mode.LOST} names exactly this shape, but
 * {@code FaultInjectingStore}'s ambiguous arms attempt the write and THEN throw,
 * so whenever the CAS would have won the fault always resolves as LANDED. A
 * sweep built on that injector cannot produce "the renew threw, nothing landed,
 * and the retry with the same version must succeed" -- half of what M4.3d and
 * M4.3h are about.
 *
 * <p>⚠️ BOTH ARMS ARE AMBIGUOUS TO THE CALLER, which is why this is a separate
 * fault CLASS and not a coin flipped inside the existing one. The caller cannot
 * distinguish them and must not assume either; the difference is what the STORE
 * did, and attributing an outcome to it is what M4.13d needs.
 */
class FaultInjectingStoreWithheldTest {

    private static final String KEY = "bins/ctl/lease/0.json";

    private static Body body(String s) {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new Body(b.length, () -> new ByteArrayInputStream(b));
    }

    /** Every class off but {@code withheldPut}, which always fires. */
    private static Faults onlyWithheld() {
        return new Faults(0, 0, 0, 1.0);
    }

    @Test
    void aWITHHELDPutIfAbsentThrowsAndNOTHINGLands() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = new FaultInjectingStore(backing, 1, onlyWithheld());

        assertThatThrownBy(() -> store.putIfAbsent(KEY, body("v1")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("nothing landed");

        // ⚠️ ASSERTED ON THE BACKING STORE, not through the injector, whose own
        // `get` can be faulted. This is the whole difference from `ambiguousPut`.
        assertThat(backing.stat(KEY)).as("the withheld write must not be there").isEmpty();
        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::kind)
                .containsExactly("withheldPut");
    }

    /**
     * ⚠️ THE SHAPE THE ROW EXISTS FOR: the renew threw, nothing landed, and the
     * retry WITH THE SAME VERSION must succeed. Against the LANDED arm that
     * retry loses -- the version has already moved -- so a lease implementation
     * that treats every ambiguous CAS failure as "I may have been fenced" is
     * indistinguishable from a correct one until this arm exists.
     */
    @Test
    void aWITHHELDPutIfMatchLeavesTheVERSIONUnchangedSoTheRetryWins() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        Version v0 = backing.put(KEY, body("v0"));

        FaultInjectingStore store = new FaultInjectingStore(backing, 1, onlyWithheld());
        assertThatThrownBy(() -> store.putIfMatch(KEY, body("v1"), v0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("nothing landed");

        Optional<Version> retry = backing.putIfMatch(KEY, body("v1"), v0);
        assertThat(retry).as("the version never moved, so the same-version retry wins").isPresent();
    }

    /**
     * ⚠️ THE PRECEDENCE IS PINNED, because both classes are independent coins
     * and both can fire on one call. The LANDED arm wins, and the withheld draw
     * is still CONSUMED -- if it were not, enabling `ambiguousPut` would shift
     * `withheldPut`'s stream, which is the aliasing this file's own comments
     * record measuring at 19x and then designing away.
     */
    @Test
    void whenBOTHFireTheLANDEDArmWinsAndTheWriteIsThere() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store =
                new FaultInjectingStore(backing, 1, new Faults(0, 1.0, 0, 1.0));

        assertThatThrownBy(() -> store.putIfAbsent(KEY, body("v1")))
                .isInstanceOf(IOException.class);

        assertThat(backing.stat(KEY)).as("LANDED won, so the bytes are there").isPresent();
        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::kind)
                .containsExactly("ambiguousPut");
    }

    /**
     * ⚠️ THE PROPERTY THE WHOLE STREAM DESIGN EXISTS FOR, asserted for the NEW
     * class rather than assumed to carry over: turning `withheldPut` on must not
     * move where the other three classes fire for the same seed. This file's
     * comments record 2000-of-2000 seeds shifting when that broke before, and
     * criterion 1's per-class attribution is what it costs.
     */
    @Test
    void enablingWITHHELDDoesNotMoveTheOtherClassesFaults() throws Exception {
        List<Integer> without = firingPositions(new Faults(0.05, 0.05, 0.1, 0));
        List<Integer> with = firingPositions(new Faults(0.05, 0.05, 0.1, 1.0));

        assertThat(without).as("the other classes must fire somewhere at all").isNotEmpty();
        assertThat(with).isEqualTo(without);
    }

    /**
     * Which of the first 200 calls {@code unreachable} or {@code ambiguousPut}
     * faulted on.
     *
     * <p>⚠️ {@code duplicatePut} IS DELIBERATELY EXCLUDED, and the distinction
     * is the one this file's sibling already draws for {@code unreachable}: a
     * class that throws only MASKS a later class where it actually fired, and
     * masking is not aliasing. {@code duplicatePut} is an action taken AFTER the
     * write, so any earlier throw legitimately preempts it -- including
     * `withheldPut`'s. Counting it here would make this test fail for the one
     * reason that is correct behaviour, which is how a real alignment defect
     * would then get "fixed" by deleting the assertion.
     */
    private static List<Integer> firingPositions(Faults faults) throws IOException {
        FaultInjectingStore store =
                new FaultInjectingStore(new MemoryBinStore(), 7, faults);
        List<Integer> at = new java.util.ArrayList<>();
        for (int i = 0; i < 200; i++) {
            int before = store.injected().size();
            try {
                store.putIfAbsent("bins/k" + i, body("x"));
            } catch (IOException expected) {
                // a fault fired; which one is read from `injected()` below
            }
            List<FaultInjectingStore.Injected> now = store.injected();
            for (int j = before; j < now.size(); j++) {
                String kind = now.get(j).kind();
                if (kind.equals("unreachable") || kind.equals("ambiguousPut")) {
                    at.add(i);
                }
            }
        }
        return at;
    }

    @Test
    void aWITHHELDProbabilityOutsideZeroToOneIsREJECTED() {
        assertThatThrownBy(() -> new Faults(0, 0, 0, 1.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("withheldPut");
        assertThatThrownBy(() -> new Faults(0, 0, 0, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("withheldPut");
    }

    /**
     * ⚠️ THE SAME PRECEDENCE ON `putIfMatch`, WHICH IS THE VERB THIS ROW IS
     * ABOUT. Pinning it on `putIfAbsent` alone is not enough and the gap was
     * MEASURED: moving the withheld branch above the ambiguous one in
     * `putIfMatch` left the whole `:sequencer` suite green, while the identical
     * flip in `putIfAbsent` was killed.
     *
     * <p>⚠️ AND IT INVERTS EXACTLY THE DISCRIMINATION THE CLASS EXISTS TO
     * CREATE. On a co-firing lease-renew CAS the wrong order resolves as
     * NOTHING-LANDED: the version does not move, so the same-version retry
     * SUCCEEDS where the LANDED arm requires it to lose. An M4.10e or M4.3h
     * lease verdict would flip from fenced to not-fenced, silently.
     */
    @Test
    void onPUTIFMATCHTooTheLANDEDArmWinsAndTheVERSIONMoves() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        Version v0 = backing.put(KEY, body("v0"));

        FaultInjectingStore store =
                new FaultInjectingStore(backing, 1, new Faults(0, 1.0, 0, 1.0));
        assertThatThrownBy(() -> store.putIfMatch(KEY, body("v1"), v0))
                .isInstanceOf(IOException.class);

        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::kind)
                .as("LANDED wins, so the ambiguous arm is what fired")
                .containsExactly("ambiguousPut");
        assertThat(backing.putIfMatch(KEY, body("v2"), v0))
                .as("the version MOVED, so the same-version retry must now lose")
                .isEmpty();
    }
}
