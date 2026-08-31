// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * ⚠️ {@link Body} shipped with validation, a defensive copy and a re-open
 * contract, and no test: gutting all three left the tree green. {@code Version}
 * and {@code ObjectStat} each got a unit test for exactly this.
 */
class BodyTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void ofBytesReportsTheRealLength() {
        assertThat(Body.ofBytes(bytes("hello")).length()).isEqualTo(5);
    }

    @Test
    void ofBytesCopiesSoALaterMutationCannotChangeTheBody() throws Exception {
        byte[] source = bytes("hello");
        Body body = Body.ofBytes(source);
        java.util.Arrays.fill(source, (byte) 0);
        assertThat(body.readFully()).isEqualTo(bytes("hello"));
    }

    @Test
    void theSupplierMayBeCalledMoreThanOnce() throws Exception {
        // ⚠️ Documented and never exercised. A retry re-reads the body from the
        // start, so a supplier handing out one exhausted stream would make every
        // second attempt write zero bytes.
        Body body = Body.ofBytes(bytes("hello"));
        assertThat(body.readFully()).isEqualTo(bytes("hello"));
        assertThat(body.readFully()).as("a retry must see the same bytes").isEqualTo(bytes("hello"));
    }

    @Test
    void readFullyRefusesAStreamShorterOrLongerThanTheDeclaredLength() {
        assertThatThrownBy(() -> new Body(99, () -> new ByteArrayInputStream(bytes("short"))).readFully())
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("99");
        assertThatThrownBy(() -> new Body(1, () -> new ByteArrayInputStream(bytes("longer"))).readFully())
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    void readFullyClosesTheStreamItOpened() throws Exception {
        // ⚠️ ByteArrayInputStream.close is a NO-OP, so dropping the
        // try-with-resources left every test green while a file-backed Body
        // leaked a descriptor per attempt — and the documented retry contract
        // means attempts multiply.
        java.util.concurrent.atomic.AtomicInteger closed =
                new java.util.concurrent.atomic.AtomicInteger();
        Body body =
                new Body(5, () -> new ByteArrayInputStream(bytes("hello")) {
                    @Override
                    public void close() throws java.io.IOException {
                        closed.incrementAndGet();
                        super.close();
                    }
                });
        body.readFully();
        assertThat(closed.get()).as("the stream must be closed").isEqualTo(1);
        body.readFully();
        assertThat(closed.get()).as("and closed again on the retry").isEqualTo(2);
    }

    @Test
    void checkedStreamRefusesAShortBodyWithoutMaterialisingIt() throws Exception {
        // ⚠️ The C8-preserving path. readFully enforces the same rule but only by
        // holding the whole object, which is what Body exists to prevent.
        Body lying = new Body(99, () -> new ByteArrayInputStream(bytes("short")));
        assertThatThrownBy(
                () -> {
                    try (java.io.InputStream in = lying.checkedStream()) {
                        in.readAllBytes();
                    }
                })
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("99");
    }

    @Test
    void checkedStreamRefusesAShortBodyEvenIfTheReaderStopsEarly() throws Exception {
        // ⚠️ A backend that reads only what it wants would otherwise never learn
        // the body was short, so the check fires on close as well as at EOF.
        Body lying = new Body(99, () -> new ByteArrayInputStream(bytes("short")));
        assertThatThrownBy(() -> lying.checkedStream().close())
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    void checkedStreamPassesAHonestBodyThrough() throws Exception {
        Body honest = Body.ofBytes(bytes("hello"));
        try (java.io.InputStream in = honest.checkedStream()) {
            assertThat(in.readAllBytes()).isEqualTo(bytes("hello"));
        }
    }

    @Test
    void checkedStreamRefusesAnOverLongBodyBeforeItReachesEndOfInput() throws Exception {
        // ⚠️ A consumer that OVER-READS but never reaches EOF. The previous
        // version used readAllBytes, which drives the stream to EOF -- and at
        // EOF the lazy design throws from inside that same call, so
        // assertThatThrownBy could not tell WHERE the throw came from. Review
        // restored the pre-fix lazy design with the whole suite green.
        //
        // 11 bytes behind a 5-byte request: read returns 5 and never sees EOF,
        // so an eager check throws and a lazy one returns 5 in silence. That is
        // the difference that matters -- a streaming backend must be stopped
        // before it commits the request, not told afterwards.
        Body lying = new Body(2, () -> new ByteArrayInputStream(bytes("much longer")));
        try (java.io.InputStream in = lying.checkedStream()) {
            byte[] buf = new byte[5];
            assertThatThrownBy(() -> in.read(buf, 0, 5))
                    .as("the read itself must refuse, before end of input")
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("more");
        } catch (java.io.IOException expectedOnClose) {
            // ⚠️ close() re-verifies; the latch means it must NOT throw again.
            throw new AssertionError("close must not throw a second time", expectedOnClose);
        }
    }

    @Test
    void checkedStreamRefusesAnOverLongBodyEvenWhenTheConsumerNeverOverReads()
            throws Exception {
        // ⚠️ THE case the eager check missed, and the one the task named: a
        // consumer that reads exactly `length` bytes and stops never triggers a
        // seen > length check, so the over-long body was accepted and S3 would
        // store a truncated object that reads back as a valid short segment.
        Body lying = new Body(5, () -> new ByteArrayInputStream(bytes("hello world")));
        java.io.InputStream in = lying.checkedStream();
        assertThat(in.readNBytes(5)).as("the consumer reads only what it was promised")
                .isEqualTo(bytes("hello"));
        assertThatThrownBy(in::close)
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("more");
    }

    @Test
    void checkedStreamRefusesMarkAndResetRatherThanMiscounting() throws Exception {
        // ⚠️ A client that marks, streams and resets to retry a 500 would see
        // seen == 2 x length at close and fail a RETRYABLE upload permanently.
        Body honest = Body.ofBytes(bytes("hello"));
        try (java.io.InputStream in = honest.checkedStream()) {
            assertThat(in.markSupported()).isFalse();
            // ⚠️ mark() must be a no-op too. FilterInputStream forwards it to the
            // delegate, so a client could mark successfully and only discover
            // the refusal at reset() -- worse than being told up front.
            in.mark(16);
            assertThatThrownBy(in::reset).isInstanceOf(java.io.IOException.class);
            // ⚠️ Drain before close, or the close-time verify refuses an
            // unread body -- which it should, and which is what an earlier
            // version of this test tripped over.
            assertThat(in.readAllBytes()).isEqualTo(bytes("hello"));
        }
    }

    @Test
    void checkedStreamClosesTheDelegateEvenWhenItRefusesTheBody() throws Exception {
        // ⚠️ The failure path is where a descriptor leak actually happens: a
        // short body throws from close(), and without the try/finally the
        // delegate is never closed. Nothing asserted it, so `verify(); in.close();`
        // survived while the comment above it claimed otherwise.
        java.util.concurrent.atomic.AtomicInteger closed =
                new java.util.concurrent.atomic.AtomicInteger();
        Body lying =
                new Body(99, () -> new ByteArrayInputStream(bytes("short")) {
                    @Override
                    public void close() throws java.io.IOException {
                        closed.incrementAndGet();
                        super.close();
                    }
                });
        java.io.InputStream in = lying.checkedStream();
        assertThatThrownBy(in::close).isInstanceOf(java.io.IOException.class);
        assertThat(closed.get()).as("the delegate must be closed anyway").isEqualTo(1);
    }

    @Test
    void checkedStreamClosesTheDelegateOnTheHappyPathToo() throws Exception {
        // ⚠️ Closing only on the failure path leaks a descriptor on EVERY
        // successful write -- far worse than leaking on the rare failure. Both
        // honest-path cases used Body.ofBytes, whose close is a no-op with no
        // counter, so `catch (IOException e) { in.close(); throw e; }` was green.
        java.util.concurrent.atomic.AtomicInteger closed =
                new java.util.concurrent.atomic.AtomicInteger();
        Body honest =
                new Body(5, () -> new ByteArrayInputStream(bytes("hello")) {
                    @Override
                    public void close() throws java.io.IOException {
                        closed.incrementAndGet();
                        super.close();
                    }
                });
        try (java.io.InputStream in = honest.checkedStream()) {
            assertThat(in.readAllBytes()).isEqualTo(bytes("hello"));
        }
        assertThat(closed.get()).as("the delegate must close on success").isEqualTo(1);
    }

    @Test
    void checkedStreamCountsSkippedBytes() throws Exception {
        Body honest = Body.ofBytes(bytes("hello"));
        try (java.io.InputStream in = honest.checkedStream()) {
            // ⚠️ Ask for MORE than exists. skip(5) on a 5-byte body makes the
            // argument indistinguishable from the return value, so counting the
            // request instead of the result survived -- and a delegate that
            // skips fewer bytes than asked would then refuse an honest body.
            assertThat(in.skip(99)).isEqualTo(5);
        }
    }

    @Test
    void aNegativeLengthIsRefused() {
        assertThatThrownBy(() -> new Body(-1, () -> new ByteArrayInputStream(new byte[0])))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Body(0, null)).isInstanceOf(NullPointerException.class);
    }
}
