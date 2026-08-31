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
    void aNegativeLengthIsRefused() {
        assertThatThrownBy(() -> new Body(-1, () -> new ByteArrayInputStream(new byte[0])))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Body(0, null)).isInstanceOf(NullPointerException.class);
    }
}
