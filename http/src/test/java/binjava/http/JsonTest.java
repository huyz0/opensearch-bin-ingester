// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The action-line reader is hand-written and reads untrusted input, so its
 * REFUSALS matter as much as its successes: a lenient parser turns a malformed
 * request into a differently-shaped accepted one.
 */
class JsonTest {

    @Test
    void anIntegerIsALongAndNotADouble() {
        // ⚠️ 2^53+1. Parsed as a double this comes back as ...992, and an
        // external _version that silently changes value loses to the wrong write.
        Object v = ((Map<?, ?>) Json.parse("{\"_version\":9007199254740993}")).get("_version");
        assertThat(v).isInstanceOf(Long.class).isEqualTo(9007199254740993L);
    }

    @Test
    void aFractionalNumberStaysADoubleSoItCanBeRejectedLater() {
        Object v = ((Map<?, ?>) Json.parse("{\"n\":1.5}")).get("n");
        // ⚠️ The VALUE too. Asserting only the type leaves `Double.valueOf(0)`
        // alive, which would silently rewrite every fractional number to zero.
        assertThat(v).isInstanceOf(Double.class).isEqualTo(1.5d);
        assertThat(((Map<?, ?>) Json.parse("{\"n\":1e3}")).get("n")).isEqualTo(1000.0d);
        assertThat(((Map<?, ?>) Json.parse("{\"n\":-2.5e-2}")).get("n")).isEqualTo(-0.025d);
    }

    @Test
    void escapesInAStringAreDecoded() {
        Object v = ((Map<?, ?>) Json.parse("{\"_id\":\"a\\\"b\\\\c\\u0041\\n\"}")).get("_id");
        assertThat(v).isEqualTo("a\"b\\cA\n");
    }

    @Test
    void deepNestingIsRefusedRatherThanOverflowingTheStack() {
        // ⚠️ A StackOverflowError is an Error, not an Exception, so it escapes
        // the 400 path entirely and takes the connection with it.
        String deep = "[".repeat(200) + "]".repeat(200);
        assertThatThrownBy(() -> Json.parse(deep))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("nested deeper");
    }

    @Test
    void theDepthLimitIsExactlyWhereItSaysItIs() {
        // ⚠️ Pins the BOUNDARY, not merely "something absurd is refused". Without
        // this, `depth > MAX_DEPTH` -> `depth >= MAX_DEPTH`, or widening the
        // constant to 100, both survive.
        //
        // ⚠️ 33 accepted / 34 refused, not 32/33: the OUTERMOST value is parsed
        // at depth 0, so the nth bracket is parsed at depth n-1 and the refusal
        // at `depth > 32` first bites on the 34th. Writing the naive 32/33 here
        // failed, which is the whole reason a boundary gets a test.
        assertThat(Json.parse("[".repeat(33) + "]".repeat(33))).isNotNull();
        assertThatThrownBy(() -> Json.parse("[".repeat(34) + "]".repeat(34)))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("nested deeper");
    }

    @Test
    void trailingContentIsRefused() {
        assertThatThrownBy(() -> Json.parse("{} {}"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("trailing content");
    }

    @Test
    void theLenientShapesAreAllRefused() {
        // Each of these is accepted by some JSON parser somewhere.
        for (String bad : new String[] {
            "{'_id':'a'}",          // single quotes
            "{_id:\"a\"}",          // unquoted key
            "{\"a\":1,}",           // trailing comma
            "{\"a\":NaN}",          // NaN
            "{\"a\":01}",           // leading zero
            "{\"a\":}",             // missing value
            "{\"a\"}",              // missing colon
            "{\"a\":\"unterminated",
            "{\"a\":1.}",          // no digit after the point
            "{\"a\":1e}",          // no digit in the exponent
        }) {
            assertThatThrownBy(() -> Json.parse(bad))
                    .as("must refuse: %s", bad)
                    .isInstanceOf(BulkParseException.class);
        }
    }

    @Test
    void aRawControlCharacterInAStringIsRefused() {
        assertThatThrownBy(() -> Json.parse("{\"a\":\"x\ny\"}"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("control character");
    }

    @Test
    void anUnknownEscapeIsRefused() {
        assertThatThrownBy(() -> Json.parse("{\"a\":\"\\q\"}"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("unknown JSON escape");
        assertThatThrownBy(() -> Json.parse("{\"a\":\"\\u00zz\"}"))
                .isInstanceOf(BulkParseException.class)
                .hasMessageContaining("four hex digits");
    }
}
