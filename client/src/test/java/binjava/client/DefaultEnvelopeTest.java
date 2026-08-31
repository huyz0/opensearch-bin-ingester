// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.OpType;
import binjava.format.SegmentRecord;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** WARNING: byte concatenation around an opaque payload -- never a parse (ADR-0020). */
class DefaultEnvelopeTest {

    private static String assemble(SegmentRecord r) {
        return new String(DefaultEnvelope.assemble(r), StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void anIndexCarriesIdOpTypeVersionAndSource() {
        String json = assemble(new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(7),
                bytes("{\"n\":1}")));
        // WARNING: _version is a JSON STRING. Not a stylistic choice -- the
        // OpenSearch DEFAULT mapper casts the field to String, so the numeric
        // literal 7 fails the batch with a ClassCastException that surfaces only
        // as "0 documents indexed". Observed in SearchableIT before the fix.
        assertThat(json).isEqualTo(
                "{\"_id\":\"doc-1\",\"_op_type\":\"index\",\"_version\":\"7\",\"_source\":{\"n\":1}}");
    }

    @Test
    void theSourceIsCopiedThroughByteForByte() {
        // WARNING: the ingester never looked inside the body and neither does
        // this. A parse would reject documents the producer considers valid and
        // put a JSON reader on the hot path of every record.
        String odd = "{\"weird\":\"x \\\" unicode\",\"trailing\":  1e10 }";
        String json = assemble(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                bytes(odd)));
        assertThat(json).endsWith(",\"_source\":" + odd + "}");
    }

    @Test
    void aDeleteHasNoSource() {
        String json = assemble(new SegmentRecord("gone", OpType.DELETE, OptionalLong.of(4),
                new byte[0]));
        // WARNING: an empty _source would make this an index of {} -- the silent
        // resurrection acceptance criterion 0 exists to refuse.
        assertThat(json).isEqualTo(
                "{\"_id\":\"gone\",\"_op_type\":\"delete\",\"_version\":\"4\"}");
        assertThat(json).doesNotContain("_source");
    }

    @Test
    void anAbsentVersionIsOmittedRatherThanZero() {
        String json = assemble(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                bytes("{}")));
        // WARNING: zero is a version OpenSearch compares against. "None sent"
        // and "sent 0" must not become the same document.
        assertThat(json).doesNotContain("_version");
        assertThat(json).isEqualTo("{\"_id\":\"d\",\"_op_type\":\"index\",\"_source\":{}}");
    }

    @Test
    void theOpTypeUsesOpenSearchsWireNames() {
        assertThat(assemble(new SegmentRecord("d", OpType.CREATE, OptionalLong.empty(),
                bytes("{}")))).contains("\"_op_type\":\"create\"");
        assertThat(assemble(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                bytes("{}")))).doesNotContain("INDEX");
        assertThat(assemble(new SegmentRecord("d", OpType.DELETE, OptionalLong.empty(),
                new byte[0]))).contains("\"_op_type\":\"delete\"");
    }

    @Test
    void anIdWithJsonMetacharactersIsEscaped() {
        // WARNING: the id is PRODUCER-SUPPLIED. Unescaped, a quote in it closes
        // the envelope early and the rest is reinterpreted -- a document that
        // rewrites its own metadata.
        String json = assemble(new SegmentRecord("a\"b\\c\nd", OpType.INDEX,
                OptionalLong.empty(), bytes("{}")));
        assertThat(json).startsWith("{\"_id\":\"a\\\"b\\\\c\\nd\"");
    }

    @Test
    void aControlCharacterInAnIdIsEscapedAsAUnicodeSequence() {
        String id = "a" + (char) 1 + "b";
        String json = assemble(new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(),
                bytes("{}")));
        assertThat(json).contains("\\u0001").doesNotContain(id);
    }
}
