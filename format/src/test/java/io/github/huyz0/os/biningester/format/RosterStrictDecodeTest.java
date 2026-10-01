// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * A roster or {@code LATEST} in anything but its canonical form is refused as
 * an {@link IOException} (ADR-0082 §3: strict decode, refusing unknown or
 * duplicate fields), never read as some other term.
 */
class RosterStrictDecodeTest {

    private static String canonical() throws IOException {
        return new String(RosterTest.golden("roster-v1.json"), StandardCharsets.UTF_8);
    }

    private static void refused(String text) {
        assertThatThrownBy(() -> Roster.decode(text.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void WHITESPACEIsRefused() throws Exception {
        refused(canonical().replace("\"epoch\":7", "\"epoch\": 7"));
        refused(canonical() + "\n");
    }

    @Test
    void anUNKNOWNOrRepeatedFieldIsRefused() throws Exception {
        refused(canonical().replace(",\"closed\":false}", ",\"closed\":false,\"extra\":1}"));
        refused(canonical().replace("{\"epoch\":7,", "{\"epoch\":7,\"epoch\":7,"));
    }

    @Test
    void REORDEREDMapKeysAreRefused() throws Exception {
        String swapped = canonical().replace(
                "\"00000000-0000-0001-0000-000000000001\":1,\"f0000000-0000-0000-0000-000000000000\":3",
                "\"f0000000-0000-0000-0000-000000000000\":3,\"00000000-0000-0001-0000-000000000001\":1");

        refused(swapped);
    }

    @Test
    void anESCAPEOrAnUnknownStateIsRefused() throws Exception {
        refused(canonical().replace("ingester-2", "ingester\\u002d2"));
        refused(canonical().replace("\"DEPARTED\"", "\"GONE\""));
    }

    @Test
    void aNONCanonicalUuidIsRefused() throws Exception {
        refused(canonical().replace("f0000000-0000", "F0000000-0000"));
    }

    @Test
    void aROSTERTheRecordRefusesIsAnIOException() throws Exception {
        refused(canonical().replace("\"fencedBy\":9", "\"fencedBy\":7"));
        refused(canonical().replace("\"predecessor\":5", "\"predecessor\":7"));
    }

    @Test
    void aNONCanonicalLatestIsRefused() {
        for (String text : new String[] {"{\"epoch\":0}", "{\"epoch\":07}", "{\"epoch\":7} ",
                "{\"epoch\":7,\"x\":1}"}) {
            assertThatThrownBy(() -> Roster.decodeLatest(text.getBytes(StandardCharsets.UTF_8)))
                    .as(text).isInstanceOf(IOException.class);
        }
    }
}
