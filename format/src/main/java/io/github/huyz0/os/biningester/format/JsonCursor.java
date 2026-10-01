// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Reads one canonical JSON control object by expecting its exact text, field
 * by field, in the order its encoder writes them (M13.26b).
 *
 * <p>⚠️ NOT A JSON PARSER, deliberately. A control object has one canonical
 * form, so a decoder that expects that form token by token refuses an unknown
 * field, a duplicate, a reordering and whitespace by construction -- the
 * strictness ADR-0082 §3 asks for -- with nothing to configure and nothing to
 * forget. Strings carry no escapes: an encoder refuses every character it
 * could not write unescaped, as {@link Lease} does.
 */
final class JsonCursor {

    private final String s;
    private final String what;
    private int at;

    private JsonCursor(String s, String what) {
        this.s = s;
        this.what = what;
    }

    static JsonCursor over(byte[] bytes, String what) throws IOException {
        if (bytes == null) {
            throw new IOException("no " + what + " bytes");
        }
        try {
            return new JsonCursor(StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString(), what);
        } catch (CharacterCodingException e) {
            throw new IOException(what + " is not UTF-8", e);
        }
    }

    IOException refused(String why) {
        return new IOException(what + " " + why + " at offset " + at);
    }

    void expect(String literal) throws IOException {
        if (!s.startsWith(literal, at)) {
            throw refused("expected " + literal);
        }
        at += literal.length();
    }

    /** Consumes {@code c} if it is next. */
    boolean take(char c) {
        if (at < s.length() && s.charAt(at) == c) {
            at++;
            return true;
        }
        return false;
    }

    long readLong() throws IOException {
        int from = at;
        if (at < s.length() && s.charAt(at) == '-') {
            at++;
        }
        while (at < s.length() && s.charAt(at) >= '0' && s.charAt(at) <= '9') {
            at++;
        }
        String digits = s.substring(from, at);
        if (digits.isEmpty() || digits.equals("-")) {
            throw refused("expected a number");
        }
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            throw refused("has a number out of range");
        }
    }

    int readInt() throws IOException {
        long value = readLong();
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw refused("has a number out of range");
        }
        return (int) value;
    }

    boolean readBoolean() throws IOException {
        if (s.startsWith("true", at)) {
            at += 4;
            return true;
        }
        if (s.startsWith("false", at)) {
            at += 5;
            return false;
        }
        throw refused("expected true or false");
    }

    /** A quoted string with no escape: the encoder never writes one. */
    String readString() throws IOException {
        expect("\"");
        int end = s.indexOf('"', at);
        if (end < 0) {
            throw refused("has an unterminated string");
        }
        String value = s.substring(at, end);
        if (value.indexOf('\\') >= 0) {
            throw refused("has an escape, which its encoder never writes");
        }
        at = end + 1;
        return value;
    }

    void end() throws IOException {
        if (at != s.length()) {
            throw refused("has content after the object");
        }
    }

    /**
     * Refuses what the encoder cannot represent without escaping: a quote, a
     * backslash, a control character or an unpaired surrogate.
     */
    static void representable(String field, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\' || c < 0x20) {
                throw new IllegalArgumentException(field
                        + " may not contain a quote, a backslash or a control character");
            }
        }
        if (value.codePoints().anyMatch(cp -> cp >= 0xD800 && cp <= 0xDFFF)) {
            throw new IllegalArgumentException(field + " may not contain an unpaired surrogate");
        }
    }
}
