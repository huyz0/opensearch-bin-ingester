// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A strict, minimal JSON reader for {@code _bulk} ACTION lines only.
 *
 * <p>⚠️ Deliberately NOT used on the document body. The ingester never parses a
 * document (ADR-0020): the payload is copied through byte for byte, so this
 * parser sees only the small, fixed-shape action lines a producer sends.
 * Pointing it at document bodies would put a hand-written parser on the hot path
 * of every record and would reject documents the producer considers valid.
 *
 * <p>⚠️ Strict on purpose. Trailing commas, comments, single quotes, NaN and
 * unquoted keys are all rejected — a lenient parser on untrusted input turns a
 * malformed request into a differently-shaped accepted one.
 */
final class Json {

    private final String s;
    private int i;

    private Json(String s) {
        this.s = s;
    }

    /** Parses one complete JSON value; trailing non-whitespace is an error. */
    static Object parse(String text) {
        Json p = new Json(text);
        p.ws();
        Object v = p.value(0);
        p.ws();
        if (p.i != text.length()) {
            throw new BulkParseException("trailing content after the JSON value");
        }
        return v;
    }

    /**
     * ⚠️ Bounded depth. Untrusted input nests as deeply as it likes, and a
     * recursive-descent parser answers that with a StackOverflowError, which is
     * an Error rather than an exception and escapes the 400 path entirely.
     */
    private static final int MAX_DEPTH = 32;

    private Object value(int depth) {
        if (depth > MAX_DEPTH) {
            throw new BulkParseException("JSON nested deeper than " + MAX_DEPTH);
        }
        if (i >= s.length()) {
            throw new BulkParseException("truncated JSON");
        }
        char c = s.charAt(i);
        switch (c) {
            case '{':
                return object(depth);
            case '[':
                return array(depth);
            case '"':
                return string();
            case 't':
                expect("true");
                return Boolean.TRUE;
            case 'f':
                expect("false");
                return Boolean.FALSE;
            case 'n':
                expect("null");
                return null;
            default:
                return number();
        }
    }

    private Map<String, Object> object(int depth) {
        i++;
        Map<String, Object> m = new LinkedHashMap<>();
        ws();
        if (i < s.length() && s.charAt(i) == '}') {
            i++;
            return m;
        }
        while (true) {
            ws();
            if (i >= s.length() || s.charAt(i) != '"') {
                throw new BulkParseException("a JSON object key must be a quoted string");
            }
            String k = string();
            ws();
            if (i >= s.length() || s.charAt(i) != ':') {
                throw new BulkParseException("expected ':' after a JSON object key");
            }
            i++;
            ws();
            m.put(k, value(depth + 1));
            ws();
            if (i >= s.length()) {
                throw new BulkParseException("truncated JSON object");
            }
            if (s.charAt(i) == ',') {
                i++;
                continue;
            }
            if (s.charAt(i) == '}') {
                i++;
                return m;
            }
            throw new BulkParseException("expected ',' or '}' in a JSON object");
        }
    }

    private List<Object> array(int depth) {
        i++;
        List<Object> l = new ArrayList<>();
        ws();
        if (i < s.length() && s.charAt(i) == ']') {
            i++;
            return l;
        }
        while (true) {
            ws();
            l.add(value(depth + 1));
            ws();
            if (i >= s.length()) {
                throw new BulkParseException("truncated JSON array");
            }
            if (s.charAt(i) == ',') {
                i++;
                continue;
            }
            if (s.charAt(i) == ']') {
                i++;
                return l;
            }
            throw new BulkParseException("expected ',' or ']' in a JSON array");
        }
    }

    private String string() {
        i++;
        StringBuilder b = new StringBuilder();
        while (true) {
            if (i >= s.length()) {
                throw new BulkParseException("unterminated JSON string");
            }
            char c = s.charAt(i++);
            if (c == '"') {
                return b.toString();
            }
            if (c != '\\') {
                if (c < 0x20) {
                    throw new BulkParseException("a raw control character in a JSON string");
                }
                b.append(c);
                continue;
            }
            if (i >= s.length()) {
                throw new BulkParseException("unterminated JSON escape");
            }
            char e = s.charAt(i++);
            switch (e) {
                case '"' -> b.append('"');
                case '\\' -> b.append('\\');
                case '/' -> b.append('/');
                case 'b' -> b.append('\b');
                case 'f' -> b.append('\f');
                case 'n' -> b.append('\n');
                case 'r' -> b.append('\r');
                case 't' -> b.append('\t');
                case 'u' -> {
                    if (i + 4 > s.length()) {
                        throw new BulkParseException("truncated \\u escape");
                    }
                    String hex = s.substring(i, i + 4);
                    for (int k = 0; k < 4; k++) {
                        if (Character.digit(hex.charAt(k), 16) < 0) {
                            throw new BulkParseException("a \\u escape is four hex digits");
                        }
                    }
                    b.append((char) Integer.parseInt(hex, 16));
                    i += 4;
                }
                default -> throw new BulkParseException("unknown JSON escape");
            }
        }
    }

    private Object number() {
        int start = i;
        if (i < s.length() && s.charAt(i) == '-') {
            i++;
        }
        // ⚠️ JSON's integer grammar is `0 | [1-9][0-9]*`: a LEADING ZERO is not
        // legal. Accepting "01" is not harmless leniency -- an external _version
        // of 01 and one of 1 would be the same write to us and different writes
        // to a producer that round-trips its own request, and the divergence
        // shows up as a lost update rather than as a parse error.
        int digitsFrom = i;
        while (i < s.length() && Character.isDigit(s.charAt(i))) {
            i++;
        }
        int intDigits = i - digitsFrom;
        if (intDigits == 0) {
            throw new BulkParseException("expected a JSON value");
        }
        if (intDigits > 1 && s.charAt(digitsFrom) == '0') {
            throw new BulkParseException("a JSON number has no leading zero");
        }
        boolean fractional = false;
        if (i < s.length() && s.charAt(i) == '.') {
            fractional = true;
            i++;
            int from = i;
            while (i < s.length() && Character.isDigit(s.charAt(i))) {
                i++;
            }
            if (i == from) {
                throw new BulkParseException("a JSON fraction needs a digit after '.'");
            }
        }
        if (i < s.length() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
            fractional = true;
            i++;
            if (i < s.length() && (s.charAt(i) == '+' || s.charAt(i) == '-')) {
                i++;
            }
            int from = i;
            while (i < s.length() && Character.isDigit(s.charAt(i))) {
                i++;
            }
            if (i == from) {
                throw new BulkParseException("a JSON exponent needs a digit");
            }
        }
        String raw = s.substring(start, i);
        try {
            // ⚠️ Long, not Double, for integers. A _version parsed as a double
            // loses precision above 2^53, and an external version IS a long.
            return fractional ? (Object) Double.valueOf(raw) : (Object) Long.valueOf(raw);
        } catch (NumberFormatException e) {
            throw new BulkParseException("malformed JSON number");
        }
    }

    private void expect(String lit) {
        if (!s.startsWith(lit, i)) {
            throw new BulkParseException("expected " + lit);
        }
        i += lit.length();
    }

    private void ws() {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                i++;
            } else {
                return;
            }
        }
    }
}
