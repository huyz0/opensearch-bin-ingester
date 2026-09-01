// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The content of {@code <prefix>/ctl/registry/indices.json}: a dense {@code
 * int} ordinal per {@code indexUUID} (research doc 01 §6; ADR-0008). Pure
 * encode/decode only — the CAS-updated, cached read/write orchestration
 * lives in {@code ingest}'s {@code IndexOrdinalRegistry}, which is the thing
 * that actually touches {@code BinStore}.
 *
 * <p>⚠️ REAL JSON, hand-written rather than pulled in as a library
 * dependency: the shape is a flat object of quoted string keys to
 * non-negative integers, which needs neither nesting, floats, nor string
 * escaping (an {@code indexUUID} is hyphens and hex digits only) — the one
 * case a general JSON library exists to handle that this one does not.
 * `.json` in the name is not decorative: an operator can {@code cat} this
 * object and read it, which a binary control record (like {@link
 * CommitDelta}) does not offer.
 */
public record IndexRegistry(Map<String, Integer> ordinals) {

    public IndexRegistry {
        Objects.requireNonNull(ordinals, "ordinals");
        for (var e : ordinals.entrySet()) {
            if (e.getKey() == null || e.getKey().isEmpty()) {
                throw new IllegalArgumentException("an indexUUID key is never empty");
            }
            if (e.getValue() == null || e.getValue() < 0) {
                throw new IllegalArgumentException(
                        "ordinal for " + e.getKey() + " is never negative: " + e.getValue());
            }
        }
        // ⚠️ DEFENSIVE COPY, sorted by key -- so encode() is deterministic
        // (byte-identical output for the same content) regardless of the
        // Map implementation or insertion order a caller handed in.
        ordinals = java.util.Collections.unmodifiableMap(new TreeMap<>(ordinals));
    }

    public static final IndexRegistry EMPTY = new IndexRegistry(Map.of());

    /** The dense ordinal for an index, or empty if not yet registered. */
    public java.util.OptionalInt ordinalFor(String indexUUID) {
        Integer v = ordinals.get(indexUUID);
        return v == null ? java.util.OptionalInt.empty() : java.util.OptionalInt.of(v);
    }

    /** This registry with one more entry, replacing any prior ordinal for the same key. */
    public IndexRegistry with(String indexUUID, int ordinal) {
        Map<String, Integer> next = new LinkedHashMap<>(ordinals);
        next.put(indexUUID, ordinal);
        return new IndexRegistry(next);
    }

    /** One past the highest ordinal assigned so far, or 0 for an empty registry. */
    public int nextOrdinal() {
        return ordinals.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
    }

    public byte[] encode() {
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (var e : ordinals.entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append('"').append(e.getKey()).append("\":").append(e.getValue());
        }
        out.append('}');
        return out.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * ⚠️ REFUSES anything that is not exactly this shape. A registry a future
     * reader cannot parse is worse than one that fails loudly now — every
     * ordinal ever handed out depends on this file remaining readable.
     */
    public static IndexRegistry decode(byte[] bytes) throws IOException {
        String s = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).strip();
        if (s.length() < 2 || s.charAt(0) != '{' || s.charAt(s.length() - 1) != '}') {
            throw new IOException("not an index registry object: " + s);
        }
        String inner = s.substring(1, s.length() - 1).strip();
        Map<String, Integer> parsed = new LinkedHashMap<>();
        if (!inner.isEmpty()) {
            for (String entry : inner.split(",")) {
                int colon = entry.indexOf(':');
                if (colon < 0) {
                    throw new IOException("malformed registry entry: " + entry);
                }
                String key = entry.substring(0, colon).strip();
                if (key.length() < 2 || key.charAt(0) != '"' || key.charAt(key.length() - 1) != '"') {
                    throw new IOException("malformed registry key: " + key);
                }
                String uuid = key.substring(1, key.length() - 1);
                int ordinal;
                try {
                    ordinal = Integer.parseInt(entry.substring(colon + 1).strip());
                } catch (NumberFormatException e) {
                    throw new IOException("malformed ordinal in entry: " + entry, e);
                }
                if (parsed.put(uuid, ordinal) != null) {
                    // ⚠️ A duplicate key means a corrupted or hand-edited
                    // object -- two ordinals for one index would silently
                    // pick whichever parsed last, and the filter it feeds
                    // (M2.4-M2.6) would encode membership under the wrong one.
                    throw new IOException("duplicate indexUUID in registry: " + uuid);
                }
            }
        }
        return new IndexRegistry(parsed);
    }
}
