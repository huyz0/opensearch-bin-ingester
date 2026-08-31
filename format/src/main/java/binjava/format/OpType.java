// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * What a record does to a document (ADR-0020).
 *
 * <p>⚠️ The ordinal is the WIRE VALUE and is part of the format. Reordering this
 * enum silently reinterprets every segment ever written, so new members are
 * appended and none is ever removed.
 */
public enum OpType {
    INDEX,
    CREATE,
    DELETE;

    private static final OpType[] BY_ORDINAL = values();

    public static OpType fromWire(int v) {
        if (v < 0 || v >= BY_ORDINAL.length) {
            throw new IllegalArgumentException("unknown op type: " + v);
        }
        return BY_ORDINAL[v];
    }
}
