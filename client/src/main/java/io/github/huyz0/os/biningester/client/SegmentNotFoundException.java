// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.io.IOException;

/**
 * The ingester answered {@code 404} for a proxied segment (M13.30n): the key
 * names nothing it can serve. For a data segment that is a failure like any
 * other, retried by policy; for a fast key it is the entry's release, and its
 * subscriber reads the committed segment instead (ADR-0082 §6).
 */
public final class SegmentNotFoundException extends IOException {

    private static final long serialVersionUID = 1L;

    public SegmentNotFoundException(String message) {
        super(message);
    }
}
