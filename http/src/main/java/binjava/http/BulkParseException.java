// SPDX-License-Identifier: Apache-2.0
package binjava.http;

/**
 * The producer's {@code _bulk} body is not one this adapter will accept.
 *
 * <p>⚠️ Always a 400, never a 500, and never silently skipped: a dropped action
 * that still returns 202 is a lost write the producer believes succeeded.
 *
 * <p>⚠️ The message names the SHAPE that was wrong. It must never quote the
 * document body, an {@code _id} or a routing value — security.md forbids those
 * in an error message, and a 400 is exactly where they leak.
 *
 * <p>⚠️ It does NOT carry a line number, and an earlier version of this javadoc
 * claimed it did. On a 10,000-action body that is the field an operator would
 * actually use, so the gap is real; it is recorded as M1.7d rather than
 * described as present.
 */
public final class BulkParseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BulkParseException(String message) {
        super(message);
    }
}
