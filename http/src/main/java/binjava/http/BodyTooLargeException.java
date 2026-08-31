// SPDX-License-Identifier: Apache-2.0
package binjava.http;

/**
 * The request body exceeded {@link BulkService#MAX_BODY_BYTES}.
 *
 * <p>⚠️ Separate from {@link BulkParseException} because it is a different
 * answer to the producer: the body was well-formed as far as it was read, and
 * the right status is 413 rather than 400. Collapsing the two would tell a
 * producer with a large batch that its JSON is malformed.
 */
public final class BodyTooLargeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BodyTooLargeException(String message) {
        super(message);
    }
}
