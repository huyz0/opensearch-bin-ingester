// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

/**
 * An explicit-partition write refused because its index already has as many
 * writes waiting for its registration as it may (M12.10, M10.30 P1): answered
 * {@code 429} with {@code Retry-After}, a load refusal, not the {@code 503} a
 * wait that timed out gets.
 */
public final class RegistrationWaitFullException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public RegistrationWaitFullException(String message) {
        super(message);
    }

    /** What the {@code 429} tells the producer to wait, in seconds. */
    public long retryAfterSeconds() {
        return 1;
    }
}
