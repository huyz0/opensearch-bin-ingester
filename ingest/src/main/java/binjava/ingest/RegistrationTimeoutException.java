// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * A routed write whose index was still not registered when its wait ran out,
 * or whose index's pending pool was already full (M6.6, ADR-0006).
 *
 * <p>⚠️ IT IS RETRYABLE AND THE STATUS SAYS SO. The index is not wrong and the
 * request is not wrong: the plugin has not pushed this index's shape yet, which
 * is a state that ends on its own. A producer told 400 would drop the batch;
 * one told 503 retries, and by then the registration has usually arrived.
 *
 * <p>⚠️ AND IT IS NEVER A FOLD TO PARTITION 0, which is what ADR-0006's
 * reject-never-fold forbids: placing an unregistered index's records in
 * partition 0 funnels a whole index into one shard while returning 202.
 *
 * <p>⚠️ IT EXTENDS {@link IllegalStateException} so existing callers keep
 * working; what it adds is a name {@code BulkService} can catch without
 * catching the append path's own invariant failures.
 */
public final class RegistrationTimeoutException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public RegistrationTimeoutException(String message) {
        super(message);
    }
}
