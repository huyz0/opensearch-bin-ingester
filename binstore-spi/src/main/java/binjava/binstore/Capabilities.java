// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

/**
 * What a backend can actually do, checked at STARTUP.
 *
 * <p>⚠️ Checked eagerly so a store lacking conditional writes fails loudly
 * rather than silently corrupting the commit log, the sequencer lease or the
 * ordinal registry: the whole coordination substrate (ADR-0002, ADR-0008)
 * rests on {@code putIfAbsent} AND {@code putIfMatch} being atomic, and a
 * backend that quietly degrades either to a read-then-write would lose
 * records with no error. Call {@link #requireConditionalWrites()} once, at
 * startup, on whatever {@link BinStore} the caller is about to hand to the
 * commit log, the lease or the registry -- never per request.
 *
 * @param conditionalWrites BOTH {@code putIfAbsent} and {@code putIfMatch} are
 *     atomic against concurrent writers (ADR-0008: "{@code
 *     Capabilities.conditionalWrites} requires both"). A backend supporting
 *     only one is not a supported backend.
 * @param batchDelete delete accepts many keys in one request
 * @param maxKeyBytes longest key the backend accepts
 * @param minPartSize smallest non-final multipart part
 * @param costs what its requests cost; M1.3's meter reads this
 */
public record Capabilities(
        boolean conditionalWrites,
        boolean batchDelete,
        long maxKeyBytes,
        long minPartSize,
        CostTable costs) {

    /**
     * Fails loudly if this backend lacks conditional writes.
     *
     * <p>⚠️ This is the ADR-0008 startup check, not aspiration: nothing called
     * it before M2.1, so a backend advertising {@code conditionalWrites=false}
     * (or a future one that never sets it {@code true}) would be wired in
     * silently and only fail the first time the lease or the registry lost a
     * race it should have won atomically.
     */
    public void requireConditionalWrites() {
        if (!conditionalWrites) {
            throw new IllegalStateException(
                    "backend does not support conditional writes (putIfAbsent AND "
                            + "putIfMatch, ADR-0008) -- refusing to start");
        }
    }
}
