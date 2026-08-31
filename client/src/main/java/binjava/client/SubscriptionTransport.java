// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import binjava.format.RunKey;

/**
 * How a consumer is told its stream advanced.
 *
 * <p>WARNING: one of the four I/O seams (architecture.md). It exists so the
 * consumer can be tested at T1 against a fake transport -- criterion 3 runs
 * 1,600 consumers that way -- while the real one speaks HTTP from the `plugin`
 * side. A consumer that reached for a socket itself could not be tested at that
 * scale inside L0's budget.
 */
public interface SubscriptionTransport {

    /** Handed each delivery for the subscribed stream. */
    @FunctionalInterface
    interface Listener {
        void onDelivery(Delivery delivery);
    }

    /** Registers interest; closing the handle unsubscribes. */
    AutoCloseable subscribe(RunKey key, Listener listener);
}
