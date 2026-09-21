// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The NON-MERGING default of {@link SubscriptionTransport#subscribe(List,
 * SubscriptionTransport.Listener)} (M5.62).
 *
 * <p>⚠️ IT IS UNREACHABLE FROM PRODUCTION AND THAT IS WHY IT NEEDS THESE.
 * {@code NodeSubscriptions} starts from an empty key list and every transport
 * in the tree that can merge overrides the method, so two fixes made inside it
 * during review -- subscribing a duplicated key ONCE, and closing the handles
 * already opened when one subscribe throws -- were both revertible with the
 * whole suite green. A fix nothing pins is a fix that comes back.
 *
 * <p>⚠️ T0: no store, no clock, no thread. The transport is a list.
 */
class MultiSubscriptionDefaultTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey A = new RunKey(INDEX, 0);
    private static final RunKey B = new RunKey(INDEX, 1);

    /** Records every subscribe and lets a chosen key's subscribe or close throw. */
    private static final class Recording implements SubscriptionTransport {
        private final List<RunKey> subscribed = new ArrayList<>();
        private final List<RunKey> closed = new ArrayList<>();
        private RunKey failSubscribeFor;
        private RunKey failCloseFor;

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            if (key.equals(failSubscribeFor)) {
                throw new IllegalStateException("no subscription for " + key);
            }
            subscribed.add(key);
            return () -> {
                if (key.equals(failCloseFor)) {
                    throw new IllegalStateException("cannot unsubscribe " + key);
                }
                closed.add(key);
            };
        }
    }

    @Test
    void aKeyLISTEDTWICEIsSubscribedONCE() {
        Recording transport = new Recording();

        transport.subscribe(List.of(A, B, A), delivery -> { });

        assertThat(transport.subscribed)
                .as("`put` overwrites the first handle and nothing can close it again -- the "
                        + "leak is invisible until the subscription is closed and one stream "
                        + "stays registered")
                .containsExactly(A, B);
    }

    @Test
    void aSubscribeThatTHROWSCLOSESTheOnesAlreadyOpen() {
        Recording transport = new Recording();
        transport.failSubscribeFor = B;

        assertThatThrownBy(() -> transport.subscribe(List.of(A, B), delivery -> { }))
                .as("the caller learns the subscription was not built")
                .isInstanceOf(IllegalStateException.class);

        assertThat(transport.closed)
                .as("and the handle already open is closed -- the caller received no "
                        + "subscription, so it holds nothing to close it with")
                .containsExactly(A);
    }

    @Test
    void aTHROWINGCloseKEEPSTheHandleSoCloseCanTryAgain() {
        Recording transport = new Recording();
        transport.failCloseFor = A;
        SubscriptionTransport.MultiSubscription subscription =
                transport.subscribe(List.of(A), delivery -> { });

        assertThatThrownBy(() -> subscription.remove(A))
                .isInstanceOf(IllegalStateException.class);
        transport.failCloseFor = null;
        subscription.close();

        assertThat(transport.closed)
                .as("dropping the mapping before the close succeeds makes the handle "
                        + "unreachable to BOTH a retried remove and to close(), so that stream "
                        + "stays subscribed with nothing able to unsubscribe it")
                .containsExactly(A);
    }

    @Test
    void addAndRemoveTouchONEKeyAndLeaveTheRestAlone() {
        Recording transport = new Recording();
        SubscriptionTransport.MultiSubscription subscription =
                transport.subscribe(List.of(A), delivery -> { });

        subscription.add(B);
        subscription.add(B);
        subscription.remove(A);

        assertThat(transport.subscribed)
                .as("adding an already-registered key registers nothing -- a second "
                        + "registration is a second delivery of every push")
                .containsExactly(A, B);
        assertThat(transport.closed)
                .as("and removing one key leaves the other registered, which is what makes "
                        + "this a subscription that can change rather than one that is rebuilt")
                .containsExactly(A);
    }
}
