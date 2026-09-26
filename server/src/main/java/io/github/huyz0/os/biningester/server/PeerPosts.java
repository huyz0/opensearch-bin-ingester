// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.http.DeltaFanOut;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;

/**
 * The HTTP senders a test may put in place of the pooled production clients,
 * each {@code null} for "the production one".
 *
 * <p>⚠️ WHY A TEST NEEDS {@code delta}: pods simulated in one JVM on loopback
 * aliases all send from 127.0.0.1, so a receiver authorizing its socket peer
 * would see every push as coming from whichever pod owns that address. A
 * sender bound to its own pod's address is what a real pod IP gives for free.
 */
record PeerPosts(DurableSegmentSignalSender.PeerPost signal, DeltaFanOut.PeerPost delta) {
}
