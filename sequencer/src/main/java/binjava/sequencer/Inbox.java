// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.format.CommitRequestFrame;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * The commit-intent inbox: where a pod that cannot reach its sequencer leaves
 * the commit for the leaseholder to apply (M8.14a, ADR-0058, research 03
 * section 10).
 *
 * <p>⚠️ **ONE OBJECT PER POD PER SLOT PER FLUSH**, keyed by the request's
 * {@code (podId, incarnationId, flushSeq)} triple, which is also what makes
 * applying it idempotent: a drained intent whose forward had in fact landed is
 * answered from the dedupe window, never committed twice (ADR-0036). Slot 0
 * is the only slot (S = 1), and the key names it so a second slot adds a
 * directory rather than a format.
 *
 * <p>⚠️ **THE BODY IS A {@link CommitRequestFrame}, UNCHANGED** -- the bytes a
 * forward would have carried, so the drain decodes exactly what the sequencer
 * would have received.
 */
public final class Inbox {

    private Inbox() {
    }

    /** {@code <prefix>/ctl/inbox/0/}: the one slot's inbox. */
    public static String prefixFor(String prefix) {
        return prefix + "/ctl/inbox/0/";
    }

    /** Where {@code request}'s intent lives. */
    public static String keyFor(String prefix, CommitRequest request) {
        return prefixFor(prefix) + request.podId() + "/" + request.incarnationId() + "/"
                + String.format(Locale.ROOT, "%016x", request.flushSeq()) + ".intent";
    }

    /**
     * Makes {@code request}'s intent durable, once.
     *
     * <p>⚠️ **AN OCCUPIED KEY IS SUCCESS**: it is this same flush, retried, and
     * the object already there is its intent.
     *
     * @return the intent's key
     * @throws IOException if the store did not take it -- ⚠️ then nothing is
     *     durable to ack on, and the flush must fail
     */
    public static String write(BinStore store, String prefix, CommitRequest request)
            throws IOException {
        Objects.requireNonNull(store, "store");
        String key = keyFor(prefix, request);
        store.putIfAbsent(key, Body.ofBytes(new CommitRequestFrame(request.podId(),
                request.incarnationId(), request.flushSeq(), request.segmentKey(),
                request.recordCounts()).encode()));
        return key;
    }

    /** One intent in the inbox: where it is, and the commit it holds. */
    public record Pending(String key, CommitRequest request) {
    }

    /**
     * Every intent in the inbox, ordered by pod, incarnation and flush.
     *
     * <p>⚠️ **THE ORDER IS THE SAFETY**: the dedupe window keeps ONE high mark
     * per pod incarnation (ADR-0036), so a pod's intents are applied oldest
     * first -- a later flush applied first would make every earlier one read
     * as a replay and be refused, an acked write lost.
     *
     * <p>⚠️ **ONE LIST PER 1,000 INTENTS AND ONE GET EACH**, paid by a drain or
     * a sweep, never on a timer.
     */
    public static List<Pending> pending(BinStore store, String prefix) throws IOException {
        List<Pending> found = new ArrayList<>();
        String after = null;
        do {
            ListPage page = store.list(prefixFor(prefix), after, 1000);
            for (ObjectStat object : page.objects()) {
                if (!object.key().endsWith(".intent")) {
                    continue;
                }
                byte[] bytes;
                try (InputStream in = store.get(object.key())) {
                    bytes = in.readAllBytes();
                }
                CommitRequestFrame frame = CommitRequestFrame.decode(bytes);
                found.add(new Pending(object.key(), new CommitRequest(frame.podId(),
                        frame.incarnationId(), frame.flushSeq(), frame.segmentKey(),
                        frame.recordCounts())));
            }
            after = page.nextStartAfter().orElse(null);
        } while (after != null);
        found.sort(Comparator.comparing((Pending p) -> p.request().podId())
                .thenComparing(p -> p.request().incarnationId())
                .thenComparingLong(p -> p.request().flushSeq()));
        return found;
    }

    /**
     * The segments the inbox's intents name.
     *
     * <p>⚠️ **THE ORPHAN SWEEP KEEPS THESE**: an intent's segment is committed
     * in all but the delta, and a sweep that took it would lose records the
     * producer was told were durable.
     */
    public static Set<String> segmentsNamed(BinStore store, String prefix) throws IOException {
        Set<String> named = new HashSet<>();
        for (Pending pending : pending(store, prefix)) {
            named.add(pending.request().segmentKey());
        }
        return named;
    }
}
