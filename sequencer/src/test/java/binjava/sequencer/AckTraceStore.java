// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.ListPage;
import binjava.binstore.MultipartWriter;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Emits I5's CONFIRMED half from the store, where it can be true (M4.50).
 *
 * <p>⚠️ WHY THIS EXISTS AT ALL. The simulation used to build both halves of the
 * acknowledgement trace from one {@code commit()} return, adjacently:
 * {@code confirmed(seq)} then {@code acked(seq)}. The confirmation therefore
 * preceded the acknowledgement by CONSTRUCTION, so
 * {@link AckOrderInvariants#checkAckOrder} could not fail however the writer
 * behaved. A {@code BatchingSequencer} acking window N+1 before window N
 * confirmed left the 1,000-seed sweep green. I5's ack clause was asserted and
 * not testable -- and the research corpus singles I5 out as "the one a
 * plausible implementation violates by accident".
 *
 * <p>⚠️ THE STORE IS THE ONLY HONEST WITNESS. It sees every PUT, in the order
 * they actually complete, and it has no view of what the writer intends to tell
 * its caller. Sourcing the two halves of the trace from two places is the
 * property that makes a disagreement between them observable at all.
 *
 * <p>⚠️ ONLY A WINNING PUT CONFIRMS. {@code putIfAbsent} is the write-once
 * primitive I1 rests on, so a losing attempt changed nothing; recording one
 * would forge a confirmation for a write that never landed, and a duplicated
 * in-flight PUT -- which the fault injector models deliberately -- would then
 * satisfy an acknowledgement that had no business being satisfied.
 *
 * <p>⚠️ ONLY {@code .delta} KEYS. Checkpoints and the newest-checkpoint pointer
 * sort under the same log prefix (M4.8b1), and neither is a commit. Counting
 * one would move the chain's floor in {@code checkAckOrder} and could mask a
 * genuinely unconfirmed commit.
 */
final class AckTraceStore implements BinStore {

    /**
     * ⚠️ MIRRORS {@link LogKeys}, WHICH IS THE ONE THING WRONG WITH THIS CLASS.
     * The grammar is written in `LogKeys.keyFor` and read here, so a change to
     * one and not the other makes this observer silently stop confirming --
     * which reads as a clean sweep, not as a broken one. {@code
     * theGrammarThisParserAssumesIsTheOneLogKeysWrites} in the test pins them
     * together; a shared parser on `LogKeys` would be rung 1 and is the better
     * answer when something else needs to parse a key.
     */
    private static final Pattern DELTA =
            Pattern.compile(".*/ctl/log/0/([0-9a-f]{16})/([0-9a-f]{16})\\.delta$");

    private final BinStore delegate;
    private final Consumer<AckOrderInvariants.AckEvent> onConfirmed;

    AckTraceStore(BinStore delegate, Consumer<AckOrderInvariants.AckEvent> onConfirmed) {
        this.delegate = delegate;
        this.onConfirmed = onConfirmed;
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        Optional<Version> won = delegate.putIfAbsent(key, body);
        if (won.isPresent()) {
            Matcher m = DELTA.matcher(key);
            if (m.matches()) {
                onConfirmed.accept(AckOrderInvariants.AckEvent.confirmed(
                        Long.parseUnsignedLong(m.group(1), 16),
                        Long.parseUnsignedLong(m.group(2), 16)));
            }
        }
        return won;
    }

    // ⚠️ EVERYTHING ELSE DELEGATES UNCHANGED. Only `putIfAbsent` writes a chain
    // entry; `put` and `multipart` are the segment path, and a segment is not a
    // commit.
    @Override public Optional<ObjectStat> stat(String k) throws IOException {
        return delegate.stat(k);
    }

    @Override public InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override public InputStream getRange(String k, long a, long b) throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override public Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override public Optional<Version> putIfMatch(String k, Body b, Version expected)
            throws IOException {
        return delegate.putIfMatch(k, b, expected);
    }

    @Override public MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override public ListPage list(String p, String a, int m) throws IOException {
        return delegate.list(p, a, m);
    }

    @Override public void delete(List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override public Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override public void close() throws IOException {
        delegate.close();
    }
}
