// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * The leader's per-stream fast cursor and its bound (ADR-0081 §4; M13.27):
 * offsets are assigned from each stream's cursor, and a stream's cursor never
 * runs more than {@code B} offsets past its committed next offset as the
 * leader knows it from the chain.
 *
 * <p>⚠️ THE BOUND IS WHAT BOUNDS A VOID: a later leader's committed next
 * offset can only be higher, so every offset any leader of an unclosed term
 * assigned lies below {@code c0 + B}, and a takeover after quorum loss voids
 * at most {@code B} offsets per stream (§5). ⚠️ A BATCH THAT WOULD PASS IT
 * WAITS -- backpressure, never a refusal (cost.md rule 14) -- and a stream at
 * {@code B / 2} uncommitted offsets triggers an upload (§7).
 *
 * <p>⚠️ THE CURSOR STARTS AT THE COMMITTED NEXT OFFSET, read where no
 * default-path commit is in flight (§8): the caller supplies it, here it is
 * only kept. A decision's resume offset resets it (§2, §5).
 */
public final class FastCursor {

    /** {@code B}: ADR-0081 §4's per-stream bound, a measurement target. */
    public static final long DEFAULT_BOUND = 65_536;

    private final long bound;
    private final Map<RunKey, long[]> streams = new HashMap<>();

    public FastCursor(long bound) {
        if (bound < 2) {
            throw new IllegalArgumentException("the per-stream bound is at least 2: " + bound);
        }
        this.bound = bound;
    }

    /**
     * Starts tracking {@code stream} at its committed next offset, its cursor
     * there too; for a stream already tracked, raises both as
     * {@link #committed} does.
     *
     * <p>⚠️ A RE-OPEN MERGES (M13.27 review round 1, P1): a stream switched
     * to {@code wal=false} and back has had default-path commits since, and a
     * cursor kept at its old value would reassign committed offsets.
     */
    public synchronized void open(RunKey stream, long committedNext) {
        Objects.requireNonNull(stream, "stream");
        requireOffset(committedNext);
        long[] s = streams.computeIfAbsent(stream,
                k -> new long[] {committedNext, committedNext});
        s[0] = Math.max(s[0], committedNext);
        s[1] = Math.max(s[1], s[0]);
    }

    /**
     * Assigns {@code count} offsets of {@code stream}.
     *
     * @return the first offset assigned, or empty when the batch would pass
     *     the bound: it waits, and -- whether or not {@link #uploadDue} says
     *     so -- the waiting batch is itself an upload trigger (§4)
     * @throws IllegalArgumentException a batch of more than {@code B} offsets,
     *     which no commit could ever admit: the caller splits it (M13.27
     *     review round 1, P2)
     */
    public synchronized OptionalLong assign(RunKey stream, int count) {
        long[] s = state(stream);
        if (count < 1 || count > bound) {
            throw new IllegalArgumentException("a batch assigns 1 to B = " + bound
                    + " offsets, never " + count + ": split it");
        }
        if (s[1] - s[0] + count > bound) {
            return OptionalLong.empty();
        }
        long first = s[1];
        s[1] += count;
        return OptionalLong.of(first);
    }

    /** The chain committed {@code stream} up to {@code committedNext}: the bound moves up. */
    public synchronized void committed(RunKey stream, long committedNext) {
        long[] s = state(stream);
        requireOffset(committedNext);
        // ⚠️ NEVER DOWN, and never past the cursor's own end except by a
        // takeover's commit of entries this leader did not assign, which a
        // decision then resets the cursor to.
        s[0] = Math.max(s[0], committedNext);
        s[1] = Math.max(s[1], s[0]);
    }

    /**
     * A decision resumes {@code stream} at {@code resumeAt} (a discard, a
     * switch, a takeover): the cursor is set there, the entries above it
     * superseded.
     */
    public synchronized void resume(RunKey stream, long resumeAt) {
        long[] s = state(stream);
        requireOffset(resumeAt);
        if (resumeAt < s[0]) {
            throw new IllegalArgumentException("a resume offset below the committed next offset "
                    + s[0] + " would reassign committed offsets: " + resumeAt);
        }
        s[1] = resumeAt;
    }

    /** The next offset {@code stream} would assign. */
    public synchronized long cursor(RunKey stream) {
        return state(stream)[1];
    }

    /** Whether {@code stream} is at {@code B / 2} uncommitted offsets: an upload is due (§7). */
    public synchronized boolean uploadDue(RunKey stream) {
        long[] s = state(stream);
        return s[1] - s[0] >= bound / 2;
    }

    private long[] state(RunKey stream) {
        long[] s = streams.get(Objects.requireNonNull(stream, "stream"));
        if (s == null) {
            throw new IllegalStateException("stream " + stream + " is not open");
        }
        return s;
    }

    private static void requireOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("an offset is never negative: " + offset);
        }
    }
}
