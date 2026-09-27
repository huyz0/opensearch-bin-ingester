// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A store read in progress, which later callers of the same key ATTACH to
 * (M5.63).
 *
 * <p>⚠️ ATTACHED, NOT BUFFERED UNTIL THE WINNER FINISHES. A joiner is handed
 * the prefix read so far -- the same cache-bound accumulator the winner is
 * filling -- and then every later chunk as the winner reads it, so a joiner
 * streams exactly as the winner's own consumers do, in the same loop. It
 * therefore shares the two-argument overload's one weakness too: a sink
 * that BLOCKS stalls the shared read (see {@code SegmentProxy.streamTo}).
 * ⚠️ AND A LATE JOINER STALLS IT WHILE IT CATCHES UP: the prefix is written to
 * its sinks under this monitor, so the first caller waits in {@link #relay}
 * for as long as that takes -- ~300 ms for 30 MiB at 100 MiB/s. And its sinks
 * run on the first caller's thread, so an {@code Error} from one fails the
 * shared read for every caller, as an {@code Error} from any sink of one call
 * already fails that call.
 * Once the segment has outgrown the cache's admission ceiling there is no
 * prefix to hand over, so a caller arriving after that reads for itself.
 *
 * <p>⚠️ EVERY FIELD IS GUARDED BY THIS OBJECT'S MONITOR, and a chunk is
 * written to the attached sinks under it, so a joiner attaching between two
 * chunks gets the prefix up to one and the stream from the next -- never a
 * chunk twice or not at all.
 */
final class InFlightRead {
    private byte[] prefix = new byte[0];
    private int prefixLength;
    private boolean prefixValid = true;
    private final List<SegmentSink> attached = new ArrayList<>();
    private boolean done;
    private Throwable failure;
    private int joiners;

    /** The winner's hand-off of one chunk, with the accumulator it now stands at. */
    synchronized void relay(byte[] chunk, int offset, int length, byte[] admitting,
            int admitted) {
        if (admitting == null) {
            prefixValid = false;
            prefix = null;
        } else {
            prefix = admitting;
            prefixLength = admitted;
        }
        for (int i = attached.size() - 1; i >= 0; i--) {
            try {
                attached.get(i).write(chunk, offset, length);
            } catch (IOException | RuntimeException slowOrDeadConsumer) {
                attached.remove(i);
            }
        }
    }

    /** Ends the read: {@code failed} is null on success, and is every joiner's otherwise. */
    synchronized void complete(Throwable failed) {
        done = true;
        failure = failed;
        notifyAll();
    }

    synchronized int joiners() {
        return joiners;
    }

    /**
     * Attaches {@code sinks} and waits for the read to end.
     *
     * @return how many of {@code sinks} received the whole segment, or
     *     {@code -1} when the segment outgrew the admission ceiling before
     *     they arrived and the caller must read for itself
     * @throws IOException if the shared read failed -- ⚠️ never retried
     *     here: retrying independently is the duplicate GET this exists to
     *     remove
     */
    synchronized int join(List<SegmentSink> sinks, int chunkBytes) throws IOException {
        if (failure != null) {
            throw failedWith(failure);
        }
        if (!prefixValid) {
            return -1;
        }
        List<SegmentSink> mine = new ArrayList<>(sinks);
        for (int offset = 0; offset < prefixLength && !mine.isEmpty(); offset += chunkBytes) {
            int length = Math.min(chunkBytes, prefixLength - offset);
            for (int i = mine.size() - 1; i >= 0; i--) {
                try {
                    mine.get(i).write(prefix, offset, length);
                } catch (IOException | RuntimeException slowOrDeadConsumer) {
                    mine.remove(i);
                }
            }
        }
        if (done) {
            return mine.size();
        }
        attached.addAll(mine);
        joiners++;
        try {
            while (!done) {
                wait();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            detach(mine);
            throw new IOException("interrupted waiting for a shared read", interrupted);
        } finally {
            joiners--;
        }
        if (failure != null) {
            throw failedWith(failure);
        }
        int whole = 0;
        for (SegmentSink sink : mine) {
            for (SegmentSink still : attached) {
                if (still == sink) {
                    whole++;
                    break;
                }
            }
        }
        return whole;
    }

    private void detach(List<SegmentSink> mine) {
        for (SegmentSink sink : mine) {
            for (int i = attached.size() - 1; i >= 0; i--) {
                if (attached.get(i) == sink) {
                    attached.remove(i);
                }
            }
        }
    }

    private static IOException failedWith(Throwable failure) {
        return new IOException("the shared read of this segment failed: "
                + failure.getMessage(), failure);
    }
}
