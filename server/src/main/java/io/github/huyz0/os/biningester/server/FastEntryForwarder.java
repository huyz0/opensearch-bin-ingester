// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastKey;
import io.github.huyz0.os.biningester.http.FastEntryService;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * A {@code /seg} read of a fast key this pod holds no pushed copy of
 * (ADR-0081 §6, ADR-0082 §2 and §6; M13.30p): forwarded to the leader of the
 * key's term, which serves it from its journal before the upload
 * ({@code /ctl/fast-entry}, M13.30o) -- or read locally when this pod is that
 * leader.
 *
 * <p>⚠️ THE ANSWER's EPOCH IS CHECKED AGAINST THIS POD's FENCE, as ADR-0082 §2
 * says: one of another term than the key's, or of a term below the highest
 * this pod was fenced at, is no answer -- a deposed leader still serving its
 * term is not read from once a successor has fenced this pod.
 *
 * <p>⚠️ BEST EFFORT: any failure is the {@code 404} a released entry gets, and
 * the consumer reads the committed segment (M13.30n).
 */
final class FastEntryForwarder {

    /** The GET to a leader's {@code /ctl/fast-entry}. */
    @FunctionalInterface
    interface Remote {
        Optional<FastEntryService.Served> fetch(String endpoint, String fastKey, String selfUid)
                throws IOException;
    }

    private final String prefix;
    private final String selfUid;
    private final JoinedTerms joined;
    private final LongSupplier fenceHighest;
    private final FastEntryService.Lookup local;
    private final Remote remote;

    FastEntryForwarder(String prefix, String selfUid, JoinedTerms joined,
            LongSupplier fenceHighest, FastEntryService.Lookup local, Remote remote) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.joined = Objects.requireNonNull(joined, "joined");
        this.fenceHighest = Objects.requireNonNull(fenceHighest, "fenceHighest");
        this.local = Objects.requireNonNull(local, "local");
        this.remote = Objects.requireNonNull(remote, "remote");
    }

    /** {@code fastKey}'s exposed entry from its term's leader, or empty. */
    Optional<byte[]> read(String fastKey) {
        Optional<FastKey.Parsed> parsed = FastKey.parse(fastKey)
                .filter(k -> k.prefix().equals(prefix));
        if (parsed.isEmpty()) {
            return Optional.empty();
        }
        long epoch = parsed.get().epoch();
        try {
            Optional<FastEntryService.Served> own = local.exposed(fastKey);
            if (own.isPresent()) {
                return Optional.of(own.get().bytes());
            }
            Optional<JoinedTerms.Leader> leader = joined.leader(epoch);
            if (leader.isEmpty()) {
                return Optional.empty();
            }
            return remote.fetch(leader.get().endpoint(), fastKey, selfUid)
                    .filter(s -> s.epoch() == epoch && s.epoch() >= fenceHighest.getAsLong())
                    .map(FastEntryService.Served::bytes);
        } catch (IOException | RuntimeException unread) {
            return Optional.empty();
        }
    }
}
