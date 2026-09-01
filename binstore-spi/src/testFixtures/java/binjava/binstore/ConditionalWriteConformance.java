// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * The CONDITIONAL-WRITE half of the store contract: {@code putIfAbsent} and
 * {@code putIfMatch}, run against every backend.
 *
 * <p>⚠️ Split out of {@link BinStoreConformance} when that file passed the
 * 500-line limit (code-structure.md rule 1: split it, never raise the limit).
 * {@link BinStoreConformance} extends this rather than the other way round so
 * every existing backend test class (which extends {@code BinStoreConformance}
 * alone) keeps compiling unchanged.
 *
 * <p>⚠️ CAS semantics are where object-store providers actually differ and
 * where a subtle divergence silently breaks the commit protocol or the lease
 * (design doc 07 §3, ADR-0008) — this is the highest-leverage test class in
 * the project, not a formality.
 */
public abstract class ConditionalWriteConformance {

    /** A fresh, empty store. Closed by the test. */
    protected abstract BinStore newStore() throws Exception;

    protected static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    protected static String read(InputStream in) throws Exception {
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void putIfAbsentWritesOnlyWhenTheKeyIsFree() throws Exception {
        try (BinStore s = newStore()) {
            Optional<Version> first = s.putIfAbsent("k", Body.ofBytes(bytes("first")));
            assertThat(first).as("a free key must be written").isPresent();

            Optional<Version> second = s.putIfAbsent("k", Body.ofBytes(bytes("second")));
            // ⚠️ EMPTY, not an exception. The commit log is a chain of these
            // (ADR-0002); a loser re-reads and retries at the next sequence
            // number, so a throw here would turn a normal race into an outage.
            assertThat(second).as("an occupied key yields empty").isEmpty();
            // ⚠️ And the body must be UNTOUCHED. An implementation that returns
            // empty but writes anyway loses the winner's record — invariant I1,
            // "no seq is ever written twice", with no error to notice.
            assertThat(read(s.get("k"))).isEqualTo("first");
        }
    }

    @Test
    void putIfAbsentSucceedsExactlyOnceUnderConcurrentWriters() throws Exception {
        // ⚠️ THE reason this suite exists (design doc 07 §3, ADR-0002). Every
        // sequential case passes against a check-then-put:
        //     if (exists(key)) return empty; write(key); return present;
        // which loses a record whenever two writers interleave -- exactly the
        // choice a filesystem backend faces, O_CREAT|O_EXCL versus Files.exists()
        // then write. Invariant I1, "no seq is ever written twice", would be
        // violated with no error to notice.
        //
        // ⚠️ MANY KEYS, not one. Against an in-memory map a single contended key
        // caught check-then-put 5 times out of 5; on a real filesystem the window
        // between exists() and create is narrow enough that it caught it only 4
        // times in 6. A gate that reports green on a broken backend one run in
        // three is worse than no gate. Contending many keys makes a miss require
        // every one of them to go the wrong way.
        final int writers = 8;
        final int keys = 24;
        try (BinStore s = newStore()) {
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            try {
                for (int k = 0; k < keys; k++) {
                    final String key = "contended-" + k;
                    CyclicBarrier start = new CyclicBarrier(writers);
                    List<Callable<Optional<Version>>> tasks = new java.util.ArrayList<>();
                    List<String> candidates = new java.util.ArrayList<>();
                    for (int i = 0; i < writers; i++) {
                        final String body = "writer-" + i;
                        candidates.add(body);
                        tasks.add(() -> {
                            start.await();   // maximise the overlap
                            return s.putIfAbsent(key, Body.ofBytes(bytes(body)));
                        });
                    }
                    long winners = 0;
                    for (Future<Optional<Version>> f : pool.invokeAll(tasks)) {
                        if (f.get().isPresent()) {
                            winners++;
                        }
                    }
                    assertThat(winners).as("exactly one writer may win %s", key).isEqualTo(1);
                    // ⚠️ EXACTLY one writer's body, not merely one that starts
                    // like it: startsWith("writer-") accepted a body truncated to
                    // "writer-", the torn-write shape a backend that creates the
                    // final path and streams into it produces.
                    assertThat(read(s.get(key))).isIn(candidates);
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void putIfMatchSucceedsWhenTheVersionMatchesAndAdvancesIt() throws Exception {
        try (BinStore s = newStore()) {
            Version v1 = s.put("k", Body.ofBytes(bytes("first")));
            // ⚠️ ROUND-TRIP FIDELITY (design doc 07 §3): the version a WRITE
            // returned is the one a LATER putIfMatch must accept -- a backend
            // whose putIfMatch parses or reformats the token would pass a
            // same-process test and fail the moment it received a version this
            // store itself produced two calls ago.
            Optional<Version> result = s.putIfMatch("k", Body.ofBytes(bytes("second")), v1);
            assertThat(result).as("a matching version must succeed").isPresent();
            assertThat(read(s.get("k"))).isEqualTo("second");
            Version v2 = s.stat("k").orElseThrow().version();
            // ⚠️ The version ADVANCES, exactly like put and putIfAbsent. A
            // constant version here would let a second putIfMatch against the
            // same stale v1 succeed twice.
            assertThat(v2).isNotEqualTo(v1);
            assertThat(result.get()).isEqualTo(v2);
        }
    }

    @Test
    void putIfMatchReturnsEmptyWhenTheVersionHasMovedAndLeavesTheObjectIntact() throws Exception {
        try (BinStore s = newStore()) {
            Version v1 = s.put("k", Body.ofBytes(bytes("first")));
            s.put("k", Body.ofBytes(bytes("second"))); // moves the version out from under v1
            // ⚠️ EMPTY, never an exception -- ADR-0008: a lease renewal or a
            // registry update that lost the race is a normal outcome the caller
            // re-reads and reacts to, not a failure that unwinds a request.
            Optional<Version> result = s.putIfMatch("k", Body.ofBytes(bytes("third")), v1);
            assertThat(result).as("a version that has moved yields empty").isEmpty();
            // ⚠️ And the body/version must be UNTOUCHED by the rejected write --
            // the same shape putIfAbsent's own case guards, for the same reason:
            // a backend that returns empty but writes anyway silently loses the
            // winner's record.
            assertThat(read(s.get("k"))).isEqualTo("second");
        }
    }

    @Test
    void putIfMatchOnAnAbsentKeyFailsDistinctlyFromAVersionMismatch() throws Exception {
        try (BinStore s = newStore()) {
            // ⚠️ NOT empty. An empty Optional is reserved for "the version
            // moved" -- there is no version to have moved from when the key was
            // never written, so folding this into the same case would make a
            // caller unable to tell "someone beat me to it" from "this object
            // does not exist", which the ordinal registry and the lease both
            // need to distinguish (ADR-0008).
            assertThatThrownBy(() -> s.putIfMatch("missing", Body.ofBytes(bytes("x")),
                    new Version("anything")))
                    .isInstanceOf(IOException.class);
            assertThat(s.stat("missing")).as("nothing may be created by a failed match").isEmpty();
        }
    }

    @Test
    void putIfMatchWithAMismatchedVersionYieldsEmptyEvenWhenTheBodyIsBad() throws Exception {
        try (BinStore s = newStore()) {
            Version v1 = s.put("k", Body.ofBytes(bytes("first")));
            s.put("k", Body.ofBytes(bytes("second"))); // moves the version away from v1
            // ⚠️ round-1 review (M2.0) found MemoryBinStore reading the body
            // BEFORE checking the version, so this exact COMBINATION -- a stale
            // version AND a body whose declared length disagrees with its
            // stream -- threw IOException instead of returning empty,
            // contradicting "never an exception" on a moved version. Neither
            // sibling case above catches it alone: the version-mismatch case
            // uses a good body, and aBodyWhoseLengthDisagreesWithItsStreamIsRefused's
            // putIfMatch case uses a MATCHING version.
            Body lying = new Body(999, () -> new java.io.ByteArrayInputStream(bytes("short")));
            Optional<Version> result = s.putIfMatch("k", lying, v1);
            assertThat(result).as("a stale version yields empty even when the body is bad").isEmpty();
            assertThat(read(s.get("k"))).as("the previous object must survive").isEqualTo("second");
        }
    }

    @Test
    void putIfMatchSucceedsExactlyOnceUnderConcurrentWriters() throws Exception {
        // ⚠️ Same shape as putIfAbsent's concurrency case, and for the same
        // reason: every sequential case above passes a read-then-write built on
        // Files.exists()/stat() followed by a separate write, which loses a
        // record whenever two writers race between the read and the write.
        // MANY KEYS, because a single contended key does not reproduce the race
        // reliably enough for a gate to trust (see putIfAbsent's own case).
        final int writers = 8;
        final int keys = 24;
        try (BinStore s = newStore()) {
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            try {
                for (int k = 0; k < keys; k++) {
                    final String key = "cas-" + k;
                    Version base = s.put(key, Body.ofBytes(bytes("base")));
                    CyclicBarrier start = new CyclicBarrier(writers);
                    List<Callable<Optional<Version>>> tasks = new java.util.ArrayList<>();
                    List<String> candidates = new java.util.ArrayList<>();
                    for (int i = 0; i < writers; i++) {
                        final String body = "writer-" + i;
                        candidates.add(body);
                        tasks.add(() -> {
                            start.await();
                            return s.putIfMatch(key, Body.ofBytes(bytes(body)), base);
                        });
                    }
                    long winners = 0;
                    for (Future<Optional<Version>> f : pool.invokeAll(tasks)) {
                        if (f.get().isPresent()) {
                            winners++;
                        }
                    }
                    assertThat(winners).as("exactly one writer may win the match on %s", key)
                            .isEqualTo(1);
                    assertThat(read(s.get(key))).isIn(candidates);
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void conditionalWritesAreAdvertisedBecauseTheCommitLogRequiresThem() throws Exception {
        try (BinStore s = newStore()) {
            // ⚠️ Checked at startup so a backend lacking atomic putIfAbsent AND
            // putIfMatch fails loudly instead of silently corrupting the commit
            // log, the lease or the ordinal registry (ADR-0002, ADR-0008).
            assertThat(s.capabilities().conditionalWrites()).isTrue();
            assertThat(s.capabilities().maxKeyBytes()).isPositive();
            // ⚠️ M2.1: the ACTUAL startup check, not just the flag it reads --
            // every real backend must pass this without throwing.
            s.capabilities().requireConditionalWrites();
        }
    }
}
