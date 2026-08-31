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
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * The contract EVERY backend must satisfy, run against each one.
 *
 * <p>⚠️ It lives in testFixtures because a conformance suite that only ever runs
 * against the in-memory fake proves nothing about S3. Each backend subclasses
 * this and supplies a store; a behaviour that differs between backends is a bug
 * in one of them, and this is where that shows up rather than in production.
 *
 * <p>⚠️ M1 runs the NON-CAS half. Lease, epoch and seal semantics arrive with
 * ADR-0002 in M4 and extend this class rather than replacing it.
 */
public abstract class BinStoreConformance {

    /** A fresh, empty store. Closed by the test. */
    protected abstract BinStore newStore() throws Exception;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String read(InputStream in) throws Exception {
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void put(BinStore s, String key, String body) throws Exception {
        s.put(key, Body.ofBytes(bytes(body)));
    }

    @Test
    void putThenGetReturnsTheSameBytes() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "a/b", "hello");
            assertThat(read(s.get("a/b"))).isEqualTo("hello");
        }
    }

    @Test
    void putOverwritesUnconditionally() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "k", "first");
            put(s, "k", "second");
            // ⚠️ put is UNCONDITIONAL. Making it behave like putIfAbsent would
            // silently keep the first body, which for a segment rewrite means
            // serving stale data forever.
            assertThat(read(s.get("k"))).isEqualTo("second");
        }
    }

    @Test
    void anOverwriteWithAShorterBodyLeavesNoTail() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "k", "0123456789");
            put(s, "k", "hi");
            // ⚠️ Both existing overwrite cases used a new body no SHORTER than
            // the old, so a backend opening the final path without O_TRUNC
            // passed them and then served "hi23456789" forever. Size is asserted
            // too: a reader that trusts stat() would fetch ten bytes.
            assertThat(read(s.get("k"))).isEqualTo("hi");
            assertThat(s.stat("k").orElseThrow().size()).isEqualTo(2);
        }
    }

    @Test
    void aRefusedWriteLeavesThePreviousObjectIntact() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "k", "good");
            Body lying = new Body(999, () -> new java.io.ByteArrayInputStream(bytes("short")));
            assertThatThrownBy(() -> s.put("k", lying)).isInstanceOf(IOException.class);
            // ⚠️ The length case only asserted nothing was CREATED. A backend that
            // streams into the final path and checks length at EOF has already
            // destroyed the previous good object by the time it refuses -- so the
            // caller loses data on a write it was told failed.
            assertThat(read(s.get("k"))).as("the previous object must survive").isEqualTo("good");
        }
    }

    @Test
    void listingAPrefixWithNoMatchesIsEmptyRatherThanAnError() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "p/a", "1");
            // ⚠️ No case listed zero matches. Files.list of a directory that does
            // not exist throws NoSuchFileException, so recovery over a FRESH
            // bucket -- the first thing that ever happens -- would die at startup.
            ListPage page = s.list("nothing-here/", null, 100);
            assertThat(page.objects()).isEmpty();
            assertThat(page.nextStartAfter()).isEmpty();
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
    void statReportsSizeAndVersionWithoutTheBody() throws Exception {
        try (BinStore s = newStore()) {
            assertThat(s.stat("missing")).as("an absent key stats empty").isEmpty();
            put(s, "k", "hello");
            Optional<ObjectStat> st = s.stat("k");
            assertThat(st).isPresent();
            assertThat(st.get().key()).isEqualTo("k");
            assertThat(st.get().size()).as("size in BYTES, not entries").isEqualTo(5);
            assertThat(st.get().version()).isNotNull();
        }
    }

    @Test
    void aVersionChangesWhenTheObjectChanges() throws Exception {
        try (BinStore s = newStore()) {
            Version v1 = s.put("k", Body.ofBytes(bytes("first")));
            // ⚠️ The version a WRITE returns must be the one stat reports.
            // Nothing read put's return value, so returning a constant lie while
            // storing the real one survived.
            assertThat(s.stat("k").orElseThrow().version()).isEqualTo(v1);
            // ⚠️ And it must be STABLE across calls. Asserting only that it
            // changes is satisfied by a version that changes every time it is
            // read -- exactly what a backend deriving one from mtime produces.
            assertThat(s.stat("k").orElseThrow().version()).isEqualTo(v1);

            put(s, "k", "second");
            Version v2 = s.stat("k").orElseThrow().version();
            // ⚠️ A constant version is the mutation that matters: every
            // compare-and-set built on it would then succeed against stale data.
            assertThat(v2).isNotEqualTo(v1);
        }
    }

    @Test
    void getRangeIsInclusiveOfBothEnds() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "k", "0123456789");
            // ⚠️ endIncl, not endExclusive. Off by one here truncates the last
            // byte of every segment run read through the directory.
            assertThat(read(s.getRange("k", 2, 5))).isEqualTo("2345");
            assertThat(read(s.getRange("k", 0, 0))).as("a single byte").isEqualTo("0");
            assertThat(read(s.getRange("k", 9, 9))).as("the final byte").isEqualTo("9");
            // ⚠️ Past EOF CLAMPS, as S3, GCS and Azure do. Throwing would force
            // every caller into stat-then-getRange, doubling requests per
            // segment read (R7) to learn what the read itself already knows.
            //
            // ⚠️ EQUIVALENT MUTANT on the in-memory backend, disclosed rather
            // than hidden: dropping its Math.min still yields "89", because
            // ByteArrayInputStream clamps count to the buffer internally. The
            // case earns its place against M1.2's LocalFsBinStore, where an
            // unclamped length reads past the file and throws.
            assertThat(read(s.getRange("k", 8, 99))).as("clamped to the end").isEqualTo("89");
            // ⚠️ But a start PAST the end must FAIL, not clamp to zero bytes.
            // Clamping there hands the caller an empty stream where it needs an
            // error -- the same defect readingAnAbsentKeyFails... forbids, and
            // the RandomAccessFile.seek default. This one a fake CAN kill, so the
            // boundary does not rest on a backend that does not exist yet.
            assertThatThrownBy(() -> s.getRange("k", 10, 12).close()).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> s.getRange("k", 5, 2).close())
                    .as("an inverted range is not a range").isInstanceOf(IOException.class);
        }
    }

    @Test
    void aBodyWhoseLengthDisagreesWithItsStreamIsRefused() throws Exception {
        // ⚠️ length was decorative -- nothing read it and deleting the component
        // left every test green. S3 is handed it as content-length, so a
        // mismatch becomes a TRUNCATED object that reads back as a valid short
        // segment. Refused at write time instead.
        try (BinStore s = newStore()) {
            Body lying = new Body(999, () -> new java.io.ByteArrayInputStream(bytes("short")));
            assertThatThrownBy(() -> s.put("k", lying)).isInstanceOf(IOException.class);
            assertThat(s.stat("k")).as("nothing may be stored").isEmpty();
            // ⚠️ putIfAbsent TOO, and it is the path that matters more: a
            // truncated put is a bad segment, a truncated putIfAbsent is a bad
            // commit record at a seq that can never be rewritten (invariant I1).
            assertThatThrownBy(() -> s.putIfAbsent("c", lying)).isInstanceOf(IOException.class);
            assertThat(s.stat("c")).isEmpty();
            assertThatThrownBy(
                    () -> s.putIfAbsent("k".repeat((int) s.capabilities().maxKeyBytes() + 1),
                            Body.ofBytes(bytes("x"))))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void listReturnsOnlyThePrefixInLexicographicOrder() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "p/c", "3");
            put(s, "p/a", "1");
            put(s, "p/b", "2");
            put(s, "other/z", "x");
            // ⚠️ BOTH sides of the prefix range. `other/z` sorts BEFORE "p/" so
            // a lower-bound seek excludes it for free — with only that key, an
            // implementation that never checks the prefix at all still passed.
            // `q/z` sorts AFTER, and is what the prefix check actually guards.
            put(s, "q/z", "y");
            List<String> keys = s.list("p/", null, 100).objects().stream()
                    .map(ObjectStat::key).collect(Collectors.toList());
            // ⚠️ Exact order AND exact membership. A count assertion passes when
            // the wrong keys come back, and recovery walks this listing in order.
            assertThat(keys).containsExactly("p/a", "p/b", "p/c");
        }
    }

    @Test
    void listResumesStrictlyAfterTheGivenKey() throws Exception {
        try (BinStore s = newStore()) {
            // ⚠️ A key sorting BEFORE the prefix must exist, or the below-prefix
            // resume assertion below cannot bite: with only p/* present, seeking
            // from "" lands on p/a and every ordering bug looks correct.
            put(s, "aaa/z", "0");
            put(s, "p/a", "1");
            put(s, "p/b", "2");
            put(s, "p/c", "3");
            List<String> keys = s.list("p/", "p/a", 100).objects().stream()
                    .map(ObjectStat::key).collect(Collectors.toList());
            // ⚠️ STRICTLY after: including the resume point makes recovery
            // reprocess one record on every page boundary.
            assertThat(keys).containsExactly("p/b", "p/c");

            // ⚠️ A resume point BELOW the prefix must not skip the prefix. Seeking
            // from startAfter alone returned nothing at all here, so recovery
            // concluded "nothing to recover" with no error. S3 ignores a
            // below-prefix start-after.
            assertThat(s.list("p/", "", 100).objects()).as("below-prefix resume").hasSize(3);
        }
    }

    @Test
    void aPrefixIsAByteRangeNotADirectory() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "p/a", "1");
            put(s, "p/ab", "2");
            put(s, "p/a/x", "3");
            // ⚠️ EVERY other list case puts each key exactly one level under a
            // prefix ending in "/", so a readdir -- Files.walk / File.list, what
            // a filesystem backend gets for free -- passes them all. A prefix is
            // a BYTE range: "p/a" matches "p/a", "p/a/x" and "p/ab" alike, and
            // the order is bytewise ('/' is 0x2F, 'b' is 0x62), not tree order.
            assertThat(s.list("p/a", null, 100).objects().stream().map(ObjectStat::key))
                    .containsExactly("p/a", "p/a/x", "p/ab");
        }
    }

    @Test
    void listPagesAtMaxKeysAndResumesWhereItStopped() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "p/a", "1");
            put(s, "p/b", "2");
            put(s, "p/c", "3");
            // ⚠️ ONE CALL IS ONE REQUEST, which is why paging is explicit rather
            // than a lazy Stream: the counting decorator (R9) must see each page.
            ListPage first = s.list("p/", null, 2);
            assertThat(first.objects().stream().map(ObjectStat::key)).containsExactly("p/a", "p/b");
            assertThat(first.nextStartAfter()).as("more remains").contains("p/b");

            ListPage second = s.list("p/", first.nextStartAfter().orElseThrow(), 2);
            assertThat(second.objects().stream().map(ObjectStat::key)).containsExactly("p/c");
            assertThat(second.nextStartAfter()).as("the listing is complete").isEmpty();

            // ⚠️ EXACTLY maxKeys is the boundary nothing hit. Reporting "more
            // remains" whenever a page is full costs one extra LIST on every
            // complete listing -- R2/R9, and invisible to every functional
            // assertion because the next page is simply empty.
            ListPage exact = s.list("p/", null, 3);
            assertThat(exact.objects()).hasSize(3);
            assertThat(exact.nextStartAfter()).as("a full final page is still final").isEmpty();
        }
    }

    @Test
    void deleteRemovesTheNamedKeysAndToleratesAbsentOnes() throws Exception {
        try (BinStore s = newStore()) {
            put(s, "p/a", "1");
            put(s, "p/b", "2");
            put(s, "p/c", "3");
            // ⚠️ The absent key comes FIRST. With it last, "abort silently at the
            // first absent key" survives -- and catch-around-the-loop is the
            // natural filesystem shape, which is exactly the GC-re-run case this
            // test's own comment describes.
            s.delete(List.of("never-existed", "p/a", "p/c"));
            // ⚠️ An absent key is NOT an error: GC re-runs after a crash and
            // would otherwise fail permanently on its own completed work.
            assertThat(s.stat("p/a")).isEmpty();
            assertThat(s.stat("p/c")).as("keys after the absent one must still go").isEmpty();
            assertThat(s.stat("p/b")).as("an unnamed key must survive").isPresent();
        }
    }

    @Test
    void readingAnAbsentKeyFailsRatherThanReturningEmptyBytes() throws Exception {
        try (BinStore s = newStore()) {
            // ⚠️ Empty bytes would be indistinguishable from a zero-length
            // object, and a segment reader would parse it as a valid empty
            // segment instead of reporting the missing object.
            assertThatThrownBy(() -> s.get("missing").close())
                    .isInstanceOf(java.io.IOException.class);
        }
    }

    @Test
    void rangeReadingAnAbsentKeyFailsToo() throws Exception {
        try (BinStore s = newStore()) {
            // ⚠️ getRange is what the segment directory calls, and it had only a
            // happy path. RandomAccessFile.seek past EOF short-reads rather than
            // throwing, so a filesystem backend lands on "zero bytes" by default.
            assertThatThrownBy(() -> s.getRange("missing", 0, 10).close())
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void aKeyLongerThanTheAdvertisedLimitIsRejected() throws Exception {
        try (BinStore s = newStore()) {
            // ⚠️ PROBED, not taken on advertisement. isPositive() is an assume:
            // the longest key any other case writes is 7 bytes, so a backend
            // with a real per-component limit passes while claiming 1024.
            long max = s.capabilities().maxKeyBytes();
            // ⚠️ BOTH sides. Asserting only the rejection lets a backend claim
            // 1024 and reject at 255 (NAME_MAX) -- the exact lie this case was
            // written to catch.
            s.put("k".repeat((int) max), Body.ofBytes(bytes("x")));
            assertThat(s.stat("k".repeat((int) max))).as("a key AT the limit is accepted").isPresent();
            // ⚠️ BYTES, not chars. The probe was all-ASCII, so counting
            // key.length() survived -- and NAME_MAX, which M1.2's filesystem
            // backend answers to, is a byte limit. "é" is two bytes in UTF-8.
            String multiByte = "é".repeat((int) max);
            assertThatThrownBy(() -> s.put(multiByte, Body.ofBytes(bytes("x"))))
                    .as("a key of max chars but 2x max bytes must be refused")
                    .isInstanceOf(IOException.class);
            assertThatThrownBy(() -> s.put("k".repeat((int) max + 1), Body.ofBytes(bytes("x"))))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void conditionalWritesAreAdvertisedBecauseTheCommitLogRequiresThem() throws Exception {
        try (BinStore s = newStore()) {
            // ⚠️ Checked at startup so a backend lacking atomic putIfAbsent fails
            // loudly instead of silently corrupting the commit log (ADR-0002).
            assertThat(s.capabilities().conditionalWrites()).isTrue();
            assertThat(s.capabilities().maxKeyBytes()).isPositive();
        }
    }
}
