// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.SignedUrl;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Charges each commit-log PUT to the indices of the delta it carries, by
 * record count (M11.22, ADR-0077 as amended): the per-index half of the
 * commit bill.
 *
 * <p>⚠️ **WHERE THE DELTA IS PUT, NOT WHERE IT WAS FLUSHED.** The leader
 * batches commits from every pod into one delta per window (M8.50), and a
 * follower's commit is PUT by the leader, so a commit PUT belongs to no one
 * flush. A decorator sits on the request itself: every commit-log PUT the pod
 * issues -- an append, a CONTINUE, a seal, a predecessor chain's seal, a lost
 * race -- passes it once, by construction, whichever {@code CommitLog} issued
 * it.
 *
 * <p>⚠️ **BELOW THE GOVERNOR AND ABOVE THE COUNTER, AND CHARGED BEFORE THE
 * DELEGATE IS CALLED**, as the counter counts: an attempt that throws was
 * billed and is charged. Which keys are commit PUTs is the counter's own
 * classifier ({@link CountingBinStore#isCommitLogDelta}), so the charged series
 * and the counted one cannot drift apart.
 *
 * <p>⚠️ **RECORD COUNTS, NOT BYTES**: a delta carries each run's record count
 * and no byte length, and the delta is what this request wrote. An entry with
 * no runs -- a CONTINUE, a seal -- and one that will not decode are charged to
 * {@code unattributed}.
 *
 * <p>⚠️ IT READS THE BODY A SECOND TIME. Commit-log bodies are small byte
 * arrays the log already encoded, and a body over {@link #MAX_DECODED_BYTES}
 * is charged unattributed rather than read.
 */
public final class CommitChargingBinStore implements BinStore {

    /** A commit-log entry larger than this is not read to be charged. */
    static final long MAX_DECODED_BYTES = 16L << 20;

    private final BinStore delegate;
    private final IndexCostLedger ledger;

    public CommitChargingBinStore(BinStore delegate, IndexCostLedger ledger) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    private void chargeIfCommit(String key, Body body) {
        if (!CountingBinStore.isCommitLogDelta(key)) {
            return;
        }
        Map<UUID, Long> records = recordsByIndex(body);
        if (records.isEmpty()) {
            ledger.unattributed(IndexCostLedger.Charge.COMMIT_PUT);
        } else {
            ledger.apportion(IndexCostLedger.Charge.COMMIT_PUT, records);
        }
    }

    private static Map<UUID, Long> recordsByIndex(Body body) {
        if (body.length() > MAX_DECODED_BYTES) {
            return Map.of();
        }
        ChainEntry entry;
        try (InputStream in = body.open().get()) {
            entry = ChainEntry.decode(in.readAllBytes());
        } catch (IOException | RuntimeException undecodable) {
            return Map.of();
        }
        if (!(entry instanceof CommitDelta delta)) {
            return Map.of();
        }
        Map<UUID, Long> records = new HashMap<>();
        for (SegmentCommit segment : delta.segments()) {
            for (RunCommit run : segment.runs()) {
                records.merge(run.key().indexId(), (long) run.recordCount(), Long::sum);
            }
        }
        return records;
    }

    @Override
    public InputStream get(String key) throws IOException {
        return delegate.get(key);
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        return delegate.getRange(key, start, endIncl);
    }

    @Override
    public Optional<ObjectStat> stat(String key) throws IOException {
        return delegate.stat(key);
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        chargeIfCommit(key, body);
        return delegate.put(key, body);
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        chargeIfCommit(key, body);
        return delegate.putIfAbsent(key, body);
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected)
            throws IOException {
        chargeIfCommit(key, body);
        return delegate.putIfMatch(key, body, expected);
    }

    @Override
    public MultipartWriter multipart(String key) throws IOException {
        return delegate.multipart(key);
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
        return delegate.list(prefix, startAfter, maxKeys);
    }

    @Override
    public SignedUrl presign(String key, java.time.Duration ttl) throws IOException {
        return delegate.presign(key, ttl);
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override
    public Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
