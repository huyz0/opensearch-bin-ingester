// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.FastWriter;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The fast path over {@link FastWriter} (M13.29c; ADR-0081 §2): a fast
 * append sent as one COMMIT under this pod's own key to the leader of the
 * newest term it joined, resent under the SAME key while the leader holds it,
 * answered at the offsets assigned once EXPOSED -- and the default path where
 * there is no term to write through, or the leader takes no fast write.
 */
class FastWriterPathTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final UUID INCARNATION = new UUID(7, 7);

    /** One call to the writer: the term, its leader, and the COMMIT. */
    private record Sent(long epoch, String leaderUid, String endpoint,
            FastWriteFrame.Commit commit) {
    }

    /** A writer answering from a queue, recording every COMMIT. */
    private static final class Writer implements FastWriterPath.Writer {
        final List<Sent> sent = new ArrayList<>();
        final Deque<FastWriter.Outcome> answers = new ArrayDeque<>();

        @Override
        public FastWriter.Outcome write(long epoch, String leaderUid, String leaderEndpoint,
                FastWriteFrame.Commit commit) {
            sent.add(new Sent(epoch, leaderUid, leaderEndpoint, commit));
            return answers.size() > 1 ? answers.poll() : answers.peek();
        }
    }

    private static JoinedTerms joined() {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(2, 0, "uid-old", "old:1");
        joined.joined(3, 0, "uid-l", "l:1");
        return joined;
    }

    private static List<SegmentRecord> docs(int n) {
        return IngestTestSupport.docs(n);
    }

    private static FastWriter.Outcome written(long first) {
        return new FastWriter.Written(new FastWriteFrame.Assigned(0,
                List.of(new FastWriteFrame.AssignedRun(S, first, 1, false))));
    }

    private static FastWriterPath path(JoinedTerms joined, Writer writer) {
        return new FastWriterPath("pod1", INCARNATION, joined, writer, 3, 100);
    }

    @Test
    void aWRITTENBatchIsAnsweredAtTheOffsetsAssigned() throws Exception {
        Writer writer = new Writer();
        writer.answers.add(written(40));

        AppendResult got = path(joined(), writer).append(S, docs(3)).orElseThrow();

        assertThat(got).isEqualTo(new AppendResult(3, 40, 42));
        assertThat(writer.sent).singleElement().satisfies(s -> {
            assertThat(s.epoch()).as("the newest term joined").isEqualTo(3);
            assertThat(s.leaderUid()).isEqualTo("uid-l");
            assertThat(s.endpoint()).isEqualTo("l:1");
            assertThat(s.commit().key().podId()).as("this pod's own key").isEqualTo("pod1");
            assertThat(s.commit().key().incarnationId()).isEqualTo(INCARNATION);
            assertThat(s.commit().runs()).singleElement()
                    .satisfies(r -> assertThat(r.stream()).isEqualTo(S));
        });
    }

    @Test
    void aBATCHOfRunsIsAnsweredByStream() throws Exception {
        // ⚠️ M13.29d: the leader's runs matched by stream, never by position.
        RunKey t = new RunKey(new UUID(2, 2), 0);
        Writer writer = new Writer();
        writer.answers.add(new FastWriter.Written(new FastWriteFrame.Assigned(0, List.of(
                new FastWriteFrame.AssignedRun(t, 5, 1, false),
                new FastWriteFrame.AssignedRun(S, 40, 1, false)))));

        assertThat(path(joined(), writer).write(List.of(
                new FastWriteFrame.CommitRun(S, docs(1)), new FastWriteFrame.CommitRun(t, docs(2)))))
                .hasValue(java.util.Map.of(S, 40L, t, 5L));
    }

    @Test
    void anASSIGNEDMissingARunIsAFailureTheProducerRetries() {
        // ⚠️ M13.29c review round 1, P1: an IOException, never an unchecked one.
        RunKey t = new RunKey(new UUID(2, 2), 0);
        Writer writer = new Writer();
        writer.answers.add(written(40));

        assertThatThrownBy(() -> path(joined(), writer).write(List.of(
                new FastWriteFrame.CommitRun(S, docs(1)), new FastWriteFrame.CommitRun(t, docs(2)))))
                .isInstanceOf(IOException.class);
    }

    @Test
    void anASSIGNEDNamingARunNotSentIsAFailureTheProducerRetries() {
        // ⚠️ M13.29d review round 1, T4.
        RunKey t = new RunKey(new UUID(2, 2), 0);
        Writer writer = new Writer();
        writer.answers.add(new FastWriter.Written(new FastWriteFrame.Assigned(0, List.of(
                new FastWriteFrame.AssignedRun(S, 40, 1, false),
                new FastWriteFrame.AssignedRun(t, 5, 1, false)))));

        assertThatThrownBy(() -> path(joined(), writer).write(List.of(
                new FastWriteFrame.CommitRun(S, docs(1))))).isInstanceOf(IOException.class);
    }

    @Test
    void aRETRYIsResentUnderTheSameKey() throws Exception {
        Writer writer = new Writer();
        writer.answers.add(new FastWriter.Retry("held"));
        writer.answers.add(written(40));

        path(joined(), writer).append(S, docs(1));

        assertThat(writer.sent).hasSize(2);
        assertThat(writer.sent.get(1).commit()).isEqualTo(writer.sent.get(0).commit());
    }

    @Test
    void aBATCHNeverExposedIsRefusedAfterItsAttempts() {
        Writer writer = new Writer();
        writer.answers.add(new FastWriter.Retry("unexposed in time"));

        assertThatThrownBy(() -> path(joined(), writer).append(S, docs(1)))
                .isInstanceOf(IOException.class);
        assertThat(writer.sent).as("three attempts, no more").hasSize(3);
    }

    @Test
    void eachAPPENDTakesAKeyOfItsOwn() throws Exception {
        Writer writer = new Writer();
        writer.answers.add(written(40));
        FastWriterPath path = path(joined(), writer);

        path.append(S, docs(1));
        path.append(S, docs(1));

        assertThat(writer.sent.get(1).commit().key().fastSeq())
                .isNotEqualTo(writer.sent.get(0).commit().key().fastSeq());
    }

    @Test
    void notFASTSendsTheBatchTheDefaultWay() throws Exception {
        Writer writer = new Writer();
        writer.answers.add(new FastWriter.NotFast("no fast journal"));

        assertThat(path(joined(), writer).append(S, docs(1))).isEmpty();
    }

    @Test
    void aPODThatJoinedNoTermWritesTheDefaultWay() throws Exception {
        Writer writer = new Writer();

        assertThat(path(new JoinedTerms(), writer).append(S, docs(1))).isEmpty();
        assertThat(writer.sent).isEmpty();
    }

    @Test
    void anAPPENDOverTheBoundWritesTheDefaultWay() throws Exception {
        Writer writer = new Writer();

        assertThat(path(joined(), writer).append(S, docs(101))).as("over B = 100").isEmpty();
        assertThat(writer.sent).isEmpty();
    }

    @Test
    void aFENCEDOrUnrosteredWriterRefusesTheAppend() {
        Writer fenced = new Writer();
        fenced.answers.add(new FastWriter.Fenced(4));
        Writer rejoin = new Writer();
        rejoin.answers.add(new FastWriter.Rejoin("not rostered"));

        assertThatThrownBy(() -> path(joined(), fenced).append(S, docs(1)))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> path(joined(), rejoin).append(S, docs(1)))
                .isInstanceOf(IOException.class);
        assertThat(fenced.sent).as("never resent to a deposed term").hasSize(1);
    }
}
