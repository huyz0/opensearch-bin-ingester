// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/**
 * Many logical pods, one seed, through the {@link Sequencer} seam.
 *
 * <p>⚠️ DETERMINISTIC BY CONSTRUCTION. Every decision — which pod acts, when a
 * leader dies, when the clock advances past a TTL — comes from one seeded
 * {@link Random}, and time comes from {@link SimulatedClock}. That is what makes
 * a failing seed a permanent named regression instead of a story about a flake,
 * and it is why nothing here reads a wall clock or sleeps.
 *
 * <p>⚠️ FAILOVER BOTH WAYS, deliberately. A leader either RELEASES its lease
 * (the polite path, milliseconds) or simply stops answering and lets the TTL
 * expire (the path that actually races a fenced leader, and the only one where
 * the old leader may still have writes in flight). A simulation that only did
 * the first would never build the chain contention the seal protocol exists for.
 *
 * <p>⚠️ PODS TAKE TURNS, ONE CALL AT A TIME, AND THAT IS DELIBERATE -- not a gap
 * awaiting M4.13. {@code FaultInjectingStore} REQUIRES single-threaded driving
 * and says so in a note addressed to this file: its {@code injected} list and
 * {@code drawCounts} map are unsynchronised and its three streams are drawn in
 * per-call order, so concurrency here would not corrupt the store, it would
 * corrupt REPRODUCIBILITY -- and a seed that no longer replays takes the whole
 * named-regression discipline with it.
 *
 * <p>⚠️ SO THIS REACHES INTERLEAVINGS OF FAILOVER AND NOT OF TWO WRITERS RACING
 * ONE SLOT IN REAL TIME -- and here is where that race IS covered, corrected
 * because an earlier version of this paragraph named the wrong test and deferred
 * it to the wrong row. {@code LeaseManagerConcurrencyTest} has a single test and
 * it races two acquires of the LEASE key on one instance. The CHAIN-SLOT race
 * lives in {@code CommitLogSealWriteTest} and in {@code LocalSequencerTest} via
 * {@code StealFirstPutStore}. And the fault classes this harness does not model
 * are owned by rows of their own since M4.13 was split by mechanism: delayed
 * writes and reordered completions by M4.13b, partitioned leaders by M4.13e,
 * the withheld-write ambiguity by M4.13c. None of their rows says anything
 * about concurrent pods, which nothing is asking for.
 */
public final class CommitProtocolSimulation {

    private static final UUID STREAM = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);
    static final String PREFIX = "bins/cluster-a";

    /**
     * How often a NON-leader pod commits by FORWARDING, in percent (M5.7).
     *
     * <p>⚠️ NOT 100. A fleet where every commit forwards never exercises the
     * leaseholder's own path, and the deployment M5 makes correct has both:
     * FR-12 mandates at least two ingester nodes per AZ, so the leaseholder is
     * one pod among many and it writes its own flushes too.
     *
     * <p>⚠️ IT IS THE DRAW RATE, AND THREE OTHER NUMBERS FALL OUT OF IT.
     * MEASURED at 1,000 seeds and 120 rounds: 32.95% of rounds become a forward
     * ATTEMPT, 6.0% of draws are discarded because the pod drawn is the one
     * leading, and only 4.02% of rounds are CONSUMED by a forward -- because
     * 82.5% of attempts are issued while nobody leads, and those fall through
     * to the election rather than spending the round.
     */
    private static final int FORWARD_RATE = 35;

    /**
     * One idempotency key as it was ISSUED, with the segment it named.
     *
     * <p>⚠️ CARRIES THE INCARNATION. An earlier version recorded only
     * `(podId, flushSeq)`, so giving each pod its own incarnation changed
     * nothing any assertion could see: both sweeps still keyed on the pair
     * ADR-0036 rejects, and a dedup keyed on it was indistinguishable from a
     * correct one in every seed.
     */
    public record Issued(String podId, String incarnationId, long flushSeq,
            String segmentKey) {
    }

    /** What one seed produced, including what it is NOT evidence about. */
    public record Result(long seed, int commits, int takeovers, long highestEpoch,
            int zombieWrites, int zombieAttempts,
            int forwardedCommits, int forwardAttempts, int forwardRefusals,
            List<FaultInjectingStore.Injected> faults,
            List<Issued> issued,
            List<Invariants.Violation> violations,
            List<AckOrderInvariants.AckEvent> acks,
            int readersChecked, int midRunDrained,
            int gracefulReleases, int gracefulReleasesRefusedForAnotherPod) {

        /**
         * ⚠️ THE TRACE AND THE READER COUNT ARE PUBLISHED SO A TEST CAN SEE THEM
         * FIRE, not for information. Review measured both arms unfalsifiable:
         * neutralising every {@code acks.add} left 388 tests green, and deleting
         * the {@code checkReader} loop left the whole suite green -- on the
         * checker that carries I3 and I4's drop clause, which is half of what
         * M4's completion condition claims, and which is the O(E-squared) loop
         * anyone optimising against the 60s budget deletes first.
         * ⚠️ THIS IS THE SHAPE THAT ALREADY SHIPPED ONCE HERE: the slot-0
         * CONTINUE emission was documented in three places and present in none,
         * and nothing could tell.
         */
        public long lowestAckedSequenceIn(long epoch) {
            return acks.stream().filter(e -> e.epoch() == epoch)
                    .mapToLong(AckOrderInvariants.AckEvent::sequence).min().orElse(-1L);
        }
    }

    private CommitProtocolSimulation() {
    }

    /**
     * ⚠️ READ FROM THE REQUEST THAT IS SENT, never from sibling locals. Built
     * from locals, the recording agreed with the request only BY ADJACENCY:
     * review measured that reverting just the request's `flushSeq` argument,
     * leaving the recording alone, kept this test and the whole suite green
     * while the driver reissued `(pod1, 0)` for two different segments.
     */
    private static Issued record(CommitRequest request) {
        return new Issued(request.podId(), request.incarnationId(), request.flushSeq(),
                request.segmentKey());
    }

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(STREAM, 0), n);
        return m;
    }

    /**
     * Runs one seed to completion and checks every chain it produced.
     *
     * @param rounds how many act-or-fail steps to take
     * @param pods how many logical pods contend for the lease
     */
    public static Result run(long seed, int rounds, int pods,
            FaultInjectingStore.Faults faults) throws IOException {
        return run(seed, rounds, pods, faults, new MemoryBinStore());
    }

    /**
     * The same, with pods that FORWARD rather than lead (M5.7).
     *
     * <p>⚠️ A PARAMETER RATHER THAN A REWRITE, because the SPEC says the
     * simulation is EXTENDED and not replaced: every seed the four-argument
     * overload has ever run stays bit-identical, so M4's evidence is not
     * silently re-based by a change to the draw sequence. The sweep turns it
     * on; the focused tests that measure the leaseholder's own path do not.
     */
    public static Result runForwarding(long seed, int rounds, int pods,
            FaultInjectingStore.Faults faults) throws IOException {
        return run(seed, rounds, pods, faults, new MemoryBinStore(),
                ReaderInvariants.ReaderView::of, null, true);
    }

    /**
     * The same, writing into a store the CALLER can then inspect.
     *
     * <p>⚠️ Exists so a failing seed can be dumped entry by entry. A seed is
     * reproducible by number, which is worth nothing if the only thing that can
     * be read back is the count of violations.
     */
    public static Result run(long seed, int rounds, int pods,
            FaultInjectingStore.Faults faults, MemoryBinStore backing) throws IOException {
        return run(seed, rounds, pods, faults, backing, ReaderInvariants.ReaderView::of);
    }

    /**
     * The same, with a METER between the injector and the store.
     *
     * <p>⚠️ FOR COUNTING REQUESTS THAT ACTUALLY REACHED A STORE. A caller that
     * wants to know whether a fault issued a real write cannot learn it from
     * {@code injected()}, which records the intent before the write is made --
     * review measured a test passing after the write it claimed to count was
     * deleted. The meter sits under the injector, so it sees what happened
     * rather than what was intended.
     */
    public static Result run(long seed, int rounds, int pods,
            FaultInjectingStore.Faults faults, MemoryBinStore backing,
            binjava.binstore.CountingBinStore meter) throws IOException {
        return run(seed, rounds, pods, faults, backing,
                ReaderInvariants.ReaderView::of, meter);
    }

    /**
     * The same, with the reader VIEW the checking loop judges supplied by the
     * caller.
     *
     * <p>⚠️ A SEAM PURELY SO THE LOOP'S OWN WIRING CAN BE PINNED, and it exists
     * because review measured the alternative failing: dropping the
     * {@code addAll} while leaving the {@code checkReader} call in place -- so
     * the verdict is computed and thrown away -- left the whole suite green.
     * {@code readersChecked} counts ITERATIONS, and a test that calls
     * {@code checkReader} itself shares no line with this loop, so neither can
     * see it. Half of M4's completion condition, I3 and I4's drop clause, could
     * be discarded by whoever optimises this O(E-squared) loop against the 60s
     * budget, with the sweep still printing its reader count and passing.
     *
     * <p>⚠️ PRODUCTION'S READER IS CORRECT, which is why the broken view has to
     * come from outside: no fault the injector can produce makes a real reader
     * over- or under-apply, so there is no other way to make this loop report.
     */
    public static Result run(long seed, int rounds, int pods,
            FaultInjectingStore.Faults faults, MemoryBinStore backing,
            java.util.function.Function<CommitLog, ReaderInvariants.ReaderView> viewFor)
            throws IOException {
        return run(seed, rounds, pods, faults, backing, viewFor, null);
    }

    static Result run(long seed, int rounds, int pods,
            FaultInjectingStore.Faults faults, MemoryBinStore backing,
            java.util.function.Function<CommitLog, ReaderInvariants.ReaderView> viewFor,
            binjava.binstore.CountingBinStore meter)
            throws IOException {
        return run(seed, rounds, pods, faults, backing, viewFor, meter, false);
    }

    static Result run(long seed, int rounds, int pods,
            FaultInjectingStore.Faults faults, MemoryBinStore backing,
            java.util.function.Function<CommitLog, ReaderInvariants.ReaderView> viewFor,
            binjava.binstore.CountingBinStore meter, boolean forwarding)
            throws IOException {
        Random random = new Random(seed);
        SimulatedClock clock = new SimulatedClock(1_000_000L);
        List<AckOrderInvariants.AckEvent> acks = new ArrayList<>();
        // ⚠️ THE CONFIRMED HALF OF THE TRACE COMES FROM THE STORE (M4.50), and
        // that is the whole reason this wrapper is here. Both halves used to be
        // appended from one `commit()` return, adjacently, so the confirmation
        // preceded the acknowledgement BY CONSTRUCTION and `checkAckOrder`
        // could not fail however the writer behaved -- a writer acking window
        // N+1 before window N confirmed left this sweep green. The store sees
        // every PUT in real completion order and has no view of what the writer
        // intends to tell its caller, so the two halves now have independent
        // authors. A trace whose halves share an author cannot catch a
        // disagreement between them.
        // ⚠️ BELOW THE FAULT INJECTOR, AND A FIRST DRAFT PUT IT ABOVE.
        // CONFIRMED means "this write is durable", not "the writer learned it
        // was durable" -- I5 is about acknowledging past an unconfirmed write,
        // and a write that reached the store IS confirmed whatever the caller
        // was told. `FaultInjectingStore`'s ambiguousPut branch writes to its
        // delegate and THEN throws, exactly modelling a landed write whose
        // response was lost; with the observer above the injector that write
        // emitted nothing, the chain's slot-0 CONTINUE went unconfirmed, and
        // the sweep's ack floor moved off 0 in 13 of 60 seeds -- measured.
        // Underneath, the layering does the discriminating for free: an
        // ambiguous write confirms because it lands, a withheld one does not
        // because it never reaches here, a duplicated one confirms once
        // because the second putIfAbsent loses, and an unreachable one throws
        // before this layer is called at all.
        AckTraceStore observed = new AckTraceStore(
                meter == null ? backing : meter, acks::add);
        FaultInjectingStore faulty = new FaultInjectingStore(observed, seed, faults);
        BinStore store = faulty;
        LocalSequencer leader = null;
        String leaderPod = null;
        // ⚠️ FENCED LEADERS THAT DO NOT KNOW IT YET. Without these the seal
        // protocol is never exercised: a harness that drops its reference the
        // moment a leader loses the lease produces a log nobody ever contends
        // for, and MEASURED -- deleting M4.6a's seal barrier from `recover`
        // entirely left this sweep green. A zombie is the ONLY thing that makes
        // I5 reachable, because I5 is about what a fenced writer manages to
        // acknowledge.
        List<LocalSequencer> zombies = new ArrayList<>();
        List<String> zombiePods = new ArrayList<>();
        int commits = 0;
        int takeovers = 0;
        // ⚠️ ATTEMPTS AND SUCCESSES ARE DIFFERENT NUMBERS, and only the first is
        // an anti-vacuity signal. A fenced leader that TRIES to commit is the
        // mechanism being exercised; one that SUCCEEDS is the barrier having
        // failed. Asserting `zombieWrites > 0` would therefore assert the
        // presence of the defect -- a test that goes red when the system is
        // fixed. So the harness counts both and each is asserted where it means
        // what the assertion says.
        int zombieWrites = 0;
        int zombieAttempts = 0;
        // ⚠️ THE FOLLOWER HALF OF THE FLEET, and a class of its own because
        // this file crossed 700 lines -- code-structure rule 1, split rather
        // than raise. The seam is real: leading is about the lease and the
        // chain, forwarding is about reaching whoever holds them.
        ForwardingPods followers = new ForwardingPods(store, faulty);
        // ⚠️ DERIVED, NOT DECLARED. Recorded at the point of issue, so a test
        // reads what the driver actually sent rather than a counter it set --
        // the shape M4.27 records as satisfiable by hard-coding.
        List<Issued> issued = new ArrayList<>();
        // ⚠️ ONE INCARNATION PER POD PER RUN, not one constant for the whole
        // fleet. A single shared "i1" makes every commit look like the same
        // incarnation, so a dedup keyed on the bare `(podId, flushSeq)` -- the
        // pair ADR-0036 rejects -- is indistinguishable from a correct one in
        // every seed. The zombie keeps its OWN incarnation for the same reason:
        // a fenced writer is a different process, and merging its watermark
        // into the leader's slot is the suppression M4.10d must not do.
        Map<String, String> incarnations = new HashMap<>();
        // ⚠️ KEYED ON (pod, incarnation), NOT on the bare pod. Keyed on the pod
        // alone the counter keeps ascending across a restart, so the RESTART
        // this simulation exists to model -- flushSeq back to 0 under a new
        // incarnation -- appeared in no seed at all.
        // ⚠️ PER POD, PER ATTEMPT -- what a real pod does. `commits` and
        // `zombieWrites` count OUTCOMES and advance only on success, so using
        // either as `flushSeq` reissues a number for a different segment after
        // every failure, and a zombie's pod name can be re-picked as the next
        // leader so the two counters collide on `podId` too. M4.10 dedups on
        // this pair; against a reissued pair that dedup suppresses a genuine
        // commit, which is worse than the duplication it exists to prevent.
        Map<String, Long> nextFlushSeq = new HashMap<>();
        java.util.Set<String> partitionedPods = new java.util.HashSet<>();
        // ⚠️ COUNTS WHAT LANDED WHILE THE RUN WAS STILL GOING, which is the
        // only thing separating a DELAYED write from one deferred to the end
        // of the run. The final drain lands everything either way.
        int midRunDrained = 0;
        GracefulReleaseMeter releases = new GracefulReleaseMeter(faulty);

        for (int round = 0; round < rounds; round++) {
            // ⚠️ PARTITIONS ARE A STATE WITH A DURATION (M4.13e), so they are
            // decided per ROUND rather than per call. `unreachable` is a
            // per-call coin flip applied to every pod equally, which models a
            // flaky store; it can never keep one pod down while another stays
            // up, and that asymmetry is the whole shape of a lease fight. A
            // pod cut off here stops renewing, does not learn it was fenced,
            // and -- once healed -- is a zombie, which is the one population
            // I5's ack clause is actually about.
            // ⚠️ DRAINED AT THE ROUND BOUNDARY (M4.13b), which is what makes a
            // held write DELAYED rather than lost. A write issued in round N
            // lands at the end of round N, after the caller has already been
            // told it failed and has already decided what to do -- and lands in
            // an order the seed picks, so several held writes complete out of
            // the order they were issued in. That is both remaining fault
            // classes from one mechanism.
            // ⚠️ AND IT IS VISIBLE ONLY BECAUSE OF M4.50: the ack trace's
            // CONFIRMED half comes from the store now, so a drained write
            // confirms late and out of order, which is exactly the input I5's
            // clause is written to judge. Under the old synthesised trace a
            // reordered completion could not have been seen however faithfully
            // it was injected.
            midRunDrained += faulty.drainPending();
            if (faults.partitionRate() > 0) {
                for (int i = 0; i < pods; i++) {
                    String pod = "pod" + i;
                    if (partitionedPods.contains(pod)) {
                        // ⚠️ HEALING IS NOT OPTIONAL. A partition that never
                        // ends is a CRASH, and a crashed leader never returns
                        // to discover its fencing -- so the seal protocol's
                        // losing branch is never exercised from that side.
                        if (random.nextDouble() < HEAL_RATE) {
                            faulty.heal(pod);
                            partitionedPods.remove(pod);
                        }
                    } else if (random.nextDouble() < faults.partitionRate()) {
                        faulty.partition(pod);
                        partitionedPods.add(pod);
                    }
                }
            }
            if (forwarding && random.nextInt(100) < FORWARD_RATE) {
                // ⚠️ A POD THAT IS NOT THE LEASEHOLDER, chosen before anything
                // else is drawn, so the draw sequence does not depend on which
                // pod happens to lead this round.
                String follower = "pod" + random.nextInt(pods);
                // ⚠️ ANY POD THAT IS NOT LEADING RIGHT NOW, including when
                // NOBODY is -- which is the only way a forward can straddle a
                // leadership change. Review MEASURED the alternative: with this
                // branch below the `leader == null` guard the lease and the
                // routing table could never disagree at the moment a forward
                // was issued, so `RemoteSequencer`'s whole refusal arm --
                // M5.5's re-read-and-resend -- was dead in all 1,000 seeds, and
                // replacing the transport's refusal with an AssertionError left
                // the sweep green.
                // ⚠️ ONE INCARNATION PER POD, sharing the `l:` key the leader
                // path uses, because a pod that forwards and a pod that leads
                // are the SAME process. Giving forwarding its own incarnation
                // gave one pod two dense flushSeq counters, which the
                // idempotency window then reads as two different pods --
                // exactly the discrimination ADR-0036 exists to make, and a
                // pod that both leads and forwards is the common case rather
                // than the corner one.
                if (!follower.equals(leaderPod)) {
                    String followerInc = incarnations.computeIfAbsent("l:" + follower,
                            k -> "inc-" + k + "-" + seed);
                    CommitRequest forwarded = new CommitRequest(follower, followerInc,
                            nextFlushSeq.merge(followerInc, 1L, Long::sum) - 1,
                            "seg/fwd-" + round, counts(1 + random.nextInt(3)));
                    issued.add(record(forwarded));
                    Optional<CommitDelta> landed = followers.forward(forwarded);
                    if (landed.isPresent()) {
                        // ⚠️ ACKED UNDER THE EPOCH OF THE POD THAT WROTE IT,
                        // because that is the chain the records landed in.
                        // Attributing it to the follower would put an ack in a
                        // chain the follower never wrote, and `checkAckOrder`
                        // bases each chain on the lowest sequence its trace
                        // shows.
                        // ⚠️ READ FROM THE HOP, never from the driver's
                        // `leader` field: this branch now runs when that field
                        // may be null, and a forward can only have landed where
                        // the transport actually routed it.
                        acks.add(AckOrderInvariants.AckEvent.acked(
                                followers.epochThatLanded(), landed.get().sequence()));
                        commits++;
                    }
                    // ⚠️ A REFUSED OR LOST FORWARD IS NOT A FAILURE OF THE RUN,
                    // and the leader is NOT stood down for it, unlike the
                    // leader's own ambiguous failure below: nothing about a
                    // follower's send tells us the leaseholder is unwell. A
                    // follower that cannot reach it simply does not commit this
                    // round -- the degraded behaviour M8's `ctl/inbox/` path
                    // exists for.
                    // ⚠️ THE ROUND IS SPENT ONLY IF SOMEBODY IS LEADING. With no
                    // leader this falls through to the election below, because
                    // a doomed forward must not cost the cluster the round it
                    // would have used to elect -- measured, forwarding in
                    // leaderless rounds and consuming them dropped takeovers
                    // while inflating attempts sixfold.
                    if (leader != null) {
                        continue;
                    }
                }
            }
            if (leader == null) {
                String pod = "pod" + random.nextInt(pods);
                faulty.actingAs(pod);
                Optional<LocalSequencer> won = tryStart(store, pod, clock);
                if (won.isPresent()) {
                    leader = won.get();
                    leaderPod = pod;
                    // ⚠️ REGISTERED UNDER THE ENDPOINT THE LEASE NAMES, never
                    // under "whichever sequencer we have". `InProcessTransport`
                    // refuses an endpoint it does not know, so a forwarder that
                    // stopped re-reading the lease reaches a pod that has gone
                    // and is REFUSED -- which is the property being tested, and
                    // which a transport routing to the only leader present
                    // would answer for free.
                    followers.leads(pod, leader);
                    takeovers++;
                    // ⚠️ THE SLOT-0 CONTINUE, EMITTED AS CONFIRMED, which M4.13's
                    // notes require and an earlier draft of this file CLAIMED to
                    // do while doing nothing -- the edit did not apply and three
                    // documents asserted it anyway. `checkAckOrder` bases each
                    // chain on the LOWEST sequence its trace shows, deliberately,
                    // because inventing a base is how its first version reported
                    // violations of a strictly serial writer. Without this the
                    // floor is 1 in every trace -- measured, 424 of 424 -- so a
                    // chain whose FIRST observed write is the lost one reads
                    // clean. `open` writes the CONTINUE at slot 0 and `start`
                    // returns only once it has landed, so confirming it here is
                    // reporting a write that happened, not inventing one.
                    // ⚠️ NO LONGER APPENDED HERE. `open` writes the CONTINUE
                    // at slot 0 through the store, so the observer emits its
                    // confirmation as it lands -- from the layer that watched
                    // it land, rather than from a driver asserting that it did.
                    // The floor this pinned is pinned the same way, by a real
                    // event instead of a stated one.
                }
                // ⚠️ A pod that could not acquire, or whose start threw an
                // injected fault, simply does not lead this round. That is the
                // correct behaviour and not an error: `start` returning empty
                // means "not the leader", never "failed".
                // ⚠️ AND TIME MUST PASS WHEN NOBODY LEADS, or the simulation
                // DEADLOCKS. A leader that stood down after a failed commit did
                // not release, so its lease is still held and unexpired -- and
                // with a frozen clock no successor can ever acquire it. MEASURED
                // before this line existed: a faulted run made 6 commits and 3
                // takeovers where the clean run made 85 and 18, then reported
                // "no violations" about a cluster that had stopped. That is the
                // "simulation proves the simulator" risk arriving as a stall
                // rather than as a missing fault.
                // ⚠️ THE NUMBERS ABOVE ARE THE BROKEN CASE, and an earlier version
                // of this comment left them reading as the fixed one -- so it
                // argued against the code it sits in. MEASURED on the fixed
                // harness over seeds 0..29 at ROUGH: 0..21 commits and 1..6
                // takeovers per seed, 208 commits in total, with ONE seed
                // committing nothing. Heavy faulting genuinely degrades this
                // cluster; what the line below prevents is it stopping dead.
                // `aFAULTEDSWEEPKeepsCOMMITTINGRatherThanSTALLING` is the floor
                // that holds those numbers, on the RANGE rather than per seed.
                if (won.isEmpty()) {
                    clock.advance(TTL.plusSeconds(1));
                }
                continue;
            }
            if (random.nextInt(100) < 20) {
                // The leader goes away. Half the time politely.
                if (random.nextBoolean()) {
                    // ⚠️ UNCONDITIONALLY (M5.26). A graceful release is the
                    // LEADER's call, so the store must judge it against the
                    // leader; naming no actor leaves `FaultInjectingStore`
                    // judging it against whoever acted last, and a release is
                    // then refused for a partition injected somewhere else.
                    // ⚠️ THE ACTOR SAT INSIDE `if (forwarding)` UNTIL M5.26,
                    // because M5.7 measured that hoisting it moves 4 of M4's
                    // 1,000 leader-only seeds (323, 372, 495, 662) and would
                    // have re-based, inside a task about forwarding, the
                    // evidence this file exists to preserve. That
                    // re-measurement is this row's whole cost and is recorded
                    // in M4's VERIFIED.md rather than absorbed silently.
                    // `GracefulReleaseMeter` sets it and reads back what the
                    // store DID.
                    releases.observe(leaderPod, leader::close);
                    followers.gone(leaderPod);
                } else {
                    // ⚠️ It just STOPS ANSWERING -- and keeps its object, which
                    // is the point. A real fenced leader does not learn it is
                    // fenced until a write loses or a renew fails, so it goes on
                    // originating commits into a chain somebody else is sealing.
                    zombies.add(leader);
                    zombiePods.add(leaderPod);
                    // ⚠️ THE ENDPOINT GOES EVEN THOUGH THE OBJECT STAYS. A
                    // zombie must not go on serving peers; keeping it routable
                    // would model a leader that is unreachable to the store and
                    // reachable to its peers, which is not the failure being
                    // injected.
                    // ⚠️ IT MODELS A POD THAT REFUSES, NOT ONE THAT IS DEAD.
                    // `InProcessTransport.gone` answers an unknown endpoint
                    // with a refusal; a pod that answers NOTHING is
                    // `unreachable`, and this sweep drives none -- M5.6j.
                    followers.gone(leaderPod);
                    clock.advance(TTL.plusSeconds(1));
                }
                leader = null;
                leaderPod = null;
                continue;
            }
            if (!zombies.isEmpty() && random.nextInt(100) < 30) {
                // ⚠️ THE WRITE THE SEAL EXISTS TO REFUSE. It must either lose to
                // the seal or be refused by the barrier `recover` remembered;
                // what it must never do is land beyond one and be acknowledged,
                // which is I5. The simulation does not assert that here -- it
                // just lets it happen, and `Invariants` reads the bytes
                // afterwards.
                int z = random.nextInt(zombies.size());
                zombieAttempts++;
                try {
                    String zombiePod = zombiePods.get(z);
                    String zombieInc = incarnations.computeIfAbsent("z:" + zombiePod,
                            k -> "inc-" + k + "-" + seed);
                    CommitRequest zombieReq = new CommitRequest(zombiePod, zombieInc,
                            nextFlushSeq.merge(zombieInc, 1L, Long::sum) - 1,
                            "seg/zombie-" + round, counts(1 + random.nextInt(3)));
                    issued.add(record(zombieReq));
                    LocalSequencer zombie = zombies.get(z);
                    faulty.actingAs(zombiePods.get(z));
                    CommitDelta zombieDelta = zombie.commit(zombieReq);
                    // ⚠️ THE FENCED WRITER'S ACKNOWLEDGEMENTS BELONG IN THE
                    // TRACE, and leaving them out contradicted this file's own
                    // javadoc: "a zombie is the ONLY thing that makes I5
                    // reachable, because I5 is about what a fenced writer
                    // manages to acknowledge". The trace carried the leader's
                    // acks alone, so the one population I5 is about was the one
                    // population `checkAckOrder` never saw.
                    // ⚠️ AND IT IS UNREACHED TODAY, stated rather than left to
                    // look like coverage: MEASURED at 0 zombie writes over 200
                    // ROUGH seeds, because M4.47's seal-the-run fences a zombie
                    // before it can commit. `zombieAttempts` still fires. This
                    // is a guard for when one does land -- M4.13e's partitioned
                    // leaders is the row that makes it likely -- not a path the
                    // sweep exercises now.
                    // ⚠️ ONLY THE ACK. The confirmation for this write was
                    // emitted by the store when the PUT landed; appending one
                    // here would restore exactly the synthesis M4.50 removed,
                    // and for the one population I5 is actually about.
                    acks.add(AckOrderInvariants.AckEvent.acked(
                            zombie.epoch(), zombieDelta.sequence()));
                    zombieWrites++;
                } catch (IOException fenced) {
                    // correct: the zombie discovered it is fenced
                    zombies.remove(z);
                    zombiePods.remove(z);
                }
                continue;
            }
            try {
                String leaderInc = incarnations.computeIfAbsent("l:" + leaderPod,
                        k -> "inc-" + k + "-" + seed);
                CommitRequest leaderReq = new CommitRequest(leaderPod, leaderInc,
                        nextFlushSeq.merge(leaderInc, 1L, Long::sum) - 1,
                        "seg/" + round, counts(1 + random.nextInt(3)));
                issued.add(record(leaderReq));
                faulty.actingAs(leaderPod);
                CommitDelta acked = leader.commit(leaderReq);
                // ⚠️ THE ACK ONLY, AND THIS IS M4.50's WHOLE POINT. The old
                // code appended the confirmation here too, one line above the
                // ack, which made "confirmed before acked" a property of this
                // file rather than of the system. The confirmation now arrives
                // from `AckTraceStore` at the moment the PUT lands, so if a
                // writer ever acknowledges ahead of its own durable write --
                // or ahead of a lower-numbered one still outstanding -- the
                // trace shows it and `checkAckOrder` reports it.
                acks.add(AckOrderInvariants.AckEvent.acked(leader.epoch(), acked.sequence()));
                commits++;
            } catch (IOException injected) {
                // ⚠️ AMBIGUOUS. The commit may have landed. A leader that
                // treats this as "nothing happened" and retries is the defect
                // M4.10 exists to close, so this one stands down instead --
                // which is the conservative behaviour available today.
                leader = null;
                leaderPod = null;
            }
        }

        // ⚠️ THE VERDICT IS A CLASS OF ITS OWN, split out when this file crossed
        // 700 lines -- code-structure rule 1, split rather than raise, as
        // `ForwardingPods` was. The seam is real: everything above DRIVES a
        // fleet, everything in there JUDGES the bytes it left behind, and the
        // judging reads the backing store directly rather than through the
        // injector the driving goes through.
        SimulationVerdict verdict =
                SimulationVerdict.of(backing, faulty, partitionedPods, acks, pods, rounds, viewFor);
        List<Invariants.Violation> violations = verdict.violations();
        long highest = verdict.highestEpoch();
        int readersChecked = verdict.readersChecked();
        return new Result(seed, commits, takeovers, highest, zombieWrites, zombieAttempts,
                followers.commits(), followers.attempts(), followers.refusals(),
                faulty.injected(), issued, violations, acks, readersChecked, midRunDrained,
                releases.released(), releases.refusedForAnotherPod());
    }

    static boolean hasChain(BinStore store, long epoch) throws IOException {
        return !store.list(new CommitLog(store, PREFIX, epoch).logPrefix(), null, 1)
                .objects().isEmpty();
    }

    /** How often a partitioned pod recovers, per round. */
    private static final double HEAL_RATE = 0.25;

    private static Optional<LocalSequencer> tryStart(BinStore store, String pod,
            SimulatedClock clock) {
        try {
            LeaseManager leases = new LeaseManager(store,
                    new LeaseConfig(PREFIX, pod, ForwardingPods.endpointOf(pod), TTL, RENEW), clock);
            // ⚠️ A TICK THAT NEVER FIRES, because this harness's javadoc says
            // nothing here reads a wall clock or sleeps, and M4.17's production
            // ticker would have made both false: every retained zombie
            // sequencer would park a real thread on a real `Thread.sleep`,
            // holding its `MemoryBinStore` live and introducing wall-clock
            // concurrency into a harness whose whole value is REPRODUCIBILITY.
            // Measured at 28ms/seed against a 3000ms first tick -- a 107x
            // margin that nothing holds, and M4.13 takes this to 1,000 seeds.
            // ⚠️ THE RENEWER EXITS IMMEDIATELY rather than parking forever.
            // A ticker that blocks is still a real thread on a real wait, and
            // it retains every zombie seed's `MemoryBinStore` for the run --
            // review measured that a `Thread.sleep(Long.MAX_VALUE)` version
            // fixed only the wall-clock-concurrency third of the problem.
            // Throwing here takes the renew loop's InterruptedException exit, so
            // the thread is gone before the seed returns and this harness's
            // "nothing here reads a wall clock or sleeps" stays true.
            return LocalSequencer.start(store, PREFIX, leases, 32, () -> {
                throw new InterruptedException("the simulation does not renew");
            });
        } catch (IOException injected) {
            return Optional.empty();
        }
    }
}
