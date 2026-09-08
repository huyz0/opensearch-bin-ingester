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
    private static final String PREFIX = "bins/cluster-a";

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
            List<FaultInjectingStore.Injected> faults,
            List<Issued> issued,
            List<Invariants.Violation> violations,
            List<AckOrderInvariants.AckEvent> acks,
            int readersChecked) {

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
        Random random = new Random(seed);
        SimulatedClock clock = new SimulatedClock(1_000_000L);
        FaultInjectingStore store = new FaultInjectingStore(backing, seed, faults);

        List<AckOrderInvariants.AckEvent> acks = new ArrayList<>();
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

        for (int round = 0; round < rounds; round++) {
            if (leader == null) {
                String pod = "pod" + random.nextInt(pods);
                Optional<LocalSequencer> won = tryStart(store, pod, clock);
                if (won.isPresent()) {
                    leader = won.get();
                    leaderPod = pod;
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
                    acks.add(AckOrderInvariants.AckEvent.confirmed(leader.epoch(), 0L));
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
                    try {
                        leader.close();
                    } catch (IOException injected) {
                        // a fault during release leaves the lease to expire,
                        // which is exactly the ungraceful path
                    }
                } else {
                    // ⚠️ It just STOPS ANSWERING -- and keeps its object, which
                    // is the point. A real fenced leader does not learn it is
                    // fenced until a write loses or a renew fails, so it goes on
                    // originating commits into a chain somebody else is sealing.
                    zombies.add(leader);
                    zombiePods.add(leaderPod);
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
                    acks.add(AckOrderInvariants.AckEvent.confirmed(
                            zombie.epoch(), zombieDelta.sequence()));
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
                CommitDelta acked = leader.commit(leaderReq);
                // ⚠️ CONFIRMED THEN ACKED, in that order, because that is what
                // `commit` returning MEANS: the PUT was confirmed by the store
                // and only then did the caller learn of it. I5's second clause
                // is about that order, and it leaves no trace in the bytes --
                // two runs producing byte-identical chains can differ in it.
                acks.add(AckOrderInvariants.AckEvent.confirmed(leader.epoch(), acked.sequence()));
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

        List<Invariants.Violation> violations = new ArrayList<>();
        long highest = 0;
        int readersChecked = 0;
        // ⚠️ ONE WALK PER EPOCH. The second `checkChain` this loop used to make
        // was a provably DEAD disjunct -- every violation is raised inside the
        // walk over entries, so a non-empty violation list already implies a
        // non-empty chain -- and it cost a full second LIST plus one GET per
        // entry in the phase M4.13's <60s budget lives in.
        long scanTo = pods * (long) rounds;
        for (long epoch = 1; epoch <= scanTo; epoch++) {
            violations.addAll(Invariants.checkChain(backing, PREFIX, epoch));
            if (hasChain(backing, epoch)) {
                highest = Math.max(highest, epoch);
                // ⚠️ I3 AND I4's DROP CLAUSE, asked of PRODUCTION'S OWN READER
                // at ITS OWN epoch (M4.13a). ⚠️ ONE CALL PER READER, NEVER SWEPT
                // OVER A RANGE: `checkChain` is safe at every epoch in a range
                // and this is not, which is why `ReaderView` carries the epoch.
                CommitLog reader = new CommitLog(backing, PREFIX, epoch);
                reader.recover();
                violations.addAll(ReaderInvariants.checkReader(
                        backing, PREFIX, viewFor.apply(reader)));
                readersChecked++;
            }
        }
        // ⚠️ AND I5's ACK-ORDERING CLAUSE, which no walk over the store can see.
        // ⚠️ IT CANNOT FAIL, AND NOT BECAUSE `CommitLog` IS SERIAL -- an earlier
        // draft of this comment said that and review refuted it. THIS DRIVER
        // synthesises both events from one `commit()` return, adjacently, on one
        // thread, so the order is the driver's construction and not the
        // writer's. A pipelining writer would leave it green. The CONFIRMED
        // event has to come from the store, which already sees every PUT, or
        // from a production callback: M4.50 owns that, and until it lands this
        // arm ASKS about I5's second clause without being able to answer.
        violations.addAll(AckOrderInvariants.checkAckOrder(acks));
        // ⚠️ THE SCAN BOUND IS ASSERTED, NOT ASSUMED. `highest` is computed by
        // the loop above, so it can never report an epoch outside the range it
        // scanned -- a run that burned more epochs than the bound would report
        // CLEAN about chains nobody read, which is this file's own anti-vacuity
        // risk arriving through the scan bound instead of through the faults.
        if (hasChain(backing, scanTo + 1)) {
            throw new IllegalStateException("the run reached epoch " + (scanTo + 1)
                    + ", beyond the scanned bound " + scanTo
                    + " -- chains past it were never checked");
        }
        return new Result(seed, commits, takeovers, highest, zombieWrites, zombieAttempts,
                store.injected(), issued, violations, acks, readersChecked);
    }

    private static boolean hasChain(BinStore store, long epoch) throws IOException {
        return !store.list(new CommitLog(store, PREFIX, epoch).logPrefix(), null, 1)
                .objects().isEmpty();
    }

    private static Optional<LocalSequencer> tryStart(BinStore store, String pod,
            SimulatedClock clock) {
        try {
            LeaseManager leases = new LeaseManager(store,
                    new LeaseConfig(PREFIX, pod, "", TTL, RENEW), clock);
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
