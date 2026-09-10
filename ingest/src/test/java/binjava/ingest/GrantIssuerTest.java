// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.SignedUrl;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The {@code direct} grant: refused at startup, bounded, and never logged
 * (M5.13, FR-6, ADR-0041, security.md rules 3 and 4).
 */
class GrantIssuerTest {

    private static final String SEGMENT = "seg/2026/09/11/abc";

    /**
     * A stand-in for the first backend that can sign, which neither shipping
     * one can.
     *
     * <p>⚠️ IT RECORDS WHAT IT WAS ASKED, because a grant issued for the wrong
     * key or the wrong duration looks identical from outside — {@code
     * SignedUrl} redacts itself, which is the point of it and also what makes
     * the arguments the only observable thing.
     */
    private static final class CapableStore implements BinStore {
        private final BinStore delegate = new MemoryBinStore();
        private final boolean capable;
        private String lastKey;
        private Duration lastTtl;

        CapableStore(boolean capable) {
            this.capable = capable;
        }

        @Override public SignedUrl presign(String key, Duration ttl) {
            this.lastKey = key;
            this.lastTtl = ttl;
            return new SignedUrl("https://store.example/" + key + "?sig=SECRETSIGNATURE",
                    Instant.now().plus(ttl));
        }

        @Override public Capabilities capabilities() {
            Capabilities c = delegate.capabilities();
            return new Capabilities(c.conditionalWrites(), c.batchDelete(), capable,
                    c.maxKeyBytes(), c.minPartSize(), c.costs());
        }

        @Override public java.io.InputStream get(String k) throws IOException {
            return delegate.get(k);
        }

        @Override public java.io.InputStream getRange(String k, long a, long b) throws IOException {
            return delegate.getRange(k, a, b);
        }

        @Override public java.util.Optional<binjava.binstore.ObjectStat> stat(String k)
                throws IOException {
            return delegate.stat(k);
        }

        @Override public binjava.binstore.Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfAbsent(String k, Body b)
                throws IOException {
            return delegate.putIfAbsent(k, b);
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfMatch(
                String k, Body b, binjava.binstore.Version v) throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public binjava.binstore.MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public binjava.binstore.ListPage list(String p, String a, int m)
                throws IOException {
            return delegate.list(p, a, m);
        }

        @Override public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override public void close() throws IOException {
            delegate.close();
        }
    }

    // ---- criterion 1: refused at STARTUP -----------------------------------

    /**
     * A backend that cannot presign makes the issuer refuse to CONSTRUCT.
     *
     * <p>⚠️ THIS IS `requirePresignedUrls()`'s FIRST PRODUCTION CALLER. M5.10
     * shipped the check and round-5 review of that task recorded that nothing
     * called it — the same state {@code requireConditionalWrites} was in
     * before M2.1. Until something does, "refused at startup" is a sentence in
     * a SPEC and the real behaviour is an
     * {@code UnsupportedOperationException} at the first consumer fetch,
     * blaming a backend that was honest about what it cannot do.
     *
     * <p>⚠️ AND {@code MemoryBinStore} IS THE REAL CASE, not a contrivance:
     * both shipping backends answer {@code presignedUrls=false} (ADR-0041), so
     * this is what every deployment enabling `direct` gets today.
     */
    @Test
    void aBackendThatCannotPresignREFUSESToConstructTheIssuer() {
        try (MemoryBinStore cannot = new MemoryBinStore()) {
            assertThatThrownBy(() -> new GrantIssuer(cannot))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("ADR-0041")
                    .hasMessageContaining("direct");
        }
    }

    /**
     * The two constants are ADR-0010's number, asserted as a VALUE.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED THE SELF-COMPARISON: this test read
     * {@code ceiling()).isEqualTo(DEFAULT_CEILING)}, so moving
     * {@code DEFAULT_CEILING} to 59 minutes, or {@code MAX_CEILING} to SIX
     * DAYS, passed the whole suite -- ten green tests while every default grant
     * lasted six days. The only surviving bound was "less than Azure's own
     * seven-day maximum", which is the backend's bound, not ours.
     *
     * <p>⚠️ SO THE NUMBER IS WRITTEN OUT HERE. ADR-0010 (accepted, FR-6) fixes
     * the grant at ≤60 s; this is the assertion that fails if someone widens it
     * without reopening that record, which is what non-negotiable 2 and
     * AGENTS.md's "re-opening a settled decision is an ADR, not a task" require.
     * The first draft of this class shipped 5 minutes and an hour.
     */
    @Test
    void bothCeilingsAreADR0010sSIXTYSECONDS() throws Exception {
        assertThat(GrantIssuer.MAX_CEILING)
                .as("ADR-0010: a direct grant is ≤60 s; widening it is an ADR, not an edit")
                .isEqualTo(Duration.ofSeconds(60));
        assertThat(GrantIssuer.DEFAULT_CEILING)
                .as("and the default may be shorter than the maximum but never longer")
                .isLessThanOrEqualTo(GrantIssuer.MAX_CEILING);
        try (CapableStore capable = new CapableStore(true)) {
            assertThat(new GrantIssuer(capable).ceiling()).isEqualTo(GrantIssuer.DEFAULT_CEILING);
        }
    }

    /**
     * The capability check runs BEFORE the ceiling checks, as its comment says.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED THE ORDERING UNPINNED: moving
     * {@code requirePresignedUrls()} below both ceiling validations survived.
     * The ordering is not cosmetic -- an operator who misconfigures BOTH should
     * be told the backend cannot presign, because that is the fact that makes
     * `direct` impossible; a complaint about the TTL sends them to change a
     * number that will not help.
     */
    @Test
    void theCAPABILITYCheckRunsBeforeTheCeilingChecks() {
        try (MemoryBinStore cannot = new MemoryBinStore()) {
            assertThatThrownBy(() -> new GrantIssuer(cannot, Duration.ofDays(7)))
                    .as("both are wrong; the capability is the one that matters")
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    // ---- criterion 2: the TTL ceiling this row owns ------------------------

    /**
     * A grant longer than the ceiling is CLAMPED to it, not refused.
     *
     * <p>⚠️ THE DIRECTION IS THE DECISION. Refusing would turn an operator's
     * over-optimistic request into a failed fetch; clamping turns it into a
     * shorter-lived one, which is the outcome security.md rule 3 wants anyway.
     * ⚠️ AND THE BOUND HAD NO OWNER BEFORE THIS ROW: {@code BinStore.presign}
     * rejects only a non-positive TTL, so {@code Duration.ofDays(30)} was a
     * legal grant and rule 3's "short TTL" was a preference with no number.
     */
    @Test
    void aGrantLongerThanTheCeilingIsCLAMPEDToIt() throws Exception {
        try (CapableStore capable = new CapableStore(true)) {
            GrantIssuer issuer = new GrantIssuer(capable, Duration.ofSeconds(30));
            issuer.grantFor(SEGMENT, Duration.ofDays(30));
            assertThat(capable.lastTtl)
                    .as("the 30-day grant M5.10 left legal is not reachable through this issuer")
                    .isEqualTo(Duration.ofSeconds(30));
        }
    }

    /** A grant shorter than the ceiling is passed through unchanged. */
    @Test
    void aGrantShorterThanTheCeilingIsPassedThroughUNCHANGED() throws Exception {
        try (CapableStore capable = new CapableStore(true)) {
            GrantIssuer issuer = new GrantIssuer(capable, Duration.ofSeconds(45));
            // ⚠️ THE ISSUER'S OWN ANSWER, not just the minted TTL. Round-2
            // review MEASURED `ceiling()` hard-coded to DEFAULT_CEILING
            // surviving all twelve tests: every other read of it happens on an
            // issuer built at the default, and the tests that SHORTEN the
            // ceiling only ever assert the fixture's `lastTtl`. A caller sizing
            // a prefetch window or a retry budget off `ceiling()` would be told
            // the grant lasts twice as long as it does -- and M5.43 is the row
            // that wires such a caller up.
            assertThat(issuer.ceiling()).isEqualTo(Duration.ofSeconds(45));
            issuer.grantFor(SEGMENT, Duration.ofSeconds(10));
            assertThat(capable.lastTtl)
                    .as("clamping must be a MAXIMUM, not a fixed value -- otherwise the ceiling "
                            + "silently becomes the only TTL any caller can have")
                    .isEqualTo(Duration.ofSeconds(10));
        }
    }

    /**
     * The ceiling itself is bounded, so "configurable" cannot mean
     * "unbounded".
     *
     * <p>⚠️ WITHOUT THIS THE CEILING IS THEATRE. An operator who can configure
     * a week has the {@code Duration.ofDays(30)} grant back through a
     * configuration file instead of a code change, and the backend will
     * happily sign it: ADR-0041 records that an Azure user-delegation key is
     * valid up to SEVEN DAYS, so the bound has to be ours rather than the
     * store's.
     */
    @Test
    void aCeilingBeyondTheMAXIMUMIsREFUSED() throws Exception {
        try (CapableStore capable = new CapableStore(true)) {
            assertThatThrownBy(() -> new GrantIssuer(capable, Duration.ofDays(7)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("security.md rule 3");
            // ⚠️ JUST ABOVE THE BOUND, not merely far above it. Round-2 review
            // MEASURED that comparing against a literal `Duration.ofHours(1)`
            // instead of MAX_CEILING passed all twelve tests -- so the constant
            // was pinned at 60 s while nothing enforced it, and
            // `new GrantIssuer(store, ofMinutes(60))` minted hour-long grants.
            // That is 60x ADR-0010 and the exact number round ONE blocked this
            // task on: the regression could return through the check rather
            // than through the constant.
            assertThatThrownBy(() -> new GrantIssuer(capable, GrantIssuer.MAX_CEILING.plusSeconds(1)))
                    .as("one second over the decided bound is over it")
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(new GrantIssuer(capable, GrantIssuer.MAX_CEILING).ceiling())
                    .as("and the maximum itself is allowed -- an exclusive bound would make the "
                            + "documented constant unusable")
                    .isEqualTo(GrantIssuer.MAX_CEILING);
        }
    }

    /** A non-positive ceiling, or a non-positive request, is refused. */
    @Test
    void aNONPOSITIVETtlIsREFUSEDAtBothEnds() throws Exception {
        try (CapableStore capable = new CapableStore(true)) {
            assertThatThrownBy(() -> new GrantIssuer(capable, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new GrantIssuer(capable, Duration.ofMinutes(-1)))
                    .isInstanceOf(IllegalArgumentException.class);

            GrantIssuer issuer = new GrantIssuer(capable);
            assertThatThrownBy(() -> issuer.grantFor(SEGMENT, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> issuer.grantFor(SEGMENT, Duration.ofSeconds(-1)))
                    .as("negative as well as zero -- M5.10 measured the negative half surviving "
                            + "a suite that probed only ZERO")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // ---- criterion 3: one key, and it is the key asked for -----------------

    /**
     * The grant is for exactly the key requested.
     *
     * <p>⚠️ {@code SignedUrl} REDACTS ITSELF, so the arguments are the only
     * observable thing — which makes this the same class as M5.37's "a presign
     * ignoring its key would have passed", and worth an assertion rather than
     * an assumption.
     */
    @Test
    void theGrantIsForEXACTLYTheKeyRequested() throws Exception {
        try (CapableStore capable = new CapableStore(true)) {
            SignedUrl granted = new GrantIssuer(capable).grantFor(SEGMENT);
            assertThat(capable.lastKey).isEqualTo(SEGMENT);
            assertThat(granted.url()).contains(SEGMENT);
        }
    }

    /**
     * The single-argument grant lasts exactly the ceiling, as its javadoc says.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED THIS UNOBSERVED: rewriting the convenience
     * overload to pass {@code Duration.ofSeconds(1)} survived. The old
     * assertion was {@code expiresAt()).isAfter(Instant.now())}, which the
     * FIXTURE computes as {@code now().plus(ttl)} and which therefore holds for
     * any positive TTL at all -- and reads a real clock a test at this tier
     * does not need.
     */
    @Test
    void theSingleArgumentGrantLastsEXACTLYTheCeiling() throws Exception {
        try (CapableStore capable = new CapableStore(true)) {
            GrantIssuer issuer = new GrantIssuer(capable, Duration.ofSeconds(45));
            issuer.grantFor(SEGMENT);
            assertThat(capable.lastTtl).isEqualTo(Duration.ofSeconds(45));
        }
    }
}
