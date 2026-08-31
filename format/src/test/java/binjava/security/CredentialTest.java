// SPDX-License-Identifier: Apache-2.0
package binjava.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * ADR-0021: the credential resolves the trust domain; it is never presented
 * alongside it. ⚠️ The RESOLVER lives in `ingest` (resolving is I/O); these are
 * the pure types the plugin presents, so they live here with `format`.
 */
class CredentialTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Principal principal(String... indices) {
        return new Principal("cluster-a", "producer-1", Set.of(indices));
    }


    @Test
    void thePrincipalCarriesTheTrustDomainItWasBuiltWith() {
        // ⚠️ ADR-0021's whole claim: the credential RESOLVES the trust domain.
        // `contains(configured)` cannot catch a blanked field, because record
        // equality compares the mutated instance against itself.
        Principal p = new Principal("cluster-a", "producer-1", Set.of("logs"));
        assertThat(p.trustDomain()).isEqualTo("cluster-a");
        assertThat(p.subject()).isEqualTo("producer-1");
        assertThatThrownBy(() -> new Principal(null, "s", Set.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Principal("d", null, Set.of()))
                .isInstanceOf(NullPointerException.class);
    }


    @Test
    void principalRefusesAnIndexOutsideItsAllowList() {
        Principal p = principal("logs", "metrics");
        assertThat(p.canWriteTo("logs")).isTrue();
        assertThat(p.canWriteTo("metrics")).isTrue();
        // ⚠️ ADR-0021: writing outside allowedIndices is a 403, not a buffered
        // write. `return true` must not survive.
        assertThat(p.canWriteTo("secrets")).as("an index outside the allow-list").isFalse();
    }

    @Test
    void anEmptyAllowListPermitsNothing() {
        // ⚠️ Empty means DENY, never "allow all". The opposite default turns a
        // misconfiguration into a silent cross-index write.
        assertThat(principal().canWriteTo("logs")).isFalse();
    }

    @Test
    void principalIsImmutableAgainstTheCallersSet() {
        Set<String> mutable = new HashSet<>(Set.of("logs"));
        Principal p = new Principal("cluster-a", "producer-1", mutable);
        mutable.add("secrets");
        assertThat(p.canWriteTo("secrets"))
                .as("the caller's later mutation must not widen the allow-list")
                .isFalse();
        assertThatThrownBy(() -> p.allowedIndices().add("secrets"))
                .as("and the exposed set must not be mutable either")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void bearerSecretNeverAppearsInToString() {
        // ⚠️ security.md: never log, trace, or place a credential in an error
        // message. A record's DEFAULT toString prints every component, so this
        // leaks the moment anyone interpolates the credential into a log line.
        Credential.Bearer bearer = new Credential.Bearer(bytes("hunter2"));
        assertThat(bearer.toString()).doesNotContain("hunter2");
        // ⚠️ POSITIVE. doesNotContain alone CANNOT FAIL: a byte[] record
        // component is rendered by String.valueOf, so the generated toString
        // prints `Bearer[secret=[B@646be2c3]` and never the bytes. The test
        // passed against exactly the default its own comment says it refuses.
        assertThat(bearer.toString()).isEqualTo("Bearer[secret=<redacted>]");
    }

    @Test
    void theSecretAccessorHandsOutACopy() {
        Credential.Bearer bearer = new Credential.Bearer(bytes("hunter2"));
        // ⚠️ The OUTBOUND half. The constructor clones what it is given, but the
        // record's generated accessor returned the internal array, so a holder
        // could zero the credential in place while the type's own comment
        // claimed it could not.
        // ⚠️ Assert the CONTENTS first. Nothing else reads the accessor --
        // matches, equals and toString all use the field -- so an accessor
        // returning an all-zero array of the right length satisfied the
        // fill-and-check below exactly.
        assertThat(bearer.secret()).isEqualTo(bytes("hunter2"));
        java.util.Arrays.fill(bearer.secret(), (byte) 0);
        assertThat(bearer.matches(bytes("hunter2")))
                .as("mutating the array handed out must not alter the credential")
                .isTrue();
    }

    @Test
    void twoBearersWithTheSameSecretAreEqual() {
        // ⚠️ The generated equals/hashCode over a byte[] are IDENTITY-based, so
        // these were unequal and a HashMap lookup returned null. It fails
        // closed, but the repair an engineer reaches for is Arrays.equals --
        // the timing oracle ADR-0021 rule 1 forbids, routed around matches().
        Credential.Bearer a = new Credential.Bearer(bytes("hunter2"));
        Credential.Bearer c = new Credential.Bearer(bytes("hunter2"));
        assertThat(a).isEqualTo(c);
        // ⚠️ DIFFERENT secrets, same hash. Equal-secrets-hash-equally is just
        // the equals/hashCode contract, which any secret-derived hash satisfies
        // and which the HashSet assertion below already kills. The claim being
        // pinned here is that the hash is CONSTANT -- a hash OF the secret is a
        // weak oracle that leaks through anything printing a hash code.
        assertThat(a.hashCode()).isEqualTo(c.hashCode());
        assertThat(new Credential.Bearer(bytes("something-else")).hashCode())
                .as("the hash must not vary with the secret")
                .isEqualTo(a.hashCode());
        assertThat(a).isNotEqualTo(new Credential.Bearer(bytes("hunter3")));
        assertThat(new java.util.HashSet<>(java.util.List.of(a)).contains(c))
                .as("a set lookup must find an equal credential")
                .isTrue();
    }

    @Test
    void bearerMatchesOnlyTheExactSecret() {
        Credential.Bearer bearer = new Credential.Bearer(bytes("hunter2"));
        assertThat(bearer.matches(bytes("hunter2"))).isTrue();
        assertThat(bearer.matches(bytes("hunter3"))).isFalse();
        assertThat(bearer.matches(bytes("hunter"))).as("a prefix is not a match").isFalse();
        assertThat(bearer.matches(new byte[0])).isFalse();
    }

    @Test
    void bearerCopiesTheSecretItWasGiven() {
        byte[] secret = bytes("hunter2");
        Credential.Bearer bearer = new Credential.Bearer(secret);
        java.util.Arrays.fill(secret, (byte) 0);
        assertThat(bearer.matches(bytes("hunter2")))
                .as("clearing the caller's array must not alter the credential")
                .isTrue();
    }
}
