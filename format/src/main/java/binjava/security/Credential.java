// SPDX-License-Identifier: Apache-2.0
package binjava.security;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/**
 * What a producer presents. Sealed over the two shapes ADR-0021 admits: a
 * bearer/basic secret, and a client certificate.
 */
public sealed interface Credential permits Credential.Bearer, Credential.ClientCertificate {

    /** A bearer or basic secret. */
    record Bearer(byte[] secret) implements Credential {

        public Bearer {
            // ⚠️ Copy. The caller may clear or reuse its buffer — a credential
            // that changes under the holder is a live authentication bug.
            secret = Objects.requireNonNull(secret, "secret").clone();
        }

        /**
         * ⚠️ A COPY. The generated accessor hands out the internal array, so
         * {@code bearer.secret()[0] = 0} mutated the credential in place — the
         * exact "changes under the holder" bug the constructor's clone was
         * written to prevent, left open on the way out. Guarding one direction
         * and not the other is worse than guarding neither, because the comment
         * claims the invariant holds.
         */
        @Override
        public byte[] secret() {
            return secret.clone();
        }

        /**
         * Constant-time comparison against an expected secret.
         *
         * <p>⚠️ {@link MessageDigest#isEqual} rather than {@link Arrays#equals}:
         * a length-or-content short circuit on a token is a real timing oracle
         * (ADR-0021, security.md).
         *
         * <p>⚠️ NOT COVERED BY A TEST. The tests here pin the RESULT, not the
         * timing, so swapping in {@code Arrays.equals} keeps them green. A
         * timing assertion on a JIT-compiled JVM is flaky, and a flaky gate is
         * worse than a named gap — so this is stated rather than pretended.
         */
        public boolean matches(byte[] expected) {
            return MessageDigest.isEqual(secret, expected);
        }

        /**
         * ⚠️ Value equality, in CONSTANT TIME. The generated equals/hashCode
         * over a {@code byte[]} are identity-based, so two Bearers holding the
         * same secret were unequal and a HashMap lookup returned null. That
         * fails closed, but the repair an engineer reaches for is
         * {@code Arrays.equals}, which is the timing oracle ADR-0021 rule 1
         * forbids — routed around {@link #matches} rather than through it.
         */
        @Override
        public boolean equals(Object o) {
            return o instanceof Bearer b && MessageDigest.isEqual(secret, b.secret);
        }

        /**
         * ⚠️ CONSTANT, deliberately. Any hash of the secret is a weak oracle and
         * would leak through anything that prints a hash code.
         *
         * <p>⚠️ SO A Credential IS NOT A MAP KEY. An earlier version of this
         * comment excused the constant with "credentials are not hot map keys",
         * which is false: authenticate() runs once per producer bulk request,
         * the hottest path the producer surface has. Every entry would collide
         * into one bucket, and Bearer is not Comparable so HashMap cannot
         * treeify it — the bucket stays a linked list and lookup degrades to N
         * constant-time comparisons per request, driven by an as-yet
         * unauthenticated caller. M2's FileCredentialSource must index on a key
         * id, not on this type.
         */
        @Override
        public int hashCode() {
            return 0;
        }

        /** ⚠️ REDACTED. A record's default toString prints every component. */
        @Override
        public String toString() {
            return "Bearer[secret=<redacted>]";
        }
    }

    /** A client certificate, identified by its subject DN. */
    record ClientCertificate(String subjectDn) implements Credential {}
}
