// SPDX-License-Identifier: Apache-2.0
package binjava.ingest.security;

import binjava.security.Credential;
import binjava.security.Principal;
import java.util.Objects;
import java.util.Optional;

/**
 * A single trust domain and no authentication.
 * // SKELETON: replaced by M2's FileCredentialSource
 *
 * <p>⚠️ Every credential resolves to the same principal, because M1's scope
 * excludes authentication. What M1 buys is the SEAM ITSELF, and nothing more:
 * an earlier version of this comment claimed "the write path already takes a
 * Principal", which was false — no write path exists before M1.6, and
 * {@code Principal} is referenced by nothing outside this package and its test.
 * Threading it is M1.6/M1.8's work, and the honest claim today is that the type
 * exists so that threading is an argument change rather than a redesign.
 *
 * <p>⚠️ This class lives in {@code ingest}, not {@code format}, because
 * resolving a credential is I/O — M2's FileCredentialSource polls a file every
 * 10 s (ADR-0021 §3) — and format/README.md says of itself "no I/O, no clock,
 * no configuration". Only {@link Credential}, which the plugin PRESENTS, is
 * shared; the plugin never resolves one.
 */
public final class StaticCredentialSource implements CredentialSource {

    private final Principal principal;

    public StaticCredentialSource(Principal principal) {
        this.principal = Objects.requireNonNull(principal, "principal");
    }

    @Override
    public Optional<Principal> authenticate(Credential presented) {
        return Optional.of(principal);
    }

    @Override
    public void close() {}
}
