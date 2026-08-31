// SPDX-License-Identifier: Apache-2.0
package binjava.ingest.security;

import java.io.Closeable;
import binjava.security.Credential;
import binjava.security.Principal;
import java.util.Optional;

/**
 * Resolves a presented credential to a {@link Principal} (ADR-0021).
 *
 * <p>⚠️ The seam exists in M1 so nothing is retrofitted through the auth path
 * later. M1 ships {@link StaticCredentialSource}; a file-backed, hot-reloading
 * source follows in M2.
 */
public interface CredentialSource extends Closeable {

    /** The principal for {@code presented}, or empty if it is unknown. */
    Optional<Principal> authenticate(Credential presented);
}
