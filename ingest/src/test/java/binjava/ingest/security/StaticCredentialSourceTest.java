// SPDX-License-Identifier: Apache-2.0
package binjava.ingest.security;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.security.Credential;
import binjava.security.Principal;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** ADR-0021: resolving a presented credential to a principal. */
class StaticCredentialSourceTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Principal principal(String... indices) {
        return new Principal("cluster-a", "producer-1", Set.of(indices));
    }

    @Test
    void staticSourceResolvesToTheConfiguredPrincipal() {
        Principal configured = principal("logs");
        try (StaticCredentialSource source = new StaticCredentialSource(configured)) {
            assertThat(source.authenticate(new Credential.Bearer(bytes("anything"))))
                    .as("M1 has no authentication, but it must still yield the trust domain")
                    .contains(configured);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void aClientCertificateAlsoResolvesToThePrincipal() {
        // ⚠️ The sealed type's second arm was never constructed, so making
        // authenticate() return empty for anything but a Bearer survived --
        // a client-cert producer would get a 401 and no test would notice.
        Principal configured = principal("logs");
        try (StaticCredentialSource source = new StaticCredentialSource(configured)) {
            assertThat(source.authenticate(new Credential.ClientCertificate("CN=producer-1")))
                    .contains(configured);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
