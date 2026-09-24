// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InstallationSecretTest {
    @TempDir Path directory;

    @Test
    void createsAndReadsAnOwnerProtectedInstallationSecret() throws Exception {
        Path secret = directory.resolve("reader.secret");
        Path secondSecret = directory.resolve("reader-2.secret");

        InstallationSecret.create(secret);
        InstallationSecret.create(secondSecret);

        assertThat(InstallationSecret.read(secret)).hasSize(32);
        assertThat(InstallationSecret.read(secret)).isNotEqualTo(InstallationSecret.read(secondSecret));
        var acl = Files.getFileAttributeView(secret,
                java.nio.file.attribute.AclFileAttributeView.class);
        if (acl != null) {
            var owner = Files.getOwner(secret);
            assertThat(acl.getAcl().stream().filter(entry -> entry.type()
                    == java.nio.file.attribute.AclEntryType.ALLOW)
                    .allMatch(entry -> entry.principal().equals(owner))).isTrue();
        } else {
            assertThat(Files.getPosixFilePermissions(secret)).containsExactlyInAnyOrder(
                    java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                    java.nio.file.attribute.PosixFilePermission.OWNER_WRITE);
        }
        assertThatThrownBy(() -> InstallationSecret.create(secret))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
    }

    @Test
    void refusesASecretThatIsNotARegularOwnedFile() throws Exception {
        Path directorySecret = directory.resolve("not-a-secret-file");
        Files.createDirectory(directorySecret);

        assertThatThrownBy(() -> InstallationSecret.read(directorySecret))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("regular");
    }

    @Test
    void refusesAnAclGrantToAnotherPrincipal() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        var acl = Files.getFileAttributeView(secret,
                java.nio.file.attribute.AclFileAttributeView.class);
        org.junit.jupiter.api.Assumptions.assumeTrue(acl != null,
                "this filesystem has no Windows-style ACL view");
        var owner = Files.getOwner(secret);
        var everyone = secret.getFileSystem().getUserPrincipalLookupService()
                .lookupPrincipalByName("Everyone");
        var ownerEntry = java.nio.file.attribute.AclEntry.newBuilder()
                .setType(java.nio.file.attribute.AclEntryType.ALLOW).setPrincipal(owner)
                .setPermissions(java.util.EnumSet.allOf(
                        java.nio.file.attribute.AclEntryPermission.class)).build();
        var openEntry = java.nio.file.attribute.AclEntry.newBuilder()
                .setType(java.nio.file.attribute.AclEntryType.ALLOW).setPrincipal(everyone)
                .setPermissions(java.util.EnumSet.of(
                        java.nio.file.attribute.AclEntryPermission.READ_DATA)).build();
        try {
            acl.setAcl(java.util.List.of(openEntry));
            assertThatThrownBy(() -> InstallationSecret.read(secret))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("beyond its owner");
        } finally {
            acl.setAcl(java.util.List.of(ownerEntry));
        }
    }

}
