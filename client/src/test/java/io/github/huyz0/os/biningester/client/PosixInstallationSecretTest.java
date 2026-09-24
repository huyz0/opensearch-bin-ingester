// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PosixInstallationSecretTest {
    @TempDir Path directory;

    @Test
    void refusesASecretReadableByOtherUsers() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        var acl = Files.getFileAttributeView(secret,
                java.nio.file.attribute.AclFileAttributeView.class);
        org.junit.jupiter.api.Assumptions.assumeTrue(acl == null,
                "this filesystem uses ACLs rather than POSIX permissions");
        Files.setPosixFilePermissions(secret, java.util.EnumSet.of(
                java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                java.nio.file.attribute.PosixFilePermission.OWNER_WRITE,
                java.nio.file.attribute.PosixFilePermission.GROUP_READ,
                java.nio.file.attribute.PosixFilePermission.OTHERS_READ));

        assertThatThrownBy(() -> InstallationSecret.read(secret))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("owner read/write only");
    }
}
