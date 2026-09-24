// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Reads the node-local installation secret only from a file protected to its owner. */
public final class InstallationSecret {
    private InstallationSecret() { }

    public static byte[] read(Path path) throws IOException {
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path)) {
            throw new IOException("installation secret must be a regular non-link file");
        }
        AclFileAttributeView acl = Files.getFileAttributeView(path, AclFileAttributeView.class);
        if (acl != null) {
            List<AclEntry> entries = acl.getAcl();
            var owner = Files.getOwner(path);
            if (entries.stream().anyMatch(entry -> entry.type()
                    == java.nio.file.attribute.AclEntryType.ALLOW
                    && !entry.principal().equals(owner))) {
                throw new IOException("installation secret ACL grants access beyond its owner");
            }
        } else {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
            if (!permissions.equals(EnumSet.of(PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE))) {
                throw new IOException("installation secret permissions must be owner read/write only");
            }
        }
        byte[] secret = Files.readAllBytes(path);
        if (secret.length != 32) {
            throw new IOException("installation secret must contain exactly 32 bytes");
        }
        return secret;
    }

    /** Creates a random owner-only secret; refuses to replace an existing path. */
    public static void create(Path path) throws IOException {
        byte[] secret = new byte[32];
        new java.security.SecureRandom().nextBytes(secret);
        try {
            Files.createFile(path, PosixFilePermissions.asFileAttribute(
                    Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)));
        } catch (UnsupportedOperationException unsupported) {
            Files.createFile(path);
        }
        try {
            AclFileAttributeView acl = Files.getFileAttributeView(path, AclFileAttributeView.class);
            if (acl != null) {
                var owner = Files.getOwner(path);
                var entry = AclEntry.newBuilder().setType(java.nio.file.attribute.AclEntryType.ALLOW)
                        .setPrincipal(owner).setPermissions(EnumSet.allOf(AclEntryPermission.class))
                        .build();
                acl.setAcl(List.of(entry));
            } else {
                Files.setPosixFilePermissions(path, EnumSet.of(PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE));
            }
            Files.write(path, secret);
        } finally {
            java.util.Arrays.fill(secret, (byte) 0);
        }
    }
}
