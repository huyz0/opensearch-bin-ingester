// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M13.6b (M12 harvest R5, M12.6 review T1): `ServerConfig` has only its
 * canonical constructor. Six older ones each filled in what a caller written
 * before a setting existed could not say -- the retention, the membership
 * watch, the pod UID, the top-K interval, the quotas, `/admin/cost` -- and a
 * new caller reaching for the shortest one got every one of those defaults
 * without saying so.
 */
class SilentDefaultsGoneTest {

    @Test
    void serverConfigHasOnlyItsCanonicalConstructor() {
        int components = ServerConfig.class.getRecordComponents().length;
        List<Constructor<?>> shorter = Arrays.stream(ServerConfig.class.getDeclaredConstructors())
                .filter(c -> c.getParameterCount() != components)
                .toList();

        assertThat(shorter).as("a ServerConfig constructor that fills in a setting").isEmpty();
    }
}
