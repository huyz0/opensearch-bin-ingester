// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * M13.6a (M12 harvest R5): no constructor of this module quietly supplies what
 * its caller should say. `IndexQuotas`' three-argument constructor defaulted
 * the aliases to none, so an override keyed by an alias matched nothing
 * through it (M12.13's defect, by the other door).
 */
class SilentDefaultsGoneTest {

    @Test
    void everyIndexQuotasConstructorIsToldTheAliases() {
        List<Constructor<?>> silent = Arrays.stream(IndexQuotas.class.getDeclaredConstructors())
                .filter(c -> !Arrays.asList(c.getParameterTypes()).contains(Function.class))
                .toList();

        assertThat(silent).as("an IndexQuotas constructor that defaults the aliases").isEmpty();
    }
}
