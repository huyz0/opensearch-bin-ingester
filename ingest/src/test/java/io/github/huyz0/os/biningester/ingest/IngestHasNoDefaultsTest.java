// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.security.Principal;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/**
 * M12.2 (M11 review F3): the {@code buffered} overloads and
 * {@code concreteIndex} are stated by every {@link Ingest}, never inherited.
 * A default {@code concreteIndex} answering the name it was given already
 * caused M11.8's round-1 defect (a quota charged to the alias, not the
 * index), and a default {@code buffered} that runs after the durable wait
 * silently keeps the admission permit across it -- the shape M11.7 removed.
 */
class IngestHasNoDefaultsTest {

    @Test
    void theBufferedAppendIsAbstract() throws Exception {
        assertAbstract(Ingest.class.getMethod("append", Principal.class, String.class, int.class,
                byte.class, Ingest.RecordSource.class, Runnable.class));
    }

    @Test
    void theBufferedRoutedAppendIsAbstract() throws Exception {
        assertAbstract(Ingest.class.getMethod("appendRouted", Principal.class, String.class,
                String.class, byte.class, Ingest.RecordSource.class, Runnable.class));
    }

    @Test
    void concreteIndexIsAbstract() throws Exception {
        assertAbstract(Ingest.class.getMethod("concreteIndex", String.class));
    }

    private static void assertAbstract(Method method) {
        assertThat(method.isDefault()).as("%s must be stated by every Ingest", method).isFalse();
    }
}
