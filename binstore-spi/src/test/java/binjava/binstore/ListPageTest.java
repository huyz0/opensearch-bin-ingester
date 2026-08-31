// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** ⚠️ Shipped with a defensive copy and a null check that nothing exercised. */
class ListPageTest {

    private static ObjectStat stat(String key) {
        return new ObjectStat(key, 1, new Version("v"));
    }

    @Test
    void copiesTheListSoALaterMutationCannotChangeThePage() {
        List<ObjectStat> mutable = new ArrayList<>(List.of(stat("a")));
        ListPage page = new ListPage(mutable, Optional.empty());
        mutable.add(stat("b"));
        assertThat(page.objects()).as("the caller's later add must not appear").hasSize(1);
        assertThatThrownBy(() -> page.objects().add(stat("c")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void lastIsAPageWithNothingAfterIt() {
        assertThat(ListPage.last(List.of(stat("a"))).nextStartAfter()).isEmpty();
    }

    @Test
    void refusesNulls() {
        // ⚠️ A null nextStartAfter would NPE at the far end of a recovery walk,
        // not here, which is the worst place to learn about it.
        assertThatThrownBy(() -> new ListPage(null, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ListPage(List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
