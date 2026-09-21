// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.StoreCounts;
import org.junit.jupiter.api.Test;

class MacroCountsJsonTest {

    @Test
    void macroCountSnapshotCarriesEveryCounter() {
        String podId = "pod\n\"a\\\t" + (char) 0;
        assertThat(FrontDoor.macroCountsJson(podId, new StoreCounts(3, 5, 7, 11, 13)))
                .isEqualTo("{\"podId\":\"pod\\n\\\"a\\\\\\t\\u0000\",\"puts\":3,\"gets\":5,\"lists\":7,\"stats\":11,\"deletes\":13}\n");
    }
}
