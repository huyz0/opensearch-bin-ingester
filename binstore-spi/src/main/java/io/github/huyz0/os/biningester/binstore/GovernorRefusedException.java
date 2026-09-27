// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.io.IOException;

/**
 * A store request the cost governor refused (FR-21, ADR-0075).
 *
 * <p>⚠️ AN {@code IOException}, deliberately: every caller that can be refused
 * -- the orphan and retention sweeps -- already treats a failed LIST as "try
 * this pass again later", which is exactly what a refusal asks for.
 */
public final class GovernorRefusedException extends IOException {

    public GovernorRefusedException(String message) {
        super(message);
    }
}
