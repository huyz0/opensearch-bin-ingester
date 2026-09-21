// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

/**
 * An operator's mistake in the settings, as distinct from a defect in this code
 * (M8.26).
 *
 * <p>⚠️ **A SEPARATE TYPE SO THE PROCESS CAN TELL THEM APART.** M8's criterion
 * 21 asks that a bad configuration fail the process at startup rather than at
 * first use; what makes that useful is that the operator gets a message naming
 * the key they got wrong, and not a stack trace from somewhere inside the
 * assembly. A caller that cannot distinguish the two prints both the same way.
 */
public class ConfigurationException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
