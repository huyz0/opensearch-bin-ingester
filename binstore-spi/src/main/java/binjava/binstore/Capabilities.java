// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

/**
 * What a backend can actually do, checked at STARTUP.
 *
 * <p>⚠️ Checked eagerly so a store lacking conditional writes fails loudly
 * rather than silently corrupting the commit log: the whole coordination
 * substrate (ADR-0002) rests on {@code putIfAbsent} being atomic, and a backend
 * that quietly degrades it to {@code put} would lose records with no error.
 *
 * @param conditionalWrites putIfAbsent is atomic against concurrent writers
 * @param batchDelete delete accepts many keys in one request
 * @param maxKeyBytes longest key the backend accepts
 * @param minPartSize smallest non-final multipart part
 * @param costs what its requests cost; M1.3's meter reads this
 */
public record Capabilities(
        boolean conditionalWrites,
        boolean batchDelete,
        long maxKeyBytes,
        long minPartSize,
        CostTable costs) {}
