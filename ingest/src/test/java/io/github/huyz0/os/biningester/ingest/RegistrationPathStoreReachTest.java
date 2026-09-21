// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Nothing on the registration path can reach the object store (M6.11, NFR-2).
 *
 * <p>⚠️ SPLIT FROM {@code IdleConsumerCostTest} AT THE 700-LINE CAP, along a
 * real seam: that file COUNTS requests over simulated time, and this one
 * asserts a property of the TYPES. They share no fixture.
 *
 * <p>⚠️ IT EXISTS BECAUSE THE COUNTING CASE CANNOT CARRY THIS. No class on the
 * registration path holds a {@code BinStore}, so no mutation of that path can
 * move a request meter -- which is exactly the by-construction non-proof
 * {@code IdleConsumerCostTest}'s class javadoc says M1 was withdrawn for. This
 * case reds the moment one of those types can reach a store, on the commit that
 * introduces it.
 *
 * <p>⚠️ THE DEFECT IT NAMES IS M6.11's OWN ACCEPTANCE CLAUSE: a registration
 * path that resolves an alias by READING the object store. It would cost one
 * request per push -- on every relevant cluster-state change, of every cluster,
 * forever -- and NFR-2 is what this service exists for.
 */
class RegistrationPathStoreReachTest {

    /**
     * NO TYPE ON THE REGISTRATION PATH CAN NAME A STORE (M6.11).
     *
     * <p>⚠️ THE COUNTING CASE ABOVE IS NOT ENOUGH ON ITS OWN, and this file's
     * own class javadoc says why: M1's baseline was never established because
     * the property held BY CONSTRUCTION -- no class on the path held a
     * {@code BinStore}, so no counting case could fail. The registration path
     * is in exactly that state today, so the counting zero is re-asserted
     * (M6's SPEC requires it) and the structural check is what can actually
     * RED: adding a store parameter to any of these types fails here, on the
     * commit that adds it, rather than a milestone later when somebody counts.
     *
     * <p>⚠️ THE PLAUSIBLE DEFECT IS NAMED IN M6.11's ROW: a registration path
     * that resolves an alias by READING the object store. It would cost one
     * request per push -- on every relevant cluster-state change, of every
     * cluster, forever -- and NFR-2 is what this service exists for.
     */
    @Test
    void NOTypeOnTheREGISTRATIONPathCanREACHAStore() {
        // ⚠️ THE SEEDS ARE HAND-NAMED, AND THE LIST IS THE WEAKEST PART OF
        // THIS CASE. The walk is transitive through DECLARED signatures, so a
        // helper reached from one of these is covered -- but a class reached
        // only through a STATIC CALL IN A BODY is in no signature and must be
        // seeded by hand. `RoutingPartitioner` is exactly that: `RoutedIngest`
        // calls `RoutingPartitioner.partitionFor(...)` and names it nowhere
        // else, so review found a store field there passing green. It is
        // seeded below; the general fix is M6.18's scanner, and until then a
        // reader adding a class to this path has to add it here.
        List<Class<?>> seeds = List.of(IndexCatalog.class, PendingPool.class,
                RoutedIngest.class, RoutingPartitioner.class,
                io.github.huyz0.os.biningester.format.IndexRegistration.class);

        List<String> reaches = storeReachableFrom(seeds);

        assertThat(reaches)
                .as("a registration path that could reach the object store would cost a "
                        + "request per push -- on every relevant cluster-state change, of "
                        + "every cluster, forever -- and NFR-2 is what this service exists "
                        + "for. ⚠️ TRANSITIVE, because review measured the one-hop version "
                        + "being defeated by a package-private helper in this package holding "
                        + "the store and being called from `register()`")
                .isEmpty();
    }

    /**
     * Every path from {@code seeds} to a {@code io.github.huyz0.os.biningester.binstore} type, through
     * declared fields, constructor and method parameters and return types.
     *
     * <p>⚠️ IT FOLLOWS OUR OWN TYPES AND STOPS AT EVERYONE ELSE'S. Expanding
     * the JDK walks the world; expanding {@code io.github.huyz0.os.biningester.*} is what catches the
     * helper the one-hop version missed. Generic arguments are walked too, so a
     * {@code Supplier<BinStore>} is not a hiding place.
     *
     * <p>⚠️ IT FOLLOWS WHAT A TYPE DECLARES AND WHAT IT INHERITS -- fields,
     * constructor and method parameters, return types, generic arguments,
     * nested types, superclass and interfaces -- through every array level.
     * Round 3 measured the first two of those escaping an earlier version.
     *
     * <p>⚠️ IT READS DECLARED SIGNATURES AND IS BLIND TO METHOD BODIES, which
     * is the hole that swallows this task's own acceptance clause and is stated
     * here rather than discovered: a static holder, a {@code new} inside
     * {@code register()}, or a lambda capture is a FIRST hop nothing here can
     * see, and transitivity only helps once the first hop is visible. Review
     * MEASURED it -- a package-private helper with a {@code static final
     * INSTANCE} field, called from a body, leaves every case in this file
     * green. Closing it needs a scanner over the source text, which is
     * {@code check-io-seam}'s shape and M6.18's row.
     *
     * <p>⚠️ AND IT IS A TEST, NOT A GATE: it runs where these four types are on
     * the classpath and says nothing about reflection, a dependency that opens
     * a socket without naming one, or a reach added in a module this case does
     * not seed. `check-io-seam` makes the same trade for the same reason and
     * says so in AGENTS.md.
     */
    private static List<String> storeReachableFrom(List<Class<?>> seeds) {
        java.util.Deque<Object[]> queue = new java.util.ArrayDeque<>();
        java.util.Set<Class<?>> seen = new java.util.HashSet<>();
        List<String> reaches = new java.util.ArrayList<>();
        seeds.forEach(seed -> queue.add(new Object[] {seed, seed.getSimpleName()}));
        while (!queue.isEmpty()) {
            Object[] head = queue.poll();
            Class<?> type = (Class<?>) head[0];
            String path = (String) head[1];
            if (type == null || type.isPrimitive() || !seen.add(type)) {
                continue;
            }
            if (isStore(type)) {
                reaches.add(path);
                continue;
            }
            if (!ours(type)) {
                continue;
            }
            for (java.lang.reflect.Field field : type.getDeclaredFields()) {
                walk(queue, field.getGenericType(), path + "." + field.getName());
            }
            for (java.lang.reflect.Constructor<?> ctor : type.getDeclaredConstructors()) {
                for (java.lang.reflect.Type param : ctor.getGenericParameterTypes()) {
                    walk(queue, param, path + ".<init>");
                }
            }
            for (java.lang.reflect.Method method : type.getDeclaredMethods()) {
                for (java.lang.reflect.Type param : method.getGenericParameterTypes()) {
                    walk(queue, param, path + "." + method.getName() + "(..)");
                }
                walk(queue, method.getGenericReturnType(),
                        path + "." + method.getName() + "()");
            }
            for (Class<?> nested : type.getDeclaredClasses()) {
                walk(queue, nested, path + "$" + nested.getSimpleName());
            }
            // ⚠️ AND WHAT IT INHERITS, which round 3 MEASURED escaping: a
            // `class StoreBase { protected BinStore store; }` with
            // `IndexCatalog extends StoreBase` left every case green, because
            // `getDeclaredFields` sees only what a class declares itself. An
            // `extends` is ordinary Java, not an evasion.
            walk(queue, type.getGenericSuperclass(), path + "^super");
            for (java.lang.reflect.Type face : type.getGenericInterfaces()) {
                walk(queue, face, path + "^implements");
            }
        }
        return reaches;
    }

    private static void walk(java.util.Deque<Object[]> queue, java.lang.reflect.Type type,
            String path) {
        if (type instanceof Class<?> raw) {
            // ⚠️ EVERY ARRAY LEVEL, not one: round 3 MEASURED `BinStore[][]`
            // escaping, because unwrapping once leaves `BinStore[]`, whose
            // `getPackage()` is null -- so it matched neither "is a store" nor
            // "is ours" and the walk stopped there.
            Class<?> element = raw;
            while (element.isArray()) {
                element = element.getComponentType();
            }
            queue.add(new Object[] {element, path});
            return;
        }
        if (type instanceof java.lang.reflect.ParameterizedType parameterized) {
            walk(queue, parameterized.getRawType(), path);
            for (java.lang.reflect.Type argument : parameterized.getActualTypeArguments()) {
                walk(queue, argument, path + "<>");
            }
            return;
        }
        // ⚠️ THE THREE OTHER SHAPES A TYPE CAN TAKE, and review measured two of
        // them escaping: `Supplier<? extends BinStore>` is a WildcardType and
        // `<T extends BinStore> void register(T)` is a TypeVariable, and
        // branching only on Class and ParameterizedType dropped both silently.
        if (type instanceof java.lang.reflect.WildcardType wildcard) {
            for (java.lang.reflect.Type bound : wildcard.getUpperBounds()) {
                walk(queue, bound, path + "<? extends>");
            }
            for (java.lang.reflect.Type bound : wildcard.getLowerBounds()) {
                walk(queue, bound, path + "<? super>");
            }
            return;
        }
        if (type instanceof java.lang.reflect.TypeVariable<?> variable) {
            for (java.lang.reflect.Type bound : variable.getBounds()) {
                walk(queue, bound, path + "<T extends>");
            }
            return;
        }
        if (type instanceof java.lang.reflect.GenericArrayType array) {
            walk(queue, array.getGenericComponentType(), path + "[]");
        }
    }

    private static boolean isStore(Class<?> type) {
        return named(type).startsWith("io.github.huyz0.os.biningester.binstore");
    }

    /** ⚠️ OUR OWN CODE, which is what the walk expands. */
    private static boolean ours(Class<?> type) {
        return named(type).startsWith("io.github.huyz0.os.biningester.");
    }

    private static String named(Class<?> type) {
        Package where = type.getPackage();
        return where == null ? "" : where.getName();
    }

}
