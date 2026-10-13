// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;

/**
 * One store a fleet's nodes share, tracing every fast-mode request each makes
 * (M13.86; ADR-0081 §1, §9): what was asked of {@code ctl/fast/}, by whom,
 * and -- for each write that landed -- what it changed in the roster it
 * replaced, so a test asserts the writes a takeover or a departure makes BY
 * WHAT THEY ARE (a fence, a close, a join) rather than by count alone.
 *
 * <p>⚠️ A POD CAN BE KILLED: every request it makes after fails, as a crashed
 * pod's would never arrive -- so its close writes nothing and its lease runs
 * out, which is a crash as the store sees one.
 */
final class FleetStoreTrace {

    private static final String FAST = "/ctl/fast/0/";

    private final MemoryBinStore shared = new MemoryBinStore();
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final List<String> writes = new CopyOnWriteArrayList<>();
    private final Map<String, byte[]> last = new ConcurrentHashMap<>();
    private final Set<String> dead = ConcurrentHashMap.newKeySet();

    /** The store {@code pod}'s node opens: the shared one, traced as {@code pod}'s. */
    UnaryOperator<BinStore> forPod(String pod) {
        return opened -> {
            try {
                opened.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                    new Class<?>[] {BinStore.class},
                    (proxy, method, args) -> invoke(pod, method, args));
        };
    }

    /** The shared store itself, untraced: what a test reads to wait on. */
    BinStore store() {
        return shared;
    }

    /** Every request {@code pod} makes from now fails. */
    void kill(String pod) {
        dead.add(pod);
    }

    /** Each fast-mode request so far: {@code "<pod> <verb> <name>"}. */
    List<String> requests() {
        return List.copyOf(requests);
    }

    /** Each fast-mode write that landed so far, by what it changed. */
    List<String> writes() {
        return List.copyOf(writes);
    }

    /** Forgets what was traced so far. */
    void clear() {
        requests.clear();
        writes.clear();
    }

    private Object invoke(String pod, Method method, Object[] args) throws Throwable {
        String verb = method.getName();
        if (verb.equals("close")) {
            return null;
        }
        String key = args != null && args.length > 0 && args[0] instanceof String s ? s : null;
        if (dead.contains(pod)) {
            throw new IOException(pod + " is dead");
        }
        boolean fast = key != null && key.contains(FAST);
        if (!fast) {
            return call(method, args);
        }
        String name = key.substring(key.indexOf(FAST) + FAST.length());
        requests.add(pod + " " + verb + " " + name);
        if (!verb.startsWith("put")) {
            return call(method, args);
        }
        byte[] bytes;
        try (InputStream in = ((Body) args[1]).open().get()) {
            bytes = in.readAllBytes();
        }
        args[1] = Body.ofBytes(bytes);
        synchronized (this) {
            Object result = call(method, args);
            boolean landed = result instanceof Optional<?> o ? o.isPresent() : result != null;
            if (landed) {
                writes.add(pod + " " + what(name, last.get(key), bytes));
                last.put(key, bytes);
            }
            return result;
        }
    }

    private Object call(Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(shared, args);
        } catch (InvocationTargetException failed) {
            throw failed.getCause();
        }
    }

    private static String what(String name, byte[] before, byte[] after) throws IOException {
        if (name.equals("LATEST")) {
            return "LATEST " + Roster.decodeLatest(after);
        }
        Roster now = Roster.decode(after);
        if (before == null) {
            return "CREATE " + now.epoch();
        }
        Roster was = Roster.decode(before);
        List<String> changed = new ArrayList<>();
        if (now.fencedBy() != was.fencedBy()) {
            changed.add("FENCE");
        }
        for (Roster.Member m : now.members()) {
            Optional<Roster.Member> old = was.member(m.incarnation().podUid());
            if (old.isEmpty()) {
                changed.add("JOIN " + m.incarnation().podId());
            } else if (m.state() != old.get().state()) {
                changed.add(m.state() + " " + m.incarnation().podId());
            }
        }
        if (!now.termRecord().equals(was.termRecord())) {
            changed.add("RECORD");
        }
        if (!now.decisions().equals(was.decisions())) {
            changed.add("DECIDE");
        }
        if (now.closed() && !was.closed()) {
            changed.add("CLOSE");
        }
        return String.join("+", changed) + " " + now.epoch();
    }
}
