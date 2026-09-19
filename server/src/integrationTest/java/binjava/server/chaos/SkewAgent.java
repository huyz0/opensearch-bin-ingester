// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.ClassTransform;
import java.lang.classfile.Opcode;
import java.lang.classfile.instruction.InvokeInstruction;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 * A Java agent that moves a process's wall clock (M8.23).
 *
 * <p>⚠️ **THE SKEW IS APPLIED TO THE PROCESS, NOT THROUGH THE {@code Clock}
 * SEAM.** Every pod takes a {@code Clock}, so a skewed one handed in would test
 * the seam, and leave untouched a lease comparison that reads the wall clock
 * directly -- which is exactly the mutation M8.17 exists to catch. So every
 * application class is rewritten as it loads, and each DIRECT static call to
 * {@code System.currentTimeMillis()}, {@code Instant.now()},
 * {@code Clock.systemUTC()}, {@code Clock.systemDefaultZone()},
 * {@code LocalDateTime.now()}, {@code OffsetDateTime.now()} or
 * {@code ZonedDateTime.now()} is redirected to a clock {@code offset} ahead.
 * That covers this project's one wall-clock read ({@code Main}) and the AWS
 * SDK's SigV4 signer, which calls {@code Clock.systemUTC()} (verified by
 * review with {@code javap}).
 *
 * <p>⚠️ **WHAT IT DOES NOT MOVE, STATED SO A ROW CANNOT LEAN ON IT:** method
 * references compiled to {@code invokedynamic} ({@code System::currentTimeMillis}),
 * {@code InstantSource.system()}, {@code Clock.system(ZoneId)}, the
 * {@code now(ZoneId)} overloads, {@code LocalDate/LocalTime/OffsetTime.now()},
 * and every read inside the JDK itself -- TLS certificate validity and
 * {@code new Date()} among them. Classes loaded by the bootstrap or platform
 * loader are left alone, because a rewritten one would name {@link Skewed},
 * which those loaders cannot see.
 *
 * <p>⚠️ **MONOTONIC TIME IS NOT MOVED.** {@code System.nanoTime} measures
 * intervals and a skewed wall clock does not change how long anything takes.
 */
public final class SkewAgent {

    private static final ClassDesc SKEWED = ClassDesc.of(Skewed.class.getName());

    private SkewAgent() {
    }

    /** The offset, as an ISO-8601 duration: {@code -javaagent:skew.jar=PT5M}. */
    public static void premain(String offset, Instrumentation instrumentation) {
        Skewed.offsetMillis = Duration.parse(offset).toMillis();
        instrumentation.addTransformer(new Rewriter());
    }

    /** The redirected clock. */
    public static final class Skewed {

        static volatile long offsetMillis;

        private Skewed() {
        }

        public static long currentTimeMillis() {
            return System.currentTimeMillis() + offsetMillis;
        }

        public static Instant now() {
            return Instant.now().plusMillis(offsetMillis);
        }

        public static Clock systemUTC() {
            return Clock.offset(Clock.systemUTC(), Duration.ofMillis(offsetMillis));
        }

        public static Clock systemDefaultZone() {
            return Clock.offset(Clock.systemDefaultZone(), Duration.ofMillis(offsetMillis));
        }

        public static LocalDateTime localNow() {
            return LocalDateTime.now(systemDefaultZone());
        }

        public static OffsetDateTime offsetNow() {
            return OffsetDateTime.now(systemDefaultZone());
        }

        public static ZonedDateTime zonedNow() {
            return ZonedDateTime.now(systemDefaultZone());
        }
    }

    /** owner, name, descriptor -> the redirect's name. */
    private static final Map<String, String> REDIRECTS = Map.of(
            "java/lang/System.currentTimeMillis()J", "currentTimeMillis",
            "java/time/Instant.now()Ljava/time/Instant;", "now",
            "java/time/Clock.systemUTC()Ljava/time/Clock;", "systemUTC",
            "java/time/Clock.systemDefaultZone()Ljava/time/Clock;", "systemDefaultZone",
            "java/time/LocalDateTime.now()Ljava/time/LocalDateTime;", "localNow",
            "java/time/OffsetDateTime.now()Ljava/time/OffsetDateTime;", "offsetNow",
            "java/time/ZonedDateTime.now()Ljava/time/ZonedDateTime;", "zonedNow");

    private static final class Rewriter implements ClassFileTransformer {

        @Override
        public byte[] transform(ClassLoader loader, String className, Class<?> redefined,
                ProtectionDomain domain, byte[] bytes) {
            if (loader == null || loader == ClassLoader.getPlatformClassLoader()
                    || className == null || className.startsWith("java/")
                    || className.startsWith("jdk/")
                    || className.startsWith("sun/") || className.startsWith("com/sun/")
                    || className.startsWith("binjava/server/chaos/SkewAgent")) {
                return null;
            }
            try {
                ClassFile files = ClassFile.of();
                ClassModel model = files.parse(bytes);
                boolean[] changed = {false};
                byte[] out = files.transformClass(model,
                        ClassTransform.transformingMethodBodies((code, element) -> {
                            if (element instanceof InvokeInstruction call
                                    && call.opcode() == Opcode.INVOKESTATIC) {
                                String key = call.owner().asInternalName() + "."
                                        + call.name().stringValue() + call.type().stringValue();
                                String redirect = REDIRECTS.get(key);
                                if (redirect != null) {
                                    changed[0] = true;
                                    code.invokestatic(SKEWED, redirect,
                                            MethodTypeDesc.ofDescriptor(call.type().stringValue()));
                                    return;
                                }
                            }
                            code.with(element);
                        }));
                return changed[0] ? out : null;
            } catch (RuntimeException unparseable) {
                // ⚠️ LEFT ALONE: a class this cannot rewrite loads unskewed
                // rather than not at all.
                return null;
            }
        }
    }
}
