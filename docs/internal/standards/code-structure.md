# Code structure

**Family:** Code
**Read when:** Adding a module, package or file; when a file nears 700 lines; or when deciding where a seam belongs.

1. **A file is at most 700 lines.** Split it rather than trimming to fit.
   ⚠️ **Raised from 500 to 700 on 2026-09-08, by decision.** The previous text
   read "never raise the limit", and removing that absolute is the point of the
   edit: a threshold that may never move is one that gets skipped instead of
   moved, and a skipped gate leaves no record. What is still forbidden is
   raising it *so that a particular file fits* — non-negotiable 2, unchanged.
   Recorded here rather than in the gate, because the number is enforced by
   `check-file-size.sh` and the reason is not. At the time of the change no
   tracked source file exceeded 500 lines; the largest was 496.
   → `scripts/check-file-size.sh`
   ⚠️ **Three files are held below 600, not 700** (M11.24): `DefaultIngest`,
   `Assembly` and `BulkService`, the files fast mode lands in, split by
   M11.24a-c after M11's splits grew back past 600 within one milestone. The
   list is named, not global, and a named file that moves or disappears fails
   the gate rather than lapsing.
   → `./gradlew gates` (`RepositoryGateChecks.SPLIT_CEILINGS`)
2. **A method is at most ~50 lines.** No gate.
3. **Business logic touches no socket, clock, or object store directly.** It
   takes a seam. If it needs I/O to test, it is in the wrong layer.
   → `scripts/check-io-seam.sh`
   ⚠️ The gate bans twelve I/O PACKAGES outright — `java.nio.file`,
   `java.nio.channels`, `java.io`, `java.util.zip`, `java.util.jar`,
   `java.util.prefs`, `java.util.logging`, `java.sql`, `javax.sql`,
   `javax.naming`, `java.net`, `javax.net` —
   so no sibling can be NAMED inside one, with the byte and checksum types that
   live in them carved out and each pinned by a case that must PASS. ⚠️ It does
   NOT close subclassing OUT of a listed package: `java.util.jar.JarFile`
   extends `java.util.zip.ZipFile` and passed until `java.util.jar` was listed
   too, so every package a reach can live in must be named. The clock and the subprocess are banned by
   CONSTRUCT instead (`.now()`, `currentTimeMillis(`, `Clock.system`, `.exec(`),
   because their packages hold `Clock`, `Instant` and `Runtime` and cannot be
   forbidden. `LocalDate.now(clock)` — reading an INJECTED clock — passes.
   `binstore-backends` is exempt, and by module rather than by `implements`:
   eleven `src/main` files implement one of the five seams against that module's
   two, so a seam-implementation carve-out is five times wider and takes
   `LocalSequencer` with it.
   ⚠️ It is a DENY-LIST and cannot be complete — reflection reaches anything,
   and a dependency can open a socket without naming one. It raises the cost of
   reaching past a seam; it does not make it impossible.
4. **The seams are few and named**: `BinStore`, `Clock`, `Sequencer`,
   `SubscriptionTransport`, `Membership`, `JournalFile` (ADR-0083, the fast
   journal's one file). A new seam is an ADR.
5. **Every seam has a fake** used by T0/T1 tests, kept in step with the real
   implementation in the same commit.
6. **The ingester and the plugin share formats and the SPI, never runtime
   choices.** The plugin's dependency surface is deliberately far smaller.
7. **Every module has a README.md** saying what it is and what it depends on.
8. **No module named `util`, `common`, `helpers` or `misc`.** A dumping ground is
   where structure goes to die.
9. **Package by feature, not by layer.**
