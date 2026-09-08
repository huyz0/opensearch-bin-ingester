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
2. **A method is at most ~50 lines.** No gate.
3. **Business logic touches no socket, clock, or object store directly.** It
   takes a seam. If it needs I/O to test, it is in the wrong layer.
4. **The seams are few and named**: `BinStore`, `Clock`, `Sequencer`,
   `SubscriptionTransport`. A new seam is an ADR.
5. **Every seam has a fake** used by T0/T1 tests, kept in step with the real
   implementation in the same commit.
6. **The ingester and the plugin share formats and the SPI, never runtime
   choices.** The plugin's dependency surface is deliberately far smaller.
7. **Every module has a README.md** saying what it is and what it depends on.
8. **No module named `util`, `common`, `helpers` or `misc`.** A dumping ground is
   where structure goes to die.
9. **Package by feature, not by layer.**
