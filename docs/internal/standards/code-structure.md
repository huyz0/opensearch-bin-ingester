# Code structure

**Family:** Code
**Read when:** Adding a module, package or file; when a file nears 500 lines; or when deciding where a seam belongs.

1. **A file is at most 500 lines.** Split it; never raise the limit.
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
