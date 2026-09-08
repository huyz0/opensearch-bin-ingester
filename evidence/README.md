# M4 evidence

The harness's own output for every commit on this branch, preserved because
`.harness/` is gitignored and would not otherwise survive the archive.

- `harness/review/` — 790 files, 760 parseable verdicts across 121 tasks and
  380 distinct reviewed hashes. Each is `{diff_sha256, verdict, findings[],
  task, role}`. 447 of them belong to M4 (3.79 rounds per task against a
  documented cap of two). Severity distribution across the whole corpus:
  1,586 minor, 615 major, 69 blocking.
- `harness/tdd/red.json`, `red.log` — the red records minted by `tdd-red.sh`.
- `harness/mutants-*.log`, `mutants-targets.txt` — PIT output. The sequencer
  run reports 109 of 119 mutants killed (92%) on the changed classes.
- `harness/check-module-*.log`, `coverage-report.log`, `harness-tests.log` —
  per-gate output from the last run on this branch.
- `baselines/review.txt` — the argued-findings file. 37 lines, 28 argued keys:
  26 `rounds:` round-cap escapes (21 of them M4), one `size:`, one legacy
  `reviewer:` key, and one entry (`M4.17-R1a`) with no role prefix that
  silences nothing and is dead text.
- `baselines/mutants.txt` — 6.7 KB of argued surviving mutants that no script
  reads; `mutants.py` never opens this file.

This directory is data, not machinery. Nothing on this branch or on `main`
loads it.
