#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Every gate judges THIS repository and nothing else.
#
# A gate that walks the filesystem sees whatever happens to be on the disk: a
# reference clone under .tmp/, a sibling checkout at ../OpenSearch, another
# project's build output. It then reports on files this project does not own --
# or, worse, passes because it never looked at ours.
#
# The rule is deterministic so it survives a new session with none of the
# context that produced it: gates discover files through `workspace_files`,
# which is derived from git, and this script fails any that do not.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-gate-scope"

# Scripts allowed to walk the filesystem, each for a stated reason.
# ⚠️ Adding a line here is a decision. Say why -- and add it in the SAME commit
# as the script it waives. A waiver granted ahead of the code is a decision made
# in a review that cannot see what it is waiving.
declare -A ALLOWED=(
  [lib.sh]="defines workspace_files itself"
  [check-gate-scope.sh]="this script; it greps the others for these patterns"
  [tdd-red.sh]="deletes stale build/test-results, which are untracked by design"
  [tdd_scan.py]="reads build/test-results and locates test sources under */src/*/java"
  [build-index.sh]="globs .agents/skills and docs/internal/standards to generate the index"
  [check-harness-tests.sh]="counts testcases in buildSrc/build/test-results, which is untracked build output"
  [harness_failures.py]="names the failing testcases in buildSrc/build/test-results, the same untracked build output check-harness-tests.sh counts -- and for the same reason: the suite runs under --console=plain -q, so its LOG holds no test names and only the XML can answer. ⚠️ The glob is anchored to that one directory and cannot reach a sibling checkout"
  [review_rounds.py]="reads .harness/review, which is gitignored BY DESIGN -- a verdict is machine-local evidence, so workspace_files cannot see it. ⚠️ The glob is anchored to that one directory and cannot reach a sibling checkout"
  [review_delta.py]="same: .harness/review is gitignored, and the prior round's findings live nowhere else"
  [coverage.py]="reads */build/classes and */build/reports/jacoco -- build output git cannot enumerate. ⚠️ The MODULE list is git-derived; only the search INSIDE a known module directory touches the filesystem, so it cannot wander into a sibling checkout"
)

for f in scripts/*.sh scripts/*.py; do
  base=$(basename "$f")
  [ -n "${ALLOWED[$base]:-}" ] && continue

  # `grep -r` whose operand list can be empty is a filesystem walk in disguise:
  # with no files, GNU grep recurses the working directory instead. It reads as
  # scoped code, which is why it survived the first version of this gate.
  # Per grep, and keyed on the variable that grep actually uses -- an earlier
  # version looked for the literal name $FILES anywhere in the file, so a second
  # unguarded list slipped through and a differently-named guarded one was
  # falsely failed.
  unguarded=""
  while IFS= read -r line; do
    var=$(printf '%s' "$line" | grep -oE '\$\{?[A-Za-z_][A-Za-z_0-9]*' | tail -1 | tr -d '${')
    [ -n "$var" ] || continue
    grep -qE "\[ -z \"?\\\$\{?$var" "$f" || unguarded="$unguarded$line
"
  done < <(grep -nE 'grep -r[a-zA-Z]* ' "$f" | grep -E '\$[A-Za-z_{]')
  if [ -n "$unguarded" ]; then
    fail "$base greps a variable file list with no empty-list guard"
    printf '%s' "$unguarded" | head -2 | sed 's/^/           /'
    echo "         An empty operand list makes grep -r search the working directory,"
    echo "         so the gate reads .tmp/, .harness/ and any sibling checkout."
    echo "         Guard it: [ -z \"\$FILES\" ] && { ok ...; finish; }"
  fi

  # A filesystem walk that is not workspace_files.
  if grep -nE '(^|[^_[:alnum:]])(find[[:space:]]+[./]|glob\.glob\(|os\.walk\()' "$f" >/dev/null 2>&1; then
    fail "$base walks the filesystem instead of using workspace_files"
    grep -nE '(^|[^_[:alnum:]])(find[[:space:]]+[./]|glob\.glob\(|os\.walk\()' "$f" | head -3 | sed 's/^/           /'
    echo "         Use: workspace_files '*.md'   -- tracked + staged, git-derived."
    echo "         If this gate genuinely needs untracked files, add it to ALLOWED"
    echo "         in scripts/check-gate-scope.sh with the reason."
  fi
done

# Nothing may reach outside the repository root, whatever it is called.
for f in scripts/*.sh scripts/*.py; do
  hits=$(grep -nE "['\"](\.\./|/home/|~/)" "$f" | grep -vE '\.\./\.\./|dirname' || true)
  if [ -n "$hits" ]; then
    fail "$(basename "$f") references a path outside the repository"
    printf '%s\n' "$hits" | head -3 | sed 's/^/           /'
    echo "         A gate that reads ../ or an absolute home path judges files this"
    echo "         project does not own, and its result depends on what else is on"
    echo "         the disk -- which is not a gate."
  fi
done

# A source file that .gitignore swallows is invisible to git, to every gate that
# derives its file list from git, and to a fresh clone -- which then fails to
# build for a reason nobody can see in the diff.
#
# ⚠️ This is not hypothetical: the package `binjava.build` put two Kotlin task
# classes in a directory named `build/`, the standard ignore pattern matched it,
# and `git add buildSrc` silently staged neither.
# `.tmp/` is excluded deliberately -- it is the quarantine, and git collapses an
# ignored directory to a single entry, so it would otherwise always match here.
ignored_src=$(git ls-files --others --ignored --exclude-standard \
              -- '*/src/*.kt' '*/src/*.java' '*/src/*.kts' 2>/dev/null \
              | grep -vE '^\.tmp/' | grep -E '\.(kt|java|kts)$' || true)
if [ -n "$ignored_src" ]; then
  fail "source file(s) under a src/ directory are gitignored:"
  printf '%s\n' "$ignored_src" | head -5 | sed 's/^/           /'
  echo "         They are absent from a fresh clone. Rename the directory rather"
  echo "         than weakening the ignore rule -- a package called 'build' is the"
  echo "         problem, not the pattern that catches build output."
fi

# .tmp/ is the quarantine for reference clones and scratch. It must stay ignored,
# or the next `git add -A` commits somebody else's repository into this one.
#
# ⚠️ FIVE earlier forms of this check were each wrong, in both directions, and
# the list is the specification:
#   `check-ignore -q .tmp`      -- a directory-only pattern matches only when the
#                                  directory EXISTS, so this failed in every
#                                  fresh checkout: a false RED accusing the repo
#                                  of not ignoring the path it ignores
#   `check-ignore -q .tmp/probe` -- passed via the DEVELOPER's ~/.config/git/ignore
#                                  or $GIT_DIR/info/exclude, neither of which is
#                                  this repository's decision
#   anchoring the match SOURCE   -- passed on a NEGATIVE pattern: `check-ignore -v`
#                                  exits 0 and prints the rule for
#                                  `!.tmp/ourclone/README`, so the gate went green
#                                  while `git add -A` staged the clone. ⚠️ What
#                                  fixes that is the `-q` EXIT STATUS below, not
#                                  a test for a leading `!`: git reports the
#                                  WINNING rule, so a winning negation means the
#                                  path is not ignored and `-q` fails first. An
#                                  explicit negation branch was written here and
#                                  deleted -- review proved it unreachable for
#                                  every input, and an untestable branch that
#                                  looks like the defence is worse than none,
#                                  because it hides which line is load-bearing
#   one literal probe path       -- passed on any pattern covering that path but
#                                  not its siblings: `README`, `.tmp/ourclone/`,
#                                  `.tmp/**/README`
#   an explicit negation branch  -- written to close the case above, then proved
#                                  UNREACHABLE and deleted; see the note below
#
# So: several paths across TWO distinct subtrees, each of which must be ignored
# (exit status, not just a printed rule), by a rule that comes from THIS
# .gitignore, and is not a negation. A rule quarantining one clone is not a
# quarantine. No pipe anywhere -- `check-ignore | grep -q` under pipefail is the
# SIGPIPE shape this repository has been bitten by four times.
_quarantine_fail=""
for _p in .tmp/ourclone/README .tmp/ourclone/src/Main.java .tmp/other/pom.xml .tmp/scratch.txt; do
  if ! git check-ignore -q "$_p" 2>/dev/null; then
    _quarantine_fail="$_p is not ignored at all"; break
  fi
  _rule=$(git check-ignore -v "$_p" 2>/dev/null || true)
  case "$_rule" in
    .gitignore:*) ;;
    *) _quarantine_fail="$_p is ignored by ${_rule%%	*}, not by this repository's .gitignore"; break ;;
  esac
done
if [ -n "$_quarantine_fail" ]; then
  fail ".tmp/ is not quarantined by this repository's .gitignore"
  echo "         $_quarantine_fail"
  echo "         It is the quarantine for reference clones and scratch work."
  echo "         Un-ignored, a 'git add -A' commits an entire upstream checkout."
fi

[ "$FAILED" -eq 0 ] && ok "$(ls scripts/*.sh scripts/*.py | wc -l | tr -d ' ') gate scripts judge this repository only"
finish
