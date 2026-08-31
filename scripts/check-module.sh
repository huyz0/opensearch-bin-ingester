#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# architecture.md § Dependency rules: a module stays inside its declared
# dependency surface.
#
#   scripts/check-module.sh              modules this change touches
#   GATE_SCOPE=full scripts/check-module.sh   all eight
#   scripts/check-module.sh format       one module, named
#
# ⚠️ The default is `delta` and may check NOTHING when a change touches no module
# directory. The success line says which it did -- an earlier header claimed a
# bare invocation checks "every module", which was false exactly when it mattered.
#
# This is the gate architecture.md rule 5 has promised since before the build
# existed -- a floor for it, not a proof of it: see the denylist note below. A Helidon type reaching `ingest` compiles fine and passes every test;
# it is wrong only against ADR-0019, which says the in-process API and the HTTP
# endpoint are two front doors over one implementation.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
# `--list-modules <settings-file>` prints the derived list and exits. It exists
# so the derivation can be constrained by a test: two line-based versions of it
# silently dropped a module, and reverting either left every hook green because
# nothing exercised it.
# ⚠️ ONE implementation. `--list-modules` first duplicated it, so the regression
# test exercised a copy and mutating the gate's own derivation left the suite
# green -- the two-copies-drift failure this project keeps hitting.
# ⚠️ Cross-checked BOTH ways against the filesystem, because deriving the list
# and trusting it is what failed twice. A module in settings.gradle.kts with no
# build file, or a build file with no settings entry, is drift the gate must
# refuse rather than quietly skip.
#
# It is the load-bearing half of this gate: a short list is otherwise
# indistinguishable from compliance. It lives behind `--check-drift` so it can
# be constrained by a test -- review found it was 100% failure path and could be
# deleted entire with the suite still green.
# check_drift <root> <all> [build-file-list]
#
# ⚠️ The candidate list is a PARAMETER because git-derived scope and fixture
# trees genuinely conflict: the gate must use the tracked set (only a tracked
# orphan can be committed, and a filesystem glob blocked every commit when a
# reference clone sat at the repo root), while a fixture tree is by definition
# not the repo. The gate passes the git list; --check-drift scans the fixture
# root and says so.
# ⚠️ The third argument is "-" for "derive from the tracked set", NOT the empty
# string. They were conflated, and an EMPTY candidate list from a fixture root
# with no depth-1 build file then fell through to this repository's tracked
# files: eight false FAILs naming format, plugin and binstore-backends against
# a fixture that contains none of them. "No candidates" and "you decide" are
# different answers and the sentinel keeps them apart.
check_drift() {
  # ⚠️ ${3--}, NOT ${3:--}. The colon form substitutes the default when the
  # argument is empty as well as when it is absent -- which is precisely the
  # conflation this sentinel exists to remove, so writing it with a colon
  # reinstated the bug in the line meant to fix it.
  local root="$1" all="$2" candidates="${3--}" m d
  # A positive signal that it RAN. Reporting only "did not fail" is satisfied by
  # not running at all -- replacing the call with `true` passed a test that
  # asserted DRIFT=ok, which is the absence-proves-presence error again.
  DRIFT_CHECKED=0
  for m in $all; do
    DRIFT_CHECKED=$((DRIFT_CHECKED+1))
    [ -f "$root/$m/build.gradle.kts" ] \
      || fail "settings lists '$m', which has no build.gradle.kts"
  done
  # ⚠️ git-derived, depth-1, NOT a filesystem glob. A glob here matched any
  # directory on disk, so a gitignored reference clone at the repo root blocked
  # every commit with "OpenSearch has a build.gradle.kts but is not in
  # settings" -- the exact scenario check-gate-scope.sh exists to prevent, and
  # invisible to it because its detector looks for find/glob.glob/os.walk.
  #
  # Only a TRACKED orphan can be committed, so the tracked set is also the right
  # semantics: an untracked build file is not part of the commit under test.
  [ "$candidates" != "-" ] || candidates=$(workspace_files '*/build.gradle.kts' \
                                     | grep -E '^[^/]+/build\.gradle\.kts$')
  for f in $candidates; do
    d="${f%%/*}"
    [ "$d" = "buildSrc" ] && continue
    case " $all " in *" $d "*) ;; *) fail "$d has a build.gradle.kts but is not in settings -- it would be silently unchecked" ;; esac
  done
}

# Which modules a set of changed paths selects. Pure, so it can be tested: the
# shared-build escalation inside it was DEAD for a whole round (pipefail + a
# piped `grep -q` returned 141) and nothing noticed, because the branch had no
# seam a test could reach.
select_modules() {
  local all="$1"; shift
  if is_shared_build_change "$@"; then
    printf '%s\n' $all
    return 0
  fi
  local m
  for m in $(printf '%s\n' "$@" | cut -d/ -f1 | sort -u); do
    case " $all " in *" $m "*) echo "$m" ;; esac
  done
}

# ⚠️ NO PIPE, and the pipe form SURVIVES the suite -- it is not equivalent, the
# killing input is just impractical to construct. Measured: `printf` with two
# paths fits the 64 KiB pipe buffer and exits 0 before grep closes the read end,
# while a 200k-line producer returns 141. So a reintroduced pipe answers
# correctly for every realistic commit and falsely for a huge one. This comment
# is the guard; there is no test.
# ⚠️ `... | grep -q` under `set -o pipefail` returns 141: grep -q exits
# at the first match and the producer dies of SIGPIPE. That killed this branch
# once outright; capturing the input first only moved the failure past the pipe
# buffer. Bash pattern matching cannot fail that way, and it is the ONE copy of
# this predicate -- it was duplicated into the WHY computation and that copy was
# reachable by no test.
is_shared_build_change() {
  local p
  for p in "$@"; do
    case "$p" in
      buildSrc/*|gradle/*|settings.gradle.kts|build.gradle.kts|gradle.properties) return 0 ;;
    esac
  done
  return 1
}

# ⚠️ THE composition -- the module list, the drift check, the reason and the
# selection -- computed once and used by both `--plan` and the gate.
#
# `--plan` was previously a SECOND, parallel composition: it re-derived ALL,
# re-called check_drift and re-ran the selection, so restoring a defect on the
# gate side alone left the whole suite green. Testing a restatement of the gate
# is not testing the gate.
# plan [--root <dir>] <changed-path>...
#
# ⚠️ The root is a PARAMETER solely so the drift FAIL branch is reachable from a
# fixture. Without it `PLAN_DRIFT=ok` was a constant no test could contradict,
# and drift would never have halted the gate. Default is the repo, which is
# what the gate passes.
plan() {
  # ⚠️ "-" means "derive the candidates from the tracked set", which is what the
  # real root wants. It is NOT the empty string: empty means "this root has no
  # depth-1 build files", a true and different answer that a fixture can give.
  local root="$ROOT" settings="settings.gradle.kts" cand="-"
  if [ "${1:-}" = "--root" ]; then
    root="$2"; settings="$2/settings.gradle.kts"; shift 2
    # Same reasoning as --check-drift: a fixture tree is not a git repo, so the
    # candidate list must come from the filesystem HERE and only here.
    cand=$(cd "$root" && ls */build.gradle.kts 2>/dev/null)
  fi
  PLAN_ALL=$(derive_modules "$settings" | tr '\n' ' ' | sed 's/ $//')
  DRIFT_CHECKED=unrun
  local before="$FAILED"
  check_drift "$root" "$PLAN_ALL" "$cand"
  PLAN_DRIFT_CHECKED="$DRIFT_CHECKED"
  if [ "$FAILED" -eq "$before" ]; then PLAN_DRIFT=ok; else PLAN_DRIFT=fail; fi
  if is_shared_build_change "$@"; then
    PLAN_WHY=shared-build
  else
    PLAN_WHY=module-dirs
  fi
  PLAN_MODULES=$(select_modules "$PLAN_ALL" "$@" | tr '\n' ' ' | sed 's/ $//')
}

derive_modules() {
  sed -n '/^include(/,/^)/p' "${1:-settings.gradle.kts}" \
    | grep -oE '"[^"]+"' | tr -d '"'
}

# ⚠️ The gate's OWN composition -- ALL from derive_modules, the drift check, the
# selection, and WHY -- for a given set of changed paths. It exists because three
# test-only doors were tested while the production path was entered by nothing:
# review restored the SIGPIPE defect gate-side only and the whole suite stayed
# green while the gate printed "ok no module changed" on a shared-build change.
# Testing the helpers is not testing the gate.
if [ "${1:-}" = "--plan" ]; then
  shift
  plan "$@"
  echo "ALL=$PLAN_ALL"
  echo "DRIFT=$PLAN_DRIFT checked=$PLAN_DRIFT_CHECKED"
  echo "WHY=$PLAN_WHY"
  echo "MODULES=$PLAN_MODULES"
  exit 0
fi

if [ "${1:-}" = "--select-modules" ]; then
  _all="$2"; shift 2
  select_modules "$_all" "$@"
  exit 0
fi

if [ "${1:-}" = "--check-drift" ]; then
  # Test door: the fixture root is not the repo, so the candidate list comes
  # from the filesystem here and ONLY here. The gate uses the tracked set.
  _cand=$(cd "$2" && ls */build.gradle.kts 2>/dev/null)
  check_drift "$2" "$(derive_modules "$2/settings.gradle.kts" | tr '\n' ' ')" "$_cand"
  [ "$FAILED" -eq 0 ] && ok "no module drift"
  finish
fi

if [ "${1:-}" = "--list-modules" ]; then
  derive_modules "${2:-settings.gradle.kts}"
  exit 0
fi

hdr "check-module"

[ -x ./gradlew ] || { fail "no ./gradlew"; finish; }

# ⚠️ Derived from settings.gradle.kts, never restated. As a third hardcoded copy
# of the module list, a module added to the build but not to this line was
# silently skipped by the delta path -- which fails loudly for the same name
# given on the command line -- so the first module added after this landed would
# have gone unchecked while the gate printed "8 module(s) checked".
# ⚠️ Every quoted string inside the include(...) block, not a line pattern.
# Two line-based regexes shipped before this and each silently dropped a module:
# one required a trailing comma, the next anchored to end-of-line so an inline
# comment (`"plugin", // the OpenSearch plugin`) dropped it. A short list is
# indistinguishable from compliance -- the gate printed "ok 7 module(s) checked"
# over a real Helidon dependency in the missing one.
ALL=$(derive_modules settings.gradle.kts | tr '\n' ' ' | sed 's/ $//')
[ -n "${ALL// /}" ] || { fail "no modules found in settings.gradle.kts -- a broken parser is not eight broken modules"; finish; }

# ⚠️ This is the PRODUCTION door, and for two rounds nothing entered it: every
# suite came in through a test-only flag, so mutations confined to these lines
# left all 38 tests green while the gate printed "ok no module changed" for
# every commit. ModuleGateTest now stages files in a scratch git repository and
# runs this script with no flag, which is the only way to reach it -- the scope
# comes from `git diff --cached` and the orphan candidates from `git ls-files`,
# neither fakeable in the real tree without staging into it.
plan $(changed_files)
ALL="$PLAN_ALL"
[ "$PLAN_DRIFT" = ok ] || finish

# ⚠️ Rule 5 names exactly these four. NOT `client` or `plugin` -- the consumer
# must talk to the ingester, so an HTTP client there is the design, not a
# violation. NOT `binstore-backends` -- S3, GCS and Azure SDKs all pull netty or
# httpcomponents transitively, and failing that module would tell its author the
# decision "belongs in ingest", pressuring them to widen this list or delete a
# group. A gate whose only escape is to weaken it is a gate that gets weakened.
NO_HTTP="format binstore-spi sequencer ingest"
# architecture.md rule 2: no cloud SDK in the OpenSearch JVM.
NO_CLOUD="client plugin"

# ⚠️ Both lists name modules, and nothing ties them to settings.gradle.kts:
# rename a module and its rule silently stops applying, with the gate still
# printing ok while enforcing strictly less. This is NOT checked at runtime --
# the gate must work against a fixture root whose modules are deliberately not
# the real ones -- so RuleListTest asserts it against the real settings file.

# Groups, matched with a following `.` OR `:` so a sub-namespace cannot slip
# through. `io\.helidon:` alone missed io.helidon.webserver, io.helidon.common
# and io.helidon.http -- i.e. every real Helidon artifact, which is the library
# rule 5 names. The two groups that happened to be verified by hand
# (io.netty, software.amazon.awssdk) are the only two with no sub-namespace.
# ⚠️ A DENYLIST, and therefore a floor rather than a proof. `io.undertow`,
# `io.vertx`, `org.apache.tomcat.embed`, `com.linecorp.armeria`,
# `org.jboss.resteasy`, `io.projectreactor.netty` (which does not contain the
# substring `io.netty`) and `io.grpc:grpc-netty-shaded` were all added after
# review pointed out they slipped. More will exist.
#
# ⚠️ `com.sun.net.httpserver` and `java.net.http` are JDK packages, not
# coordinates -- they were briefly listed here, where they could never match.
# MT13: a jar dropped in a module's `libs/` and pulled in with `files(...)` is
# also invisible, because Gradle's dependency report omits file collections.
#
# ⚠️ `java.net.http` needs no coordinate at all: a production class can open
# an HTTP connection with the JDK alone, and NO classpath check can see it. This
# gate raises the cost of reaching for a framework; it does not prove rule 5.
HTTP_GROUPS='io\.helidon|org\.eclipse\.jetty|io\.netty|jakarta\.servlet|org\.apache\.httpcomponents|com\.squareup\.okhttp3|org\.glassfish\.jersey|io\.undertow|io\.vertx|org\.apache\.tomcat|com\.linecorp\.armeria|org\.jboss\.resteasy|io\.projectreactor\.netty|io\.grpc|org\.springframework'
CLOUD_GROUPS='software\.amazon\.awssdk|com\.amazonaws|com\.google\.cloud|com\.azure'

# Content door: pins WHICH modules each rule names. Placed here, directly under
# the assignments and ABOVE every selection path, because below the
# `no module changed` early exit it printed nothing whenever the staged change
# touched no module -- making RuleListTest a function of the index, green only
# while a shared-build file happened to be staged.
#
# ⚠️ A FLAG, not an environment variable. As `CHECK_MODULE_PRINT_RULES=1` an
# exported value silently turned the hook into a green no-op: it exits 0 before
# the loop, so every commit would have passed while checking nothing.
#
# ⚠️ NOT the anti-shadow guard. Inserting `NO_HTTP=""` below this line leaves it
# printing the strong value; that job belongs to the rule counts taken inside
# the loop.
if [ "${1:-}" = "--print-rules" ]; then
  echo "NO_HTTP=$NO_HTTP"
  echo "NO_CLOUD=$NO_CLOUD"
  exit 0
fi

if [ $# -gt 0 ]; then
  MODULES="$*"; WHY="named on the command line"
elif [ "$GATE_SCOPE" = "full" ]; then
  MODULES="$ALL"; WHY="GATE_SCOPE=full"
else
  # ⚠️ A change to the shared build alters EVERY module's resolved classpath
  # while touching no module directory. The conventions plugin already injects a
  # `dependencies {}` block into all eight, so adding Helidon there is precisely
  # the change this gate must catch -- and mapping paths to module names would
  # route around it.
  # ⚠️ Mutating this to `MODULES=` makes the gate print "ok no module changed"
  # for every commit -- the exact false green this file exists to prevent.
  # ModuleGateTest.aSharedBuildChangeEscalatesToEveryModule is what catches it.
  MODULES="$PLAN_MODULES"
  case "$PLAN_WHY" in
    shared-build) WHY="the shared build changed, so every module's classpath may have" ;;
    *)            WHY="changed module directories" ;;
  esac
  [ -n "${MODULES// /}" ] || { ok "no module changed$(scope_note)"; finish; }
fi

# ⚠️ Counted INSIDE the loop, from the same expansion each rule tests, and
# reported in the summary. Two weaker designs failed first: reading the
# assignment text with a regex constrained a COPY, and printing the values just
# above the loop still let `NO_HTTP=""` be inserted BETWEEN the print and the
# loop -- both left rules 2 and 5 inert for every module with the whole suite
# green. A count taken where the rule is applied cannot be shadowed without
# changing the number.
checked=0
applied_r5=0
applied_r2=0
for m in $MODULES; do
  case " $ALL " in *" $m "*) ;; *) fail "unknown module: $m"; continue ;; esac
  mkdir -p .harness
  if ! ./gradlew ":$m:build" --console=plain -q > ".harness/check-module-$m.log" 2>&1; then
    fail "$m: build failed -- see .harness/check-module-$m.log"
    tail -10 ".harness/check-module-$m.log" | sed 's/^/           /'
    continue
  fi

  # ⚠️ BOTH configurations. Rule 5's text is "compile without Helidon on the
  # classpath", and a `compileOnly` dependency never reaches runtimeClasspath --
  # so a compileOnly netty plus a production class calling io.netty.util.Version
  # passed a runtime-only check. Rule 4 (nothing depends on `http`) is also a
  # compile-time fact.
  #
  # ⚠️ The exit status is checked. Discarding it made an empty `deps` grep clean
  # and print the success line over bytes never read: a failed resolution, a
  # daemon killed under the 512 MiB cap, and a typo'd module name all looked
  # identical to compliance.
  deps=""
  bad_conf=""
  for conf in runtimeClasspath compileClasspath; do
    if ! one=$(./gradlew ":$m:dependencies" --configuration "$conf" \
               --console=plain -q 2>".harness/check-module-$m.err"); then
      bad_conf="$conf"; break
    fi
    [ -n "$one" ] || { bad_conf="$conf (empty report)"; break; }
    deps="$deps
$one"
  done
  if [ -n "$bad_conf" ]; then
    fail "$m: could not resolve $bad_conf -- see .harness/check-module-$m.err"
    head -5 ".harness/check-module-$m.err" | sed 's/^/           /'
    continue
  fi
  checked=$((checked+1))

  case " $NO_HTTP " in *" $m "*)
    applied_r5=$((applied_r5+1))
    hits=$(printf '%s\n' "$deps" | grep -oE "($HTTP_GROUPS)[.:][a-zA-Z0-9.-]*" | sort -u || true)
    if [ -n "$hits" ]; then
      fail "$m resolves an HTTP dependency (architecture.md rule 5, ADR-0019):"
      printf '%s\n' "$hits" | sed 's/^/           /'
      echo "         Two front doors, one implementation. If this module needs HTTP,"
      echo "         the decision belongs in \`ingest\` and the adapter in \`http\`."
    fi ;;
  esac

  # architecture.md rule 4, as written: nothing depends on `http`. Cited in this
  # file as a compile-time fact and never asserted -- `client` with
  # api(project(":http")) passed green, because `http` has no external
  # dependencies today and the group greps therefore saw nothing.
  if [ "$m" != "http" ] && printf '%s\n' "$deps" | grep -qE "project '?:http'?"; then
    fail "$m depends on http (architecture.md rule 4)"
    echo "         Nothing depends on \`http\`: it is a front door, and a front door"
    echo "         something else depends on has stopped being one. Via \`client\` or"
    echo "         \`plugin\` this puts Helidon in the OpenSearch JVM."
  fi

  # architecture.md rule 2, as written: `plugin` depends on `client`, never on
  # `binstore-backends`. The cloud-SDK grep below does NOT cover it -- the
  # backend has no external dependencies yet, so a direct project dependency
  # passed clean and would only start failing once someone added an SDK to it.
  if [ "$m" = "plugin" ] && printf '%s\n' "$deps" | grep -qE "project '?:binstore-backends'?"; then
    fail "plugin depends on binstore-backends (architecture.md rule 2)"
    echo "         The plugin's dependency surface is deliberately minimal: no cloud"
    echo "         SDK in the OpenSearch JVM. Route through \`client\` and \`format\`."
  fi

  case " $NO_CLOUD " in *" $m "*)
    applied_r2=$((applied_r2+1))
    hits=$(printf '%s\n' "$deps" | grep -oE "($CLOUD_GROUPS)[.:][a-zA-Z0-9.-]*" | sort -u || true)
    if [ -n "$hits" ]; then
      fail "$m resolves a cloud SDK (architecture.md rule 2):"
      printf '%s\n' "$hits" | sed 's/^/           /'
      echo "         This runs inside someone else's JVM; its dependency surface is"
      echo "         a liability, not a convenience."
    fi ;;
  esac
done

# ⚠️ The rule counts are part of the summary, not a debug aid. "8 module(s)
# checked" is equally true when every rule was skipped -- which is exactly what
# shadowing NO_HTTP/NO_CLOUD produced, invisibly, with the suite green.
[ "$FAILED" -eq 0 ] \
  && ok "$checked module(s) checked, rule5 tested $applied_r5, rule2 tested $applied_r2 ($WHY)$(scope_note)"
finish

