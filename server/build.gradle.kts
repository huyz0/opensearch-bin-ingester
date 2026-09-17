// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    // ⚠️ THE COMPOSITION ROOT DEPENDS ON EVERYTHING AND NOTHING DEPENDS ON IT.
    // That is what makes it a leaf in the dependency surface rather than a
    // cycle, and it is why `main()` lives here and not in `http`: `http` is the
    // thin adapter (architecture.md rule 4, ADR-0019) and the in-process API is
    // the primary one, so a root there would make the HTTP surface the assembly
    // point and hand every in-process embedder a web server.
    // ⚠️ `implementation`, NOT `api`: nothing depends on this module, so there
    // is no consumer for an exported API -- and `api` on a leaf is a claim that
    // someone downstream compiles against these types, which would be false.
    implementation(project(":ingest"))
    implementation(project(":sequencer"))

    // ⚠️ THE ONLY MODULE THAT MAY NAME A BACKEND IN `src/main` (M8's criterion
    // 2). Every other module takes a `BinStore`; this one chooses which. M8.29
    // extends `check-module.sh` to refuse this dependency from anywhere else --
    // until it lands, the rule is a convention and this comment, which is rung
    // 7 and says so.
    implementation(project(":binstore-backends"))
}
