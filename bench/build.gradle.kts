// SPDX-License-Identifier: Apache-2.0

// The benchmark harness (ADR-0059). It depends on NOTHING in the production
// module graph today -- the load generator and results writer are benchmark
// tooling only -- and that boundary is the point: these dependencies reach no
// module that ships.
//
// ⚠️ NOTHING MAY DEPEND ON THIS MODULE. It is a leaf like `server`, for the
// opposite reason: `server` may name everything, `bench` may be named by
// nothing. check-module.sh enforces both leaves; ADR-0059 § Consequences says
// why that boundary matters.
plugins { id("io.github.huyz0.os.biningester.java-conventions") }

dependencies {
    implementation(libs.hdrhistogram)
}
