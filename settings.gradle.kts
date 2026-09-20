// SPDX-License-Identifier: Apache-2.0

// The module list is docs/internal/product/architecture.md § Modules, in
// dependency order. Adding a module here without adding it there is how a
// component map stops being true.
rootProject.name = "opensearch-bin-ingester"

dependencyResolutionManagement {
    repositories { mavenCentral() }
}

include(
    "binstore-spi",
    "binstore-backends",
    "format",
    "sequencer",
    "ingest",
    "http",
    "client",
    "plugin",
    // The benchmark harness: the load generator and, from M9.4, the macro
    // harness (ADR-0059). Nothing depends on it.
    "bench",
    // ⚠️ LAST, because it depends on everything above it and nothing depends on
    // it -- the composition root is a leaf, which is what keeps the dependency
    // surface acyclic (M8.1, ADR-0052).
    "server",
)
