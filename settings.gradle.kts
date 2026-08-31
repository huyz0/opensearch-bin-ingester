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
)
