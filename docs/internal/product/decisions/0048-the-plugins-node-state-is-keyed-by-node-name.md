<!-- SPDX-License-Identifier: Apache-2.0 -->
# 0048. The plugin's node state is keyed by `node.name`, and its constructor takes `Settings`

Status: accepted
Date: 2026-09-17
Requirements: FR-7
Amends: M6.13's acceptance (2), which said "no-arg constructor"

## Context

A node constructs `BinStorePlugin` **reflectively**, so nothing can be passed
in. Since M1 the node-level state — one `NodeSubscriptions` per node — has been
a `private static volatile` field, installed before the node boots. That is
correct in production, where a JVM **is** a node, and wrong wherever two nodes
share a process.

`InternalTestCluster` runs every node of a cluster in one JVM. With a static,
every node shares one `NodeSubscriptions`, so M6's criterion 11 — "each node
holds ONE subscription however many shards it hosts" — reads as 1 whatever the
node count is. The criterion becomes **unmeasurable**, not merely unmeasured,
which is why M6.9 depends on this row.

⚠️ It is not only a test problem. The same static is why nothing can hold two
ingester-facing identities in one process: a future in-JVM tool, or a node
serving two clusters, has the same collision with no test in sight.

## Decision

### 1. The state is a factory plus a map keyed by the node's own name

`install(Function<String, NodeSubscriptions>)` replaces
`install(NodeSubscriptions)`. Each plugin instance asks the factory for its
node's subscriptions through `computeIfAbsent(nodeName, factory)`, so one node
asking twice — as it does across a restart — gets the same object rather than a
second subscriber whose predecessor is still taking deliveries.

The ENTRY POINT is still static, because reflection leaves no alternative. What
changed is that the static is a factory rather than an instance, so the STATE is
per node.

### 2. The constructor takes `Settings`

`PluginsService.loadPlugin` accepts one public constructor, looking for
`(Settings, Path)`, then `(Settings)`, then `()`. `Settings` carries
`node.name`, which is the only thing the NODE supplies to a reflectively-built
plugin.

⚠️ **This amends M6.13's acceptance (2)**, which asked for the no-arg
constructor to be kept. As written, (2) and (3) are jointly unsatisfiable: with
no argument the plugin has nothing node-supplied to key by, and the only key
left is call order, which (3) forbids. What (2) exists to protect is named
inside it — "still constructed reflectively … and still loads in
`SingleNodeBootIT`" — and a `(Settings)` constructor preserves exactly that, on
a booting node. `SPEC.md`'s own test-plan row says "reflective constructor
intact", not "no-arg".

### 3. Install order is never the key

Handing out installed instances in order passes every deterministic boot and
hands node B node A's subscription the moment two nodes start at once — which
is the ordinary case in a test cluster, and a restart in production.
`PerNodeInstallTest` constructs eight nodes concurrently off one latch for
exactly this reason.

### 4. A node with no name gets nothing

Not a shared default: keying two unnamed nodes together is the static this ADR
removes, wearing a default's clothes. Every real node has a name.

## Consequences

- A test that installs a factory gets a distinct `NodeSubscriptions` per node
  and can count subscribers per node honestly (M6.9's criterion 11).
- `install` clears the per-node map, so one test class's nodes cannot inherit
  another's state when the framework reuses a node name in the same JVM.
- The plugin has exactly ONE public constructor, still. A second one makes the
  plugin unloadable by a real node ("no unique public constructor") while every
  in-process test stays green, which is why the test-only constructor is
  package-private and a case asserts the count.
- ⚠️ The static is smaller, not gone. A process still has one factory, so two
  deployments in one JVM would need a second mechanism; nothing needs that yet
  and this ADR does not pretend to have solved it.
