# client

The consumer: subscription, fetch-mode dispatch, coalescing and decode. Bundled
into the plugin and running inside the OpenSearch process.

**Depends on:** `format` only.

That single dependency is the point. This code runs in someone else's JVM, so its
dependency surface is a liability rather than a convenience.
