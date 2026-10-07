# 0084. Peer routes are served on their own listener, with mutual TLS

Status: accepted (amends ADR-0010's "In transit" row for `peer.tls = off`)
Date: 2026-10-07
Requirements: FR-17, NFR-9
Research: docs/research/50-open-questions.md#Q10 (ADR-0010)

## Context

Four routes are called only by another pod: `/ctl/commit` (a forwarded
commit), `/ctl/drain` (an inbox drain), `/ctl/durable-segment` (a
durable-segment hint) and `/ctl/fast` (every fast-mode frame, ADR-0082 §2).
They are served on the producer port, beside the plugin's own `/ctl/register`
and `/ctl/progress`, with no authentication but one: `/ctl/durable-segment`
checks the caller's source address against the EndpointSlice membership
(M13.52, from M13.27h's review). Two harms follow, reachable by anyone who
can open a TCP connection to a pod:

- **A pod deposed.** `/ctl/fast` admits a frame's epoch at its header, before
  its body, and the epoch fence keeps the highest epoch it admits (ADR-0081
  §3). A caller who learns a pod's UID -- every `NOT_ROSTERED` answer names
  it -- sends one frame at epoch `Long.MAX_VALUE`, and the pod refuses every
  later term's frames. A pod with a fast journal keeps that epoch in its
  `epoch` file, so it stays deposed across restarts until the file is deleted
  by hand; a diskless pod recovers its fence from the lease at its next start.
- **Forged commits.** `/ctl/commit` applies a forwarded commit to the chain
  the leader holds; nothing proves the forwarder is a pod of this trust
  domain.

ADR-0010 put TLS on every connection ("In transit: TLS everywhere, including
intra-AZ") and authentication at the producer and plugin edges ("mTLS/token
at both edges"); the pod-to-pod routes were built after it, without either.
The user chose, on 2026-10-07, mutual TLS between pods over a shared token and
over a fence bound (below).

## Decision

1. **A peer listener for exactly four routes.** `/ctl/commit`, `/ctl/drain`,
   `/ctl/durable-segment` and `/ctl/fast` -- named, never a `/ctl/*` prefix --
   are served on their own port, `peer.port`, and only there; the producer
   port answers those four 404 and keeps every other route it serves,
   `/ctl/register` and `/ctl/progress` among them. `peer.port` is a fleet-wide
   setting, the same on every pod (as `http.port` is), 1 to 65535 -- or 0, an
   ephemeral port, for a test whose pods share one host (amended by M13.52b).
2. **Every caller reaches it.** The advertised `endpoint` -- which the lease
   and the roster carry, and the commit forward, the drain and the fast
   frames dial -- names the peer listener, `https://` when TLS is on. The
   durable-segment sender, which dials an EndpointSlice address on its own
   `http.port`, dials that address on `peer.port` instead, over the same TLS.
3. **Mutual TLS on it.** TLS 1.3; the pod presents `peer.tls.cert` with
   `peer.tls.key` (PEM), and REQUIRES a client certificate chaining to
   `peer.tls.ca` (PEM), the trust domain's own CA -- a client with no
   certificate, or one chaining elsewhere, is refused at the handshake. Every
   pod-to-pod client presents the same pair and trusts only that CA.
4. **What it proves, and what it does not.** A connection proves its peer
   holds a key the trust domain's CA certified -- some pod of this domain. It
   does NOT, by itself, bind a frame's sender UID, or a forwarded commit's
   pod, to the certificate presented: a pod's leaked key would forge any pod
   of the domain for the certificate's lifetime. Decision 8 binds them
   (amended by M13.52e).
5. **No silent default.** `peer.tls` is a REQUIRED setting, `mutual` or `off`.
   With `mutual`, the three files are required and read at start; a missing or
   unreadable one refuses the start. `off` serves the peer listener in
   plaintext and logs a warning at every start naming the four routes it
   leaves open -- ⚠️ an exception to ADR-0010's "TLS everywhere", scoped to a
   single node, a development fleet or a test, which this record amends
   ADR-0010 to allow.
6. **Rotation and transition are restarts.** The files are read once, at
   start; a rotated certificate takes effect when the pod restarts. A fleet
   changes `peer.tls` or `peer.port` all at once: a pod in one mode cannot
   reach a pod in the other, so during a rolling change a commit forward fails
   and is deferred to the inbox (one PUT per deferred flush), the drain it asks
   for over `/ctl/drain` fails too, and the leader's inbox sweep applies it
   (corrected by M13.52b); joins are retried by the lease watch; and
   durable-segment hints are lost (they are best effort).
7. **Built in steps**: this record (M13.52a); the settings (M13.52b); the
   listener with client authentication (M13.52c); the clients (M13.52d); the
   identity binding, decided by M13.52e and built as M13.52f (the
   certificate's name, read and checked at start) and M13.52g (the four
   routes checking it); M13.71 the live-incarnation check. Test certificates are committed fixtures,
   valid until 2126, never generated by spawning a tool.
8. **A certificate names its pod and incarnation** (M13.52e). Under
   `mutual` a pod's certificate carries exactly one URI SAN in the trust
   domain, `spiffe://<trust.domain>/<path>/<pod-name>/<pod-uid>`: the issuer
   chooses the path -- at least one segment, none empty (M13.52f) -- and its
   LAST TWO segments are the Kubernetes pod name and UID -- what a per-pod issuer can template (cert-manager's CSI driver,
   SPIRE; M13.38's deployment docs give one). `pod.uid` is that UID, and
   `pod.id` is that name with its dashes removed (`ingester-0` is
   `ingester0`: M8.47 refuses a dash in `pod.id`). The pod reads its own
   certificate at start and REFUSES to start unless the SAN names its
   `trust.domain`, its `pod.uid` and a name whose dashes removed are its
   `pod.id` -- a mis-issued certificate, or a `pod.id` set some other way,
   fails at the pod that holds it. A certificate with no such SAN, or
   several, is refused at start and by every route.

   ⚠️ **THE PREFIX IS THE FLEET.** Everything before the last two segments
   -- the trust domain and the issuer's path, a namespace in it -- is this
   fleet's PREFIX, read from the pod's own certificate at start. A trust
   domain usually spans every workload of a cluster, so a peer's
   certificate binds only when its prefix EQUALS the receiving pod's: a pod
   called `ingester-0` in another namespace, or in a second fleet under the
   same domain, speaks for no pod here. Within the prefix, pod names must
   stay distinct with their dashes removed; one StatefulSet's
   `<set>-<ordinal>` names always do, and M13.38 says a fleet is one. A
   prefix that is only the trust domain is refused at start (M13.52f), but
   no check can tell a path that names the fleet from one that names the
   cluster (`spiffe://<domain>/pod/...`): the issuer template must put the
   fleet's namespace and StatefulSet in it, and M13.38 says so.

   Each peer route reads the client's certificate and refuses, `403`, one
   whose prefix is not this pod's, and a request whose claimed identity is
   not the certificate's:
   - `/ctl/fast`, BY UID: a frame whose sender UID is not the
     certificate's, and a JOIN whose incarnation's UID or `pod.id` is not --
     checked BEFORE the epoch is admitted, so a refused frame never moves
     the fence. The term also refuses a COMMIT or CONFIRM whose key's pod is
     not the sender's rostered incarnation.
   - `/ctl/commit`, `/ctl/drain` and `/ctl/durable-segment`, BY NAME: a
     commit, a drain requester or a hint writer whose `pod.id` is not the
     certificate's name, dashes removed. Under `mutual` a drain names its
     requester or is refused.

   ⚠️ **WHAT IT DOES NOT CLOSE.** These three routes carry a `pod.id`, not
   a UID, so a leaked key speaks for its pod NAME until the certificate
   expires -- across a StatefulSet's replacement of that pod; a short-lived
   certificate is the bound (M13.38 says so). And a leaked key can still
   raise every pod's fence to `Long.MAX_VALUE` under its OWN UID, as
   M13.52's caller could, until it expires: binding stops it speaking for
   another pod, not for a pod that is gone. Refusing a frame from a UID that
   is no live incarnation, before admission, is M13.71's.

   Under `off` there is no certificate and nothing is bound; decision 5's
   warning names that too.

## Alternatives considered

- **A shared peer token** (an HMAC of each request under a cluster-wide
  secret file). Cheaper to build -- no listener, no certificates. Rejected by
  the user: a token is one secret every pod holds and that never expires, so
  a leak lasts until the secret is rotated on every pod at once; a pod's
  certificate is its own and expires, so a leaked key lasts its lifetime and
  rotation is per pod. Neither binds a frame to its sender (decision 4).
- **Bounding the fence only** (verify a raised epoch against the lease,
  one GET at most once per renew interval). Closes the deposition without new
  configuration, but leaves `/ctl/commit` open to forged forwards. Rejected:
  it fixes one of the two harms.
- **Client authentication on the producer port** (OPTIONAL client auth, and
  a per-route check that the four routes saw a certificate). It works, on one
  port. Rejected: every producer's handshake would carry a certificate
  request it cannot answer, and the protection would rest on a check each
  route must remember -- one route added without it is open; a listener whose
  every connection is a pod makes it structural.
- **The pod's identity in the subject's CN** (as the test fixtures had it).
  Rejected for decision 8: a CN is free text, RFC 6125 deprecates it for
  identity, and it holds one string where the binding needs two -- the
  pod's name, behind the `pod.id` a forwarded commit, a drain and a hint
  carry, and its UID, which a fast frame carries.
- **The last two segments only**, the issuer's path ignored. Rejected (the
  same review, round 2): a same-named pod in another namespace, or another
  fleet under the domain, would bind as this fleet's pod.
- **`pod.id` itself in the SAN.** No per-pod issuer can template it: it is
  a setting, and a StatefulSet's pod name has a dash `pod.id` refuses.
  Rejected for the name with its dashes removed, which an issuer writes and
  a pod checks at start.
- **Carrying the UID on the other three routes** (a wire-format change to
  the commit frame, the drain and the hint) so that every route binds the
  incarnation, not the name. Deferred, not rejected: the name binding,
  within the fleet's prefix, already stops one pod of the fleet speaking for
  another, and the residual -- a leaked
  key speaking for its own pod's later incarnation until it expires -- is
  bounded by the certificate's lifetime.
- **TLS off by default.** Every existing configuration keeps working.
  Rejected: an insecure default is the defect being fixed; `peer.tls` is
  required, and `off` is a choice someone wrote down.

## Consequences

- **Cost.** A handshake per peer REQUEST, not per connection (amended by
  M13.52d): every pod-to-pod client runs without keep-alive (M10.36, against
  Helidon's connection-return race, M10.35), so each commit forward, drain,
  fast frame and durable-segment hint opens its own TLS 1.3 connection. Those
  requests scale with segments and terms, so the handshakes do too, never
  with records (non-negotiable 6); no object-store request. Restoring
  keep-alive would make it one per connection. The CPU cost is not measured
  here: M13.37's latency measurement runs with `mutual`.
- **Every configuration names `peer.tls` and `peer.port`.** Every
  property-built config in the tree, the tests' included, gains them --
  M13.51's lesson that a newly required key breaks every config not updated.
- **Operators supply certificates** for a fleet with `mutual`, one per pod
  from the domain's CA; the deployment docs (M13.38) say how.
- **The tests that hold it** (named by M13.52c and M13.52d): the producer
  port still serving `/ctl/register`, `/ctl/progress`, `/sub` and `/seg`; a
  client with no certificate refused, and the `Long.MAX_VALUE` frame replayed
  without one leaving the fence unchanged; a foreign-CA certificate refused;
  a commit forwarded, a term joined and a durable-segment hint delivered
  between two pods over mutual TLS.
- **Forecloses** a peer route reachable from a producer's network position
  without a key the domain's CA certified.
