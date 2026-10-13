# Operating fast mode and the peer listener

For whoever deploys an ingester fleet. This page covers the settings fast mode
([ADR-0081](../internal/product/decisions/0081-fast-mode-is-sequenced-held-published-and-uploaded-by-the-leader.md))
and the peer listener
([ADR-0084](../internal/product/decisions/0084-peer-routes-are-served-on-their-own-listener-with-mutual-tls.md))
ask of you, and what each one costs if it is wrong. The reasoning lives in
those two records; this page says what to do.

## What fast mode is

An index set to `wal=true` is acked once `wal_quorum` copies of each batch sit
in the journals of ingester nodes in that many zones. The ack does not wait for
the segment upload. It stays opt-in, per index, and the guarantee is weaker
than the default path's (NFR-14 against NFR-8): an acked fast record is lost
only if every holder of it is gone before the upload commits it.

## Per-index settings (set in OpenSearch, pushed by the plugin)

| Setting | Meaning |
|---|---|
| `index.ingestion_source.param.wal` | Exactly `true` or `false`. `true` makes the index fast. Default `false`. |
| `index.ingestion_source.param.wal_quorum` | Copies, in distinct zones, before the ack: exactly `1`, `2` or `3`. Default `2`. |
| `index.ingestion_source.param.flush_timer` | The index's deadline, a positive whole number of `ms`, `s`, `m` or `h`. Default `5s`. ⚠️ It is registered and validated, but **does not yet change the upload cadence**: a leader uploads at the fixed `min_upload_interval` (250 ms), and the tightest-timer trigger ADR-0013 describes is backlog M13.31b. Tuning it today changes nothing. |

A malformed value keeps the index at its last good settings, and the plugin
logs an error (ADR-0082 §1).

⚠️ **Do not switch `wal` on a live index yet.** The switch under skew (ADR-0081
§8) is not built (backlog M13.34). Set it when the index is created.

⚠️ **A `wal_quorum = 3` index on three zones stops acking while one zone is
lost**, and acks again when it returns (ADR-0081 §2.3).

## Ingester node settings

| Setting | Required | Meaning |
|---|---|---|
| `pod.id` | yes | The pod's name with its dashes removed (`ingester-0` is `ingester0`). |
| `pod.uid` | yes | The Kubernetes Pod UID, from the Downward API's `metadata.uid`. |
| `pod.az` | yes | The pod's zone, from the node's `topology.kubernetes.io/zone`. |
| `fast.journal.dir` | for fast mode | An `emptyDir` for the journal and the epoch file. Absent, the node is diskless. |
| `fast.journal.cap` | no | The journal's cap in bytes. Default 512 MiB. Only with `fast.journal.dir`. |
| `peer.tls` | yes | `mutual` or `off`. There is no default. |
| `peer.port` | yes | The peer listener's port, 1 to 65535: the same on every pod of the fleet. (0, an ephemeral port, is for a test whose pods share a host.) |
| `peer.tls.cert`, `peer.tls.key`, `peer.tls.ca` | with `mutual` | The pod's PEM chain, its PKCS#8 key, and the trust domain's CA. |

⚠️ **Disks only where an index is fast.** A fleet with no `wal=true` index
needs no `fast.journal.dir` on any node; its takeovers and departures cost a
few roster writes per term and nothing per record (`FastWalFalseFleetTest`).
A fleet with a fast index needs a journal on every node: a node with none takes
no copy, and as leader it runs no takeover of the fast terms before it.

## The peer listener

Pod-to-pod traffic (`/ctl/commit`, `/ctl/drain`, `/ctl/durable-segment` and
`/ctl/fast`) is served on `peer.port` and only there. The producer port answers
those four `404`. The endpoint a pod advertises names the peer listener,
`https://` under `mutual`.

`peer.tls = off` is for a single node, a development fleet or a test. It logs a
warning at every start naming the four routes it leaves open.

A fleet changes `peer.tls` or `peer.port` all at once. During a rolling change,
a pod in one mode cannot reach a pod in the other: commit forwards defer to the
inbox (one PUT per deferred flush), joins are retried by the lease watch, and
durable-segment hints are lost (they are best effort). Rotating a certificate
takes a restart; the files are read once, at start.

## The peer certificate

Under `mutual`, each pod presents a certificate that names exactly that pod.
The pod REFUSES to start if the certificate names any other.

- **One URI SAN** in the trust domain:
  `spiffe://<trust.domain>/<path>/<pod-name>/<pod-uid>`. Its last two
  segments are the Kubernetes pod name and UID.
- **The path names the fleet.** Everything before the last two segments is the
  fleet's prefix, and a peer's certificate binds only when its prefix equals
  the receiving pod's. Put the fleet's namespace and StatefulSet in it:
  `spiffe://<domain>/ns/<namespace>/sts/<statefulset>/<pod-name>/<pod-uid>`.
  ⚠️ A path that names only the cluster (`spiffe://<domain>/pod/...`) lets a
  pod of another fleet under the same trust domain speak for this one, and no
  check can tell the two apart. A prefix that is only the trust domain is
  refused at start.
- **One StatefulSet per fleet.** Its `<set>-<ordinal>` names stay distinct
  with their dashes removed, which `pod.id` needs.
- **Issued per pod**, from the trust domain's CA, by an issuer that can
  template the pod name and UID: cert-manager's CSI driver, or SPIRE.
- **A short lifetime, and a restart inside it.** The certificate's lifetime is
  how long a leaked key speaks for its pod. ⚠️ **The files are read once, at
  start** (ADR-0084 decision 6), so a pod keeps the certificate it started
  with: restart every pod before its certificate expires, or peer handshakes
  fail fleet-wide at expiry, and commit forwards, joins and fast frames stop.
  Choose a lifetime longer than your longest interval between restarts, with
  margin, and alert on certificate expiry.

An issuer template, for cert-manager's CSI driver (`csi.cert-manager.io`),
which substitutes the pod's own name, namespace and UID when it mounts the
volume. ⚠️ Written from the driver's documentation, not run against a
cluster: check every attribute name against the version you deploy. The URI
SAN's shape is what matters, and a wrong one fails safe -- the pod refuses to
start:

```yaml
volumes:
  - name: peer-tls
    csi:
      driver: csi.cert-manager.io
      readOnly: true
      volumeAttributes:
        csi.cert-manager.io/issuer-name: ingester-ca        # the trust domain's CA
        csi.cert-manager.io/issuer-kind: Issuer
        csi.cert-manager.io/uri-sans: "spiffe://<trust.domain>/ns/${POD_NAMESPACE}/sts/ingester/${POD_NAME}/${POD_UID}"
        csi.cert-manager.io/duration: 168h                   # longer than your restart interval
        csi.cert-manager.io/key-encoding: PKCS8
```

with `peer.tls.cert`, `peer.tls.key` and `peer.tls.ca` pointing at the
mounted `tls.crt`, `tls.key` and `ca.crt`, and `pod.uid` and `pod.id` from
the Downward API (`metadata.uid`; `metadata.name`, its dashes removed).

## Metrics

Each is pod-level, with no label
([observability.md](../internal/standards/observability.md)):

| Name | What |
|---|---|
| `biningester_fast_ack_seconds` | A fast write's ack, from its COMMIT to its EXPOSED. NFR-15's p99 target is 10 ms. |
| `biningester_fast_quorum_wait_seconds` | A batch's wait at the leader, from assignment to exposure. |
| `biningester_fast_journal_bytes` | What the pod's journal holds. Near `fast.journal.cap`, writes slow. |
| `biningester_fast_uploads_timer_total`, `biningester_fast_uploads_leave_total` | Uploads, by cause. |
| `biningester_fast_voided_offsets_total` | Offsets a takeover voided after a quorum loss: acked fast records lost, as counted gaps. Should be zero. |

## What is not built yet

- `flush_timer` does not yet set the upload cadence (M13.31b, above).
- Consumers see a fast record when its upload commits, about one
  `min_upload_interval` (250 ms) after the ack. The fast publication that
  delivers it sooner (backlog M13.30b) and the catch-up from the leader's
  journal (M13.32) are not built.
