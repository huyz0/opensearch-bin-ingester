# Peer TLS test fixtures (ADR-0084)

⚠️ **Test material only.** No deployment trusts these; their CAs' private keys
were deleted when they were made, so nothing more can be issued from them.

Every pod certificate names its pod in one URI SAN (ADR-0084 decision 8):
`spiffe://<trust domain>/<path>/<pod name>/<pod UID>`. The path before the last
two segments is the fleet's prefix; the pod name keeps its dash, and `pod.id`
is that name with the dash removed (`pod-1` is `pod1`).

| File | What |
|---|---|
| `ca.pem` | the test trust domain's CA (`CN=cluster-a peer CA`), EC P-256 |
| `pod1.pem`, `pod1.key` | `spiffe://cluster-a/ns/test/pod/pod-1/uid-pod1`, from `ca.pem`, with its PKCS#8 key |
| `pod2.pem`, `pod2.key` | `spiffe://cluster-a/ns/test/pod/pod-2/uid-pod2`, the same way |
| `nosan.pem`, `.key` | from `ca.pem`, with no `spiffe://` SAN |
| `twosan.pem`, `.key` | from `ca.pem`, with pod-1's and pod-2's SANs both |
| `wrongdomain.pem`, `.key` | `spiffe://cluster-b/ns/test/pod/pod-1/uid-pod1`: another trust domain |
| `otherns.pem`, `.key` | `spiffe://cluster-a/ns/other/pod/pod-1/uid-pod1`: pod-1's name and UID, another fleet's prefix |
| `bareprefix.pem`, `.key` | `spiffe://cluster-a/pod-1/uid-pod1`: a prefix that is only the trust domain |
| `foreign-ca.pem` | another domain's CA (`CN=other-domain CA`) |
| `foreign.pem`, `foreign.key` | pod-1's SAN, from `foreign-ca.pem`: refused by `ca.pem`'s trust |

Every leaf also carries `DNS:localhost` and `IP:127.0.0.1`, and server and
client authentication. All are valid until 2126-09-13 (M13.52f, 2026-10-07;
ADR-0084 decision 7). Made once with OpenSSL 3.5 (EC P-256, `-days 36500`,
`openssl req -addext subjectAltName=...` then `openssl x509 -req
-copy_extensions copy`) and committed; the tests never spawn a tool to make
their own.
