# Peer TLS test fixtures (ADR-0084)

⚠️ **Test material only.** No deployment trusts these; their CA's private key
was deleted when they were made, so nothing more can be issued from it.

| File | What |
|---|---|
| `ca.pem` | the test trust domain's CA (`CN=cluster-a peer CA`), EC P-256 |
| `pod1.pem`, `pod1.key` | a pod certificate from `ca.pem` and its PKCS#8 key; SAN `localhost`, `127.0.0.1`; server and client auth |
| `pod2.pem`, `pod2.key` | a second, the same way |
| `foreign-ca.pem` | another domain's CA (`CN=other-domain CA`) |
| `foreign.pem`, `foreign.key` | a certificate from `foreign-ca.pem`, refused by `ca.pem`'s trust |

All are valid until 2126-09-12 (M13.52b, 2026-10-07; ADR-0084 decision 7).
Made once with OpenSSL 3.5 (EC P-256, `-days 36500`) and committed; the tests
never spawn a tool to make their own.
