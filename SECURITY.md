# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | no (historical) |
| 0.3.0   | yes (pre-1.0) |
| 0.3.1   | yes (current release, pre-1.0) |

Pre-1.0 releases are covered while the M0–M7 milestones are open; after the
1.0 tag, security fixes are backported to the latest two minor releases.

## Reporting a Vulnerability

Do **not** open a public issue for a security concern.

- Report privately via GitHub Security Advisories: <https://github.com/Dmitry-Ku/gpstream/security/advisories/new>
- Subject: `gpstream: <short description>`
- Include: affected version, reproduction steps or proof-of-concept, and the
  impact you believe the issue has (data exposure, auth bypass, DoS, …).

We aim to acknowledge reports within **48 hours** and to ship a fix or a
documented risk-acceptance decision within **30 days** for confirmed issues.
After a fix ships we credit the reporter in the release notes unless they ask
to remain anonymous.

## Threat Model Summary

gpstream moves production data between Greenplum clusters. The security
boundaries that matter most:

- **Transport** — TLS ≥ 1.2 with cert validation on the streaming path
  (F-13); `--ssl-ca` / `--ssl-cert` for mTLS. The dry-run diagnostic reports the
  configured minimum before start.
- **Credentials** — connection strings via the `--source`/`--dest` flags or the
  `source`/`dest` keys of a config file (mode 0600); endpoints are **not
  env-driven** (there is no `GPSTREAM_SRC_*`/`GPSTREAM_DST_*` — see
  docs/CONFIGURATION.md). Credentials are never written to the manifest, logs,
  or `/metrics`.
- **Integrity** — per-frame checksums, per-chunk manifest records, and
  destination reconciliation after resume (F-02/F-07).
- **Least privilege** — the streaming role needs CREATE/CONNECT on the
  target database (what the dry-run checks) plus SELECT on the source objects
  and INSERT/CREATE on the destination (object privileges are NOT checked by
  the dry-run); run it as a dedicated role.

## Disclosure

We follow coordinated disclosure: no public detail until a fix is available
and a window for patching has passed.
