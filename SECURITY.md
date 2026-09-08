# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | yes (pre-1.0, pilot) |

Pre-1.0 releases are covered while the M0–M7 milestones are open; after the
1.0 tag, security fixes are backported to the latest two minor releases.

## Reporting a Vulnerability

Do **not** open a public issue for a security concern.

- Email: `security@<project-domain>` (see README contact table)
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
  (F-13); `--ssl-ca` / `--ssl-cert` for mTLS. The doctor reports the
  configured minimum before start.
- **Credentials** — connection strings via environment (`GPSTREAM_SRC_*`,
  `GPSTREAM_DST_*`) or `--config` file (mode 0600). Credentials are never
  written to the manifest, logs, or `/metrics`.
- **Integrity** — per-frame checksums, per-chunk manifest records, and
  destination reconciliation after resume (F-17).
- **Least privilege** — the streaming role needs only the privileges the
  doctor checks (CREATE/CONNECT on the target database plus COPY privileges
  on the objects being copied); run it as a dedicated role.

## Disclosure

We follow coordinated disclosure: no public detail until a fix is available
and a window for patching has passed.
