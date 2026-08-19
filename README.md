# A-share sealed public-data runner

This repository is a deliberately narrow execution adapter. It retrieves public
SH/SZ market data from BaoStock and returns encrypted raw daily and intraday
snapshots. It does **not** contain or execute trading strategies, candidate
ranking, portfolio logic, account data, positions, P&L, acceptance gates, or
live-trading code.

## Boundary

- The repository and workflow source are public.
- A private caller encrypts each request with AES-256-GCM before dispatch.
- The request is decrypted only inside the ephemeral GitHub-hosted job.
- Only raw public-market rows are collected; indicators and signals are computed
  later in the private control plane.
- The job encrypts its result before upload. The one-day artifact contains only
  ciphertext.
- Workflow logs contain group, partition count, and status only. They never print
  symbols, dates, prices, request plaintext, or provider error messages.

## One-time setup

1. Keep the default branch named `main`.
2. Add a repository Actions secret named `SEALED_JOB_KEY_B64`. Its value must be
   base64 for exactly 32 random bytes and must match the key held by the private
   control plane.
3. Do not add `push`, `pull_request`, `pull_request_target`, `workflow_run`, or
   `repository_dispatch` triggers to the sealed workflow.
4. Keep default workflow-token permission read-only and require reviewed changes
   to `main` if collaborators are added.

The workflow accepts only manual dispatches on `main` by the repository owner. It
uses a six-way matrix, bounded two-process provider concurrency inside each job,
immutable action commit pins, read-only repository permission, and one-day result
retention.

## Local verification

```bash
python -m unittest discover -s tests -v
```

Network fetches are intentionally excluded from the unit suite.

