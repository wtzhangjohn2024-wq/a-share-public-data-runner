# Security model

The confidentiality goal is to prevent private request selection and returned
rows from becoming public through repository contents, workflow inputs, logs, or
artifacts. AES-256-GCM authenticates both directions with a job-specific context.

The plaintext request necessarily exists in memory and ephemeral disk on the
GitHub-hosted runner while the public data is fetched. GitHub infrastructure and
the repository owner are therefore inside the execution trust boundary. This is
an execution-isolation design, not confidential computing.

Rotate `SEALED_JOB_KEY_B64` immediately if it is printed, committed, included in
an artifact, or shared outside the private control plane. Previously uploaded
ciphertext should be treated as exposed after a key compromise.

Never accept strategy source, rankings, orders, accounts, positions, holdings,
P&L, returns, gates, or acceptance thresholds in this repository. The request
validator rejects these field families before any provider query starts.

