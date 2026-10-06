# Adapter-host shared-Postgres recovery harness

Sample-only deployment: three CSharpWebHost nodes sharing Postgres, using actual
AddLegate/UseLegate/factory/request DI. An explicitly registered Process runtime is
a deliberate trusted-sample choice, not a production sandbox. No Redis is needed
because distributed LLM admission is disabled here. Enabling it requires explicit
Redis configuration and retains its startup canary.

```sh
python samples/CSharpWebHost/cluster/smoke.py
```

Requires Docker Linux containers, Python 3 and ports 8181-8183. The script owns
the `legate-web-415` compose project, builds the .NET 10 image, starts Postgres and
three symmetric registered-agent hosts, and tears down its containers/network
after success or failure. Compose development credentials are local sample values,
not production credentials. Never put real secrets in this file.

The 30-second sample provider hold creates an observable active call. The harness
opens on web-1, subscribes from web-2, reconciles the active-call node with durable
claim owner/attempt/expiry and pending-inbox metadata, reads the exact receiving-node
control target, then SIGKILLs that owner. It issues no rescue prompt, reply, abort,
restart, or database write. Within the unchanged 240-second bound, an eligible
survivor must autonomously recover the same real-entry target under a later fenced
attempt to one Completed settlement. Durable sequences must be gap-free and replay
identical on both survivors.

This corrects the earlier stranded Running/expired-claim behavior: background
services no longer resolve a scoped request facade, and a separate bounded Running
keyset sweep discovers work even with no pending inbox. It sends existing wakes;
TryRecoverControlTarget is still the sole takeover authority. Before expiry, with
a live owner, or with stopped/terminal-pending control, activation remains refused.
The context/provider owns all execution settings and resources; a receiving root is
never an execution fallback.

Startup validates composition and explicit cluster choices, not durability or
topology. Atomic settlement compatibility is not a shared-backend certificate.
This harness proves only the stated Postgres/sample deployment behavior; custom
providers need their own deployment verification. Symmetric bindings/models/tools/
policies/completion destinations and genuinely shared durable storage remain host
obligations. S3 is optional unless configured functionality requires blobs.
