# SQLite workload envelope (issue 392)

Measurement-first characterization of real SQLite contention over an
isolated file-backed database. The workloads run against the real
`Legate.Storage.Sqlite` provider (`tests/Legate.Tests/SqliteContentionEnvelopeTests.fs`);
InMemory or delayed mocks never substitute for these measurements.

## Workloads

All three share one provider instance over one temp-file database per test,
with WAL mode and the default five-second busy timeout:

- **single-baseline** (Task 1): one session, five turns. Each turn queues one
  inbox message, claims the turn, appends a five-event journal batch, replays
  the journal, and settles. Full evidence per run.
- **concurrent-mix** (Task 2): eight independent sessions sharing one
  provider, four iterations each of inbox append, turn claim, four-event
  journal append, replay, inbox read, and settle, all launched concurrently.
- **slow-contended** (Task 3): one session appending a single 52-event batch
  while four unrelated sessions run three small turns each and a host abort
  request is processed on the slow session. Control acceptance is recorded
  separately from durable completion of the slow turn.

## Observed results

Sample run, `origin/main` at `6a245fd5e9c7a901b68248ee9cfb686af58148fe`
(`simplify C# web hosting and recover expired sessions`), .NET 10.0.10 on
Windows 10.0.26200 X64 with 32 CPUs, `Microsoft.Data.Sqlite` 10.0.12.0.
Exact numbers vary by hardware; they are samples, not targets or promises.

| workload | size | wall | outcome |
|---|---|---|---|
| single-baseline | 1 session, 5 turns, 25 events | 3.8 ms | append p50 0.144 ms, replay p50 0.144 ms; journal gap-free, inbox drained |
| concurrent-mix | 8 sessions, 4 iterations, 128 events | 170.6 ms | 750 events/s, 0 failures; per-session end-to-end p50 3.3 ms, max 75.5 ms |
| slow-contended | 1 x 52-event batch vs 4 x 3 small turns | 17.9 ms | slow batch 0.7 ms; unrelated sessions all completed (p50 2.0 ms); abort request processed with outcome `NoCurrentTurn`; cancelled-token read completed despite cancellation |

Two truthful readings need emphasis:

- The abort request on the slow session returned `NoCurrentTurn`, not
  `Accepted`: that turn was claimed directly through `ClaimNextTurn`, not
  through the control-target binding flow, so no control target existed. The
  evidence is that the control path was *processed* (a well-typed receipt)
  while the large batch held the gate. Control processing progressed;
  nothing is claimed about durable completion beyond the measured settle.
- A read issued with an already-cancelled token still completed. The provider
  executes synchronously under the gate and does not cooperatively cancel
  in-flight database work, so cancellation of running database calls is
  non-cooperative. Bounds hold because concurrency in these workloads is
  bounded (fixed session and iteration counts); no unbounded fan-out is used.

Attribution that is unavailable is reported as unavailable: the provider holds
a single in-process gate around synchronous database calls with no per-phase
timers, so gate-wait versus database-work versus end-to-end delay cannot be
split. Only end-to-end operation latency is measured.

## Envelope and contention limits

- Representative small-turn traffic (a few events per turn, single-digit
  concurrent sessions) completes with sub-millisecond median append/replay
  and zero failures; unrelated sessions progress while one large batch holds
  the gate.
- Under eight concurrent sessions the tail spreads (p50 3.3 ms vs max
  75.5 ms end-to-end per session): that spread is gate serialization, the
  design working as intended, not a defect found by these samples.
- The measured envelope covers single-process hosts with modest session
  counts and small batches. Larger session counts, larger batches, and
  multi-process or Postgres behavior are outside these samples and no claim
  is made about them. No latency target, throughput promise, parallel-writer
  design, or original-incident root cause is inferred.

## Correctness rationale for retained serialization

Serialization is retained because the samples give no evidence against it:

- Journal ordering held in every run: per-session sequences gap-free,
  journals isolated across sessions and tenants, inbox positions consumed
  exactly once per turn.
- Claim fencing held: appends fence on the live claim token and settlements
  are atomic; the takeover race leaves the loser with zero effects (covered
  by the existing conformance and fence suites alongside these workloads).
- Transaction atomicity, tenant isolation, and claim fencing were preserved
  in all three workloads with zero failures or cancellations lost.

## Corrections (Task 5)

None. The measurements justify retaining the current design; no narrowly
evidence-justified provider correction emerged, so `src/Legate.Storage.Sqlite/**`
is untouched by this change. A larger redesign (parallel writers, async I/O
restructuring) would be separate work for approval, not part of this issue.

## Final re-run status (Task 6)

Baseline characterization above ran against `origin/main` `6a245fd` without
waiting for the broader batch, as permitted. The final integrated re-run
after the sibling responsiveness corrections (#390, #391) is gated on those
issues landing; both were still open when this change was prepared, so no
post-sibling comparison is recorded here and no incompatible-baseline
comparison is made. Re-run the `SqliteContentionEnvelope` suite and append
the tested revisions once the siblings land.
