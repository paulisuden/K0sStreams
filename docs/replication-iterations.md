# Replication: iteration log

What changed in the replication block (C) in each iteration, how it was checked, and what the rest of the team needs to know. The design and the open issues live in [replication.md](replication.md); this file is the history.

Newest iteration first. Add a new section at the top for each one.

---

## Iteration 2 · Phase 3: the leader side (quorum)

| | |
| --- | --- |
| Date | 8 Oct 2026 |
| Branch | `feature/replicacion` |
| Phase | 3 (15–21 Oct), done ahead of schedule |
| Status | Done on the branch, not merged into `main` yet. The 3-process demo waits for D (see below). |

### In short

The leader side exists:
- **Confirmation needs a majority.** A write counts as confirmed only once 2 of 3 brokers have it.
- **Followers are fed by loops.** One loop per follower keeps it up to date, catches it up if it fell behind, and sends heartbeats.
- **A replaced leader stops confirming.** As soon as it learns it was replaced, it confirms nothing more.

`IReplicator` is now `QuorumReplicator`. With no peers configured (a single broker), it still confirms at once, so single-broker use is unchanged.

Also in this iteration, every topic is a single log on the replication side ([DEC-001](decisions.md)), with partition 0 pinned in `SingleLog.cs`.

### What was built

| File | What it does |
| --- | --- |
| `QuorumReplicator.cs` | The `IReplicator`. It manages leadership terms, waits until a quorum has a write, and steps down (`NotLeaderException`) on `EpochChanged`, on a follower's `STALE_EPOCH`, or at shutdown. With no peers configured, it runs in single-broker mode. |
| `TopicReplication.cs` | Per topic and term: each follower's verified match, the high watermark (advanced only for a record of the current epoch), and the pending waits. |
| `PeerReplicator.cs` | One loop per follower and topic. It pushes `Append` batches, steps back on `LOG_MISMATCH`, sends heartbeats when idle, and backs off on failures. |
| `LeaderTerm.cs`, `AsyncSignal.cs` | One period of leadership, stopped as a unit, and the signal that wakes a loop. |
| `ReplicationOptions.cs` | New settings: `HeartbeatInterval` (500 ms), `RetryBackoff` (100 ms) and `MaxRetryBackoff` (2 s). |
| `GrpcReplicationPeer.cs` | Reconnect backoff capped at 5 s. gRPC's default of 2 minutes would keep a restarted follower unreachable. |
| `ServiceCollectionExtensions.cs` | Registers `QuorumReplicator` as `IReplicator`, with the peers from `PeerDirectory`. |

Tests: 80 in total, 25 of them new.

| Test class | Tests | Covers |
| --- | --- | --- |
| `QuorumReplicatorTests` | 21 | Quorum, followers down, slow or restarted, divergent tails, lost responses, 200 concurrent writes, the HW epoch rule, stepping down, cancellation, disposal, single-broker mode. |
| `GrpcQuorumTests` | 1 | Three brokers over real gRPC: a follower stops, writes continue, it restarts on the same port and catches up. |
| `RegistrationTests` | +3 | The registered `IReplicator`, and validation of the new settings. |

New helpers: `FaultyPeer` (a link that can be disconnected, paused or lose a response), `ReplicationCluster` (a leader and two followers in one process, on fake time) and `Eventually`. `TestNode` and `ReplicationTestServer` can now act as the leader and take settings.

### How it was checked

- **Build:** `dotnet build` is clean.
- **Tests:** every suite passes (Contracts 32, Storage 13, Broker 7, Replication 80), and the replication suite passed 5 more runs in a row.
- **Phase 3 done criterion:**
  - in process: `A_write_is_confirmed_with_one_follower_down` and `A_follower_restarted_empty_catches_up_and_counts_again`;
  - over real gRPC: `Over_grpc_a_follower_down_does_not_block_writes_and_catches_up_after_restart`.
- **Breaking two safety rules on purpose:**
  - without the epoch rule, `The_high_watermark_waits_for_a_record_of_the_current_epoch` fails;
  - with a step-back that stalls, `A_divergent_follower_tail_is_replaced` times out.
- **Not checked:** the 3-process / docker compose demo, which needs D's fixed leader.

### Decisions taken

The one marked 👥 needs the team to know about it.

1. 👥 **`Microsoft.Extensions.TimeProvider.Testing` (10.10.0) added to the shared `Directory.Packages.props`.** contexto.md already planned it for B's tests. Tests get a fake clock: time moves only while a test waits for something, so backoffs and heartbeats need no real waiting.
2. **A follower's progress is what the request verified** (`prev_offset` + records sent), never the end offset the follower reports. Past the batch it may hold records nobody checked.
3. **The high watermark only advances for a record of the leader's own epoch** (Raft's rule). Older records are confirmed together with the next current-epoch record.
4. **Steady state is push only (`Append`).** `Fetch` stays for phase 4's promotion step.
5. **One request in flight per follower.** A dead or slow follower never blocks the other.
6. **No peers configured means single-broker mode**, with a warning in the log.

### What the rest of the team should know

Shared files: `Directory.Packages.props` gets one test package. `Contracts` is untouched.

| Who | What |
| --- | --- |
| A, Storage | The leader counts its own copy as soon as `AppendAsync` returns, so `AppendAsync` must only return once the record is on disk (fsync). `SegmentedLog` currently writes without forcing it to disk (`Flush(true)`). With group commit, `EndOffset` must only cover durable records. |
| B, Queue and API | After each append, call `WaitForQuorumAsync(topic, 0, offset, ct)` with a timeout (about 5 s) and answer 503 when it expires. Map `NotLeaderException` to 307 or 503. With both followers down, the wait lasts until your timeout. |
| D, Platform | The 3-broker demo needs a fixed leader, for example `Broker:LeaderId` with `IsLeader = NodeId == LeaderId`. Today every broker is the leader at epoch 1, and two leaders would make the followers diverge. `Replication:Peers` must list the three brokers' gRPC addresses. Raise `EpochChanged` only after `IsLeader` and `CurrentEpoch` are updated. |

### Known limitations, on purpose

- A topic starts replicating on its first write in a term. An idle topic isn't pushed to a restarted follower until it gets a new write.
- There's no `EpochChange` record and no promotion step yet (phase 4, [replication.md §10.1](replication.md#101--failover-as-currently-described-can-lose-confirmed-messages-c-d-a)).
- A divergent tail is stepped back about one offset per round trip.
- With `InMemoryLog`, a restarted follower loses records it had acknowledged. That's inherent to the fake log, and the leader logs a warning.
- A call that times out (`DeadlineExceeded`) still has no test of its own.

### Next: iteration 3 · Phase 4 (22–28 Oct)

- The promotion step for a new leader (§10.1). It needs team agreement on `ILeaderPromotion` and two proto fields.
- An `EpochChange` record at the start of each term.
- Followers refusing `Append`s while they are themselves the leader, once D's real cluster state exists.
- The 3-process / docker compose demo, once D's fixed leader exists.

---

## Iteration 1 · Phase 2: gRPC between brokers

| | |
| --- | --- |
| Date | 6 Oct 2026 |
| Branch | `feature/replicacion` |
| Phase | 2 (8–14 Oct), replication part. The queue part of phase 2 (retries, DLQ, scheduled messages) belongs to B. |
| Status | Done on the branch, not merged into `main` yet. |

### In short

The replication protocol between brokers now works end to end: `Append`, `Fetch` and `GetState` from [replication.proto](../src/K0sStreams.Contracts/Protos/replication.proto), including every check a follower must make before writing anything.

The leader side doesn't exist yet. Nothing sends records to followers on its own, and nothing waits for 2 of 3 copies; that is phase 3. Until then `IReplicator` is still `InstantReplicator`, so a single broker behaves exactly as before.

### What was built

Code in `src/K0sStreams.Replication/`:

| File | What it does |
| --- | --- |
| `AppendHandler.cs` | Follower side of `Append` ([replication.md §5.3](replication.md#53-what-a-follower-does-with-an-append)). It validates the topic, fences older epochs and adopts newer ones, checks the record before the batch, and checks CRC, offsets and epochs. It overwrites a divergent tail, skips records it already has, and updates the follower's high watermark. One append per partition at a time. |
| `ReplicationNode.cs` | This broker's side of the protocol: `Append` (through `AppendHandler`), `Fetch` (batched by record count and by bytes) and `GetState`. |
| `ReplicationGrpcService.cs` | The gRPC endpoint on port 9091. It only forwards calls to `ReplicationNode`. |
| `IReplicationPeer.cs` | "Another broker" as seen from this one. Implemented locally by `ReplicationNode` and remotely by `GrpcReplicationPeer`. |
| `GrpcReplicationPeer.cs` | gRPC client: a deadline on every unary call, and message limits that fit a 16 MiB record. |
| `PeerDirectory.cs` | One long-lived gRPC channel per broker listed in `Replication:Peers`, except this one. |
| `EpochTracker.cs` | This broker's epoch for fencing: the highest of D's epoch and the highest epoch accepted from a leader. |
| `ReplicationOptions.cs` | The `Replication` configuration section (`Peers`, `RpcTimeout`, `MaxBatchRecords`, `MaxBatchBytes`), checked at startup. |
| `PartitionAddress.cs` | Topic and partition validation shared by all three operations. Replaced by `SingleLog.cs` in [DEC-001](decisions.md). |
| `ServiceCollectionExtensions.cs` | `AddK0sReplication` registers all of the above plus gRPC; `MapK0sReplication` maps the service. |

Tests in `tests/K0sStreams.Replication.Tests/`, 54 in total:

| Test class | Tests | Covers |
| --- | --- | --- |
| `AppendHandlerTests` | 27 | The normal append, every follower rejection, divergent tails, repeated appends, high watermark rules, queue events, concurrent appends. |
| `ReplicationNodeTests` | 12 | `Fetch` batching and limits, high watermark in each batch, `GetState`, invalid requests. |
| `GrpcReplicationTests` | 5 | Two brokers over real HTTP/2 on loopback: a record replicated byte-identical, fencing, a 6 MiB record, error statuses, unreachable broker. |
| `EpochTrackerTests` | 5 | Epoch rules, including concurrent updates. |
| `RegistrationTests` | 5 | Configuration binding, peers exclude this broker, out-of-range settings rejected. |

Helpers in `Support/`: `TestNode` (one broker's replication stack over `InMemoryLog`), `ControllableClusterState` (a cluster state whose leader and epoch the test can change), `ReplicationTestServer` (the replication block served by Kestrel on a free port), and `TestRecords` (record and request builders).

### How it was checked

- `dotnet build`: clean, with warnings treated as errors.
- `dotnet test`: the 54 new tests pass. The existing Contracts (32) and Broker (7) tests still pass, so the host still starts with gRPC registered.
- **Phase 2 done criterion** ("replicate a record between two local processes"): two brokers ran as separate processes with `dotnet run`, on ports 9090/9091 and 9190/9191. A small gRPC client, not committed, appended a record to broker-0, fetched it back, and appended it to broker-1. broker-1 ended up with a byte-identical record and the same high watermark. Neither broker logged a warning.
- **Not checked:** the grpcurl commands in [replication.md §8.3](replication.md#83-trying-it-by-hand). grpcurl wasn't available on that machine. They perform the same steps as the client above.

### Decisions taken

The ones marked 👥 need the team to agree.

1. 👥 **Language.** Code, comments and test names in this block are in English, while the rest of the repo is in Spanish. The team should settle on one convention.
2. **The generated protobuf messages are the request and response types.** There are no duplicate C# DTOs. A local node and a remote one share `IReplicationPeer` and fail the same way (`RpcException` with `InvalidArgument`), so phase 3 can put a simulated faulty network between in-process nodes.
3. **Topic names are validated before touching the log**, with the same rules as topic creation. The name becomes a folder on disk, so a request for `../etc` never reaches `ILog`.
4. 👥 **Followers don't check `ITopicCatalog`.** Topics only exist on the broker that created them, so checking would reject everything ([replication.md §10.3](replication.md#103--topics-on-followers-a-b-c-open-topic-1-in-contextomd)).
5. **`Fetch` doesn't lock the partition.** A long catch-up stream must not block appends. Whoever receives the stream verifies continuity, which `Append` already does with its previous-record check.
6. **A conflict with a confirmed record is never hidden.** `ILog` refuses to truncate below the high watermark; the handler logs it as critical and the call fails. If it ever happens, the protocol has a bug.
7. **gRPC isn't restricted to port 9091 in code.** Port 9090 only speaks HTTP/1.1 and gRPC needs HTTP/2, so in practice gRPC is only reachable on 9091.
8. **A leader that receives an `Append` doesn't reject it yet.** `StaticClusterState` makes every broker believe it is the leader, so the check comes with D's real cluster state in phase 4.

### What the rest of the team should know

No shared files changed: `Contracts`, `Program.cs` and `Directory.Packages.props` are untouched.

| Who | What |
| --- | --- |
| A, Storage | Followers write a batch with one `AppendAsync` per record. With the disk log that means one fsync per record, so we need ordered pipelined appends or an `AppendBatchAsync` ([§10.4](replication.md#104--expectations-on-ilog-a)). `EndOffset`, `HighWatermark` and `ReadAsync` get called for partitions that may never have been written (for example by `GetState`); they should return -1 or nothing without creating files. `TruncateAsync` is only called for divergent tails, never below the high watermark. |
| B, Queue and API | Nothing changes yet: `IReplicator` is still `InstantReplicator`. For phase 3, see [§10.5](replication.md#105--expectations-on-b): stamp records with the current epoch, call `WaitForQuorumAsync` with a timeout, and map `NotLeaderException`. |
| D, Platform | For phase 3: `StaticClusterState` needs a configurable fixed leader (for example `Broker:LeaderId`), docker compose has to set `Replication:Peers`, and port 9091 must be reachable between brokers over cleartext HTTP/2 ([§10.2](replication.md#102--who-are-my-peers-c-d)). The failover proposal in [§10.1](replication.md#101--failover-as-currently-described-can-lose-confirmed-messages-c-d-a) is for phase 4. |

### Known limitations, on purpose

- No leader side: nothing pushes records to followers yet, and there are no heartbeats (phase 3).
- The epoch adopted from leaders is kept in memory only. After a restart, a follower relies on D's epoch and on its own log.
- Each record is written to the follower's log separately (see A above).

### Next: iteration 2 · Phase 3 (15–21 Oct)

- `QuorumReplicator : IReplicator`: match offsets and pending waiters per partition, high watermark computation, `NotLeaderException`.
- `PeerReplicator`: one loop per follower, catch-up through `LOG_MISMATCH`, heartbeats (new `HeartbeatInterval` setting).
- Test support: a simulated faulty network over `IReplicationPeer`, and `FakeTimeProvider`. The latter adds a package to `Directory.Packages.props`, which is a shared file, so tell the group first.
- Needs a fixed leader from D, and B calling `WaitForQuorumAsync`.
- Done when a docker compose of 3 brokers keeps accepting writes with one follower stopped, and the follower catches up when restarted.
