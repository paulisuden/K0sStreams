# Replication: iteration log

What changed in the replication block (C) in each iteration, how it was checked, and what the rest of the team needs to know. The design and the open issues live in [replication.md](replication.md); this file is the history.

Newest iteration first. Add a new section at the top for each one.

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
