# Parallel Replay

## 1. Feature Overview

Parallel replay is used to optimize the page reconstruction performance of Pageserver in data branch scenarios. Its goal is to reduce the queueing time when multiple pages trigger WAL redo simultaneously.

In a storage-compute separation architecture, when compute reads a data page at a certain LSN, if Pageserver has only an earlier base image and subsequent WAL records, it needs to reconstruct the target page through page reconstruction. During page reconstruction, some records can be processed directly by Pageserver's built-in logic, while the rest require the openGauss walredo child process to perform replay.

walredo is essentially an openGauss process started in a special mode. It does not provide SQL services externally as a normal database instance. Instead, it only receives page images and WAL records sent by Pageserver, performs WAL replay, and returns the reconstructed pages to Pageserver.

In the original mode, each tenant shard maintains only one walredo child process. When multiple GetPage, compaction, or other page reconstruction requests require openGauss WAL redo at the same time, they are concentrated on the same walredo child process, which easily causes queueing.

This feature expands the walredo of each tenant shard from a single process to a configurable process pool, and controls resource usage through a global extra process quota. Parallel replay does not change WAL semantics or page reconstruction results. It only improves the concurrency of page reconstruction that requires external walredo.

## 2. Applicable Scenarios

Parallel replay is more suitable for scenarios with high-concurrency page reconstruction, such as concurrent read requests that intensively access pages requiring WAL redo, long WAL chains caused by the lack of a nearby image layer, and background compaction and the read path triggering page reconstruction at the same time.

If page reads mostly hit existing page versions in Pageserver, the LFC, or the local cache, or if most WAL records can be processed directly within the Pageserver process, the benefit of parallel replay is not significant.

## 3. Parallel Replay Mechanism

Each tenant shard maintains a walredo process pool, and the pool size is controlled by `wal_redo_concurrency`. Each slot in the process pool corresponds to a walredo child process. Child processes are lazily started on demand and are not all created immediately when Pageserver starts.

When a request requires openGauss WAL redo, Pageserver selects a walredo slot in a round-robin manner. Different requests can be distributed to different child processes, thereby reducing queuing on a single walredo child process.

Slot 0 is the guaranteed process and does not consume the global extra process quota. Slots 1 through N-1 are extra processes, and they must acquire a global extra permit before starting. If the global extra quota is insufficient, the request falls back to slot 0 instead of waiting for the extra quota to be released.

The overall request distribution relationship is as follows:

```text
GetPage/compaction page reconstruction request
        |
        v
walredo manager
        |
        v
Select walredo slot by round-robin
        |
        +-- slot 0: guaranteed walredo process
        |
        +-- slot 1..N-1: extra walredo processes, subject to the global quota limit
```

## 4. Constraints and Limitations

Note the following constraints when using parallel replay:

1. Parallel replay only improves the concurrency of page reconstruction that requires processing by openGauss walredo child processes.
2. A single walredo child process still processes requests in its own order; the process pool improves the horizontal concurrency of multiple independent redo batches.
3. When the global extra quota is insufficient, requests fall back to slot 0. The feature does not become unavailable, but the concurrency benefit decreases.
4. Increasing `wal_redo_concurrency` increases the potential number of processes, which must be evaluated together with CPU, memory, file descriptors, and the number of tenants.

## 5. Parameter Description

The following parameters are configured in `pageserver.toml` and take effect only after the Pageserver is restarted.

| Parameter Name | Description | Default Value | Value Constraint |
| --- | --- | --- | --- |
| `wal_redo_concurrency` | Size of the walredo process pool for a single tenant shard. | `1` | Greater than 0, with a maximum of `8` |
| `wal_redo_global_extra_concurrency` | Total quota of extra walredo processes at the Pageserver level. Slot 0 does not consume this quota. | `wal_redo_concurrency - 1` when not explicitly configured | Maximum of `64` |

Configuration example:

```toml
# Keep at most 4 walredo process slots for a single tenant shard.
wal_redo_concurrency = 4

# All tenant shards share a quota of 16 extra walredo processes.
wal_redo_global_extra_concurrency = 16
```

To keep behavior close to the original single-process mode, use the default value or set it explicitly:

```toml
wal_redo_concurrency = 1
```

## 6. Observation Methods

You can view the walredo process status through the Pageserver tenant status API:

```bash
curl http://127.0.0.1:<pageserver_http_port>/v1/tenant/<tenant_shard_id>
```

The `walredo.processes` field in the returned result lists the walredo child processes that have been started:

```json
{
  "walredo": {
    "last_redo_at": "2026-06-05T02:00:00Z",
    "processes": [
      { "pid": 12345 },
      { "pid": 12346 }
    ],
    "process": { "pid": 12345 }
  }
}
```

Field description:

| Field | Description |
| --- | --- |
| `last_redo_at` | The time when WAL redo last occurred |
| `processes` | The list of walredo child processes that have been started |
| `process` | A field for compatibility with older clients, returning the first process in `processes` |

You can also observe the startup of walredo child processes through Pageserver logs:

```text
launched walredo process ... pid=<pid> pool_idx=<idx>
```

Here, `pool_idx=0` indicates the baseline process, and `pool_idx>0` indicates an extra process.

## 7. Notes

- For a single-tenant stress test or a small number of hot tenants, you can start verification with `wal_redo_concurrency = 4`.
- In multi-tenant scenarios, it is recommended to also set `wal_redo_global_extra_concurrency` to prevent the number of extra walredo processes from becoming uncontrollable.
- If `processes` shows only one process for a long time, it may be due to insufficient concurrency pressure, no external walredo being triggered, or insufficient global extra quota.
- After increasing concurrency, it is recommended to monitor Pageserver CPU, memory, file descriptors, walredo startup count, redo latency, and read request latency.
- This feature does not change data visibility, branch isolation, or WAL replay correctness.
