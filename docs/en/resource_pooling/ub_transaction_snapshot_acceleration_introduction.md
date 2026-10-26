# UB Transaction Snapshot Acceleration for Resource Pooling

## Availability

This feature has been available since **openGauss 7.0.0**.

## Feature Overview

In openGauss resource pooling (single-primary-multiple-standby) scenarios, standby nodes frequently rely on DMS messaging or broadcast mechanisms to fetch transaction metadata—such as **CLOG**, **CSNLOG**, **Snapshot**, and **OldestXmin**—from the primary when performing transaction state checks, visibility determination, and snapshot-related logic. As concurrency and transaction volume increase, high-frequency cross-node TCP communication introduces significant message overhead and places heavy processing pressure on the primary, creating a performance hotspot in transaction state query and visibility judgment paths.

This feature leverages the **UB (Unified Bus) shared memory capability** provided by **Kunpeng SuperPoD (LingQu)**. It establishes shared memory acceleration channels for CLOG, CSNLOG, Snapshot, and OldestXmin within a resource pooling cluster. This optimization shifts high-frequency transaction metadata access from "message fetching" to "shared memory direct read," reducing cross-node access latency, alleviating primary node message-processing pressure, and improving transaction processing efficiency in resource pooling scenarios.

## Customer Value

1.  **Reduced Transaction Access Latency**: Significantly lowers cross-node latency when standbys access transaction state and visibility information, improving access efficiency on hot transaction paths.
2.  **Reduced Primary Load**: Decreases the primary's burden of handling DMS requests and broadcast synchronization, mitigating message load under high concurrency and enhancing multi-node read/write scalability.
3.  **Balanced Performance and Reliability**: Adopts a "shared memory first, DMS fallback" approach without altering existing transaction semantics or exception-handling frameworks, delivering performance gains while guaranteeing functional reliability.

## Feature Description

In resource pooling scenarios, this feature utilizes UB shared memory to establish acceleration channels for four types of transaction metadata transfer between primary and standby nodes:

1.  **CLOG Fetch Acceleration**: The primary synchronously maintains a CLOG shared memory cache when updating transaction states. Standbys prioritize reading this shared memory when querying transaction commit, abort, or in-progress states, falling back to DMS upon failure.
2.  **CSNLOG Fetch Acceleration**: The primary synchronously maintains a CSNLOG shared memory cache when writing transaction CSNs. Standbys prioritize reading CSN information from shared memory for visibility checks, falling back to DMS upon failure.
3.  **Snapshot Fetch Acceleration**: The primary synchronously maintains snapshot slots in shared memory when refreshing the latest MVCC snapshot. Standbys prioritize reading `xmin`, `xmax`, and `csn` from shared memory when acquiring snapshots, falling back to broadcast or DMS upon failure.
4.  **OldestXmin Sync Acceleration**: Standbys periodically write their local `oldest_xmin` to a designated slot in the primary's shared memory. The primary maintains the global `OldestXmin` based on shared memory and local state, falling back to DMS upon failure.
5.  **Reform Scenario Adaptation**: Defines rules for shared memory initialization, mapping, reconstruction, hot cache copy, pre-warming, and invalidation during **Normal Reform**, **Switchover**, and **Failover** scenarios, ensuring controllable behavior during faults and role transitions.

This feature does not alter the disk formats of CLOG or CSNLOG, change transaction visibility semantics, or introduce new user-visible SQL interfaces. The existing DMS path is retained as an exception fallback mechanism for the shared memory path.

Compared to the legacy solution relying on DMS messaging and broadcasting, this feature introduces the following enhancements based on UB shared memory:

1.  **CLOG/CSNLOG Shared Memory as Cross-Node L1 Hot Cache**: Local SLRU caches remain as node-local L2 caches. When the primary commits a transaction or advances its state, it updates the SLRU pages via the existing flow and then synchronizes to UB shared memory, preserving the original transaction state persistence order and recovery semantics.
2.  **Snapshot Double-Read Validation**: Snapshot partitions employ a "version + xmin/xmax/csn + version" double-read validation scheme. Leveraging LingQu's atomic 128-bit read/write capability prevents reading partially written states and works in coordination with broadcast snapshots to ensure standbys always acquire the latest valid snapshot.
3.  **Hot Cache Migration**: During **Switchover** and **Online Failover**, the new primary can copy the old primary's CLOG/CSNLOG hot cache into its own shared memory, reducing cache cold-start costs after a switchover.
4.  **SLRU Pre-warming**: During **Startup Reform**, an SLRU pre-warming mechanism refills the CLOG/CSNLOG shared memory in the tail phase of `StartupXLOG`. The pre-warming window size is controlled by the `ss_init_clog_size` parameter.
5.  **Automatic Shared Memory Lifecycle Management**: All nodes initialize their local shared memory during startup. During normal operation, the primary's shared memory provides acceleration, while standby shared memory acts as a reserved resource for future role transitions, minimizing the need for manual shared memory cleanup.

## Enhancements

None.

## Constraints

1.  This feature is effective **only** in openGauss **Resource Pooling** architecture. All cluster nodes must be equipped with **Kunpeng SuperPoD (LingQu)** capability, and the UB dynamic library path must be configured in the database parameter file.
2.  This feature must be uniformly enabled across **all nodes** in the cluster. Partial enablement on a subset of nodes is **not supported**.
3.  Due to conflicts between the transaction log memory copy flow and on-demand recovery, this feature **cannot** be enabled simultaneously with **On-Demand Recovery + Real-time Build** (`enable_ondemand_recovery`).
4.  The CLOG partition consumes approximately **8 GB**, the CSNLOG partition **2 GB**, and the OldestXmin and Snapshot partitions are aligned to a minimum granularity of **128 MB** each. The total contiguous shared memory footprint is approximately **11 GB**. Ensure the SuperPoD has sufficient shared memory resources.
5.  The shared memory created by this feature is primarily used for transaction and snapshot cache acceleration. It is **not cleaned up upon exit** to facilitate reuse during the next startup.
6.  Due to permission constraints regarding UB shared memory mapping, the **UID** of the OS user running openGauss must be **identical across all nodes** in the cluster.
7.  The UBS Memory software package depends on specific versions of underlying components. It is **not recommended** to use the bundled `libstdc++.so.6` included in the installation package. Instead, use the dynamic library located at `/usr/lib64/` on the system; otherwise, startup may fail.
8.  Handling of UB memory failure scenarios relies on OS-level reliability features, which are currently under adaptation and development. Therefore, explicit handling for these scenarios is **not yet included** in this feature.
9.  SLRU pre-warming during primary startup increases boot time; higher `ss_init_clog_size` values result in higher pre-warming overhead. During **Online Failover** and **Switchover**, the new primary copies the old primary's shared memory to avoid cold starts, which adds to the RTO duration.

## Dependencies

1.  **Hardware Dependency**: All cluster nodes must support the **Kunpeng SuperPoD (LingQu) UB shared memory** capability.
2.  **Software Dependency**: Depends on the **UBS Memory service** for UB Memory shared domain creation, shared memory allocation, and mapping capabilities. Depends on the **DMS component** to provide the transaction state synchronization fallback mechanism in resource pooling scenarios.
3.  **Parameter Dependency**: The master switch `enable_ub` must be set to `on`. The `ss_init_clog_size` parameter controls the upper limit of the CLOG/CSNLOG pre-warming window during startup.

## Basic Principles

This feature adopts a layered architecture: **"Local persistence path unchanged, UB shared memory prioritized, DMS as fallback."**

**Shared Memory Organization**: Each node creates a fixed-length contiguous shared memory block (named `ub_<inst_id>_<user_id>_txn_cache`), divided into Control Block, CLOG Area, CSNLOG Area, OldestXmin Area, and Snapshot Area. All nodes map the primary's shared memory; remapping occurs to the new primary's memory following a role switch.

**CLOG Cache Structure**: Uses a contiguous shared memory slot array `clog_buf` of length 2^32. Each slot is 16 bits (14-bit timeline + 2-bit clogvalue), totaling 8 GB. Slot index is calculated as `idx = xid % ClogCacheSize`. Timeline `timeline = (xid / ClogCacheSize) % (2^14)` is used for validation. Atomic 16-bit reads/writes prevent partial reads/writes.

**CSNLOG Cache Structure**: Uses a contiguous shared memory slot array `csnlog_buf` of length 2^27. Each slot is 128 bits (high 64-bit timeline + low 64-bit csn), totaling 2 GB. Slot index is `idx = xid % CsnlogCacheSize`. Timeline `timeline = xid / CsnlogCacheSize`. Atomic 128-bit reads/writes prevent partial reads/writes.

**Snapshot Cache Structure**: Employs a single-slot "version + xmin/xmax/csn + version" double-read validation. The primary writes an odd version first, then the data, then an even version. The standby reads successfully only if the leading and trailing versions match and are even.

**OldestXmin Cache Structure**: Organized as a fixed-length array of 128 slots indexed by `inst_id`. Each slot holds a 64-bit `oldest_xmin`; an initial value of 0 indicates invalid.

**Access Path**: Standbys prioritize reading the primary's shared memory for transaction metadata. On hit, data is returned immediately. On miss, validation failure, during reform, or if shared memory is unavailable, the system falls back to the existing DMS path.

**Reform Adaptation**: Shared memory acceleration is paused immediately at the start of a reform, forcing all transaction state access to fall back to DMS. After reform completes, nodes remap the primary's shared memory and resume the fast path. **Switchover/Online Failover** attempts to migrate the old primary's CLOG/CSNLOG hot cache to the new primary. **Startup Reform** refills the cache via SLRU pre-warming.

## Usage Guide

### Parameter Configuration

| Parameter | Type / GUC Level | Default | Value Range | Effective Mode | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **enable_ub** | bool / PGC_POSTMASTER | false | on / off | Restart | Master switch for this feature. When enabled, resource pooling nodes initialize the UB shared domain, transaction shared memory, and four partitions during startup, and enable the "shared memory first, DMS fallback" acceleration logic in transaction access paths. |
| **ss_init_clog_size** | int (kB) / PGC_POSTMASTER | 0 | 0–1GB | Restart | Controls the upper limit of the CLOG/CSNLOG pre-warming window during **Startup Reform**. Larger values increase the hot transaction hit rate post-switchover but extend recovery tail-phase duration. Smaller values result in faster recovery but more noticeable cache warm-up ramp-up. |

### Configuration Strategy

1.  **Deployment & Startup**: To utilize the UB memory acceleration path, set `enable_ub = on`. `ss_init_clog_size` can be adjusted or left at the default 0.
2.  **Cleanup**: If unknown residual shared memory blocks are detected during initial startup, it is recommended to clean them manually using `ubsectl delete memory -t share --name xxx` to prevent accessing incorrect legacy memory regions.
3.  **Library Dependency**: Due to the dependency file restriction mentioned in Constraint 7, after installing the cluster via `gs_install`, locate `libstdc++.so.6` within the openGauss directory. Back it up or remove it using `mv` or `rm` so that `gaussdb` links against the system library automatically.
4.  **SDK Path**: As this feature depends on the UBS Memory dynamic library, add the library path to the system configuration parameters. For example: `ubs_mem_path = '/usr/local/ubs_mem/lib/libubsm_sdk.so'` (adjust the path based on the actual UBS installation location).

### Parameter Interaction Constraints

1.  `enable_ub` is the prerequisite switch. Other parameters only take effect when `enable_ub` is set to `on`.
2.  `ss_init_clog_size` only takes effect during **primary startup** scenarios; it does not trigger pre-warming logic during Normal Reform, Switchover, or Online Failover.
3.  This feature **cannot** be enabled simultaneously with `enable_ondemand_recovery`.

## Use Cases

In **single-primary-multiple-standby resource pooling clusters**, standbys frequently execute read-only queries requiring transaction visibility checks. The CLOG/CSNLOG shared memory direct-read capability significantly reduces cross-node round-trip overhead for individual transaction state queries and accelerates snapshot acquisition on standby nodes.
