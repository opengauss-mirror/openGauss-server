# Ultimate RTO On-Demand Playback

## Availability

This feature has been available since **openGauss 5.1.0**.

## Feature Overview

- Supports rapid recovery after a primary node restart in resource pooling deployments.
- Accelerates standby failover in resource pooling deployments to reduce RTO.
- Evolves from the existing Ultimate RTO capability.

## Customer Value

In resource pooling deployments, standbys do not replay the primary's logs. When the primary fails, the standby must recover starting from the primary's last checkpoint location. This recovery process can take a long time, rendering the database unavailable and severely impacting system availability.

Enabling Ultimate RTO On-Demand Recovery reduces the database unavailability window after a primary failure, thereby improving availability.

## Feature Description

When the Ultimate RTO On-Demand Recovery switch is enabled, the system constructs only the metadata essential for recovery during the failure recovery phase, without performing actual replay. The database then immediately opens to accept connections, reducing RTO. Background log replay continues after the database is open, and user queries trigger on-demand replay to ensure data consistency (results match the state prior to the failure).

This feature introduces two new states in the `Cluster Status` field of the `control file`: **`in on-demand build`** and **`in on-demand redo`** (see [pg_controldata](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/pg_controldata.md)). When a failover occurs with this feature enabled, the new primary first sets `Cluster Status` to **`in on-demand build`**, entering the on-demand build construction phase. During this phase, the cluster is constructing the metadata required for replay and cannot accept connections. Once construction is complete, the status changes to **`in on-demand redo`**, entering the on-demand replay phase. The database can now accept connections while background log replay continues until completion. As an emergency escape mechanism, if the primary exits before completing either phase, the new primary will fall back to the standard Ultimate RTO recovery mode.

## Enhancements

### 1. On-Demand Recovery with Real-time Build

- **Available since**: **openGauss 6.0.0-RC1**.
- On-demand recovery supports real-time build, which constructs the metadata required for recovery during normal cluster operation, further reducing RTO.

### 2. Partial DDL Support During the Redo Phase

- **Available since**: **openGauss 6.0.0**.
- Supports executing a subset of DDL syntax during the on-demand redo phase (i.e., after failover when the new primary is open but background replay is still ongoing).
- Specifically, this feature supports `CREATE`, `DROP`, and `ALTER` operations for `SCHEMA`, `TABLE`, `INDEX`, `VIEW`, and `PROCEDURE`, as well as `ALTER DATABASE`. Other syntax will result in reasonable errors. Stored procedures are supported provided all contained syntax falls within the supported range.
- Note: Before executing DDL, the system must retrieve and replay all un-replayed logs related to the affected database objects, which may result in longer DDL execution times.

### 3. Real-time Build Flow Control

- This feature is available since openGauss 7.0.0-RC1. It adapts the `recovery_time_target` parameter for resource pooling real-time build scenarios. This feature is effective only on the **primary node** in a resource pooling cluster. When enabled, the standby periodically sends its latest real-time build WAL LSN to the primary. The primary estimates the RTO based on received LSNs over several intervals. If the build speed is projected to miss the RTO target, the primary throttles business traffic on the primary to relieve pressure on the standby and shorten the actual RTO.
- In resource pooling, `recovery_time_target` takes effect only on the primary. To use this feature, set `recovery_time_target > 0` on the primary and `ss_ondemand_realtime_build = on` on the standby. The primary will then begin throttling based on the standby's XLOG build speed.
- This feature is **disabled during Reform**; it dynamically deactivates during Reform and reactivates afterward.
- To avoid extreme blocking scenarios, `recovery_time_target` serves as a reference value rather than a hard guarantee. The primary will throttle traffic if the standby falls behind, but it will not completely block business. Therefore, the actual RTO may exceed the configured `recovery_time_target` value.

## Constraints

- This feature is supported **only** in **Resource Pooling** deployments.
- During the on-demand redo phase (while the database is open but replay is ongoing), only a subset of SQL syntax is supported: `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `SET`, `SHOW`, and the specific DDL types listed in [Enhancements](#enhancements). Query the status using the `pg_controldata` tool.
- **Autovacuum is disabled** during the on-demand redo phase.
- Real-time build capability takes effect **only** during standby failover scenarios.
- Ultimate RTO On-Demand Recovery performance may fluctuate based on disk array environmental conditions.
- This feature consumes significant memory. It is recommended **only for systems with ample memory and strict RTO requirements**. For other systems, the standard Ultimate RTO Recovery is advised (see [Ultimate RTO](https://atomgit.com/opengauss/docs/blob/master/docs/en/characteristic_description/ultimate_rto.md)).
- For guidance on selecting the recovery mode in resource pooling, refer to [Resource Pooling HA System Configuration](resource_pooling_ha_system_configuration.md).

## Dependencies

None.

## Basic Principles

### 1. Ultimate RTO On-Demand Recovery

The core concept splits recovery into a **Build Phase** and a **Redo Phase**.

- **Build Phase**: Occurs during the failover Reform period. The promoted node replays only segment-page logs and transaction logs. Other XLOG types are parsed into page-grained log segments and stored in a hashmap.
- **Redo Phase**: Occurs after Reform completion. The `PageRedoManager` thread distributes log segments to `RedoPageWorker` threads for replay. If a business request requires a specific page, the system retrieves all pending XLOGs for that page from the hashmap and forces an early replay.

### 2. On-Demand Recovery with Real-time Build

This enhancement moves the Build Phase operations (reading XLOG, parsing XLOG, building the hashmap) from the Reform period to normal cluster operation. When a failover occurs, the replay thread continues from the existing hashmap progress, significantly shortening the Reform window.

### 3. Partial DDL Support During the Redo Phase

This enhancement handles DDL execution during the Redo Phase. Before executing DDL (e.g., `ALTER TABLE`, `DROP TABLE`), the system identifies the table's `relnode` based on the table name and tablespace. It then retrieves all pending XLOGs matching that `relnode` from the hashmap and replays them. Additionally, it recursively checks for associated relations or partitions, including indexes (global/local), partitioned tables (level 1/2), and TOAST tables (each partition may have its own TOAST). If found, their `relnode` values are collected and all pending XLOGs are retrieved and replayed from the hashmap. After this, the specific DDL operation executes as if in a normal state.

### 4. Real-time Build Flow Control

This enhancement throttles the primary to protect the standby during real-time build.

- When the primary is configured with `recovery_time_target > 0`, it broadcasts a message to standbys.
- Upon receiving the broadcast, the standby periodically reports its XLOG build progress to the primary via DMS.
- The primary calculates the estimated RTO for each node. If `estimated RTO > recovery_time_target / 2`, the primary calculates a `globalSleepTime` based on Gaussian distribution.
- Before transaction commit, the primary sleeps for this duration to slow down traffic generation, easing the burden on the standby.

## Usage Guide

Ultimate RTO On-Demand Recovery and its enhancements are effective only in **Resource Pooling** scenarios:

- **Standard On-Demand Recovery**:
  - Set `ss_enable_ondemand_recovery = on`
  - Set `ss_enable_ondemand_realtime_build = off`
  - Set `recovery_parse_workers > 1`
  - Set `recovery_redo_workers >= 1`
  - Automatically triggered when the primary performs failure recovery with the above parameters.
  - *Recommendation*: Set `ss_ondemand_recovery_mem_size` to **2x** the size of the pending XLOGs to be replayed.

- **On-Demand Recovery with Real-time Build**:
  - Set `ss_enable_ondemand_recovery = on`
  - Set `ss_enable_ondemand_realtime_build = on`
  - Set `recovery_parse_workers > 1`
  - Set `recovery_redo_workers >= 1`
  - Automatically activated on the standby when the cluster state is `Normal`.
  - *Recommendation*: Set `ss_ondemand_recovery_mem_size` to **2x** the size of the pending XLOGs to be replayed.

- **Partial DDL Support**:
  - No parameter control; automatically active during the on-demand redo phase.
  - Execution method is identical to non-redo phases, but expect increased latency. Unsupported syntax results in reasonable errors.

- **Real-time Build Flow Control**:
  - Set `recovery_time_target > 0` on the **Primary**.
  - Enable real-time build on the **Standby** (`ss_ondemand_realtime_build = on`).
  - The primary will estimate RTO based on standby progress and throttle traffic when necessary.

## Application Scenarios

The Resource Pooling On-Demand Recovery feature suite is primarily intended for **resource pooling clusters with strict RTO requirements and sufficient memory resources**. For specific configuration guidance, refer to [Resource Pooling High Availability System Configuration](resource_pooling_ha_system_configuration.md#recovery-mode-selection).
