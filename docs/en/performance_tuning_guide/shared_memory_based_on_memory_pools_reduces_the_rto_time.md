# Reducing RTO Time Based on Memory Pool Shared Memory

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:50:17.483Z -->

## Availability

This feature is introduced since openGauss 7.0.0-RC2.

## Feature Description

This feature performs checkpoint operations to a logically non-volatile UBS Memory shared memory on the Lingqu System. Working in coordination with traditional disk-based checkpointing, it reduces the volume of logs that need to be replayed during database recovery, thereby further reducing the RTO time. The following scenarios are supported:

- The scenario where log synchronization between the primary and standby servers accelerates the standby server promotion to primary.
- The scenario where database host restart recovery is accelerated.

## Customer Benefits

Typically, after a host failure, data recovery takes a long time, during which the database is unavailable, severely affecting system availability.

Currently, openGauss has reduced RTO through technologies such as parallel replay and Extreme RTO. Building on these, this feature further reduces data recovery time and improves availability through memory pool shared memory. At 700,000 tpmC, with Extreme RTO enabled, the standby node failover to primary takes less than 6 seconds.

## Feature Description

UBS Memory is a high-level service capability provided on the Lingqu supernode based on the underlying UB Memory capability, enabling memory borrowing and sharing on the UB (Unified Bus, Lingqu Bus) system, and offering upper-layer apps an easy-to-use interface for UB Memory. By encapsulating and integrating the underlying hardware capabilities and OS-layer interfaces, it provides POSIX-like logical operation interfaces. The UBS Memory feature is divided into two categories: memory borrowing and memory sharing. Shared memory can be used by multiple nodes simultaneously, and this feature utilizes the memory sharing function of UBS Memory.

Under normal circumstances, when the database modifies a page, the corresponding page is asynchronously written to shared memory. The shared memory is independent of the database process and is not affected by database failures. When the primary node fails, during the primary node restart or standby node promotion process, the log replay for pages already present in shared memory is skipped. After the database starts providing services, the corresponding pages are fetched from shared memory. If a page that has not yet been fetched from shared memory is accessed, it will be fetched on demand. This feature accelerates the failure recovery process by reducing the amount of logs that need to be replayed during recovery.

The GUC parameter max_smb_memory controls the switch of this feature. The value of max_smb_memory specifies the size of the shared memory. A value of 0 indicates that this feature is not used.

### max_smb_memory

**Parameter description**: Specifies the size of shared memory allocated from the memory pool, used to store pages modified by the database.

This is a POSTMASTER-type parameter. For the setting method, see [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: Integer, from 0 to 13107200, in units of 8 kB.

max_smb_memory must be set to an integer multiple of BLCKSZ. BLCKSZ is currently set to 8 kB, meaning that max_smb_memory must be set to an integer multiple of 8 kB.

**Default value**: 0

**Recommended settings:**

In a primary/standby scenario with extreme RTO enabled, 10 GB is generally sufficient. In a standalone scenario, 50 GB is recommended.

## Feature Enhancement

None.

## Feature Constraints

- It can be used together with parallel replay or extreme RTO, but not with extreme RTO on-demand replay or extreme RTO real-time build.
- This feature does not support deployment in resource pooling mode.
- This feature does not support standby read.
- max_smb_memory currently supports a maximum of 50 GB.
- To use this feature, you must use the smb_mgr tool to request shared memory before starting the database, and also use the smb_mgr tool to release the shared memory after the database is shut down. The specific command is smb_mgr start/stop -D [database path] -L [libubsm_sdk.so path].

## Dependencies

None.