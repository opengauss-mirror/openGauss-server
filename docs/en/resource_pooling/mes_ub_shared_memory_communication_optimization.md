# MES SHM Communication Optimization

## Availability<a name="section15406143204715"></a>

This feature has been available since the introduction of the openGauss resource pool architecture and applies exclusively to this architecture. All cluster nodes must support UnifiedBus SuperPoD (UB) capabilities, the UBS Memory service, and the `ubs-atomic` open-source library runtime environment.

## Feature Overview<a name="section740615433477"></a>

openGauss introduces a new SHM (Shared Memory) transmission mode in the MES (Message Exchange Service) communication layer, serving as a third inter-node communication method alongside TCP and RDMA/UBC. The SHM mode leverages the Lingqu HyperNode UBS Memory service and `ub_dist_comm_queue` (provided by the `ubs-atomic` library) to establish shared memory ring buffers between nodes, enabling zero-copy message transmission. Interconnect types, queue CPU affinity, and transmission performance statistics are fully configurable via GUC parameters.

## Customer Value<a name="section13406743164715"></a>

- This feature provides a UB shared memory-based underlying transport channel for MES inter-node communication in resource pool scenarios, supporting low-latency cross-node message interactions in small-packet, high-frequency scenarios.
- This feature eliminates protocol stack context switching and multiple memory copies. Compared to URMA, SHM offers lower protocol overhead and superior latency in small-packet, high-frequency scenarios, significantly reducing inter-node message transmission latency and increasing throughput.
- SHM operates mutually exclusively with existing TCP and RDMA/UBC modes, preserving existing transport behaviors and MES message protocols. It provides per-queue performance statistics and CPU affinity capabilities, facilitating performance tuning and troubleshooting.

## Feature Description<a name="section16406154310471"></a>

- This feature implements the SHM transmission mode within the CBB MES module: Each node creates shared memory regions and nine `ub_queue` objects (priorities 0–7 and the `prio6-mirror` queue) via the UBS Memory API. It maps the shared memory of peer nodes and handles message transmission, reception, connection management, and disconnection cleanup using `ub_dist_comm_queue`.
- Introduces the `DMS_CONN_MODE_SHM` connection mode and `DMS_CS_TYPE_SHM` channel type in the DMS layer, propagating SHM queue CPU affinity configurations from openGauss-server to MES.
- This feature extends `ss_interconnect_type` in openGauss-server to support the `SHM` value. Adds the `ss_shm_ub_comm_cpu_bind` parameter (CPU affinity for nine queues, format `[id,id,...]` or `[lo~hi]`) and the `ss_mes_elapsed_switch` parameter (MES elapsed time statistics switch). These GUC parameters are `PGC_POSTMASTER` level and require a restart to take effect.
- In SHM mode, message reception is driven by internal callback threads within `ub_dist_comm_queue`; no independent MES receiver thread is launched. Priority 6 messages rotate among the `prio6`, `prio7`, and `prio6-mirror` queues to alleviate pressure on high-priority queues.
- SHM supports per `ub_queue` (0–8) statistics, including send latency (avg/max/min), send count, and receive count. When `ss_mes_elapsed_switch` is enabled, statistics are output to the run logs approximately every 10 seconds (this switch is off by default).

## Enhancements<a name="section1340684315478"></a>

- SHM transmission is added to the MES communication layer as an alternative to TCP and RDMA/UBC.
- This feature implements zero-copy inter-node message transmission based on UB shared memory.
- This feature supports nine independent shared memory objects for `ub_queue` queues and `ub_comm_queue` handles, along with per-queue CPU affinity binding.
- This feature introduces SHM transmission performance statistics and reliability mechanisms such as shared memory mapping retries and disconnection unmapping.

## Constraints<a name="section06531946143616"></a>

- This feature is available only when `ss_interconnect_type` is set to `SHM`. It is mutually exclusive with TCP and RDMA/UBC modes and does not affect other modes.
- This feature depends on the UBS Memory service and the `ubs-atomic` dynamic library, loaded dynamically via `dlopen` at runtime.
- Configurations for `ss_shm_ub_comm_cpu_bind`, `ss_mes_elapsed_switch`, and interconnect types are static (`PGC_POSTMASTER`) and do not support dynamic reload; a cluster node restart is required after modification.
- Shared memory mapping retries a maximum of 60 times (1-second interval) to tolerate node startup timing differences. Corresponding pipes cannot be used for transmission until the peer's shared memory is ready.

## Dependencies<a name="section8406643144716"></a>

- This feature requires the openGauss resource pool architecture and MES inter-node communication capabilities.
- This feature requires UnifiedBus SuperPoD hardware, the UBS Memory service, and the `ubs-atomic` dynamic library.
- This feature involves coordinated modifications across the CBB, DMS, and openGauss-server repositories; no standalone deliverables are provided.

## Basic Principles<a name="section8406643144716"></a>

- **Shared Memory Management Layer**: Creates named regions (`region_mes_<user>_<index>`) and per-queue shared memory objects (`shm_mes_<user>_<index>_<queue_idx>`) on each node using the UBS Memory API. The segment is divided into a Boot area and a Ring area (128 MiB aligned). It maps local and peer node shared memory into the local address space.
- **Communication Queue Layer**: Initializes ring buffers for each priority queue using `ub_dist_comm_queue` and registers MES command callbacks. During transmission, `ub_comm_queue_send` writes messages to the target node's Ring Buffer. Reception is driven by internal queue callback threads and delivered to the MES message processing flow.

## Usage Guide<a name="section8406643144716"></a>

- Configure the interconnect type to SHM: Set the GUC parameter `ss_interconnect_type = 'SHM'` and restart to apply.
- Optionally configure SHM queue CPU affinity: Set `ss_shm_ub_comm_cpu_bind` using the format `[id,id,...]` or `[lo~hi]`. Nine slots correspond to the nine `ub_queue`s. If unconfigured or parsing fails, `cpu_id = -1` (no affinity).
- Optionally enable transmission performance statistics: Set `ss_mes_elapsed_switch = on` and restart. Run logs will display per-queue send latency (avg/max/min), send counts, and receive counts (output approximately every 10 seconds).
- SHM mode is mutually exclusive with TCP and RDMA/UBC. Ensure the UBS Memory and `ub_dist_comm_queue` runtime environments are deployed and available on all cluster nodes before enabling.

## Application Scenarios<a name="section8406643144716"></a>

- Scenarios involving small-packet, high-frequency MES inter-node communication in resource pool clusters, such as DMS/MES collaborative paths sensitive to cross-node latency and high communication frequency.
- Deployment scenarios utilizing UnifiedBus SuperPoDs where shared memory is preferred over TCP or URMA as the underlying MES interconnect.
