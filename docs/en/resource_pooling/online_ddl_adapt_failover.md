# openGauss Support for Online Failover

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 7.0.0 and applies exclusively to the resource pool architecture.

## Feature Overview<a name="section740615433477"></a>

This feature targets the primary-write-standby-read scenario in openGauss resource pooling. When a failover is triggered, the legacy standby instances participating in the current failover round must terminate backend processes and certain auxiliary threads on those instances that may block the online reform. These legacy threads may reside on the newly promoted primary node or on nodes that remain as standbys without being promoted. Previously, the following issues existed:

1.  If these threads were in `ReadBuffer_common`, `LockBuffer`, or segment-page metadata page read/lock flows, they might enter DMS-related retry loops upon request failures.

2.  Due to `HOLD_INTERRUPTS()`/`RESUME_INTERRUPTS()` protection during `ReadBuffer_common`, threads could not respond promptly to termination signals, causing `FailoverCleanBackends` to wait excessively long or even time out.

3.  If backend threads carried resources such as page pins, I/O status, locks, or read hints across the reform boundary, they could interfere with page replay and subsequent consistency.

4.  The previous approach used direct error reporting to handle blocked threads. However, due to inconsistent error points and uncontrollable cleanup of resources like pins and I/O states, significant risks remained.

This design optimizes identified, openGauss-side blockable points by changing them from "immediate `ERROR` at the blocking point" to "perform layered cleanup based on resource ownership, then unwind to a safe boundary before raising an `ERROR`."

## Customer Value<a name="section13406743164715"></a>

In online failover scenarios, this feature enables backends and related auxiliary threads to exit promptly, allowing the online reform to proceed. This prevents failover from being blocked or causing the openGauss-server process on the new primary to exit due to timeout. Additionally, it reduces the risk of page resource leaks or state residues caused by raising errors directly at blocking points.

## Feature Description<a name="section16406154310471"></a>

In failover scenarios under the resource pool architecture, backend and related auxiliary threads exit promptly to prevent failover blockage or timeout-induced termination of the openGauss-server process on the new primary.

## Enhancements<a name="section1340684315478"></a>

This feature enhances the online reform capability within the resource pool architecture.

## Constraints<a name="section06531946143616"></a>

1.  This feature targets threads that can block online reform progression, including ordinary backends, thread pool workers, autovacuum workers, statement threads, job workers, job schedulers, and backend-list auxiliary threads that may enter page read paths and affect failover cleanup granularity.

2.  This feature only handles well-analyzed openGauss-side blocking points, including ordinary data/index page read retries, page retries requested by `LockBuffer`, retries after `DmsReleaseOwner` failures during normal page buffer replacement, and metadata page paths such as segment head, target fork head, `seg_nblocks`, and `seg_get_physical_location`.

3.  This feature does not alter DMS/MES internal call semantics and does not handle cases where single calls like `dms_request_page` or `mes_alloc_room` fail to return for extended periods. If such issues occur, the system enters the forced process exit flow.

4.  For unrecognized blocking points or paths where resource ownership cannot be confirmed, the system does not force layered unwinding but retains the original direct error reporting logic. If threads remain unexited and block the online reform, `FailoverCleanBackends` times out, prints residual backend, key auxiliary thread, full thread stack, and LWLock information, and then forces process exit to prevent permanent reform stalls.

## Dependencies<a name="section8406643144716"></a>

This feature depends on the reform feature.