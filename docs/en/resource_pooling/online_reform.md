# Online Reform

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 6.0.0-RC1 and applies exclusively to the resource pool architecture.

## Feature Overview<a name="section740615433477"></a>

**Reform** refers to the process by which a cluster transitions from an unstable state to a stable state following topology changes, such as new nodes joining or nodes restarting. 
Single changes within a cluster are categorized into the following scenarios:

```
1. New primary node joins the cluster (occurs only during the initial cluster build).
2. New standby node joins the cluster.
3. Primary node fails and restarts.
4. Standby node fails and restarts.
5. Standby node is evicted from the cluster.
6. Failover (primary node is evicted).
7. Switchover.
8. New node fails to join repeatedly and is eventually evicted (a composite of scenarios 2 and 5, listed separately due to special handling requirements).
```

These correspond to four types of reform:

```
Normal Reform:    The holder of the reformer lock does not change. Covers scenarios 1, 2, 3, 4, and 5.
Failover Reform:  The reformer lock ownership transfers to another node. Corresponds to scenario 6.
Switchover Reform: Corresponds to scenario 7.
Full Clean Reform: Corresponds to scenario 8.
```

The **reformer lock** is provided by the Cluster Manager (CM). Database nodes compete for this lock; the node that acquires it is designated the **reformer**, while others are **partners**. The reformer periodically obtains cluster status from CM to detect changes and orchestrates the reform process to restore cluster health.

- Complex scenarios (e.g., primary eviction combined with a new standby join) are handled as **Failover Reform** if the lock holder changes.
- If the lock holder remains unchanged (e.g., standby restart combined with another standby eviction), it is handled as **Normal Reform**.
- Switchover occurs only in stable clusters; overlapping failures during switchover trigger other reform types.
- If failures overlap during Full Clean Reform, the system handles it as Failover or Normal Reform based on whether the lock holder changed.

Prior to version 6.0.0-RC1, all reform scenarios required terminating all user sessions across the cluster. To mitigate this service interruption, **Online Reform** was introduced.

**Online** implies that during a reform, business operations on non-faulty nodes initiated before the reform are not interrupted and can continue running after the reform completes.

Consequently, Online Reform is not a distinct reform type but an **attribute** of a reform. It indicates whether a specific cluster change supports uninterrupted operation.

Scenarios involving new standby nodes joining, standby restarts, standby evictions, or repeated join failures (standby-side scenarios) are conceptually similar. Currently, Online Reform supports these standby-side scenarios and primary node restarts. Failover and switchover do not support Online Reform. Since there are no active business connections before the initial primary node joins, that scenario is not applicable.

Scenario support

| ID | Scenario | Online Reform Supported |
| :--- | :--- | :--- |
| 1 | New primary node joins | N/A |
| 2 | New standby node joins | Yes |
| 3 | Primary node restart | Yes |
| 4 | Standby node restart | Yes |
| 5 | Standby node eviction | Yes |
| 6 | Failover | No |
| 7 | Switchover | No |
| 8 | New node fails to join and is evicted | Yes |

Reform type support

| ID | Reform Type | Online Reform Supported |
| :--- | :--- | :--- |
| 1 | Normal Reform | Yes |
| 2 | Failover Reform | No |
| 3 | Switchover Reform | No |
| 4 | Full Clean Reform | Yes |

## Customer Value<a name="section13406743164715"></a>

In the resource pool architecture, when cluster changes occur in standby-side scenarios or during primary restarts, business operations on healthy nodes remain uninterrupted. This significantly improves cluster availability and ensures continuous service delivery to users.

## Feature Description<a name="section16406154310471"></a>

Under the resource pool architecture, during specific reform scenarios (standby-side events and primary restarts), existing business transactions are not interrupted by the reform process. Once the reform completes, these transactions can continue execution.

## Enhancements<a name="section1340684315478"></a>

This feature enhances the existing Reform capability within the resource pool architecture.

## Constraints<a name="section06531946143616"></a>

- In the resource pool architecture, Online Reform is **enabled by default**; there is no configuration parameter to disable it.
- In single-cluster deployments, Failover and Switchover scenarios do not support Online Reform; all other scenarios do.
- During supported Online Reform scenarios, existing sessions are preserved but may be paused until the page access phase completes.
- If the **Standby Transaction Transparent Write Forwarding** feature is enabled, primary restart scenarios no longer support Online Reform.
- In dual-cluster deployments, the standby cluster does not support Online Reform in any scenario. Support on the primary cluster follows the same rules as single-cluster deployments.

## Dependencies<a name="section8406643144716"></a>

This feature depends on the **Reform** feature.
