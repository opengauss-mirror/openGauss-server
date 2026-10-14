# CM Supports Switchover for the Standby Cluster in Dual-Cluster Deployments

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 6.0.0.

## Feature Overview<a name="section740615433477"></a>

This enhancement extends the Cluster Manager (CM) capabilities to support switchover operations for the primary standby node within a resource pool deployment's standby cluster in a dual-cluster architecture.

## Customer Value<a name="section13406743164715"></a>

This feature enhances the manageability of standby clusters within resource pool deployment dual-cluster environments.

## Feature Description<a name="section16406154310471"></a>

CM supports the switchover between the primary standby and secondary standby nodes in the standby cluster of a dual-cluster setup. This ensures that after the switchover, the cluster establishes proper connections with the primary cluster and maintains normal log synchronization status.

## Feature Enhancements<a name="section1340684315478"></a>

None.

## Constraints<a name="section06531946143616"></a>

- The original primary standby node and the node to be promoted must be in a normal operational state.
- Avoid performing switchover operations during periods of heavy business traffic.
- The disaster recovery relationship between the primary and standby clusters must be normal, with no network or other abnormalities present.

## Dependencies<a name="section8406643144716"></a>

None.

## Basic Principles<a name="section8406461564716"></a>

The CM client tool `cm_ctl` sends a switchover message to the `cm_server`. Upon receiving the message, `cm_server` dispatches arbitration commands to the `cm_agent` on the target node. Subsequently, `cm_agent` executes `gs_ctl switchover` to promote the target node to the primary standby role within the standby cluster.

During the timeout period, `cm_ctl` continuously polls `cm_server` for node status updates to determine the success of the operation and reports the result to the frontend. During the switchover execution, `cm_server` temporarily suspends certain cluster arbitration tasks to prevent interference that could lead to failure.

Upon successful completion, the new primary standby node actively establishes a connection with the primary cluster, and the secondary standby node connects to the new primary standby node. This ensures normal disaster recovery status between the primary and standby clusters, as well as proper connectivity between the primary and secondary standbys within the standby cluster.

## Usage Guide<a name="section8445461564354"></a>

The command syntax aligns with single-cluster switchover operations. For reference, see [cm_ctl Tool Reference](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/cm_ctl.md).

## Usage Scenarios<a name="section8445461564132"></a>

Direct operations on the primary node or the primary standby node of the standby cluster in production environments carry inherent risks, potentially leading to cluster unavailability under severe circumstances. This feature facilitates safer management practices.

Typical scenarios include infrastructure changes such as replacing a node's IP address or hostname. To minimize impact on cluster services, administrators should use the `switchover` command to demote the primary or primary standby node to a standard standby role before performing maintenance operations.