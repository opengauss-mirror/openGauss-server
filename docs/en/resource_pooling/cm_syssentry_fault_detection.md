# CM SysSentry Fault Detection

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 7.0.0.

## Feature Overview<a name="section740615433477"></a>

CM supports integration with the SysSentry fault event channel to achieve rapid perception of node failures.

## Customer Value<a name="section13406743164715"></a>

This feature enhances the cluster's ability to perceive node anomalies promptly, reduces manual intervention costs, and strengthens the closed-loop capability for fault handling.

## Feature Description<a name="section16406154310471"></a>

CM receives SysSentry events via `cm_agent` and reports critical fault events to `cm_server`. The cluster control plane then executes subsequent handling procedures, such as node eviction or triggering arbitration workflows.

## Feature Enhancements<a name="section1340684315478"></a>

- openGauss 7.0.0 introduces support for CM SysSentry fault detection capabilities.

## Constraints<a name="section06531946143616"></a>

- Servers must comply with the UnifiedBus protocol.
- The feature requires SysSentry capabilities on the operating system side.
- The feature becomes available only after its configurations are enabled on `cm_agent`.
- Only key fault handling scenarios defined by CM are covered.

## Basic Principles<a name="section8406643144716"></a>

- `cm_agent` subscribes to fault events and parses node information, enabling rapid perception of node-level failures through SysSentry.
- After events are reported to `cm_server`, CM executes corresponding handling policies based on the cluster state.

## Usage Guide<a name="section8406643144716"></a>

- Start the SysSentry service and enable detection for target fault events.
- Enable the `cm_agent` event detection capability and configure node mapping correctly.
- Restart `cm_agent` to apply the configuration.

## Usage Scenarios<a name="section8406643144716"></a>

- CM arbitration must be rapidly triggered when node-level faults occur, such as panics or reboots.