# gs_collector Adaptation for Resource Pooling

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 6.0.0-RC1.

## Feature Overview<a name="section740615433477"></a>

gs_collector supports collection of the following items under the resource pool architecture:

- DMS and DSS logs, and DSS configuration files.
- xlog, pg_control, and replication slots.
- Disk array LUNs/registration information and disk information.

## Customer Value<a name="section13406743164715"></a>

By using gs_collector, administrators can collect DMS, DSS, and disk array information under the resource pool architecture, which facilitates fault localization.

## Feature Description<a name="section16406154310471"></a>

gs_collector supports collection of the following items under the resource pool architecture:

- DMS and DSS logs and DSS configuration files: collecting RUN logs, DEBUG logs, operation logs, and black box logs; collecting DSS instance and volume configurations, as well as CM configurations.
- xlog, pg_control, and replication slots: under the resource pool architecture, collecting xlog, pg_control, and replication slots from the shared disk array.
- Disk array LUNs/registration and disk information.

## Enhancements<a name="section1340684315478"></a>

This feature enhances gs_collector's capability to collect DSS, DMS, and disk array information under the resource pool architecture.

## Constraints<a name="section06531946143616"></a>

None.

## Dependencies<a name="section8406643144716"></a>

This feature depends on the resource pool disk array device.

## Basic Principles<a name="section8406643144716"></a>

gs_collector leverages built-in openGauss tools and OS utilities to collect various database information.

## Usage Guide<a name="section8406643144716"></a>

For details, refer to [gs_collector](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/gs_collector.md).
