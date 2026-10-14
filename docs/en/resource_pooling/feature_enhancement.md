# DSS Feature Enhancements

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 5.1.0 and applies exclusively to the resource pool architecture.

## Feature Overview<a name="section740615433477"></a>

- DSS supports thread pools, enabling configurable DSS access capabilities and resource consumption.
- DSS supports generating black box logs when the DSS server crashes.
- DSS supports NoF/NoF+ APIs.

## Customer Value<a name="section13406743164715"></a>

- By supporting thread pools, DSS enables on-demand usage, configurability, and scalability of server CPU and memory resources.
- Black box logging supplements fault localization methods when DSS server failures occur, enhancing DFX capabilities.
- Support for NoF/NoF+ APIs expands the types and forms of underlying storage supported by DSS.

## Feature Description<a name="section16406154310471"></a>

- The enhancement supports on-demand configuration of concurrent DB operations accessing DSS. The thread pool controls DSS resource consumption (such as memory and CPU), improving memory and CPU utilization rates. Configuration parameters are static and do not support dynamic updates.
- When the DSS server process crashes, stack information from the fault site can be analyzed to determine the process context at the time of failure, facilitating fault localization. The black box log feature dumps heap, stack, and register information for processes and threads during a system crash, aiding developers in fault localization and issue tracing. This feature is enabled by default and can be disabled by setting `_LOG_LEVEL=0`, which takes effect after restarting the node service.
- DSS supports NoF/NoF+ APIs, allowing clusters to connect to centralized enterprise storage that supports the NoF protocol via NoF/NoF+ networking, thereby improving cluster I/O performance.

## Enhancements<a name="section1340684315478"></a>

Enhancements have been made to DSS configurability, DFX capabilities, and supported underlying storage protocols.

## Constraints<a name="section06531946143616"></a>

None.

## Dependencies<a name="section8406643144716"></a>

This feature depends on the resource pool architecture.

## Basic Principles<a name="section8406643144716"></a>

- The DSS thread pool utilizes `epoll` to implement asynchronous I/O between the server and clients, thereby enhancing DSS concurrent message processing capabilities.
- The DSS black box logging capability is triggered based on signals; different signals determine the process information collection scenario, thereby improving DSS fault localization capabilities.

## Usage Guide<a name="section8406643144716"></a>

- The message processing capacity of the DSS thread pool can be controlled by adjusting the DSS server parameters `IO_THREADS` and `WORK_THREADS`.
- The DSS black box log is enabled by default. To disable it, configure the DSS server parameter `_LOG_LEVEL=0`. This change takes effect after the DSS server restarts.
For configuration parameter details, refer to [dssserver](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/dssserver.md).

## Application Scenarios<a name="section8406643144716"></a>

- DSS thread pooling is primarily applied to message communication scenarios between DSS clients and the DSS server.
- DSS black box logging is primarily applied to fault localization scenarios involving abnormal termination of the DSS server process.
