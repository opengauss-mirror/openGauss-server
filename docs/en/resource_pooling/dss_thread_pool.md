# DSS Thread Pool

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 5.1.0.

## Feature Overview<a name="section740615433477"></a>

openGauss provides DSS thread pool capabilities, supporting configurable DSS access capabilities and resource consumption.

## Customer Value<a name="section13406743164715"></a>

This feature enables on-demand utilization of device resources.

## Feature Description<a name="section16406154310471"></a>

This feature supports on-demand configuration of concurrent DB operations accessing DSS. The thread pool controls DSS resource consumption (such as memory and CPU), improving memory and CPU utilization rates.

## Enhancements<a name="section1340684315478"></a>

- openGauss 5.1.0 introduces support for the DSS thread pool.

## Constraints<a name="section06531946143616"></a>

Configuration is static and does not support dynamic updates.

## Dependencies<a name="section8406643144716"></a>

None.

## Basic Principles<a name="section8406643144716"></a>

- The DSS thread pool utilizes `epoll` to implement asynchronous I/O between the server and clients, thereby enhancing DSS concurrent message processing capabilities.

## Usage Guide<a name="section8406643144716"></a>

- The message processing capacity of the DSS thread pool can be controlled by adjusting the DSS Server parameters `IO_THREADS` and `WORK_THREADS`.

## Application Scenarios<a name="section8406643144716"></a>

- The DSS thread pool is primarily applied to message communication scenarios between DSS clients and the DSS server.
