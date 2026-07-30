# DSS功能增强

## 可获得性<a name="section15406143204715"></a>

本特性自openGauss 5.1.0版本开始引入，仅适用于资源池化架构。

## 特性简介<a name="section740615433477"></a>

- DSS支持线程池，支持DSS接入能力与资源占用的可配置。
- DSS支持DSS server发生故障崩溃时，生成黑匣子日志的能力。
- DSS支持NoF/NoF+接口。

## 客户价值<a name="section13406743164715"></a>

- DSS通过支持线程池，实现了对服务器CPU和内存资源的按需使用、可配置和可扩展性。
- DSS通过黑匣子日志的能力，补充了当DSS Server故障发生时的可定位手段，对DFX能力进行了增强。
- DSS支持NoF/NoF+接口，扩展了DSS当前支持的底层存储的形态和类型。

## 特性描述<a name="section16406154310471"></a>

- 支持按需配置数据库接入到DSS的并发操作的能力。通过线程池控制DSS对内存，CPU等资源的占用，提高服务器内存，CPU的使用率。配置参数属于静态配置，不支持动态生效。
- DSS Server进程崩溃的时候，通过故障现场的堆栈信息可以分析出故障发生时的进程上下文，方便故障定位。黑匣子日志具有在系统崩溃时，dump出进程和线程的堆、栈、寄存器信息的功能，可用于开发人员的故障定位和问题追溯。黑匣子日志功能默认开启，可通过配置参数_LOG_LEVEL=0关闭, 重启节点服务生效。
- DSS支持NoF/NoF+接口，可通过NoF/NoF+组网对接支持NoF协议的集中式企业存储，提升集群的IO性能。

## 特性增强<a name="section1340684315478"></a>

对DSS的可配置能力、DFX能力以及支持的底层存储协议能力进行了增强。

## 特性约束<a name="section06531946143616"></a>

无

## 依赖关系<a name="section8406643144716"></a>

本特性依赖资源池化架构。

## 基本原理<a name="section8406643144716"></a>

- DSS线程池利用epoll实现服务端和客户端的异步I/O，从而提高DSS并发消息处理能力。
- DSS黑匣子日志能力，基于信号量触发，根据不同信号量决定进程信息收集场景，从而提高DSS问题定位能力。

## 使用指导<a name="section8406643144716"></a>

- DSS线程池化中的消息处理能力可通过调整DSS Server参数IO_THREADS和WORK_THREADS来控制。
- DSS黑匣子日志默认开启，如需关闭，需配置 DSS Server参数_LOG_LEVEL=0，重启 DSS Server后生效。
配置参数细则参见[dssserver](https://docs.opengauss.org/zh/docs/latest/tool_and_commandreference/dssserver.html)

## 使用场景<a name="section8406643144716"></a>

- DSS线程池化主要运用于DSS客户端和服务端的消息通讯场景。
- DSS黑匣子日志主要运用于DSS Server进程异常退出的问题定位场景。
