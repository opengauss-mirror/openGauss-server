# DSS线程池

## 可获得性<a name="section15406143204715"></a>

本特性自openGauss 5.1.0版本开始引入。

## 特性简介<a name="section740615433477"></a>

openGauss提供DSS线程池能力。支持DSS接入能力与资源占用的可配置。

## 客户价值<a name="section13406743164715"></a>

实现了设备资源的按需使用。

## 特性描述<a name="section16406154310471"></a>

支持按需配置DB接入到DSS的并发操作的能力。通过线程池控制DSS对内存，CPU等资源的占用，提高内存，CPU的使用率。

## 特性增强<a name="section1340684315478"></a>

- openGauss 5.1.0 支持DSS线程池。

## 特性约束<a name="section06531946143616"></a>

静态配置，不支持动态生效。

## 依赖关系<a name="section8406643144716"></a>

暂无。

## 基本原理<a name="section8406643144716"></a>

- DSS线程池利用epoll实现服务端和客户端的异步I/O，从而提高DSS并发消息处理能力。

## 使用指导<a name="section8406643144716"></a>

- DSS线程池化中的消息处理能力可通过调整DSS Server参数IO_THREADS和WORK_THREADS来控制。

## 使用场景<a name="section8406643144716"></a>

- DSS线程池化主要运用于DSS客户端和服务端的消息通讯场景。
