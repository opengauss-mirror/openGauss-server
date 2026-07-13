# gs_collector适配资源池化

## 可获得性

本特性自openGauss 6.0.0-RC1版本开始引入。

## 特性简介

gs_collector支持收集资源池化架构下：

- DMS、DSS日志，DSS配置文件。
- xlog、pg_control、复制槽。
- 磁阵LUN/注册信息、磁盘信息。

## 客户价值

使用gs_collector，可收集资源池化架构下DMS、DSS、磁阵信息，便于故障定位。

## 特性描述

gs_collector支持收集资源池化架构下：

- DMS、DSS日志及DSS配置文件：支持收集RUN日志、DEBUG日志、操作日志、黑匣子日志，支持收集dss实例及其卷配置信息、cm配置信息。
- xlog、pg_control、复制槽: 资源池化架构下，支持收集共享磁阵中xlog、pg_control、复制槽。
- 磁阵LUN/注册信息、磁盘信息。

## 特性增强

本特性是对gs_collector收集资源池化架构下DSS、DMS及磁阵信息进行的增强。

## 特性约束

无。

## 依赖关系

本特性依赖资源池化磁阵设备。

## 基本原理

gs_collector借助openGauss内置工具以及操作系统工具，实现对数据库各项信息的收集。

## 使用指导

具体参考[gs_collector](../tool_and_commandreference/gs_collector.md)。
