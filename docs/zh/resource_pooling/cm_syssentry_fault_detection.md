# CM SysSentry故障检测

## 可获得性<a name="section15406143204715"></a>

本特性自openGauss 7.0.0版本开始引入。

## 特性简介<a name="section740615433477"></a>

CM支持对接SysSentry故障事件通道，实现节点故障快速感知。

## 客户价值<a name="section13406743164715"></a>

提升集群对节点异常的感知速度，降低人工介入成本，增强故障处理闭环能力。

## 特性描述<a name="section16406154310471"></a>

CM通过cm_agent接收SysSentry事件，并将关键故障事件上报cm_server，由集群控制面执行后续处置（如节点踢出、仲裁流程触发）。

## 特性增强<a name="section1340684315478"></a>

- openGauss 7.0.0 支持CM SysSentry故障检测能力。

## 特性约束<a name="section06531946143616"></a>

- 满足灵衢总线协议的服务器。
- 依赖操作系统侧SysSentry能力。
- 需要在cm_agent侧开启对应配置后生效。
- 当前仅覆盖CM定义的关键故障处理场景。

## 基本原理<a name="section8406643144716"></a>

- cm_agent订阅故障事件并解析节点信息，通过SysSentry快速感知节点级故障。
- 事件上报至cm_server后，CM按集群状态执行对应处置策略。

## 使用指导<a name="section8406643144716"></a>

- 启动SysSentry服务并开启目标故障事件检测。
- 开启cm_agent事件检测能力并正确配置节点映射。
- 重启cm_agent使配置生效。

## 使用场景<a name="section8406643144716"></a>

- 发生panic、reboot等节点级故障时，需快速触发CM仲裁。
