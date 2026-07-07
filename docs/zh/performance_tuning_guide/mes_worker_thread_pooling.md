# MES worker线程池化

## 可获得性<a name="section15406143204715"></a>

本特性自openGauss 6.0.0-RC1版本开始引入，仅适用于资源池化架构。

## 特性简介<a name="section740615433477"></a>

在资源池化架构下，MES worker线程提供池化的功能，根据节点间消息压力动态调整worker线程的数量。

## 客户价值<a name="section13406743164715"></a>

在资源池化架构下，动态调整worker线程从而更好利用CPU资源。

## 特性描述<a name="section16406154310471"></a>

在资源池化架构下，MES worker线程提供池化的选项。开启后，worker线程将以线程池的方式进行管理。当消息压力大的时候系统自动增加worker线程，当消息压力小的时候系统自动减少worker线程，从而更加有效利用CPU，提高软件的可用性。

## 特性增强<a name="section1340684315478"></a>

本特性是在资源架构下，对于原有固定的MES worker线程配置方式的一种扩展。

## 特性约束<a name="section06531946143616"></a>

- 在资源池化架构下，ss_work_thread_pool_attr参数表示mes worker线程池化是否开启。具体参数说明请参见[资源池化参数](../database_reference/resource_pooling_parameters.md)。
- 在资源池化架构下，MES提供了保序能力，由同一业务线程发的消息，在开启保序功能后，会由同一个worker线程进行处理，从而做到发消息和处理消息的顺序一致。当前保序功能在worker线程池化开启后，将不可用。当前DMS组件并未使用MES的保序能力，DMS也默认关闭这一能力，用户也无法开启这一能力。所以开启worker线程池化、关闭保序能力对于当前用户没有影响。

## 依赖关系<a name="section8406643144716"></a>

暂无
