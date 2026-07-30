# DMS资源统计函数

## 可获得性<a name="section15406143204715"></a>

本特性自openGauss 6.0.0-RC1版本开始引入。

## 特性简介<a name="section740615433477"></a>

DMS资源统计函数目前收集如下信息：

- DMS中reform流程相关的状态信息。
- DMS资源池中所有的页面信息和锁信息。
- DMS相关命令字的等待时延等信息。

## 客户价值<a name="section13406743164715"></a>

通过查找DMS资源统计函数，可收集资源池化架构下DMS运行时的资源信息，便于故障定位。

## 特性描述<a name="section16406154310471"></a>

DMS资源统计函数：

- reform流程的状态信息：通过query_node_reform_info_from_dms函数查找。具体函数说明请参见[资源池化函数](https://docs.opengauss.org/zh/docs/latest/sql_reference/resource_pooling_function.html)。
- 页面信息和锁信息：通过query_all_drc_info函数查找。具体函数说明请参见[资源池化函数](https://docs.opengauss.org/zh/docs/latest/sql_reference/resource_pooling_function.html)。
- 命令字等待信息：通过get_instr_wait_event函数查找，该函数原本用于各种数据库等待事件的统计信息查询，本特性在其中新增了命令字内容。具体函数说明请参见[统计信息函数](https://docs.opengauss.org/zh/docs/latest/sql_reference/statistics_information_functions.html)。

## 特性增强<a name="section1340684315478"></a>

本特性增强了对资源池化架构下DMS中reform流程、页面信息、锁信息、命令字信息的收集。

## 特性约束<a name="section06531946143616"></a>

需要开启资源池化特性，如未开启，查询query_node_reform_info_from_dms和query_all_drc_info会返回相应报错，查询get_instr_wait_event返回内容不包括DMS命令字相关信息。

## 依赖关系<a name="section8406643144716"></a>

无。
