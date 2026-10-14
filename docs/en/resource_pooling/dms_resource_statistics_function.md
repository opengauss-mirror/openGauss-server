# DMS Resource Statistics Functions

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 6.0.0-RC1.

## Feature Overview<a name="section740615433477"></a>

The DMS resource statistics functions currently collect the following information:

- Status information related to the Reform process in DMS.
- Page and lock information for all resources within the DMS resource pool.
- Wait latency information for DMS command words.

## Customer Value<a name="section13406743164715"></a>

By querying DMS resource statistics functions, administrators can collect runtime resource information for DMS under the resource pool architecture, facilitating fault localization.

## Feature Description<a name="section16406154310471"></a>

DMS resource statistics functions include:

- **Reform Process Status Information**: Query using the `query_node_reform_info_from_dms` function. For detailed function descriptions, refer to [Resource Pooling Functions](https://gitcode.com/opengauss/docs/blob/master/docs/en/sql_reference/resource_pooling_function.md).
- **Page and Lock Information**: Query using the `query_all_drc_info` function. For detailed function descriptions, refer to [Resource Pooling Functions](https://gitcode.com/opengauss/docs/blob/master/docs/en/sql_reference/resource_pooling_function.md).
- **Command Word Wait Information**: Query using the `get_instr_wait_event` function. Originally designed for general database wait event statistics, this feature adds DMS command word content. For detailed function descriptions, refer to [Statistics Information Functions](https://gitcode.com/opengauss/docs/blob/master/docs/en/sql_reference/statistics_information_functions.md).

## Enhancements<a name="section1340684315478"></a>

This feature enhances the collection of Reform process status, page information, lock information, and command word information for DMS within the resource pool architecture.

## Constraints<a name="section06531946143616"></a>

The resource pooling feature must be enabled. If disabled: (1) Queries to `query_node_reform_info_from_dms` and `query_all_drc_info` return corresponding errors. (2) Queries to `get_instr_wait_event` do not include DMS command word-related information.

## Dependencies<a name="section8406643144716"></a>

None.
