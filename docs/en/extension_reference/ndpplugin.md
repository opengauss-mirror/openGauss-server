# NDPPlugin

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:27:43.331Z pushedAt=2026-09-21T03:29:44.099Z -->

## Overview

openGauss provides the NDPPlugin Extension (version ndpplugin-1.0.0). The NDPPlugin Extension is an operator offloading extension for openGauss in resource pooling scenarios. Although shared storage offers benefits such as elasticity and reliability, performance degrades significantly compared with a standalone local disk deployment. This degradation is primarily caused by network I/O and the inherent latency of distributed storage. In particular, for large-scale queries where the buffer pool cannot cache the data, a large amount of data must be transferred from storage nodes to compute nodes. After filtering, the effective data content in most scenarios accounts for a very small proportion of the bulk data, resulting in a significant waste of network I/O time and poor performance. By offloading data filtering to the storage side through operator offloading, unnecessary data is eliminated, thereby reducing the volume of network communication data and improving end-to-end performance.

## Installation

The NDPPlugin extension is compiled and installed by default in openGauss version 5.1.0. The usage steps are as follows:

1. Obtain LibSmartScan_5.1.0_openEuler_aarch64.tar.gz and decompress it.

```
tar -zxvf LibSmartScan_5.1.0_openEuler_aarch64.tar.gz
```

2. Add the following environment variables:

```
export LD_LIBRARY_PATH=/path/to/LibSmartScan_5.1.0_openEuler_aarch64/LibSmartScan_ThirdParty/ceph/openEuler_2003_armlib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/path/to/LibSmartScan_5.1.0_openEuler_aarch64/LibSmartScan_ThirdParty/rpc/openEuler_2003_armlib:$LD_LIBRARY_PATH
```

3. Add the following GUC parameters in postgresql.conf:

```
shared_preload_libraries = 'ndpplugin'
synchronize_seqscans = off
```

4. Start the libsmartscan service. See **[libsmartscan installation](libsmartscan.md)**.

5. Create a database and connect to it to begin using.

```
openGauss=# create extension ndpplugin;
CREATE EXTENSION
```

## Limitations

- Currently, the plugin can only be loaded through shared_preload_libraries.
- TOAST table scenarios are not supported.
- Ustore scenarios are not supported.
- synchronize_seqscans is not supported.

## System Views

The pushdown_statics view displays basic statistics of pushdown queries.

|Name|Type|Description|
| ------------ | ------------ | ------------ |
|query|unsigned long|Number of pushdown queries|
|total_pushdown_page|unsigned long|Number of pushdown pages|
|back_to_gauss|unsigned long|Number of pages returned to native processing|
|received_scan|unsigned long|Number of pages after data filtering by the received scan operator|
|received_agg|unsigned long|Number of pages after data aggregation by the received agg operator|
|failed_backend_handle|unsigned long|Number of pages failed in libsmartscan processing on the storage side|
|failed_sendback|unsigned long|Number of pages failed to be sent back|

## Viewing Views

The NDPPlugin views are used to view detailed statistical information about query statement pushdown, helping users determine the pushdown status of statements.

```
openGauss=# select * from pushdown_statics();
 query | total_pushdown_page | back_to_gauss | received_scan | received_agg | failed_backend_handle | failed_sendback 
-------+---------------------+---------------+---------------+--------------+-----------------------+-----------------
     0 |                   0 |             0 |             0 |            0 |                     0 |               0
(1 row)
```

## GUC Parameter Description

### ndpplugin.enable_ndp

**Parameter Description**: The parameter value is of Boolean type. This parameter is used to enable the plugin.

**Value Range**: Boolean type

- on indicates that the operator offloading feature is enabled.
- off indicates that the operator offloading feature is disabled.

**Default Value**: off

### ndpplugin.pushdown_min_blocks

**Parameter Description**: The parameter value is an integer. This parameter limits the pushdown page count threshold. Tables whose number of pages is less than the threshold will not go through the pushdown process even if they meet the pushdown conditions.

**Value Range**: [0, INT_MAX / 1000]

**Default Value**: 0

### ndpplugin.ndp_port

**Parameter Description**: The parameter value is an integer. This parameter specifies the port number on which the storage cluster libsmartscan process listens, and is used for communicating with the libsmartscan process and sending tasks.

**Value Range**: string

**Default Value**: ./

### ndpplugin.crl_path

**Parameter Description**: A string parameter. This parameter is valid only when SSL is enabled, and specifies the path to the CRL certificate.

**Value Range**: String

**Default Value**: ./