# Configuring DPA Hash Aggregation Acceleration

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:50:02.060Z -->


The DPA (Data Processing Accelerator) hash aggregation acceleration feature of openGauss is a hardware acceleration capability implemented based on the [UADK (Unified Accelerator Development Kit)](https://docs.openeuler.org/zh/docs/22.03_LTS/docs/UADK/UADK-quick-start.html) framework. It allows offloading vectorized hash aggregation operations to hardware accelerators for execution, significantly improving the execution efficiency of aggregation queries through hardware parallel processing capabilities.



## Applicable Scenarios and Limitations



Environment requirements: An ARM64 (aarch64) hardware platform is required, and the UADK acceleration library (libwd_dae.so) must be installed and configured in the system.



- Applicable Scenarios

    - Large-scale data hash aggregation with vectorized execution: VecHashAgg operations performed on column-store tables or in row-store to vectorized scenarios

    - Simple aggregate functions: Queries using basic aggregate functions such as SUM and COUNT



- Supported aggregation operators:

    - Vector Hash Aggregate



- Hardware limitations:

    - Only ARM64 (aarch64) architecture platforms are supported

    - A hardware accelerator that supports UADK is required



## Supported Data Types and Aggregate Functions



The DPA feature imposes strict restrictions on data types and aggregate functions. When these conditions are not met, execution automatically falls back to the CPU.



### Data Types Supported by GROUP BY Key



| Data Type | Length Limit | Description |

|---------|---------|------|

| INT4 (INTEGER) | None | 32-bit integer |

| INT8 (BIGINT) | None | 64-bit integer |

| CHAR/BPCHAR | ≤ 32 bytes | Fixed-length string |

| VARCHAR | ≤ 30 bytes | Variable-length string |



### Supported Aggregate Functions



| Aggregate Function | Input Type | Description |

|---------|---------|------|

| SUM | INT8 (BIGINT) | Supports summation of BIGINT type only |

| COUNT | INT4, INT8, CHAR, VARCHAR | Supports counting of multiple types |



>[!TIP]Note

>

>- Currently, aggregate operations such as `count(*)` for full-row counting and `sum(numeric)` are not supported.

>- The `MAX` and `MIN` aggregate functions are not supported.



### Column Count Limit



| Limit Item | Maximum Value | Description |

|-------|-------|------|

| GROUP BY key column count | 9 | Up to 9 GROUP BY key columns |

| Aggregate input column count | 9 | Up to 9 input columns for aggregate operations |

| Number of CHAR/VARCHAR key columns | 5 | Up to 5 grouping keys of character type |



## Configuration and Usage



To enable the DPA hash aggregation acceleration feature, the following configurations are required:



1. Install the UADK acceleration library. Refer to the [UADK documentation](https://docs.openeuler.org/zh/docs/22.03_LTS/docs/UADK/UADK-quick-start.html) for installation.



2. Modify the database configuration file to set the UADK dynamic library path (postgresql.conf). This parameter requires restarting openGauss to take effect.



```

# Set the libwd_dae.so dynamic library path. The default value is libwd_dae.so.

uadk_path = 'libwd_dae.so'

```



3. Enable DPA hash aggregation acceleration



```sql

-- Enable at the session level.

SET enable_dpa_hashagg = on;



-- Or set in the configuration file.

# enable_dpa_hashagg = on

```



## Usage Example



The following is a query example suitable for DPA acceleration:



```sql

-- Enable DPA acceleration.

SET enable_dpa_hashagg = on;



-- Aggregation query suitable for DPA acceleration.

SELECT 

    region_id,           -- INT4 type grouping key.

    product_code,        -- Grouping key of VARCHAR(20) type

    SUM(amount),         -- amount must be of INT8 type

    COUNT(order_id)      -- order_id can be INT4/INT8/CHAR/VARCHAR

FROM orders

GROUP BY region_id, product_code;

```



## Usage Restrictions and Precautions



1. **Platform restriction**: The DPA feature is only available on ARM64 architecture platforms and cannot be used on x86 architecture. For details, see [Usage Requirements](https://docs.openeuler.org/zh/docs/22.03_LTS/docs/UADK/UADK-quick-start.html#usage-requirements)



2. **GROUP BY clause required**: DPA does not support full-table aggregation without grouping keys. A GROUP BY clause must be included.



3. **Data type restrictions**: The data types of GROUP BY columns and aggregate columns must be within the supported range; otherwise, execution automatically falls back to the CPU.



4. **String length restrictions**: For GROUP BY grouping keys, the maximum length of CHAR type is 32 bytes, and the maximum length of VARCHAR type is 30 bytes. If the limit is exceeded, execution falls back to the CPU.



5. **Vectorized execution requirement**: DPA takes effect only in the HashAgg operator of the vectorized execution engine. Ensure that the query follows the vectorized execution path.



6. **Automatic fallback mechanism**: When encountering unsupported scenarios, the system automatically falls back to CPU execution and outputs WARNING messages in the log, without causing query failures.



## Performance Impact



When properly configured and the query meets DPA acceleration conditions, the following benefits can be achieved:



1. Leverage the parallel processing capability of the hardware accelerator to improve the execution efficiency of hash aggregation operations.

2. Reduce CPU overhead on aggregation computation, freeing up CPU resources for other operations.

3. For large-scale data aggregation queries, the performance improvement is more significant.



>[!NOTE]

>The DPA acceleration effect depends on the performance of the hardware accelerator and the data scale. For queries with small data volumes, the acceleration effect may be insignificant, or performance may even degrade due to data transfer overhead. It is recommended to use it in large-scale data aggregation scenarios.



## Troubleshooting



When the DPA feature does not work properly, you can diagnose the issue as follows:



1. **Check the platform architecture**: Verify that the runtime environment is ARM64 architecture.



    ```bash

    uname -m

    # The output should be aarch64.

    ```



2. **Check UADK library loading**: Check the database logs to confirm whether the UADK library is loaded successfully.



    ```

    LOG:  UADK AGG library loaded successfully

    ```



3. **Check WARNING logs**: When DPA falls back to CPU execution, relevant WARNING messages are output. Common WARNINGs include:

   - `DPA: Key columns number X exceeds hardware limit 9, fallback to CPU`

   - `DPA: Unsupported key column type OID: X`

   - `DPA: CHAR(X) length exceeds hardware limit 32`

   - `DPA: Key VARCHAR(X) exceeds hardware limit 30 bytes, fallback to CPU`

   - `DPA: Unsupported aggregation function OID: X`

   - `DPA: SUM aggregation only supports INT8`

   - `DPA: No key columns for GROUP BY, fallback to CPU`



## Parameter Description



### enable_dpa_hashagg



**Parameter description**: Controls whether to enable the DPA hash aggregation hardware acceleration feature.



This parameter is of the USERSET type. For the setting method, see the corresponding method in [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).



**Value range**: Boolean



- `on`/`true`: enables DPA hash aggregation acceleration.

- `off`/`false` indicates that DPA hash aggregation acceleration is not enabled.



**Default value**: `off`/`false`.



**Recommendation:**

On the ARM64 platform with the UADK acceleration library configured, it is recommended to enable this parameter for large-scale data aggregation query scenarios to achieve performance improvement.



### uadk_path



**Parameter description**: Sets the file path of the UADK DAE dynamic library (libwd_dae.so).



This parameter is of the POSTMASTER type. After modification, restart the database for the change to take effect. For the setting method, see the corresponding method in [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).



**Value range**: string



**Default value**: `libwd_dae.so`



**Recommended setting:**

If `libwd_dae.so` is not in the system default library search path, you need to specify the full path, for example, `/usr/local/lib/libwd_dae.so`.