# 配置DPA哈希聚合加速



openGauss的DPA（Data Processing Accelerator）哈希聚合加速功能是基于[UADK（Unified Accelerator Development Kit）](https://docs.openeuler.org/zh/docs/22.03_LTS/docs/UADK/UADK-quick-start.html)框架实现的硬件加速特性。它允许将向量化哈希聚合操作卸载到硬件加速器上执行，通过硬件并行处理能力显著提升聚合查询的执行效率。



## 适用场景与限制



环境需求：需要ARM64（aarch64）架构的硬件平台，并且系统中已安装配置UADK加速库（libwd_dae.so）。



- 适用场景

    - 向量化执行的大规模数据哈希聚合：在列存表或行存转向量化场景下执行的VecHashAgg操作

    - 简单聚合函数：使用SUM、COUNT基础聚合函数的查询



- 支持的聚合算子：

    - Vector Hash Aggregate（向量化哈希聚合）



- 硬件限制：

    - 仅支持ARM64（aarch64）架构平台

    - 需要支持UADK的硬件加速器



## 支持的数据类型与聚合函数



DPA功能对数据类型和聚合函数有严格的限制，不满足条件时会自动回退到CPU执行。



### GROUP BY分组键支持的数据类型



| 数据类型 | 长度限制 | 说明 |

|---------|---------|------|

| INT4（INTEGER） | 无 | 32位整数 |

| INT8（BIGINT） | 无 | 64位整数 |

| CHAR/BPCHAR | ≤ 32字节 | 定长字符串 |

| VARCHAR | ≤ 30字节 | 变长字符串 |



### 支持的聚合函数



| 聚合函数 | 输入类型 | 说明 |

|---------|---------|------|

| SUM | INT8（BIGINT） | 仅支持BIGINT类型的求和 |

| COUNT | INT4、INT8、CHAR、VARCHAR | 支持多种类型的计数 |



>[!TIP]须知

>

>- 当前不支持`count(*)`全行计数、`sum(numeric)`等聚合操作。

>- 不支持`MAX`、`MIN`聚合函数。



### 列数量限制



| 限制项 | 最大值 | 说明 |

|-------|-------|------|

| GROUP BY键列数 | 9 | 分组键最多9列 |

| 聚合输入列数 | 9 | 聚合操作的输入列最多9列 |

| CHAR/VARCHAR键列数 | 5 | 字符类型的分组键最多5个 |



## 配置与使用



启用DPA哈希聚合加速功能，需要进行以下配置：



1. 安装UADK加速库，参考[UADK文档](https://docs.openeuler.org/zh/docs/22.03_LTS/docs/UADK/UADK-quick-start.html)进行安装。



2. 修改数据库配置文件，设置UADK动态库路径（postgresql.conf）。该参数需要重启openGauss生效。



```

# 设置libwd_dae.so动态库路径，默认值为libwd_dae.so

uadk_path = 'libwd_dae.so'

```



3. 开启DPA哈希聚合加速



```sql

-- 会话级别开启

SET enable_dpa_hashagg = on;



-- 或在配置文件中设置

# enable_dpa_hashagg = on

```



## 使用示例https://docs.opengauss.org/zh/docs/latest-lite/database_administration_guide/reset_parameters.html



以下是一个适合DPA加速的查询示例：



```sql

-- 开启DPA加速

SET enable_dpa_hashagg = on;



-- 适合DPA加速的聚合查询https://docs.opengauss.org/zh/docs/latest-lite/database_administration_guide/reset_parameters.html

SELECT 

    region_id,           -- INT4类型分组键

    product_code,        -- VARCHAR(20)类型分组键

    SUM(amount),         -- amount必须是INT8类型

    COUNT(order_id)      -- order_id可以是INT4/INT8/CHAR/VARCHAR

FROM orders

GROUP BY region_id, product_code;

```



## 使用限制与注意事项



1. **平台限制**：DPA功能仅在ARM64架构平台上可用，x86架构无法使用该功能。详情参考[使用要求](https://docs.openeuler.org/zh/docs/22.03_LTS/docs/UADK/UADK-quick-start.html#%E4%BD%BF%E7%94%A8%E8%A6%81%E6%B1%82)



2. **必须有GROUP BY子句**：DPA不支持无分组键的全表聚合，必须包含GROUP BY子句。



3. **数据类型限制**：GROUP BY列和聚合列的数据类型必须在支持范围内，否则自动回退CPU执行。



4. **字符串长度限制**：GROUP BY分组键 CHAR类型最大长度32字节，VARCHAR类型最大长度30字节，超出限制将回退CPU执行。



5. **向量化执行要求**：DPA仅在向量化执行引擎的HashAgg算子中生效，需要确保查询走向量化执行路径。



6. **自动Fallback机制**：当遇到不支持的场景时，系统会自动回退到CPU执行，并在日志中输出WARNING信息，不会导致查询失败。



## 性能影响



当配置适当且查询满足DPA加速条件时，可以获得以下收益：



1. 利用硬件加速器的并行处理能力，提升哈希聚合操作的执行效率

2. 减少CPU在聚合计算上的开销，释放CPU资源用于其他操作

3. 对于大规模数据的聚合查询，性能提升效果更加明显



>[!NOTE]说明

>DPA加速效果取决于硬件加速器的性能和数据规模。对于小数据量查询，加速效果可能不明显，甚至因为数据传输开销导致性能下降。建议在大规模数据聚合场景下使用。



## 故障诊断



当DPA功能无法正常工作时，可以通过以下方式进行诊断：



1. **检查平台架构**：确认运行环境为ARM64架构。



    ```bash

    uname -m

    # 输出应为 aarch64

    ```



2. **检查UADK库加载**：查看数据库日志，确认UADK库是否加载成功。



    ```

    LOG:  UADK AGG library loaded successfully

    ```



3. **查看WARNING日志**：当DPA回退CPU执行时，会输出相关WARNING信息。常见的WARNING包括：

   - `DPA: Key columns number X exceeds hardware limit 9, fallback to CPU`

   - `DPA: Unsupported key column type OID: X`

   - `DPA: CHAR(X) length exceeds hardware limit 32`

   - `DPA: Key VARCHAR(X) exceeds hardware limit 30 bytes, fallback to CPU`

   - `DPA: Unsupported aggregation function OID: X`

   - `DPA: SUM aggregation only supports INT8`

   - `DPA: No key columns for GROUP BY, fallback to CPU`



## 参数说明



### enable_dpa_hashagg



**参数说明**：控制是否启用DPA哈希聚合硬件加速功能。



该参数属于USERSET类型参数，请参考[表1](https://docs.opengauss.org/zh/docs/latest-lite/database_administration_guide/reset_parameters.html#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846)中对应设置方法进行设置。



**取值范围**：布尔型



- `on`/`true` 表示启用DPA哈希聚合加速。

- `off`/`false` 表示不启用DPA哈希聚合加速。



**默认值**：`off`/`false`。



**设置建议：**

在ARM64平台且已配置UADK加速库的环境下，对于大规模数据聚合查询场景，建议开启该参数以获得性能提升。



### uadk_path



**参数说明**：设置UADK DAE动态库（libwd_dae.so）的文件路径。



该参数属于POSTMASTER类型参数，修改后需要重启数据库生效。请参考[表1](https://docs.opengauss.org/zh/docs/latest-lite/database_administration_guide/reset_parameters.html#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846)中对应设置方法进行设置。



**取值范围**：字符串



**默认值**：`libwd_dae.so`



**设置建议：**

如果libwd_dae.so不在系统默认库搜索路径中，需要指定完整路径，例如`/usr/local/lib/libwd_dae.so`。

