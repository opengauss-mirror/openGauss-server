# Memory<a name="EN-US_TOPIC_0289900310"></a>

This section describes memory parameters.

>[!TIP]NOTICE 
>These parameters, except  **local\_syscache\_threshold**, take effect only after the database restarts.

## memorypool\_enable<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s630c23ad11044fafae4ed851bc89169a"></a>

**Parameter description**: Specifies whether to enable a memory pool.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: Boolean

- **on**  indicates that the memory pool is enabled.
- **off**  indicates that the memory pool is disabled.

**Default value**:  **off**

## memorypool\_size<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sd277355ba87045b6b802fef9f49f4725"></a>

**Parameter description**: Specifies the memory pool size.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 128 x 1024 to  *INT\*MAX_/2. The unit is kB.

**Default value**:  **512 MB**

## enable\_memory\_limit<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s2cf6c862bad443aea7e115ff83941f94"></a>

**Parameter description**: Specifies whether to enable the logical memory management module.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: Boolean

- **on**  indicates that the logical memory management module is enabled.
- **off**  indicates that the logical memory management module is disabled.

**Default value**:  **on**

>[!WARNING]CAUTION 
>
>- If the value of  **max\_process\_memory**  minus  **shared\_buffer**  minus  **cstore\_buffers**  minus metadata size is less than 2 GB, openGauss forcibly sets  **enable\_memory\_limit**to  **off**. Metadata is the memory used in openGauss and is related to some concurrent parameters, such as  **max\_connections**,  **thread\_pool\_attr**  and  **max\_prepared\_transactions**.
>- If this parameter is set to  **off**, the memory used by the database is not limited. When a large number of concurrent or complex queries are performed, too much memory is used, which may cause OS OOM problems.

## max\_process\_memory<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sbebcee7acf2042dc8824982f22a2b4a8"></a>

**Parameter description**: Specifies the maximum physical memory of a database node.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 2 x 1024 x 1024 to  *INT\*MAX_. The unit is kB.

**Default value**:  **12 GB**

**Setting suggestions**:

The value on the database node is determined based on the physical memory of the system and the number of master database nodes deployed on a single node. The recommended calculation formula is as follows: \(Physical memory – vm.min\_free\_kbytes\) \\ x 0.7 /\(1 + Number of primary nodes\) This parameter is used to prevent node OOM caused by memory usage increase, ensuring system reliability.  **vm.min\_free\_kbytes**  indicates the OS memory reserved for the kernel to receive and send data. Its value is at least 5% of the total memory. That is, max\_process\_memory = Physical memory x 0.665 / \(1 + Number of primary nodes\)

>[!WARNING]CAUTION 
>If this parameter is set to a value greater than the physical memory of the server, the OS OOM problem may occur.

## enable\_memory\_context\_control<a name="en-us_topic_0283136786_en-us_topic_0237124699_section83355314353"></a>

**Parameter description**: Specifies whether to enable the function of checking whether the number of memory contexts exceeds the specified limit. This parameter applies only to the DEBUG version.

This parameter is a SIGHUP parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: Boolean

- **on**  indicates that the function of checking the number of memory contexts is enabled.
- **off**  indicates that the function of checking the number of memory contexts is disabled.

**Default value**:  **off**

## uncontrolled\_memory\_context<a name="en-us_topic_0283136786_en-us_topic_0237124699_section93539377411"></a>

**Parameter description**: Specifies which memory texts will not be checked when the  **enable\_memory\_context\_control**  parameter is set to  **on**. This parameter applies only to the DEBUG version.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

During the query, the title meaning string "MemoryContext white list:" is added to the beginning of the parameter value.

**Value range**: a string

**Default value**: empty

## shared\_buffers<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s55a43fb6d0464430a59031671b37cd07"></a>

**Parameter description**: Specifies the size of shared memory used by openGauss. Increasing the value of this parameter causes openGauss to request more System V shared memory than the default configuration allows.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 16 to 1073741823. The unit is 8 kB.

The value of  **shared\_buffers**  must be an integer multiple of  **BLCKSZ**. Currently,  **BLCKSZ**  is set to  **8 kB**. That is, the value of  **shared\_buffers**  must be an integer multiple of 8 kB. The minimum value changes according to  **BLCKSZ**.

**Default value**:  **8 MB**

**Setting suggestions**:

Set  **shared\_buffers**  to a value less than 40% of the memory. Set it to a large value for row-store tables and a small value for column-store tables. For column-store tables:  **shared\_buffers**  = \(Memory of a single server/Number of database nodes on the server\) x 0.4 x 0.25

If  **shared\_buffers**  is set to a larger value, increase the value of  **checkpoint\_segments**  because a longer period of time is required to write a large amount of new or changed data.

## segment\_buffers<a name="section1581274312490"></a>

**Parameter description**: Specifies the memory size of an openGauss segment-paged metadata page.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 16 to 1073741823. The unit is 8 kB.

The value of  **segment\_buffers**\_buffers**  must be an integer multiple of  **BLCKSZ**. Currently,  **BLCKSZ**  is set to  **8 kB**. That is, the value of  **segment\_buffers**  must be an integer multiple of 8 kB. The minimum value changes according to the value of  **BLCKSZ**.

**Default value**: 8 MB

**Setting suggestions**:

**segment\_buffers**  is used to cache the content of segment-paged headers, which is key metadata information. To improve performance, it is recommended that the segment headers of common tables be cached in the buffer and not be replaced. You are advised to set this parameter based on the following formula: Number of tables \(including indexes and toast tables\) x Number of partitions x 3 + 128. This is because each table \(partition\) has some extra metadata segments. Generally, a table has three segments. At last,  **+ 128**  is added because segment- and page-based tablespace management requires a certain number of buffers.

If this parameter is set to a small value, it takes a long time to create a segment-paged table for the first time. Therefore, you are advised to set this parameter to the recommended value.

## bulk\_write\_ring\_size<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sa239f9a6b4234f04949abc3615970502"></a>

**Parameter description:**  Specifies the size of the ring buffer used by the operation when a large amount of data is written \(for example, the copy operation\).

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 16384 to 2147483647. The unit is kB.

**Default value**:  **2 GB**

**Setting suggestions**: Increase the value of this parameter on database nodes if a huge amount of data will be imported.

## standby\_shared\_buffers\_fraction<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sfda2b709d047441cab1a59ac63bccb08"></a>

**Parameter description**: Specifies the  **shared\_buffers**  proportion used on the server where a standby instance is deployed.

This parameter is a SIGHUP parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: a double-precision floating-point number ranging from 0.1 to 1.0

**Default value:**  0.3

## temp\_buffers<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s2a60d22e5f524adbbf493dfe3a29a4c6"></a>

**Parameter description**: Specifies the maximum size of local temporary buffers used by a database session.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**temp\_buffers**  can be modified only before the first use of temporary tables within each session. Subsequent attempts to change the value of this parameter will not take effect on that session.

A session allocates temporary buffers based on the value of  **temp\_buffers**. If a large value is set in a session that does not require many temporary buffers, only the overhead of one buffer descriptor is added. If a buffer is used, additional 8192 bytes will be consumed for it.

**Value range**: an integer ranging from 100 to 1073741823. The unit is 8 kB.

**Default value**:  **1 MB**

## max\_prepared\_transactions<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s0baf9251722144d492151b31104dd73c"></a>

**Parameter description**: Specifies the maximum number of transactions that can stay in the  **prepared**  state simultaneously. Increasing the value of this parameter causes openGauss to request more System V shared memory than the default configuration allows.

When openGauss is deployed as an HA system, set this parameter on standby servers to a value greater than or equal to that on primary servers. Otherwise, queries will fail on the standby servers.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 0 to 262143

**Default value**:  **10**

>[!NOTE]NOTE 
>Generally, explicit PREPARE operations are not required for transactions. If explicit PREPARE operations are performed for transactions, increase the value of this parameter to be greater than the number of concurrent services that require PREPARE to prevent preparation failures.

## work\_mem<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sd27c81d651ce4d2585febca76c4cc34e"></a>

**Parameter description**: Specifies the amount of memory to be used by internal sort operations and hash tables before they write data into temporary disk files. Sort operations are required for  **ORDER BY**,  **DISTINCT**, and merge joins. Hash tables are used in hash joins, hash-based aggregation, and hash-based processing of  **IN**  subqueries.

In a complex query, several sort or hash operations may run in parallel; each operation will be allowed to use as much memory as this parameter specifies. If the memory is insufficient, data will be written into temporary files. In addition, several running sessions could be performing such operations concurrently. Therefore, the total memory used may be many times the value of  **work\_mem**.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 64 to 2147483647. The unit is kB.

**Default value**:  **64 MB**

>[!TIP]NOTICE 
>**Setting suggestions**:
>If the physical memory specified by  **work\_mem**  is insufficient, additional operator calculation data will be written into temporary tables based on query characteristics and the degree of parallelism. This reduces performance by five to ten times, and prolongs the query response time from seconds to minutes.
>
>- For complex serial queries, each query requires five to ten associated operations. Set  **work\_mem**  using the following formula:  **work\_mem**  = 50% of the memory/10.
>- For simple serial queries, each query requires two to five associated operations. Set  **work\_mem**  using the following formula:  **work\_mem**  = 50% of the memory/5.
>- For concurrent queries, set  **work\_mem**  using the following formula:  **work\_mem**  =  **work\_mem**  for serial queries/Number of concurrent SQL statements.
>- BitmapScan hash tables are also restricted by  **work\_mem**, but will not be forcibly flushed to disks. In the case of complete lossify, every 1 MB memory occupied by the hash table corresponds to a 16 GB page of BitmapHeapScan \(32 GB for Ustore\). After the upper limit of  **work\_mem**  is reached, the memory increases linearly with the data access traffic based on this ratio.

## query\_mem<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_section12283034151318"></a>

**Parameter description**: Specifies the memory used by a query.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: 0 or an integer greater than 32 MB. The default unit is kB.

**Default value**:  **0**

>[!TIP]NOTICE 
>
>- If the value of  **query\_mem**  is greater than 0, the optimizer adjusts the memory cost estimate to this value when generating an execution plan.
>- If the value is set to a negative value or a positive integer less than 32 MB, the default value  **0**  is used. In this case, the optimizer does not adjust the estimated query memory.

## query\_max\_mem<a name="en-us_topic_0283136786_en-us_topic_0237124699_section1258420917117"></a>

**Parameter description**: Specifies the maximum memory that can be used by a query.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: 0 or an integer greater than 32 MB. The default unit is kB.

**Default value**:  **0**

>[!TIP]NOTICE 
>
>- If the value of  **query\_max\_mem**  is greater than 0, an error is reported when the query memory usage exceeds the value.
>- If the value is set to a negative value or a positive integer less than 32 MB, the default value  **0**  is used. In this case, the optimizer does not limit the query memory.

## maintenance\_work\_mem<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s123a0cc8d6434028a6709bbfa876e8b0"></a>

**Parameter description:**  Specifies the maximum amount of memory to be used by maintenance operations, such as  **VACUUM**,  **CREATE INDEX**, and  **ALTER TABLE ADD FOREIGN KEY**. This parameter may affect the execution efficiency of  **VACUUM**,  **VACUUM FULL**,  **CLUSTER**, and  **CREATE INDEX**.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 1024 to  *INT\*MAX_. The unit is kB.

**Default value**:  **16 MB**

>[!TIP]NOTICE 
>**Setting suggestions**:
>
>- The value of this parameter must be greater than that of  **[work\_mem](#en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sd27c81d651ce4d2585febca76c4cc34e)**  so that database dumps can be more quickly cleared or restored. In a database session, only one maintenance operation can be performed at a time. Maintenance is usually performed when there are not many running sessions.
>- When the  [Automatic Vacuuming](https://docs.opengauss.org/en/docs/latest/database_reference/automatic_vacuuming.html)  process is running, up to  **[autovacuum\_max\_workers](https://docs.opengauss.org/en/docs/latest/database_reference/automatic_vacuuming.html#en-us_topic_0283137694_en-us_topic_0237124730_en-us_topic_0059778244_s76932f79410248ba8923017d19982673)**  times this memory may be allocated. In this case, set  **maintenance\_work\_mem**  to a value greater than or equal to that of  **[work\_mem](#en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sd27c81d651ce4d2585febca76c4cc34e)**.
>- If a large amount of data is to be clustered, increase the value of this parameter in the session.

## psort\_work\_mem<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_scb2890bc578f4811b63b341f7866057a"></a>

**Parameter description**: Specifies the memory capacity to be used for partial sorting in a column-store table before writing to temporary disk files. This parameter can be used for inserting tables having a partial cluster key or index, creating a table index, and deleting or updating a table.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

>[!TIP]NOTICE 
>Such operations may be performed in multiple running sessions concurrently. Therefore, the total memory used may be many times the value of  **psort\_work\_mem**.

**Value range**: an integer ranging from 64 to 2147483647. The unit is kB.

**Default value**:  **512 MB**

## max\_loaded\_cudesc<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s180b94f02cee4806be39f36f8d2e2a28"></a>

**Parameter description**: Specifies the number of cudesc cached in each column when a column-store table is scanned. Increasing the value will improve query performance and increase memory usage, particularly when there are many columns in the column-store table.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

>[!TIP]NOTICE 
>If  **max\_loaded\_cudesc**  is set to a large value, memory may be insufficient.

**Value range**: 100 to 1073741823

**Default value:** **1024**

## max\_stack\_depth<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s7417ae6acda9409f8ff99365a0e8bb11"></a>

**Parameter description**: Specifies the maximum safe depth of the openGauss execution stack. The safety margin is required because the stack depth is not checked in every routine in the server, but only in key potentially-recursive routines, such as expression evaluation.

This parameter is a SUSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 100 to  *INT\*MAX_. The unit is kB.

**Default value**:

- If the value of  **ulimit -s**  minus 640 kB is greater than or equal to 2 MB, the default value of this parameter is  **2 MB**.
- If the value of  **ulimit -s**  minus 640 kB is less than 2 MB, the default value of this parameter is the value of  **ulimit -s**  minus 640 kB.

>[!TIP]NOTICE 
>When setting this parameter, comply with the following principles:
>
>- The database needs to reserve 640 kB stack depth. Therefore, the ideal value of this parameter is the actual stack size limit enforced by the OS kernel \(as set by  **ulimit -s**\) minus 640 kB.
>- If the value of this parameter is greater than the value of  **ulimit -s**  minus 640 kB before the database is started, the database fails to be started. During database running, if the value of this parameter is greater than the value of  **ulimit -s**  minus 640 kB, this parameter does not take effect.
>- If the value of  **ulimit -s**  minus 640 kB is less than the minimum value of this parameter, the database fails to be started.
>- Setting this parameter to a value greater than the actual kernel limit means that a running recursive function may crash an individual backend process.
>- Since not all OSs provide this function, you are advised to set a specific value for this parameter.
>- The default value is  **2 MB**, which is relatively small and does not easily cause system breakdown.

## cstore\_buffers<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s00a05d7c1a374988b114e167735a552d"></a>

**Parameter description**: Specifies the shared buffer size used in column-store tables.

This parameter is a POSTMASTER parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 256 to 1073741823. The unit is kB.

**Default value**:  **1 GB**

**Setting suggestions**:

Column-store tables use the shared buffer specified by  **cstore\_buffers**  instead of that specified by  **shared\_buffers**. When column-store tables are mainly used, reduce the value of  **shared\_buffers**  and increase that of  **cstore\_buffers**.

## bulk\_read\_ring\_size<a name="en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_s43b2a38b07f647039f73f31d71db7b26"></a>

**Parameter description:**  Specifies the size of the ring buffer used by the operation when a large amount of data is queried \(for example, during large table scanning\).

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: an integer ranging from 256 to 2147483647. The unit is kB.

**Default value**:  **16 MB**

## enable\_early\_free<a name="en-us_topic_0283137548_en-us_topic_0237124743_section18463123172910"></a>

**Parameter description**: Specifies whether the operator memory can be released in advance.

This parameter is a USERSET parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: Boolean

- **on**  indicates that the operator memory can be released in advance.
- **off**  indicates that the operator memory cannot be released in advance.

**Default value**:  **on**

## local\_syscache\_threshold<a name="section103001521193016"></a>

**Parameter description**: Specifies the size of system catalog cache in a session.

This parameter is a PGC\_SIGHUP parameter. Set it based on instructions provided in  [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en-us_topic_0283137176_en-us_topic_0237121562_en-us_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

If  **enable\_global\_plancache** is enabled,  **local\_syscache\_threshold** does not take effect when it is set to a value less than 16 MB to ensure that GPC takes effect. The minimum value is 16 MB.

If  **enable\_global\_syscache**  and  **enable\_thread\_pool**  are enabled, this parameter indicates the total cache size of the current thread and sessions bound to the current thread.

**Value range**: an integer ranging from 1 x 1024 to 512 x 1024. The unit is kB.

**Default value**: **256MB**
