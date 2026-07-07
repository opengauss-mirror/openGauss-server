# Hint的错误、冲突及告警<a name="ZH-CN_TOPIC_0289899816"></a>

Plan Hint的结果会体现在计划的变化上，可以通过explain来查看变化。

Hint中的错误不会影响语句的执行，只是不能生效，该错误会根据语句类型以不同方式提示用户。对于explain语句，hint的错误会以warning形式显示在界面上，对于非explain语句，会以debug1级别日志显示在日志中，关键字为PLANHINT。

hint的错误分为以下类型：

- 语法错误

    语法规则树归约失败，会报错，指出出错的位置。

    例如：hint关键字错误，leading hint或join hint指定2个表以下，其它hint未指定表等。一旦发现语法错误，则立即终止hint的解析，所以此时只有错误前面的解析完的hint有效。

    例如：

    ```
    leading((t1 t2)) nestloop(t1) rows(t1 t2 #10)
    ```

    nestloop\(t1\)存在语法错误，则终止解析，可用hint只有之前解析的leading\(\(t1 t2\)\)。

- 语义错误
    - 表不存在，存在多个，或在leading或join中出现多次，均会报语义错误。
    - scanhint中的index不存在，会报语义错误。
    - 另外，如果子查询提升后，同一层出现多个名称相同的表，且其中某个表需要被hint，hint会存在歧义，无法使用，需要为相同表增加别名规避。

- hint重复或冲突

    如果存在hint重复或冲突，只有第一个hint生效，其它hint均会失效，会给出提示。

    - hint重复是指，hint的方法及表名均相同。例如：nestloop\(t1 t2\) nestloop\(t1 t2\)。
    - hint冲突是指，table list一样的hint，存在不一样的hint，hint的冲突仅对于每一类hint方法检测冲突。

        例如：nestloop \(t1 t2\) hashjoin \(t1 t2\)，则后面与前面冲突，此时hashjoin的hint失效。注意：nestloop\(t1 t2\)和no mergejoin\(t1 t2\)不冲突。

        >[!TIP]须知
        >leading hint中的多个表会进行拆解。例如：leading \(\(t1 t2 t3\)\)会拆解成：leading\(\(t1 t2\)\) leading\(\(\(t1 t2\) t3\)\)，此时如果存在leading\(\(t2 t1\)\)，则两者冲突，后面的会被丢弃。（例外：指定内外表的hint若与不指定内外表的hint重复，则始终丢弃不指定内外表的hint。）

- 子链接提升后hint失效

    子链接提升后的hint失效，会给出提示。通常出现在子链接中存在多个表连接的场景。提升后，子链接中的多个表不再作为一个整体出现在join中。

- hint未被使用
    - 非等值join使用hashjoin hint或mergejoin hint。
    - 不包含索引的表使用indexscan hint或indexonlyscan hint。
    - 通常只有在索引列上使用过滤条件才会生成相应的索引路径，全表扫描将不会使用索引，因此使用indexscan hint或indexonlyscan hint将不会使用。
    - indexonlyscan只有输出和谓词条件列仅包含索引列才会使用，否则指定时hint不会被使用。
    - 多个表存在等值连接时，仅尝试有等值连接条件的表的连接，此时没有关联条件的表之间的路径将不会生成，所以指定相应的leading，join，rows hint将不使用，例如：t1 t2 t3表join，t1和t2, t2和t3有等值连接条件，则t1和t3不会优先连接，leading\(t1 t3\)不会被使用。
    - 如果子链接未被提升，则blockname hint不会被使用。
    - 对于skew hint，hint未被使用可能由于：
        - hint指定倾斜信息有误或不完整，如对于join优化未指定值。
        - 倾斜优化的GUC参数处于关闭状态。
