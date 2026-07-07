# UDF结果缓存

UDF 结果缓存（Result Cache）特性表示在一个计划执行周期（即单条语句的执行周期）内，支持共享确定性自定义函数（UDF, User Defined Function）的调用结果。通过节省过程语言引擎的调用和运行开销，大幅提高UDF的执行性能。

IMMUTABLE和STABLE函数在同一次计划的执行过程中，能够保证相同的参数返回相同的结果。通过缓存函数参数对应的执行结果，在使用相同参数反复调用时，不必再调用过程语言引擎，可以大幅提升函数返回结果的速度。

此功能受GUC参数 enable_func_cache 选项控制。开启此选项后，允许确定性函数使用结果缓存以提升非首次运行UDF的性能。

## 适用场景与限制

UDF结果缓存使用时，用户需要确认函数在同一次计划的执行过程中，相同的入参返回相同的结果，才能给函数增加RESULT_CACHE属性，使用该特性。否则执行可能出现错误的结果。

UDF结果缓存在函数传入的参数值重复率高的场景下，能够起到较好的性能提升效果；在函数传入的参数值重复率不高的场景下，反而可能引起性能的劣化。

- 适用场景
    - 支持缓存的参数类型：
        - 整型：SMALLINT、INT、BIGINT
        - 浮点：FLOAT、DOUBLE PRECISION
        - 字符串：CHAR、VARCHAR、TEXT、VARCHAR2
        - 数值：NUMERIC
        - 时间：DATE、TIME、TIMESTAMP、TIMESTAMPTZ
        - 其他：BOOL、NVARCHAR2

- 非适用场景
    - 不支持结果缓存的函数类型：
        - 使用内置过程语言（INTERNAL、C、SQL）的函数。
        - RESULT_CACHE 属性为 FALSE 的函数。
        - 含有非 IN 入参的函数。
        - 函数返回结果集、存在可变参数、存在不支持的参数类型。
        - 具有 VOLATILE 属性的函数、聚集函数、窗口函数、存在安全定义或参数数量不一致的函数。
        - 函数中存在子程序、自治事务。
        - 参数超过16个的函数。
    
    - 不支持结果缓存的场景：
        - 除SELECT语句外的语句类型。
        - 开启了SMP并行查询。
        - 传入的参数值超过 127 个字符。
        - 单条语句内UDF数量超过14个，第14个以后的函数不进行缓存。

## 示例

1. 创建测试表，构造并插入测试数据。由于测试表tt_result_cache中的数据量较大且存在很多重复的数据，因此更容易体现出开启结果缓存后的优化效果。

    ```
    CREATE TABLE tt_result_cache(
        id int,
        col_bool bool,
        col_smallint smallint,
        col_int int,
        col_bigint bigint,
        col_float float,
        col_double_precision double precision,
        col_char char(20),
        col_varchar varchar(20),
        col_varchar2 varchar2(20),
        col_timestamp timestamp,
        col_numeric numeric,
        col_text text
    );

    INSERT INTO tt_result_cache VALUES(
    generate_series(1,10000),
    false,
    generate_series(1,10000)%5,
    generate_series(1,10000)%5,
    generate_series(1,10000)%5,
    generate_series(1,10000)%5,
    generate_series(1,10000)%5,
    random()*10%10||'_txt',
    random()*10%10||'_txt',
    random()*10%10||'_txt',
    '2024-11-22 16:36:45',
    generate_series(1,10000)%5,
    random()*10%10||'_txt'
    );
    ```

2. 分别创建具有或不具有RESULT_CACHE标记的确定性函数。
    - 创建无RESULT_CACHE标记的函数：

        ```
        CREATE OR REPLACE FUNCTION func_smallint(p SMALLINT) RETURNS SMALLINT AS $$
        BEGIN
                IF p = 1 THEN
                        raise notice 'param:%',p;
                END IF;
                RETURN p+1;
        END;
        $$ LANGUAGE PLPGSQL STABLE;
        ```

    - 创建具有RESULT_CACHE标记的函数：

        ```
        CREATE OR REPLACE FUNCTION func_smallint_cache(p SMALLINT) RETURNS SMALLINT AS $$
        BEGIN
                IF p = 1 THEN
                        raise notice 'param:%',p;
                END IF;
                RETURN p+1;
        END;
        $$ LANGUAGE PLPGSQL STABLE RESULT_CACHE;
        ```

3. 开启 GUC 参数 enable_func_cache。

    ```
    set enable_func_cache=on;
    ```

4. 分别查看两个函数的执行时间。

    ```
    EXPLAIN (COSTS OFF, ANALYZE) SELECT col_smallint,func_smallint(col_smallint) FROM tt_result_cache;
    EXPLAIN (COSTS OFF, ANALYZE) SELECT col_smallint,func_smallint_cache(col_smallint) FROM tt_result_cache;
    ```

    返回结果如下，可见在启用了UDF结果缓存特性后，具有RESULT_CACHE的确定性函数在（非首次）执行时够显著提升执行效率：

    ```
    QUERY PLAN
    ---------------------------------------------------------------------------
    Seq Scan on tt_result_cache (actual time=1.452..81.903 rows=10000 loops=1)
    Total runtime: 83.118 ms
    (2 rows)

    QUERY PLAN
    ---------------------------------------------------------------------------
    Seq Scan on tt_result_cache (actual time=1.087..7.730 rows=10000 loops=1)
    Total runtime: 9.407 ms
    (2 rows)
    ```
