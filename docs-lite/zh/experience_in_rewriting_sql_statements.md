# 经验总结：SQL语句改写规则<a name="ZH-CN_TOPIC_0289899939"></a>

根据数据库的SQL执行机制以及大量的实践，总结发现：通过一定的规则调整SQL语句，在保证结果正确的基础上，能够提高SQL执行效率。如果遵守这些规则，常常能够大幅度提升业务查询效率。

- **使用union all代替union**

    union在合并两个集合时会执行去重操作，而union all则直接将两个结果集合并、不执行去重。执行去重会消耗大量的时间，因此，在一些实际应用场景中，如果通过业务逻辑已确认两个集合不存在重叠，可用union all替代union以便提升性能。

- **join列增加非空过滤条件**

    若join列上的NULL值较多，则可以加上is not null过滤条件，以实现数据的提前过滤，提高join效率。

- **not in转not exists**

    not in语句需要使用nestloop anti join来实现，而not exists则可以通过hash anti join来实现。在join列不存在null值的情况下，not exists和not in等价。因此在确保没有null值时，可以通过将not in转换为not exists，通过生成hash join来提升查询效率。

    如下所示，如果t2.d2字段中没有null值\(t2.d2字段在表定义中not null\)查询可以修改为

    ```
    SELECT * FROM t1 WHERE  NOT EXISTS (SELECT * FROM t2 WHERE t1.c1=t2.d2);
    ```

    产生的计划如下：

    ```
    QUERY PLAN
    ------------------------------
    Hash Anti Join
    Hash Cond: (t1.c1 = t2.d2)
    ->  Seq Scan on t1
    ->  Hash
    ->  Seq Scan on t2
    (5 rows)
    ```

- **选择hashagg。**

    查询中GROUP BY语句如果生成了groupagg+sort的plan性能会比较差，可以通过加大work\_mem的方法生成hashagg的plan，因为不用排序而提高性能。

- **尝试将函数替换为case语句。**

    openGauss函数调用性能较低，如果出现过多的函数调用导致性能下降很多，可以根据情况把可下推函数的函数改成CASE表达式。

- **避免对索引使用函数或表达式运算。**

    对索引使用函数或表达式运算会停止使用索引转而执行全表扫描。

- **尽量避免在where子句中使用!=或<\>操作符、null值判断、or连接、参数隐式转换。**
- **对复杂SQL语句进行拆分。**

    对于过于复杂并且不易通过以上方法调整性能的SQL可以考虑拆分的方法，把SQL中某一部分拆分成独立的SQL并把执行结果存入临时表，拆分常见的场景包括但不限于：

    - 作业中多个SQL有同样的子查询，并且子查询数据量较大。
    - Plan cost计算不准，导致子查询hash bucket太小，比如实际数据1000W行，hash bucket只有1000。
    - 函数（如substr,to\_number）导致大数据量子查询选择度计算不准。
