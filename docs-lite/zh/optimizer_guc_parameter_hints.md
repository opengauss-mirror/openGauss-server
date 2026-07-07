# 优化器GUC参数的Hint<a name="ZH-CN_TOPIC_0000001096400532"></a>

## 功能描述<a name="section290819468377"></a>

设置本次查询执行内生效的查询优化相关GUC参数。hint的推荐使用场景可以参考各guc参数的说明，此处不作赘述。

## 语法格式<a name="section530131664410"></a>

```
set(param value)
```

## 参数说明<a name="section41303128143838"></a>

- **param**表示参数名。
- **value**表示参数的取值。
- 目前支持使用Hint设置生效的参数有
    - 布尔类：

        enable\_bitmapscan, enable\_hashagg, enable\_hashjoin, enable\_indexscan, enable\_indexonlyscan, enable\_material, enable\_mergejoin, enable\_nestloop, enable\_index\_nestloop, enable\_seqscan, enable\_sort, enable\_tidscan，partition\_iterator\_elimination，partition\_page\_estimation, var\_eq\_const\_selectivity, enable\_functional\_dependency, enable\_inner\_unique\_opt

    - 整形类：

        query\_dop

    - 浮点类：

        cost\_weight\_index, default\_limit\_rows, seq\_page\_cost, random\_page\_cost, cpu\_tuple\_cost, cpu\_index\_tuple\_cost, cpu\_operator\_cost, effective\_cache\_size

    - 枚举类型：

        try\_vector\_engine\_strategy

>[!NOTE]说明
>
>- 设置不在白名单中的参数，参数取值不合法，或hint语法错误时，不会影响查询执行的正确性。使用explain\(verbose on\)执行可以看到hint解析错误的报错提示。
>- GUC参数的hint只在最外层查询生效——子查询内的GUC参数hint不生效。
>- 视图定义内的GUC参数hint不生效。
>- CREATE TABLE ... AS ... 查询最外层的GUC参数hint可以生效。
