# 不同用户查询同表显示数据不同

## 问题现象<a name="section4581718173618"></a>

2个用户登录相同数据库human\_resource，同样执行如下查询语句，查询同一张表areas时，查询结果却不一致。

```
select count(*) from areas;
```

## 原因分析<a name="section1611323143617"></a>

1. 检查同名表是否是同一张表。在关系型数据库中，确定一张表通常需要3个因素：database、schema、table。从问题现象描述看，database、table已经确定，分别是human\_resource、areas。
2. 检查同名表的schema是否一致。使用omm、user01分别登录发现，search\_path依次是public和“$user”。omm作为数据库管理员，默认不会创建omm同名的schema，即不指定schema的情况下所有表都会建在public下。而对于普通用户如user01，则会在创建用户时，默认创建同名的schema，即不指定schema时表都会创建在user01的schema下。
3. 如果最终判断是同一张表，存在不同用户访问数据不同情况，则需要进一步判断当前该表中对象针对不同的用户是否存在不同的访问策略。

## 处理办法<a name="section8271112913368"></a>

- 对于不同schema下同名表的查询，在查询表时加上schema引用。格式如下。

    ```
    schema.table
    ```

- 对于不同访问策略造成对同一表查询结果不同时，可以通过查询pg\_rlspolicy系统表来确认具体的访问准则。
