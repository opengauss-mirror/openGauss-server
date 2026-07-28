# DROP PROC

## 功能描述<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

删除已存在的存储过程。

## 注意事项<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。原openGauss的DROP PROCEDURE语法请参考章节[DROP PROCEDURE](https://docs.opengauss.org/zh/docs/latest/sql_reference/drop_procedure.html)。
- 新增支持通过DROP PROC方式删除存储过程，功能和DROP PROCEDURE方式保持一致。

## 语法格式<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

```
DROP { PROCEDURE | PROC } [ IF EXISTS ] procedure_name 
[ ( [ {[ argname ] [ argmode ] argtype} [, ...] ] ) [ CASCADE | RESTRICT ] ];
```

## 参数说明<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **PROC**

    D库新增通过DROP PROC方式删除存储过程，功能和DROP PROCEDURE方式保持一致。

## 示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```sql
create schema test_proc;
set current_schema to test_proc;

create procedure p1()
is
begin
RAISE INFO 'call procedure: p1';
end;
/

create proc p2()
is
begin
RAISE INFO 'call procedure: p2';
end;
/

drop proc p1;
drop procedure p2;
```

## 相关链接<a name="section156744489391"></a>

[DROP PROCEDURE](https://docs.opengauss.org/zh/docs/latest/sql_reference/drop_procedure.html)
