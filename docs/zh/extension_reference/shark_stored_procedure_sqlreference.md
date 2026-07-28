# 存储过程

商业规则和业务逻辑可以通过程序存储在openGauss中，这个程序就是存储过程。

存储过程是SQL和PL/SQL的组合。存储过程使执行商业规则的代码可以从应用程序中移动到数据库。从而，代码存储一次能够被多个程序使用。用户可以进行反复调用，从而减少SQL语句的重复编写数量，提高工作效率。

## 注意事项<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。
- shark插件新增一种过程语言pltsql，create procedure默认创建的是pltsql。

## 示例<a name="zh-cn_topic_0283136560_zh-cn_topic_0237122104_zh-cn_topic_0059778837_scc61c5d3cc3e48c1a1ef323652dda821"></a>

```
openGauss=# create procedure p1
is
begin
null;
end;
/
CREATE PROCEDURE
--定义存储过程
openGauss=# select l.lanname from pg_language l join pg_proc p on l.oid = p.prolang and p.proname = 'p1';
 lanname
---------
 pltsql
(1 row)

```
