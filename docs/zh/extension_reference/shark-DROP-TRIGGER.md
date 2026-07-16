# DROP TRIGGER

## 功能描述<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_se9507fb26df547a795ac7940e3a19ebf"></a>

删除触发器。

## 注意事项<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_sfc96c070e8574f4ea9a2726e898fda17"></a>

触发器的所有者或者被授予了DROP ANY TRIGGER权限的用户可以执行DROP TRIGGER操作，系统管理员默认拥有此权限。

## 语法格式<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s84baecef89484d5f87f57b0545b46202"></a>

```
DROP TRIGGER [ IF EXISTS ] trigger_name ON table_name [ CASCADE | RESTRICT ];
```

D兼容性下新增语法

```
DROP TRIGGER [ IF EXISTS ] trigger_name [ CASCADE | RESTRICT ];
```

## 参数说明<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s6df87c0dd87c49e29a034e0ff3385ca7"></a>

- **IF EXISTS**

    如果指定的触发器不存在，则发出一个notice而不是抛出一个错误。

- **trigger\_name**

    要删除的触发器名称。

    取值范围：已存在的触发器。

- **table\_name**

    要删除的触发器所在的表名称。

    取值范围：已存在的含触发器的表。

- **CASCADE | RESTRICT**
    - CASCADE：级联删除依赖此触发器的对象。
    - RESTRICT：如果有依赖对象存在，则拒绝删除此触发器。此选项为缺省值。

## 示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table animals (id int, name char(30));
create table food (id int, foodtype varchar(32), remark varchar(32), time_flag timestamp);
CREATE OR REPLACE FUNCTION insert_food_fun1 RETURNS TRIGGER AS
$$
BEGIN
    insert into food(id, foodtype, remark, time_flag) values (1, 'bamboo', 'healthy', now());
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER animals_trigger1 AFTER INSERT ON animals
FOR EACH ROW
EXECUTE PROCEDURE insert_food_fun1();
CREATE OR REPLACE FUNCTION insert_food_fun2 RETURNS TRIGGER AS
$$
BEGIN
    insert into food(id, foodtype, remark, time_flag) values (2, 'water', 'healthy', now());
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER animals_trigger2 AFTER INSERT ON animals
FOR EACH ROW
EXECUTE PROCEDURE insert_food_fun2();
select tgname from pg_trigger;
      tgname      
------------------
 animals_trigger1
 animals_trigger2
(2 rows)

select count(*) from food;
 count 
-------
     0
(1 row)

insert into animals(id, name) values (1, 'panda');
select * from animals;
 id |              name              
----+--------------------------------
  1 | panda                         
(1 row)

select count(*) from food;
 count 
-------
     2
(1 row)

delete from animals;
delete from food;
drop trigger animals_trigger1;
drop trigger if exists animals_trigger2;
select tgname from pg_trigger;
 tgname 
--------
(0 rows)
```

## 相关链接<a name="section156744489391"></a>

[DROP TRIGGER](../sql_reference/drop_trigger.md)
