# DROP TRIGGER

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:06.500Z pushedAt=2026-09-22T01:43:24.512Z -->

## Description<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_se9507fb26df547a795ac7940e3a19ebf"></a>

Deletes a trigger.

## NOTE<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_sfc96c070e8574f4ea9a2726e898fda17"></a>

The owner of a trigger or a user granted the DROP ANY TRIGGER privilege can execute the DROP TRIGGER operation. System administrators have this privilege by default.

## Syntax<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s84baecef89484d5f87f57b0545b46202"></a>

```
DROP TRIGGER [ IF EXISTS ] trigger_name ON table_name [ CASCADE | RESTRICT ];
```

Newly added syntax under D compatibility

```
DROP TRIGGER [ IF EXISTS ] trigger_name [ CASCADE | RESTRICT ];
```

## Parameter Description<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s6df87c0dd87c49e29a034e0ff3385ca7"></a>

- **IF EXISTS**

    If the specified trigger does not exist, a notice is issued instead of raising an error.

- **trigger\_name**

    Name of the trigger to be deleted.

    Value range: an existing trigger.

- **table\_name**

    Name of the table containing the trigger to be deleted.

    Value range: an existing table that contains triggers.

- **CASCADE | RESTRICT**
    - CASCADE: Cascade delete the objects that depend on this trigger.
    - RESTRICT: Refuse to delete this trigger if any dependent objects exist. This is the default value.

## Examples<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## Related Links<a name="section156744489391"></a>

[DROP TRIGGER](../sql_reference/drop_trigger.md)