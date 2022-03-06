--
-- MERGE INTO 
--

-- part 1
-- initial
CREATE SCHEMA merge_where_col;
SET current_schema = merge_where_col;

drop table if exists merge_nest_tab1,dt2;
create table merge_nest_tab1(co1 numeric(20,4),co2 varchar2,co3 number,co4 date);
insert into merge_nest_tab1 values(generate_series(1,10),'hello'||generate_series(1,10),generate_series(1,10)*10,sysdate);
create table dt2(c1 numeric(20,4),c2 boolean,c3 character(40),c4 binary_double,c5 nchar(20)) WITH (ORIENTATION = COLUMN);
insert into dt2 values(generate_series(20,50),false,generate_series(20,50)||'gauss',generate_series(20,50)-0.99,'openopen');

-- we can't use columns of target table in insertion subquery(co1<45) for 'where'
BEGIN; 
merge into merge_nest_tab1 a
USING dt2 b
    ON a.co1=b.c1-20
    WHEN NOT matched THEN
    insert(co1,co2,co3) values(100,
    (SELECT 666)||'good',
        (SELECT sum(c.c1)
        FROM dt2 c
        INNER JOIN merge_nest_tab1 d
            ON c.c1=d.co1 ))
    WHERE co1<45;
END; 

-- we can use columns of source table in insertion subquery(c1<45) for 'where'
BEGIN; 
merge into merge_nest_tab1 a
USING dt2 b
    ON a.co1=b.c1-20
    WHEN NOT matched THEN
    insert(co1,co2,co3) values(100,
    (SELECT 666)||'good',
        (SELECT sum(c.c1)
        FROM dt2 c
        INNER JOIN merge_nest_tab1 d
            ON c.c1=d.co1 ))
    WHERE c1<45;
SELECT co1, co2, co3 FROM merge_nest_tab1 order by 1; 
ROLLBACK; 

-- we can use columns of source table in insert subquery(c1<45) for 'where'
BEGIN; 
merge into merge_nest_tab1 a
USING dt2 b
    ON a.co1=b.c1-20
    WHEN matched THEN
        UPDATE SET a.co3=a.co3 + b.c4,
         a.co2='hello',
         a.co4=(SELECT last_day(sysdate))
    WHERE c1 BETWEEN 1 AND 50;
SELECT co1, co2, co3 FROM merge_nest_tab1 order by 1; 
ROLLBACK; 

-- part 2
-- initial
drop table if exists tb_a,tb_b;

create table tb_a(id int, a int, b int, c int, d int);
create table tb_b(id int, a int, b int, c int, d int);
insert into tb_a values(1, 1, 1, 1, 1);
insert into tb_a values(2, 2, 2, 2, 2);
insert into tb_a values(3, 3, 3, 3, 3);
insert into tb_a values(4, 4, 4, 4, 4);

insert into tb_b values(1, 100, 1, 1, 1);
insert into tb_b values(2, 2, 2, 2, 2);
insert into tb_b values(3, 3, 3, 3, 3);
insert into tb_b values(4, 4, 4, 4, 4);

-- if the column has the same name, the column in the target table takes precedence
BEGIN; 
MERGE INTO tb_b bt
USING tb_a at
    ON (at.id = bt.id)
    WHEN MATCHED THEN
    UPDATE SET a = at.a + 100 WHERE a =100;
SELECT * FROM tb_b ORDER BY  1; 
ROLLBACK; 

-- clean up
DROP SCHEMA merge_where_col CASCADE;
