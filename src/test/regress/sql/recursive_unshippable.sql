/*
 * Verify ALL term in recursive CTE is not shippable
 */
explain (costs off) with recursive rq as
(
    select id, name from  chinamap where id = 11
    union
    select origin.id, rq.name || ' > ' || origin.name
    from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;

with recursive rq as
(
    select id, name from  chinamap where id = 11
    union
    select origin.id, rq.name || ' > ' || origin.name
    from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;

/*
 * Verify if CtePlan node is pruned is not shippable
 */
explain (costs off)
select *, row_number() over() rn from
(
    with recursive r1 as
    (
        select t.area_code, to_char(rank() over (order by t.bank_code DESC NULLS LAST, t.group_code DESC NULLS LAST, t.area_name), '9999') levelsup
        from gcms.gcm_mag_area_h t
        where country_code <> '6'and country_code <> '7' and mag_area_status IN ( '1' ,'2' ) and t.area_code = 1000
        union all
        select t1.area_code, t2.levelsup || to_char(rank() over (order by t1.bank_code DESC NULLS LAST, t1.group_code DESC NULLS LAST, t1.area_name), '9999')
        from gcms.gcm_mag_area_h t1,r1 t2
        where t1.country_code <> '6'and t1.country_code <> '7' and t1.mag_area_status IN ( '1' ,'2' ) and t1.belong_area_code=t2.area_code
    )  select area_code  from r1 order by levelsup
);

select *, row_number() over() rn from
(
    with recursive r1 as
    (
        select t.area_code, to_char(rank() over (order by t.bank_code DESC NULLS LAST, t.group_code DESC NULLS LAST, t.area_name), '9999') levelsup
        from gcms.gcm_mag_area_h t
        where country_code <> '6'and country_code <> '7' and mag_area_status IN ( '1' ,'2' ) and t.area_code = 1000
        union all
        select t1.area_code, t2.levelsup || to_char(rank() over (order by t1.bank_code DESC NULLS LAST, t1.group_code DESC NULLS LAST, t1.area_name), '9999')
        from gcms.gcm_mag_area_h t1,r1 t2
        where t1.country_code <> '6'and t1.country_code <> '7' and t1.mag_area_status IN ( '1' ,'2' ) and t1.belong_area_code=t2.area_code
    )  select area_code  from r1 order by levelsup
);

create table test_rec_part(dm int, sj_dm int, name text) with(orientation=column)
partition by range(sj_dm)
(
partition p0 values less than(1),
partition p1 values less than(2),
partition p2 values less than(3),
partition p3 values less than(4),
partition p4 values less than(5),
partition p5 values less than(6),
partition p6 values less than(7),
partition p7 values less than(8),
partition p8 values less than(9),
partition p9 values less than(10),
partition p10 values less than(11),
partition p11 values less than(12),
partition p12 values less than(13),
partition p13 values less than(14),
partition p14 values less than(15),
partition p15 values less than(16),
partition p16 values less than(17),
partition p17 values less than(18),
partition p18 values less than(19),
partition p19 values less than(maxvalue)
);
insert into test_rec_part values(1,0,'universe');
insert into test_rec_part values(2,1,'galaxy');
insert into test_rec_part values(3,2,'sun');
insert into test_rec_part values(4,3,'earth');
insert into test_rec_part values(5,4,'asia');
insert into test_rec_part values(6,5,'China');
insert into test_rec_part values(7,6,'shaanxi');
insert into test_rec_part values(8,7,'xian');
insert into test_rec_part values(9,8,'huawei');
insert into test_rec_part values(10,9,'v10');
insert into test_rec_part values(11,10,'v10-3L');
insert into test_rec_part values(12,11,'gauss');
insert into test_rec_part values(13,12,'test');
insert into test_rec_part values(14,13,'test');
insert into test_rec_part values(15,14,'test');
insert into test_rec_part values(16,15,'test');
insert into test_rec_part values(17,16,'test');
insert into test_rec_part values(18,17,'test');
insert into test_rec_part values(19,18,'test');
insert into test_rec_part values(20,19,'test');
create index on test_rec_part(dm) local;
create index on test_rec_part(sj_dm) local;
create index on test_rec_part(name) local;
explain (costs off)
WITH recursive t_result AS (
	select * from(
	SELECT dm,sj_dm,name,1 as level
	FROM test_rec_part
	WHERE sj_dm < 10 limit 6 offset 2)
	UNION all
	select * from (SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1
	FROM t_result t1
	JOIN test_rec_part t2 ON t2.sj_dm = t1.dm limit 6 offset 2)
)
SELECT *
FROM t_result t;

drop table test_rec_part;

/*
 * Verify iteration is max at 200 limit
 */
create table rec_tb1 (id int ,parentID int ,name varchar(100)) WITH (ORIENTATION = COLUMN);
create table rec_tb2 (id int ,parentID int ,name varchar(100))WITH (ORIENTATION = COLUMN) ;
insert into rec_tb1 values(1,0,'河南省');
insert into rec_tb1 values(2,1,'信阳市');
insert into rec_tb1 values(3,2,'淮滨县');
insert into rec_tb1 values(4,3,'芦集乡');
insert into rec_tb1 values(12,3,'邓湾乡');
insert into rec_tb1 values(13,3,'台头乡');
insert into rec_tb1 values(14,3,'谷堆乡');
insert into rec_tb1 values(8,2,'固始县');
insert into rec_tb1 values(9,8,'李店乡');
insert into rec_tb1 values(10,2,'息县');
insert into rec_tb1 values(11,10,'关店乡');

insert into rec_tb1 values(5,1,'安阳市');
insert into rec_tb1 values(6,5,'滑县');
insert into rec_tb1 values(7,6,'老庙乡');

insert into rec_tb1 values(15,1,'南阳市');
insert into rec_tb1 values(16,15,'方城县');

insert into rec_tb1 values(17,1,'驻马店市');
insert into rec_tb1 values(18,17,'正阳县');


create index i_rec_tb2_1 on rec_tb2(id);
create index i_rec_tb2_2 on rec_tb2(parentID);
create index i_rec_tb2_3 on rec_tb2(name);

create table rec_tb1_rep (id int ,parentID int ,name varchar(100)) ;;
create table rec_tb2_rep (id int ,parentID int ,name varchar(100)) ;;
insert into rec_tb2 select * from rec_tb1;
insert into rec_tb1_rep select * from rec_tb1;
insert into rec_tb2_rep select * from rec_tb2;


set explain_perf_mode=pretty;
explain (costs off)
with recursive cte as
 (select a.*,1 as level
    from rec_tb1 a
   inner join rec_tb2 b
      on a.id > b.parentID
  union all
  select d.id, d.parentID, d.name,level+1
    from rec_tb1 c
   inner join cte d
      on c.id = d.parentID where level<201
   group by d.id, d.parentID, d.name,d.level )
select count(*)
  from cte
 inner join rec_tb1
    on rec_tb1.id > cte.parentID;

    
with recursive cte as
 (select a.*,1 as level
    from rec_tb1 a
   inner join rec_tb2 b
      on a.id > b.parentID
  union all
  select d.id, d.parentID, d.name,level+1
    from rec_tb1 c
   inner join cte d
      on c.id = d.parentID where level<201
   group by d.id, d.parentID, d.name,d.level )
select count(*)
  from cte
 inner join rec_tb1
    on rec_tb1.id > cte.parentID;
    
explain (costs off)
with recursive cte as
 (select a.*,1 as level
    from rec_tb1_rep a
   inner join rec_tb2_rep b
      on a.id > b.parentID
  union all
  select d.id, d.parentID, d.name,level+1
    from rec_tb1_rep c
   inner join cte d
      on c.id = d.parentID where level<201
   group by d.id, d.parentID, d.name,d.level )
select count(*)
  from cte
 inner join rec_tb1_rep
    on rec_tb1_rep.id > cte.parentID;

with recursive cte as
 (select a.*,1 as level
    from rec_tb1_rep a
   inner join rec_tb2_rep b
      on a.id > b.parentID
  union all
  select d.id, d.parentID, d.name,level+1
    from rec_tb1_rep c
   inner join cte d
      on c.id = d.parentID where level<201
   group by d.id, d.parentID, d.name,d.level )
select count(*)
  from cte
 inner join rec_tb1_rep
    on rec_tb1_rep.id > cte.parentID;

with recursive cte as
 (select a.*,1 as level
    from rec_tb1_rep a
   inner join rec_tb2_rep b
      on a.id > b.parentID
  union all
  select d.id, d.parentID, d.name,level+1
    from rec_tb1_rep c
   inner join cte d
      on c.id = d.parentID where level<201
   group by d.id, d.parentID, d.name,d.level )
select count(*)
  from cte
 inner join rec_tb1_rep
    on rec_tb1_rep.id > cte.parentID;
    
with recursive cte as
 (select a.*,1 as level
    from rec_tb1 a
   inner join rec_tb2 b
      on a.id > b.parentID
  union all
  select d.id, d.parentID, d.name,level+1
    from rec_tb1 c
   inner join cte d
      on c.id = d.parentID where level<201
   group by d.id, d.parentID, d.name,d.level )
select count(*)
  from cte
 inner join rec_tb1
    on rec_tb1.id > cte.parentID;

create table rec_tb3 (id int ,parentID int ,name varchar(100))WITH (ORIENTATION = COLUMN)  ;

create index i_rec_tb3_1 on rec_tb3(id);
create index i_rec_tb3_2 on rec_tb3(parentID);
create index i_rec_tb3_3 on rec_tb3(name);

create table rec_tb4 (id int ,parentID int ,name varchar(100))WITH (ORIENTATION = COLUMN)  partition by range(parentID)
(
PARTITION P1 VALUES LESS THAN(2),
PARTITION P2 VALUES LESS THAN(8),
PARTITION P3 VALUES LESS THAN(16),
PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

create view view_cte1 as
with recursive tmp as
(select id, parentid, name, substr(name, 5)
 from rec_tb1
 union ALL
 select tmp.id, rec_tb3.parentid, tmp.name, substr(tmp.name, 5)
 from rec_tb3
 inner join tmp
 on tmp.parentid = rec_tb3.id),
tmp2 AS
(select id, parentid, name, substr(name, 5) name1 from tmp )
select tmp.* from tmp,tmp2 where tmp2.id not in (select parentid from tmp2);

create view view_cte2 as
with recursive cte as
(select a.*
 from rec_tb1 a
 join rec_tb2 b
 on a.id = b.parentid
 union all
 select d.* from rec_tb4 d join cte e on e.parentid = d.id)
select * from cte;

WITH  WITH_029 AS (SELECT * FROM view_cte1 INNER JOIN view_cte2 ON EXISTS( SELECT * FROM view_cte2,view_cte1)) SELECT * FROM WITH_029 limit 1;

-- This is for dynamic memory, see in SetMinimumDMem()
create view view_cte3 as
with recursive cte as
(select * from rec_tb1
 union all
 select * from rec_tb4)
select distinct * from cte;

WITH  WITH_029 AS(SELECT(CASE WHEN (NOT EXISTS(SELECT * FROM view_cte3)) THEN ('b') ELSE ('K') END))
SELECT * FROM WITH_029;

drop view view_cte1;
drop view view_cte2;
drop view view_cte3;
drop table rec_tb1;
drop table rec_tb2;
drop table rec_tb1_rep;
drop table rec_tb2_rep;
drop table rec_tb3;
drop table rec_tb4;


/*
 * verify CteScan's DN Pruning + stream on outer
 */
create table test_rec_part(dm int, sj_dm int, name text) with(orientation=row) 
partition by range(sj_dm)
(
partition p0 values less than(1),
partition p1 values less than(2),
partition p2 values less than(3),
partition p3 values less than(4),
partition p4 values less than(5),
partition p5 values less than(6),
partition p6 values less than(7),
partition p7 values less than(8),
partition p8 values less than(9),
partition p9 values less than(10),
partition p10 values less than(11),
partition p11 values less than(12),
partition p12 values less than(13),
partition p13 values less than(14),
partition p14 values less than(15),
partition p15 values less than(16),
partition p16 values less than(17),
partition p17 values less than(18),
partition p18 values less than(19),
partition p19 values less than(maxvalue)
);
insert into test_rec_part values(1,0,'universe');
insert into test_rec_part values(2,1,'galaxy');
insert into test_rec_part values(3,2,'sun');
insert into test_rec_part values(4,3,'earth');
insert into test_rec_part values(5,4,'asia');
insert into test_rec_part values(6,5,'China');
insert into test_rec_part values(7,6,'shaanxi');
insert into test_rec_part values(8,7,'xian');
insert into test_rec_part values(9,8,'huawei');
insert into test_rec_part values(10,9,'v10');
insert into test_rec_part values(11,10,'v10-3L');
insert into test_rec_part values(12,11,'gauss');
insert into test_rec_part values(13,12,'test');
insert into test_rec_part values(14,13,'test');
insert into test_rec_part values(15,14,'test');
insert into test_rec_part values(16,15,'test');
insert into test_rec_part values(17,16,'test');
insert into test_rec_part values(18,17,'test');
insert into test_rec_part values(19,18,'test');
insert into test_rec_part values(20,19,'test');
create index on test_rec_part(dm) local;
create index on test_rec_part(sj_dm) local;
create index on test_rec_part(name) local;

explain (costs off)
WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_rec_part
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1 
FROM t_result t1
JOIN test_rec_part t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t;

WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_rec_part
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1
FROM t_result t1
JOIN test_rec_part t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t order by 1,2,3,4;

drop table test_rec_part;
