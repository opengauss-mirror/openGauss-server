set current_schema=public;
/* 测试不下推场景下计划显示正确 */
explain (costs false) with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;
with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;

set explain_perf_mode=pretty;
explain (costs false) with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;

with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;

explain (costs false) 
with recursive rq as
(
  select id, name from  chinamap2 where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap2 origin on origin.pid = rq.id
)
select id, name from rq order by 1;

with recursive rq as
(
  select id, name from  chinamap2 where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap2 origin on origin.pid = rq.id
)
select id, name from rq order by 1;

explain (costs false) 
with recursive rq as
(
  select id, name from  chinamap join t1 on id = t1.c2 where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;

with recursive rq as
(
  select id, name from  chinamap join t1 on id = t1.c2 where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;

explain (costs false) 
with recursive cte as (  
	select  ID,  PID,  NAME 
	from a
	where a.NAME = 'm'
	union all  
	select parent.ID, parent.PID, parent.NAME 
	from cte as child join  a as parent 
	on child.pid=parent.id
)
select * from cte order by ID;

with recursive cte as (
    select  ID,  PID,  NAME
    from a
    where a.NAME = 'm'
    union all
    select parent.ID, parent.PID, parent.NAME
    from cte as child join  a as parent
    on child.pid=parent.id
)
select * from cte order by ID;

explain (costs false) 
with recursive cte as (  
	select ID,PID,NAME
	from a
	where a.NAME = 'b'
	union all  
	select child.ID, child.PID, child.NAME
	from cte as parent join a as child 
	on child.pid=parent.id  
)  
select * from cte order by ID;

with recursive cte as (
    select ID,PID,NAME
    from a
    where a.NAME = 'b'
    union all
    select child.ID, child.PID, child.NAME
    from cte as parent join a as child
    on child.pid=parent.id
)
select * from cte order by ID;

explain (costs false) 
with recursive cte as (  
	select 
		ID,
		PID,
		NAME,
		1 as level 
	from
		a
	where
		a.NAME = 'm'
	union all  
	select
		parent.ID,
		parent.PID,
		parent.NAME,
		child.level+1
	from 
		cte as child 
	join
		a as parent
	on
		child.pid=parent.id and child.level < 3 
)  
select * from cte order by ID;

with recursive cte as (
    select
        ID,
        PID,
        NAME,
        1 as level
    from
        a
    where
        a.NAME = 'm'
    union all
    select
        parent.ID,
        parent.PID,
        parent.NAME,
        child.level+1
    from
        cte as child
    join
        a as parent
    on
        child.pid=parent.id and child.level < 3
)
select * from cte order by ID;

-----------
explain (costs false) 
with recursive cte as (  
select
	ID,
	PID,
	NAME,
	1 as level
from
	a
where
	a.NAME = 'b'
union all  
select
	child.ID,
	child.PID,
	child.NAME,
	parent.level+1
from
	cte as parent 
join
	a as child
on 
	child.pid=parent.id and parent.level < 3 
)  
select * from cte order by ID;

with recursive cte as (
select
    ID,
    PID,
    NAME,
    1 as level
from
    a
where
    a.NAME = 'b'
union all
select
    child.ID,
    child.PID,
    child.NAME,
    parent.level+1
from
    cte as parent
join
    a as child
on
    child.pid=parent.id and parent.level < 3
)
select * from cte order by ID;
--------------
explain (costs false) 
select
	b.NAME
from 
	b
where 
	b.ID in
	(
		with recursive cte as 
		(
			select 
							ID,
							PID,
							NAME
			from 
							a
			where 
							a.NAME = 'g'
			union all
			select 
							child.ID, 
							child.PID, 
							child.NAME 
			from 
							cte as parent 
			join 
							a as child
			on child.pid=parent.id 
		)
		select ID from cte
	) order by 1;

select
    b.NAME
from
    b
where
    b.ID in
    (
        with recursive cte as
        (
            select
                            ID,
                            PID,
                            NAME
            from
                            a
            where
                            a.NAME = 'g'
            union all
            select
                            child.ID,
                            child.PID,
                            child.NAME
            from
                            cte as parent
            join
                            a as child
            on child.pid=parent.id
        )
        select ID from cte
    ) order by 1;

------------
explain (costs false) 
WITH RECURSIVE  TABLE_COLUMN(T,B,C,D)
AS
(
	SELECT   area_code,belong_area_code,name,rnk
    FROM   area
    UNION ALL
    SELECT   area_code,b.T||'|'||a.area_code,b.C||a.name,rnk
    FROM   area a JOIN   TABLE_COLUMN b ON   a.belong_area_code=b.T
)
SELECT  T,B,C FROM  Table_Column
ORDER BY 1,2,3;

WITH RECURSIVE  TABLE_COLUMN(T,B,C,D)
AS
(
    SELECT   area_code,belong_area_code,name,rnk
    FROM   area
    UNION ALL
    SELECT   area_code,b.T||'|'||a.area_code,b.C||a.name,rnk
    FROM   area a JOIN   TABLE_COLUMN b ON   a.belong_area_code=b.T
)
SELECT  T,B,C FROM  Table_Column
ORDER BY 1,2,3;

/* recursive-cte关联外层 */
explain (costs false) select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME
        from b
        where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent
        on child.pid=parent.id
    )
    select NAME from cte
    where cte.ID % 2 = 0
    limit 1
) cName
from a
order by 1,2;

select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME
        from b
        where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent
        on child.pid=parent.id
    )
    select NAME from cte
    where cte.ID % 2 = 0
    limit 1
) cName
from a
order by 1,2;

/*
 * 多层stream
 * --------------------------------------------------------
 *   ->RecursiveUnion
 *      ->Scan
 *      ->Join
 *          ->Scan
 *          ->Streaming <<<
 *              ->Join
 *                  ->Streaming
 *                      ->Scan
 *                  ->WorkTableScan
 * --------------------------------------------------------
 */
explain (costs false) with recursive rq as
(
    select id, name from  chinamap where id = 11
    union all
    select origin.id, rq.name || ' > ' || origin.name
    from rq join chinamap origin on origin.pid = rq.id, t2
	where t2.c2 = rq.id
)
select * from rq order by 1;

with recursive rq as
(
    select id, name from  chinamap where id = 11
    union all
    select origin.id, rq.name || ' > ' || origin.name
    from rq join chinamap origin on origin.pid = rq.id, t2
	where t2.c2 = rq.id
)
select * from rq order by 1;

explain (costs false) select * from chinamap where pid = 11
union
select * from chinamap where id in
(
    with recursive rq as
    (   
        select * from chinamap where id = 110 
        union all 
        select origin.* from chinamap origin join rq on origin.pid = rq.id
    )   
     select id from rq
) order by id; 

select * from chinamap where pid = 11
union
select * from chinamap where id in
(
    with recursive rq as
    (   
        select * from chinamap where id = 110 
        union all 
        select origin.* from chinamap origin join rq on origin.pid = rq.id
    )   
     select id from rq
) order by id; 

explain (costs false) select * from 
(
    with recursive cte1 as
    (
        select  ID,  PID,  NAME
        from a
        where a.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME
        from cte1 as child join  a as parent
        on child.pid=parent.id
    )select * from cte1
)
union all
(
    with recursive cte2 as
    (
        select  ID,  PID,  NAME
        from a
        where a.NAME = 'b'
        union all
        select parent.ID, parent.PID, parent.NAME
        from cte2 as child join  a as parent
        on child.pid=parent.id
    ) select * from cte2
) order by id;

select * from 
(
    with recursive cte1 as
    (
        select  ID,  PID,  NAME
        from a
        where a.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME
        from cte1 as child join  a as parent
        on child.pid=parent.id
    )select * from cte1
)
union all
(
    with recursive cte2 as
    (
        select  ID,  PID,  NAME
        from a
        where a.NAME = 'b'
        union all
        select parent.ID, parent.PID, parent.NAME
        from cte2 as child join  a as parent
        on child.pid=parent.id
    ) select * from cte2
) order by id;



/*
 * 测试复制表replicate-plan场景
 */
/* a:b H:H */
explain (costs false)
with recursive cte as (
        select  ID,  PID,  NAME from a where a.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b as parent on child.pid=parent.id
)
select * from cte order by ID;

with recursive cte as (
        select  ID,  PID,  NAME from a where a.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b as parent on child.pid=parent.id
)
select * from cte order by ID;


/* a:b R:H */
explain (costs false)
with recursive cte as (
        select  ID,  PID,  NAME from a_rep where a_rep.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b as parent on child.pid=parent.id
)
select * from cte order by ID;
with recursive cte as (
        select  ID,  PID,  NAME from a_rep where a_rep.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b as parent on child.pid=parent.id
)
select * from cte order by ID;

/* a:b H:R */
explain (costs false)
with recursive cte as (
        select  ID,  PID,  NAME from a where a.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b_rep as parent on child.pid=parent.id
)
select * from cte order by ID;

with recursive cte as (
        select  ID,  PID,  NAME from a where a.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b_rep as parent on child.pid=parent.id
)
select * from cte order by ID;

/* a:b R:R */
explain (costs false)
with recursive cte as (
        select  ID,  PID,  NAME from a_rep where a_rep.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b_rep as parent on child.pid=parent.id
)
select * from cte order by ID;

with recursive cte as (
        select  ID,  PID,  NAME from a_rep where a_rep.NAME = 'm'
        union all
        select parent.ID, parent.PID, parent.NAME from cte as child join  b_rep as parent on child.pid=parent.id
)
select * from cte order by ID;

explain (costs false)
with recursive rq as
(
    select a.name address, b.name, a.id,a.pid
    from chinamap a,
    (
        with recursive rq as
        (
            select pid,id, name, mapid from  chinamap3
            union all
            select rq.pid,origin.id, rq.name || ' > ' || origin.name, origin.mapid
            from rq join chinamap3 origin on origin.pid = rq.id
        )select * from rq where pid is null
    ) b
    where a.id = b.mapid
    union all
    select chinamap.name, rq.name, rq.pid, chinamap.pid
    from rq ,chinamap
    where rq.pid=chinamap.id
)select address,name from rq order by address,name;

with recursive rq as
(
    select a.name address, b.name, a.id,a.pid
    from chinamap a,
    (
        with recursive rq as
        (
            select pid,id, name, mapid from  chinamap3
            union all
            select rq.pid,origin.id, rq.name || ' > ' || origin.name, origin.mapid
            from rq join chinamap3 origin on origin.pid = rq.id
        )select * from rq where pid is null
    ) b
    where a.id = b.mapid
    union all
    select chinamap.name, rq.name, rq.pid, chinamap.pid
    from rq ,chinamap
    where rq.pid=chinamap.id
)select address,name from rq order by address,name;

explain (costs false)
with recursive rq as
(
    select a.name address, b.name, a.id,a.pid
    from chinamap a,
    (
        with recursive rq as
        (
            select pid,id, name, mapid from  chinamap4
            union all
            select rq.pid,origin.id, rq.name || ' > ' || origin.name, origin.mapid
            from rq join chinamap4 origin on origin.pid = rq.id
        )select * from rq where pid is null
    ) b
    where a.id = b.mapid
    union all
    select chinamap.name, rq.name, rq.pid, chinamap.pid
    from rq ,chinamap
    where rq.pid=chinamap.id
)select address,name from rq order by address,name;

with recursive rq as
(
    select a.name address, b.name, a.id,a.pid
    from chinamap a,
    (
        with recursive rq as
        (
            select pid,id, name, mapid from  chinamap4
            union all
            select rq.pid,origin.id, rq.name || ' > ' || origin.name, origin.mapid
            from rq join chinamap4 origin on origin.pid = rq.id
        )select * from rq where pid is null
    ) b
    where a.id = b.mapid
    union all
    select chinamap.name, rq.name, rq.pid, chinamap.pid
    from rq ,chinamap
    where rq.pid=chinamap.id
)select address,name from rq order by address,name;

/* correlated subquery */
explain (costs false)
select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

explain (costs false)
select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
        where parent.id = a.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
        where parent.id = a.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

/* verify conflict dop lead to unshippable recursive plan */
/* correlated subquery */
explain (costs false)
select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

explain (costs false)
select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
        where parent.id = a.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
        where parent.id = a.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

explain (costs false)
select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
        where parent.id = a.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

select  a.ID,a.Name,
(
    with recursive cte as (
        select ID, PID, NAME from b where b.ID = a.ID
        union all
        select parent.ID,parent.PID,parent.NAME
        from cte as child join b as parent on child.pid=parent.id
        where parent.id = a.id
    )
    select NAME from cte limit 1
) cName
from
(
    select id, name, count(*) as cnt
    from a group by id,name
) a order by 1,2;

explain (costs false)
with recursive cte as (
    select ID, PID, NAME from b where b.ID = 5 
    union all
    select parent.ID,parent.PID,parent.NAME
    from cte as child join b as parent on child.pid=parent.id
    where parent.id <= 5 
),
tmp as (select * from cte)
select cte.NAME from cte join tmp on cte.id = tmp.id
where tmp.id in (select id from tmp) order by 1;

with recursive cte as (
    select ID, PID, NAME from b where b.ID = 5 
    union all
    select parent.ID,parent.PID,parent.NAME
    from cte as child join b as parent on child.pid=parent.id
    where parent.id <= 5 
),
tmp as (select * from cte)
select cte.NAME from cte join tmp on cte.id = tmp.id
where tmp.id in (select id from tmp) order by 1;

drop table if exists rec_tb4;
create table rec_tb4 (id int ,parentID int ,name varchar(100))  partition by range(parentID)
(
PARTITION P1 VALUES LESS THAN(2),
PARTITION P2 VALUES LESS THAN(8),
PARTITION P3 VALUES LESS THAN(16),
PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
insert into rec_tb4 values(1,0,'河南省');
insert into rec_tb4 values(2,1,'信阳市');
insert into rec_tb4 values(3,2,'淮滨县');
insert into rec_tb4 values(4,3,'芦集乡');
insert into rec_tb4 values(12,3,'邓湾乡');
insert into rec_tb4 values(13,3,'台头乡');
insert into rec_tb4 values(14,3,'谷堆乡');
insert into rec_tb4 values(8,2,'固始县');
insert into rec_tb4 values(9,8,'李店乡');
insert into rec_tb4 values(10,2,'息县');
insert into rec_tb4 values(11,10,'关店乡');

insert into rec_tb4 values(5,1,'安阳市');
insert into rec_tb4 values(6,5,'滑县');
insert into rec_tb4 values(7,6,'老庙乡');

insert into rec_tb4 values(15,1,'南阳市');
insert into rec_tb4 values(16,15,'方城县');

insert into rec_tb4 values(17,1,'驻马店市');
insert into rec_tb4 values(18,17,'正阳县');

with recursive cte as (select * from rec_tb4 where id<4 union all  select h.id,h.parentID,h.name from (with recursive cte as (select * from rec_tb4 where id<4 union all  select h.id,h.parentID,h.name from rec_tb4 h inner join cte c on h.id=c.parentID) SELECT id ,parentID,name from cte order by parentID) h inner join cte c on h.id=c.parentID) SELECT id ,parentID,name from cte order by parentID,1,2,3;

explain (costs false)
with recursive cte as (select * from rec_tb4 where id<4 union all  select h.id,h.parentID,h.name from (with recursive cte as (select * from rec_tb4 where id<4 union all  select h.id,h.parentID,h.name from rec_tb4 h inner join cte c on h.id=c.parentID) SELECT id ,parentID,name from cte order by parentID) h inner join cte  c on h.id=c.parentID) SELECT id ,parentID,name from cte order by parentID,1,2,3;

drop table rec_tb4;

drop table if exists test_perf3;
create table test_perf3
(
etl_tx_dt varchar2(8),
area_code varchar2(30) primary key,
area_name varchar2(360),
area_short_name varchar2(360),
local_name varchar2(240),
belong_area_code varchar2(30),
bank_level varchar2(24),
country_code varchar2(15),
part_code varchar2(15),
time_zone varchar2(27),
bank_code varchar2(30),
group_code varchar2(15),
mag_area_status varchar2(3),
mag_area_broad varchar2(3),
mag_area_grade varchar2(9)
)
;

explain (costs false)
with recursive cte1 as
(select area_code, belong_area_code, area_code as chain, 1 as level
from test_perf3
where area_code='100000'
union all
select h.area_code, h.belong_area_code, cast(cte1.chain||'>'||h.area_code as varchar2(30)),cte1.level+1
from cte1
join test_perf3 h
on h.belong_area_code=cte1.area_code and cte1.level <3 and h.area_code not in (select regexp_split_to_table(cte1.chain,'>'))
),
cte2 as (
select area_code, belong_area_code, area_code as chain, 1 as level
from cte1
where area_code='100000'
union all
select h.area_code, h.belong_area_code, cast(cte2.chain||'>'||h.area_code as varchar2(30)),cte2.level+1
from cte2
join cte1 h
on h.belong_area_code=cte2.area_code and cte2.level <3 and h.area_code not in (select regexp_split_to_table(cte2.chain,'>'))
)
select * from cte2;

with recursive cte1 as
(select area_code, belong_area_code, area_code as chain, 1 as level
from test_perf3
where area_code='100000'
union all
select h.area_code, h.belong_area_code, cast(cte1.chain||'>'||h.area_code as varchar2(30)),cte1.level+1
from cte1
join test_perf3 h
on h.belong_area_code=cte1.area_code and cte1.level <3 and h.area_code not in (select regexp_split_to_table(cte1.chain,'>'))
),
cte2 as (
select area_code, belong_area_code, area_code as chain, 1 as level
from cte1
where area_code='100000'
union all
select h.area_code, h.belong_area_code, cast(cte2.chain||'>'||h.area_code as varchar2(30)),cte2.level+1
from cte2
join cte1 h
on h.belong_area_code=cte2.area_code and cte2.level <3 and h.area_code not in (select regexp_split_to_table(cte2.chain,'>'))
)
select * from cte2;
drop table test_perf3;


reset explain_perf_mode;

/*update-delte语句中含有with recursive,且with recursive引用目标表*/
drop table if exists bom_ptbl;
drop table if exists bom_locator;

create table bom_ptbl(
		id varchar(3) , 
		pid varchar(3) , 
		name varchar(10)
) with (orientation=row, compression=no) ;
create table bom_locator(
		zone varchar(3)
) with (orientation=row, compression=no) ;

insert into bom_ptbl values('002' , 0 , '浙江省'); 
insert into bom_ptbl values('001' , 0 , '广东省'); 
insert into bom_ptbl values('003' , '002' , '衢州市');  
insert into bom_ptbl values('004' , '002' , '杭州市') ; 
insert into bom_ptbl values('005' , '002' , '湖州市');  
insert into bom_ptbl values('006' , '002' , '嘉兴市') ; 
insert into bom_ptbl values('007' , '002' , '宁波市');  
insert into bom_ptbl values('008' , '002' , '绍兴市') ; 
insert into bom_ptbl values('009' , '002' , '台州市');  
insert into bom_ptbl values('010' , '002' , '温州市') ; 
insert into bom_ptbl values('011' , '002' , '丽水市');  
insert into bom_ptbl values('012' , '002' , '金华市') ; 
insert into bom_ptbl values('013' , '002' , '舟山市');  
insert into bom_ptbl values('014' , '004' , '上城区') ; 
insert into bom_ptbl values('015' , '004' , '下城区');  
insert into bom_ptbl values('016' , '004' , '拱墅区') ; 
insert into bom_ptbl values('017' , '004' , '余杭区') ; 
insert into bom_ptbl values('018' , '011' , '金东区') ; 
insert into bom_ptbl values('019' , '001' , '广州市') ; 
insert into bom_ptbl values('020' , '001' , '深圳市') ;
insert into bom_locator values('002');
insert into bom_locator values('007');

explain (costs off, verbose on) update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

explain (costs off, verbose on) delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

insert into bom_ptbl values('002' , 0 , '浙江省'); 
insert into bom_ptbl values('001' , 0 , '广东省'); 
insert into bom_ptbl values('003' , '002' , '衢州市');  
insert into bom_ptbl values('004' , '002' , '杭州市') ; 
insert into bom_ptbl values('005' , '002' , '湖州市');  
insert into bom_ptbl values('006' , '002' , '嘉兴市') ; 
insert into bom_ptbl values('007' , '002' , '宁波市');  
insert into bom_ptbl values('008' , '002' , '绍兴市') ; 
insert into bom_ptbl values('009' , '002' , '台州市');  
insert into bom_ptbl values('010' , '002' , '温州市') ; 
insert into bom_ptbl values('011' , '002' , '丽水市');  
insert into bom_ptbl values('012' , '002' , '金华市') ; 
insert into bom_ptbl values('013' , '002' , '舟山市');  
insert into bom_ptbl values('014' , '004' , '上城区') ; 
insert into bom_ptbl values('015' , '004' , '下城区');  
insert into bom_ptbl values('016' , '004' , '拱墅区') ; 
insert into bom_ptbl values('017' , '004' , '余杭区') ; 
insert into bom_ptbl values('018' , '011' , '金东区') ; 
insert into bom_ptbl values('019' , '001' , '广州市') ; 
insert into bom_ptbl values('020' , '001' , '深圳市') ;

explain (costs off, verbose on) update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

explain (costs off, verbose on) delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

drop table if exists bom_ptbl;
drop table if exists bom_locator;

create table bom_ptbl(
		id varchar(3) , 
		pid varchar(3) , 
		name varchar(10)
) with (orientation=column, compression=low) ;
create table bom_locator(
		zone varchar(3)
) with (orientation=column, compression=low) ;

insert into bom_ptbl values('002' , 0 , '浙江省'); 
insert into bom_ptbl values('001' , 0 , '广东省'); 
insert into bom_ptbl values('003' , '002' , '衢州市');  
insert into bom_ptbl values('004' , '002' , '杭州市') ; 
insert into bom_ptbl values('005' , '002' , '湖州市');  
insert into bom_ptbl values('006' , '002' , '嘉兴市') ; 
insert into bom_ptbl values('007' , '002' , '宁波市');  
insert into bom_ptbl values('008' , '002' , '绍兴市') ; 
insert into bom_ptbl values('009' , '002' , '台州市');  
insert into bom_ptbl values('010' , '002' , '温州市') ; 
insert into bom_ptbl values('011' , '002' , '丽水市');  
insert into bom_ptbl values('012' , '002' , '金华市') ; 
insert into bom_ptbl values('013' , '002' , '舟山市');  
insert into bom_ptbl values('014' , '004' , '上城区') ; 
insert into bom_ptbl values('015' , '004' , '下城区');  
insert into bom_ptbl values('016' , '004' , '拱墅区') ; 
insert into bom_ptbl values('017' , '004' , '余杭区') ; 
insert into bom_ptbl values('018' , '011' , '金东区') ; 
insert into bom_ptbl values('019' , '001' , '广州市') ; 
insert into bom_ptbl values('020' , '001' , '深圳市') ;
insert into bom_locator values('002');
insert into bom_locator values('007');

explain (costs off, verbose on) update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

explain (costs off, verbose on) delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

insert into bom_ptbl values('002' , 0 , '浙江省'); 
insert into bom_ptbl values('001' , 0 , '广东省'); 
insert into bom_ptbl values('003' , '002' , '衢州市');  
insert into bom_ptbl values('004' , '002' , '杭州市') ; 
insert into bom_ptbl values('005' , '002' , '湖州市');  
insert into bom_ptbl values('006' , '002' , '嘉兴市') ; 
insert into bom_ptbl values('007' , '002' , '宁波市');  
insert into bom_ptbl values('008' , '002' , '绍兴市') ; 
insert into bom_ptbl values('009' , '002' , '台州市');  
insert into bom_ptbl values('010' , '002' , '温州市') ; 
insert into bom_ptbl values('011' , '002' , '丽水市');  
insert into bom_ptbl values('012' , '002' , '金华市') ; 
insert into bom_ptbl values('013' , '002' , '舟山市');  
insert into bom_ptbl values('014' , '004' , '上城区') ; 
insert into bom_ptbl values('015' , '004' , '下城区');  
insert into bom_ptbl values('016' , '004' , '拱墅区') ; 
insert into bom_ptbl values('017' , '004' , '余杭区') ; 
insert into bom_ptbl values('018' , '011' , '金东区') ; 
insert into bom_ptbl values('019' , '001' , '广州市') ; 
insert into bom_ptbl values('020' , '001' , '深圳市') ;

explain (costs off, verbose on) update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

update bom_ptbl set name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) where bom_ptbl.name is not null;

explain (costs off, verbose on) delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

delete bom_ptbl where name =
(
	with recursive cte as
	(
	select a.id, a.name, a.pid from bom_ptbl a where a.id = bom_ptbl.id
	union all
	select b.id, b.name, b.pid from cte,bom_ptbl b where cte.id = b.pid
	) 
	select cte.name from cte left join bom_locator on cte.id = bom_locator.zone 
	order by cte.id limit 1
) and bom_ptbl.name is not null;

drop table if exists bom_ptbl;
drop table if exists bom_locator;

reset current_schema;
