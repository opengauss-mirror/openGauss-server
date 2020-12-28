set current_schema=rq_cstore;
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

reset explain_perf_mode;
reset current_schema;

