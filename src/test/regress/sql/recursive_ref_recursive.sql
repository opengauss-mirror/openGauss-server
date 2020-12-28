create schema recursive_ref_recursive;
set search_path to recursive_ref_recursive;

drop table if exists rec_tb1;
create table rec_tb1 (id int ,parentID int ,name varchar(100));
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

drop table if exists rec_tb2;
create table rec_tb2 (id int ,parentID int ,name varchar(100)) ;
insert into rec_tb2 values(1,0,'河南省');
insert into rec_tb2 values(2,1,'信阳市');
insert into rec_tb2 values(3,2,'淮滨县');
insert into rec_tb2 values(4,3,'芦集乡');
insert into rec_tb2 values(12,3,'邓湾乡');
insert into rec_tb2 values(13,3,'台头乡');
insert into rec_tb2 values(14,3,'谷堆乡');
insert into rec_tb2 values(8,2,'固始县');
insert into rec_tb2 values(9,8,'李店乡');
insert into rec_tb2 values(10,2,'息县');
insert into rec_tb2 values(11,10,'关店乡');
insert into rec_tb2 values(5,1,'安阳市');
insert into rec_tb2 values(6,5,'滑县');
insert into rec_tb2 values(7,6,'老庙乡');
insert into rec_tb2 values(15,1,'南阳市');
insert into rec_tb2 values(16,15,'方城县');
insert into rec_tb2 values(17,1,'驻马店市');
insert into rec_tb2 values(18,17,'正阳县');

drop table if exists rec_tb3;
create table rec_tb3 (id int ,parentID int ,name varchar(100)) ;
insert into rec_tb3 values(1,0,'河南省');
insert into rec_tb3 values(2,1,'信阳市');
insert into rec_tb3 values(3,2,'淮滨县');
insert into rec_tb3 values(4,3,'芦集乡');
insert into rec_tb3 values(12,3,'邓湾乡');
insert into rec_tb3 values(13,3,'台头乡');
insert into rec_tb3 values(14,3,'谷堆乡');
insert into rec_tb3 values(8,2,'固始县');
insert into rec_tb3 values(9,8,'李店乡');
insert into rec_tb3 values(10,2,'息县');
insert into rec_tb3 values(11,10,'关店乡');
insert into rec_tb3 values(5,1,'安阳市');
insert into rec_tb3 values(6,5,'滑县');
insert into rec_tb3 values(7,6,'老庙乡');
insert into rec_tb3 values(15,1,'南阳市');
insert into rec_tb3 values(16,15,'方城县');
insert into rec_tb3 values(17,1,'驻马店市');
insert into rec_tb3 values(18,17,'正阳县');

drop table if exists rec_tb4;
create table rec_tb4 (id int ,parentID int ,name varchar(100))

partition by range(parentID)
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

--
----case: 1
--
explain(costs off)
with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join (
    with recursive cte as 
    (
        select * from cte1
        union all 
        select distinct h.*  from rec_tb1 h inner join cte c on h.parentID = c.col1
    )
    select * from cte
) m
on e.col2 = m.col1 join cte n on n.id =m.col2
order by 1, 2;

with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join (
    with recursive cte as 
    (
        select * from cte1
        union all 
        select distinct h.*  from rec_tb1 h inner join cte c on h.parentID = c.col1
    )
    select * from cte
) m
on e.col2 = m.col1 join cte n on n.id =m.col2
order by 1, 2;


--
----case: 2
--
explain(costs off)
with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join (
    with recursive cte2 as 
    (
        select * from cte1
        union all 
        select distinct h.*  from rec_tb1 h inner join cte2 c on h.parentID = c.col1
    )
    select * from cte2
) m
on e.col2 = m.col1 join cte n on n.id =m.col2
order by 1, 2;

with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join (
    with recursive cte2 as 
    (
        select * from cte1
        union all 
        select distinct h.*  from rec_tb1 h inner join cte2 c on h.parentID = c.col1
    )
    select * from cte2
) m
on e.col2 = m.col1 join cte n on n.id =m.col2
order by 1, 2;

--
---- case:3
--
explain(costs off)
with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join cte1 m on e.col2 = m.col1 
join cte n on n.id =e.col2
order by 1, 2;

with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join cte1 m on e.col2 = m.col1 
join cte n on n.id =e.col2
order by 1, 2;

--
---- case:4
--
explain(costs off)
with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join cte1 m on e.col2 = m.col1 
order by 1, 2;

with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join cte1 m on e.col2 = m.col1 
order by 1, 2;

--
---- case:5
--
explain(costs off)
with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join cte1 m on e.col2 = m.col1 
join cte n on n.id =e.col2
order by 1, 2;

with recursive cte as
(
    select id, parentID, substr(name, 1) as name from (with tmp as (select * from rec_tb1 where id<4) select *from tmp)
    intersect all
    select id, parentID, name from (with tmp as (select * from rec_tb4 where id<4) select * from tmp)
    union all
    select id, parentID, name from (with tmp as (select * from rec_tb3 where id<4) select * from tmp)   
    union all
    select a.* from rec_tb4 a join cte b on a.id = b.parentID join rec_tb3 c on c.parentid=b.id where c.id<4
),
cte1 as
(
    select distinct b.id col1, b.parentID col2, b.name from rec_tb4 a join rec_tb2 b on a.id = b.id group by col1, col2, b.name
    union all
    select d.col2, c.id, d.name from rec_tb1 c join cte1 d on substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 where d.col1 in (select id from cte)group by c.id, d.col2, d.name
)
select 
    e.*
from cte1 e
join cte1 m on e.col2 = m.col1 
join cte n on n.id =e.col2
order by 1, 2;

--
---- case6
--
explain(costs off)
WITH RECURSIVE cte AS
(
    SELECT id, parentID, substr(name, 1) AS name 
    FROM rec_tb1 
    WHERE id<4

    UNION ALL

    SELECT a.* 
    FROM rec_tb4 a 
    JOIN cte b ON a.id = b.parentID 
    JOIN rec_tb3 c ON c.parentid=b.id 
    WHERE c.id<4
)
select * from cte t1 join cte t2 on t1.id = t2.id order by 1, 2;

WITH RECURSIVE cte AS
(
    SELECT id, parentID, substr(name, 1) AS name 
    FROM rec_tb1 
    WHERE id<4

    UNION ALL

    SELECT a.* 
    FROM rec_tb4 a 
    JOIN cte b ON a.id = b.parentID 
    JOIN rec_tb3 c ON c.parentid=b.id 
    WHERE c.id<4
)
select * from cte t1 join cte t2 on t1.id = t2.id order by 1, 2;

--
---- case6
--
explain(costs off)
WITH RECURSIVE cte AS
(
    SELECT id, parentID, substr(name, 1) AS name 
    FROM rec_tb1 
    WHERE id<4

    UNION ALL

    SELECT a.* 
    FROM rec_tb4 a 
    JOIN cte b ON a.id = b.parentID 
    JOIN rec_tb3 c ON c.parentid=b.id 
    WHERE c.id<4
),

cte1 AS
(
    select * from cte
),

cte2 AS
(
    SELECT distinct b.id col1, b.parentID col2, b.name 
    FROM rec_tb4 a JOIN rec_tb2 b ON a.id = b.id 
    GROUP BY col1, col2, b.name

    UNION ALL

    SELECT d.col2, c.id, d.name FROM rec_tb1 c 
    JOIN cte2 d ON substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 
    WHERE d.col1 in (SELECT id FROM cte1) 
    GROUP BY c.id, d.col2, d.name
)
SELECT 
    e.*
FROM cte2 e
JOIN cte2 m ON e.col2 = m.col1 
JOIN cte n ON n.id =e.col2
ORDER BY 1, 2;

WITH RECURSIVE cte AS
(
    SELECT id, parentID, substr(name, 1) AS name 
    FROM rec_tb1 
    WHERE id<4

    UNION ALL

    SELECT a.* 
    FROM rec_tb4 a 
    JOIN cte b ON a.id = b.parentID 
    JOIN rec_tb3 c ON c.parentid=b.id 
    WHERE c.id<4
),

cte1 AS
(
    select * from cte
),

cte2 AS
(
    SELECT distinct b.id col1, b.parentID col2, b.name 
    FROM rec_tb4 a JOIN rec_tb2 b ON a.id = b.id 
    GROUP BY col1, col2, b.name

    UNION ALL

    SELECT d.col2, c.id, d.name FROM rec_tb1 c 
    JOIN cte2 d ON substr(c.parentID, 1) + 1 = substr(d.col2, 1) + 1 
    WHERE d.col1 in (SELECT id FROM cte1) 
    GROUP BY c.id, d.col2, d.name
)
SELECT 
    e.*
FROM cte2 e
JOIN cte2 m ON e.col2 = m.col1 
JOIN cte n ON n.id =e.col2
ORDER BY 1, 2;

--
---- case7
--
explain(costs off)
WITH RECURSIVE cte as (
     SELECT distinct a.* FROM rec_tb1 a  JOIN rec_tb2 b ON a.id=b.parentID AND b.name not in (SELECT name FROM rec_tb3 c WHERE c.id=a.parentID AND 
     c.name is not null  ) AND a.id<4
     UNION ALL
     SELECT d.* FROM cte e JOIN rec_tb3 d ON d.id=e.parentID AND d.parentID in (SELECT id FROM rec_tb1 WHERE e.id=rec_tb1.id)  AND e.name like '%市%'
 ) SELECT * FROM cte ORDER BY 1,2,3;
 
WITH RECURSIVE cte as (
     SELECT distinct a.* FROM rec_tb1 a  JOIN rec_tb2 b ON a.id=b.parentID AND b.name not in (SELECT name FROM rec_tb3 c WHERE c.id=a.parentID AND 
     c.name is not null  ) AND a.id<4
     UNION ALL
     SELECT d.* FROM cte e JOIN rec_tb3 d ON d.id=e.parentID AND d.parentID in (SELECT id FROM rec_tb1 WHERE e.id=rec_tb1.id)  AND e.name like '%市%'
 ) SELECT * FROM cte ORDER BY 1,2,3;
 
explain(costs off)
WITH RECURSIVE tmp as                                                                                               
(
	SELECT id, parentid, name, substr(name, 5) FROM (SELECT id,parentid ,name FROM rec_tb1)                                                                    
	UNION ALL                                                                                                         
	SELECT tmp.id, rec_tb3.parentid, tmp.name, substr(tmp.name, 5) FROM rec_tb3 JOIN tmp ON tmp.parentid = rec_tb3.id where (1,3,1) in 
	(SELECT id, parentid, ( (SELECT id FROM rec_tb2 where rec_tb4.id = rec_tb2.parentid   ORDER BY 1 limit 1)) i_name FROM rec_tb4)
 ) SELECT * FROM tmp  JOIN tmp tmp2 ON tmp.parentid=tmp2.id WHERE tmp.id in (1,2,3,4,5,6,7,8) ORDER BY 1,2,3,4,5,6,7,8;
 
WITH RECURSIVE tmp as                                                                                               
(
	SELECT id, parentid, name, substr(name, 5) FROM (SELECT id,parentid ,name FROM rec_tb1)                                                                    
	UNION ALL                                                                                                         
	SELECT tmp.id, rec_tb3.parentid, tmp.name, substr(tmp.name, 5) FROM rec_tb3 JOIN tmp ON tmp.parentid = rec_tb3.id where (1,3,1) in 
	(SELECT id, parentid, ( (SELECT id FROM rec_tb2 where rec_tb4.id = rec_tb2.parentid   ORDER BY 1 limit 1)) i_name FROM rec_tb4)
 ) SELECT * FROM tmp  JOIN tmp tmp2 ON tmp.parentid=tmp2.id WHERE tmp.id in (1,2,3,4,5,6,7,8) ORDER BY 1,2,3,4,5,6,7,8;
 
--
--
drop table if exists rec_tb1;
create table rec_tb1 (id int ,parentID int ,name varchar(100))  partition by range(parentID)
(
PARTITION P1 VALUES LESS THAN(2),
PARTITION P2 VALUES LESS THAN(8),
PARTITION P3 VALUES LESS THAN(16),
PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
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

drop table if exists rec_tb2;
create table rec_tb2 (id int ,parentID int ,name varchar(100))  partition by range(parentID)
(
PARTITION P1 VALUES LESS THAN(2),
PARTITION P2 VALUES LESS THAN(8),
PARTITION P3 VALUES LESS THAN(16),
PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
insert into rec_tb2 values(1,0,'河南省');
insert into rec_tb2 values(2,1,'信阳市');
insert into rec_tb2 values(3,2,'淮滨县');
insert into rec_tb2 values(4,3,'芦集乡');
insert into rec_tb2 values(12,3,'邓湾乡');
insert into rec_tb2 values(13,3,'台头乡');
insert into rec_tb2 values(14,3,'谷堆乡');
insert into rec_tb2 values(8,2,'固始县');
insert into rec_tb2 values(9,8,'李店乡');
insert into rec_tb2 values(10,2,'息县');
insert into rec_tb2 values(11,10,'关店乡');
insert into rec_tb2 values(5,1,'安阳市');
insert into rec_tb2 values(6,5,'滑县');
insert into rec_tb2 values(7,6,'老庙乡');
insert into rec_tb2 values(15,1,'南阳市');
insert into rec_tb2 values(16,15,'方城县');
insert into rec_tb2 values(17,1,'驻马店市');
insert into rec_tb2 values(18,17,'正阳县');

explain(verbose on, costs off)
with recursive tmp as  (
	select id, parentid, name, substr(name, 5) 
		from rec_tb1 where id>4   
	union ALL   
	select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
		from rec_tb2  
		inner join 
		tmp 
		on tmp.parentid = rec_tb2.id ), tmp2 
			AS (select id, parentid, name, substr(name, 5) name1     from tmp) 
	select * from tmp,tmp2 where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2)  order by tmp.parentid,1,2,3,4,5,6,7,8;
with recursive tmp as  (
        select id, parentid, name, substr(name, 5)
                from rec_tb1 where id>4
        union ALL
        select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
                from rec_tb2
                inner join
                tmp
                on tmp.parentid = rec_tb2.id ), tmp2
                        AS (select id, parentid, name, substr(name, 5) name1     from tmp)
        select count(*) from tmp,tmp2     where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2);

explain(verbose on, costs off)
with recursive tmp as  (
        select id, parentid, name, substr(name, 5)
                from rec_tb1 where id>4
        union ALL
        select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
                from rec_tb2
                inner join
                tmp
                on tmp.parentid = rec_tb2.id ), tmp2
                        AS (select id, parentid, name, substr(name, 5) name1     from tmp)
        select * from tmp,tmp2 where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2)  order by tmp.parentid,1,2,3,4,5,6,7,8;
with recursive tmp as  (
        select id, parentid, name, substr(name, 5)
                from rec_tb1 where id>4
        union ALL
        select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
                from rec_tb2
                inner join
                tmp
                on tmp.parentid = rec_tb2.id ), tmp2
                        AS (select id, parentid, name, substr(name, 5) name1     from tmp)
        select count(*) from tmp,tmp2     where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2);

drop table if exists rec_tb1;
create table rec_tb1 (id int ,parentID int ,name varchar(100)) ;
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

drop table if exists rec_tb2;
create table rec_tb2 (id int ,parentID int ,name varchar(100)) ;
insert into rec_tb2 values(1,0,'河南省');
insert into rec_tb2 values(2,1,'信阳市');
insert into rec_tb2 values(3,2,'淮滨县');
insert into rec_tb2 values(4,3,'芦集乡');
insert into rec_tb2 values(12,3,'邓湾乡');
insert into rec_tb2 values(13,3,'台头乡');
insert into rec_tb2 values(14,3,'谷堆乡');
insert into rec_tb2 values(8,2,'固始县');
insert into rec_tb2 values(9,8,'李店乡');
insert into rec_tb2 values(10,2,'息县');
insert into rec_tb2 values(11,10,'关店乡');
insert into rec_tb2 values(5,1,'安阳市');
insert into rec_tb2 values(6,5,'滑县');
insert into rec_tb2 values(7,6,'老庙乡');
insert into rec_tb2 values(15,1,'南阳市');
insert into rec_tb2 values(16,15,'方城县');
insert into rec_tb2 values(17,1,'驻马店市');
insert into rec_tb2 values(18,17,'正阳县');

explain(verbose on, costs off)
with recursive tmp as  (
        select id, parentid, name, substr(name, 5)
                from rec_tb1 where id>4
        union ALL
        select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
                from rec_tb2
                inner join
                tmp
                on tmp.parentid = rec_tb2.id ), tmp2
                        AS (select id, parentid, name, substr(name, 5) name1     from tmp)
        select * from tmp,tmp2 where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2)  order by tmp.parentid,1,2,3,4,5,6,7,8;
with recursive tmp as  (
        select id, parentid, name, substr(name, 5)
                from rec_tb1 where id>4
        union ALL
        select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
                from rec_tb2
                inner join
                tmp
                on tmp.parentid = rec_tb2.id ), tmp2
                        AS (select id, parentid, name, substr(name, 5) name1     from tmp)
        select count(*) from tmp,tmp2     where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2);

explain(verbose on, costs off)
with recursive tmp as  (
        select id, parentid, name, substr(name, 5)
                from rec_tb1 where id>4
        union ALL
        select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
                from rec_tb2
                inner join
                tmp
                on tmp.parentid = rec_tb2.id ), tmp2
                        AS (select id, parentid, name, substr(name, 5) name1     from tmp)
        select * from tmp,tmp2 where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2)  order by tmp.parentid,1,2,3,4,5,6,7,8;
with recursive tmp as  (
        select id, parentid, name, substr(name, 5)
                from rec_tb1 where id>4
        union ALL
        select tmp.id, rec_tb2.parentid, tmp.name, substr(tmp.name, 5)
                from rec_tb2
                inner join
                tmp
                on tmp.parentid = rec_tb2.id ), tmp2
                        AS (select id, parentid, name, substr(name, 5) name1     from tmp)
        select count(*) from tmp,tmp2     where  tmp.parentid = tmp2.parentid  and tmp2.id not in (select parentid from tmp2);

reset search_path;
drop schema recursive_ref_recursive cascade;