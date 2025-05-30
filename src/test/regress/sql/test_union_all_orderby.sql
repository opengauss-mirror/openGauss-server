set enable_union_all_subquery_orderby=on;
create table tb1(a int);
insert into tb1 values(1);
insert into tb1 values(3);
insert into tb1 values(2);
create table tb2(a int);
insert into tb2 values(5);
insert into tb2 values(4);
create table tb3(a int);
insert into tb3 values(7);
insert into tb3 values(6);

(select * from tb1 order by a) union all (select * from tb2 order by a);
(select * from tb1 order by a) union all (select * from tb2 order by a desc);
(select * from tb1 order by a) union all (select * from tb2 order by a) union all (select * from tb3 order by a);

reset enable_union_all_subquery_orderby;

-- test union all with none targetlist
drop table if exists union_pseudo_tlist cascade;
create table union_pseudo_tlist(id int);
insert into union_pseudo_tlist values(1);
create index on union_pseudo_tlist(id);
set enable_seqscan=off;

select count(*) from union_pseudo_tlist where now() is not null and id is not null;

explain(costs off) select count(*) from (select * from union_pseudo_tlist union all select * from union_pseudo_tlist) where now() is not null;
select count(*) from (select * from union_pseudo_tlist union all select * from union_pseudo_tlist) where now() is not null;
select count(*) from (select * from union_pseudo_tlist union all select * from union_pseudo_tlist) where now() is not null and id is not null;

-- use pbe
prepare test_index_pseudo as select count(*) from (select * from union_pseudo_tlist union all select * from union_pseudo_tlist) where id in (1) and id=$1;
execute test_index_pseudo(1);

drop table if exists union_pseudo_tlist cascade;
reset enable_seqscan;
