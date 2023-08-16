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