create table pt1(a int primary key, b int)with(storage_type=ustore) partition by range(a)(partition p1 values less than(10), partition p2 values less than(100)) ;
create table pt2(a int references pt1, b int)with(storage_type=ustore) partition by range(a)(partition p1 values less than(10), partition p2 values less than(100)) ;

insert into pt2 values(1,1);
insert into pt1 values(1,1);
insert into pt2 values(1,1);
insert into pt2 values(2,2);

update pt2 set a=2,b=2;

insert into pt1 values(2,2);
update pt2 set a=2,b=2;


insert into pt2 values(3,3);
update pt1 set a=3,b=3 where a=1;

insert into pt2 values(3,3);

delete from pt1;

insert into pt1 values(4,4);
delete from pt1 where a=4;


insert into pt1 values(20,20);
insert into pt2 values(20,20);

delete from pt2;
delete from pt1;

drop table pt2 cascade;
drop table pt1 cascade;