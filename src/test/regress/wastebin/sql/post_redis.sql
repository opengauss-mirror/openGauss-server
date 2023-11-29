---select to verify
select count(*) from redistable_001;
create table redistable_002(id int);
insert into redistable_002 values(123);
select * from redistable_002;

---check group change
select group_name,in_redistribution from pgxc_group;
