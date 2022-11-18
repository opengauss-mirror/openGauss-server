




drop table if exists add_column_test_default_val;
create table add_column_test_default_val (a integer , b text);
insert into add_column_test_default_val (a,b) values (generate_series(1,10),'test');

alter table add_column_test_default_val add column c integer  DEFAULT 3 ,add column d numeric(8,2) default 5555,add column e char(10) default '12345' ,alter column b type varchar;

select * from add_column_test_default_val order by a desc limit 5;