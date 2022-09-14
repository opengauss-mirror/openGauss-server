--prepare
DROP SCHEMA subpartition_truncate CASCADE;
CREATE SCHEMA subpartition_truncate;
SET CURRENT_SCHEMA TO subpartition_truncate;

--truncate partition/subpartition
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201902', '2', '1', 1);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
insert into list_list values('201903', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
select * from list_list;

select * from list_list partition (p_201901);
alter table list_list truncate partition p_201901;
select * from list_list partition (p_201901);

select * from list_list partition (p_201902);
alter table list_list truncate partition p_201902;
select * from list_list partition (p_201902);
select * from list_list;

insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201902', '2', '1', 1);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
insert into list_list values('201903', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);

select * from list_list subpartition (p_201901_a);
alter table list_list truncate subpartition p_201901_a;
select * from list_list subpartition (p_201901_a);

select * from list_list subpartition (p_201901_b);
alter table list_list truncate subpartition p_201901_b;
select * from list_list subpartition (p_201901_b);

select * from list_list subpartition (p_201902_a);
alter table list_list truncate subpartition p_201902_a;
select * from list_list subpartition (p_201902_a);

select * from list_list subpartition (p_201902_b);
alter table list_list truncate subpartition p_201902_b;
select * from list_list subpartition (p_201902_b);

select * from list_list;

drop table list_list;

drop table if exists subpar_range_list_tb;
create table subpar_range_list_tb(
c1 int default floor(random()*1000),
c2 bigint not null unique,
c3 numeric,
c4 varchar(100))
with(parallel_workers=4)partition by range(c2) subpartition by list(c4)
(partition p1 values less than(50)(
subpartition p1_1 values('a','b'),
subpartition p1_2 values('c','e','s')),
partition p2 values less than(100)(
subpartition p2_1 values('a','b','c'),
subpartition p2_2 values('d','e','s'))
);

insert into subpar_range_list_tb(c2,c3,c4) values(2,3,'c');

create index ind_subpar_rl_22 on subpar_range_list_tb(c2);
alter table subpar_range_list_tb truncate subpartition p1_2 update global index;
insert into subpar_range_list_tb(c2,c3,c4) values(2,3,'c');
alter table subpar_range_list_tb truncate subpartition p1_2 update global index;
alter table subpar_range_list_tb truncate subpartition p1_2 update global index;
alter table subpar_range_list_tb truncate subpartition p1_2 update global index;
alter table subpar_range_list_tb truncate subpartition p1_2 update global index;
alter table subpar_range_list_tb truncate subpartition p1_2 update global index;
alter table subpar_range_list_tb truncate subpartition p1_2 update global index;
insert into subpar_range_list_tb(c2,c3,c4) values(2,3,'c');
drop table if exists subpar_range_list_tb;
DROP SCHEMA subpartition_truncate CASCADE;
