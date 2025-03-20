drop table if exists abs_tab;
CREATE TABLE abs_tab
( prod_id NUMBER(6)
    , cust_id NUMBER
    , time_id double precision
    , channel_id CHAR(1)
    , promo_id NUMBER(6)
    , quantity_sold NUMBER(3)
    , amount_sold NUMBER(10,2)
)
PARTITION BY RANGE (abs(time_id))
( PARTITION p0 VALUES LESS THAN (abs('-5')),
  PARTITION p1 VALUES LESS THAN (abs('-20'))
);

insert into abs_tab values(1, 1, '1', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '-2', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '-3', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '4', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '-9', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '11', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '-19', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '-18', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '-15', 'a', 1, 1, 1);
insert into abs_tab values(1, 1, '5', 'a', 1, 1, 1);

alter table abs_tab split partition p1 at (abs('-10')) into (partition p1_1, partition p1_2);
select * from abs_tab partition (p0);
select * from abs_tab partition (p1_1);
select * from abs_tab partition (p1_2);
drop table abs_tab;
