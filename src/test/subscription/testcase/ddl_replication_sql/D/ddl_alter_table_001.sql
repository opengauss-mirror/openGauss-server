create table altertable_rangeparttable
(
	c1 int,
	c2 float,
	c3 real,
	c4 text
) 
partition by range (c1, c2, c3, c4)
(
	partition altertable_rangeparttable_p1 values less than (10, 10.00, 19.156, 'h'),
	partition altertable_rangeparttable_p2 values less than (20, 20.89, 23.75, 'k'),
	partition altertable_rangeparttable_p3 values less than (30, 30.45, 32.706, 's')
);

alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (36, 45.25, 37.39, 'u');

create table altertable_rangeparttable2
(
	c1 int,
	c2 float,
	c3 real,
	c4 text
) 
partition by range (abs(c1))
(
	partition altertable_rangeparttable_p1 values less than (10),
	partition altertable_rangeparttable_p2 values less than (20),
	partition altertable_rangeparttable_p3 values less than (30)
);
alter table altertable_rangeparttable2 add partition altertable_rangeparttable_p4 values less than (36);


CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);

alter table range_range add partition p_202001 values less than ('202002') (subpartition p_202001_a values less than('2') , subpartition p_202001_b values less than('3') );

-- comes from function_get_table_def.sql
create table table_range4 (id int primary key, a date, b varchar)
partition by range (id)
(
    partition table_range4_p1 start (10) end (40) every (10),
    partition table_range4_p2 end (70),
    partition table_range4_p3 start (70),
    partition table_range4_p4 start (100) end (150) every (20)
);

alter table table_range4 add partition table_range4_p5 start (150) end (300) every (20);
alter table table_range4 add partition table_range4_p6 values less than (310), add partition table_range4_p7 values less than (320);

create table table_interval1 (id int, a date, b varchar)
partition by range (a)
interval ('1 day')
(
    partition table_interval1_p1 values less than('2020-03-01'),
    partition table_interval1_p2 values less than('2020-05-01'),
    partition table_interval1_p3 values less than('2020-07-01'),
    partition table_interval1_p4 values less than(maxvalue)
);
alter table table_interval1 add partition table_interval1_p5 start ('2020-08-01') end ('2020-09-01');

create table table_list1 (id int, a date, b varchar)
partition by list (id)
(
    partition table_list1_p1 values (1, 2, 3, 4),
    partition table_list1_p2 values (5, 6, 7, 8),
    partition table_list1_p3 values (9, 10, 11, 12)
);
alter table table_list1 add partition table_list1_p4 values (13, 14, 15, 16);
alter table table_list1 add partition table_list1_p5 values (default);

create table table_list2 (id int, a date, b varchar)
partition by list (b)
(
    partition table_list2_p1 values ('1', '2', '3', '4'),
    partition table_list2_p2 values ('5', '6', '7', '8'),
    partition table_list2_p3 values ('9', '10', '11', '12')
);
alter table table_list2 add partition table_list2_p4 values ('13', '14', '15', '16');
alter table table_list2 add partition table_list2_p5 values ('DEFAULT');
alter table table_list2 add partition table_list2_p6 values ('default');
alter table table_list2 add partition table_list2_p7 values (default);


create table table_list3 (id int, a date, b varchar)
partition by list (id, b)
(
    partition table_list3_p1 values ((1, 'a'), (2,'b'), (3,'c'), (4,'d')) ,
    partition table_list3_p2 values ((5, 'a'), (6,'b'), (7,'c'), (8,'d')) 

);
alter table table_list3 add partition table_list3_p3 values ((15, 'a'), (16,'b'), (17,'c'), (18,'d'));
alter table table_list3 add partition table_list3_p4 values (default);

create table table_hash1 (id int, a date, b varchar)
partition by hash (id)
(
    partition table_hash1_p1,
    partition table_hash1_p2,
    partition table_hash1_p3
);


CREATE TABLE list_hash_2 (
    col_1 integer primary key,
    col_2 integer,
    col_3 character varying(30) unique,
    col_4 integer
)
WITH (orientation=row, compression=no)
PARTITION BY LIST (col_2) SUBPARTITION BY HASH (col_3)
(
    PARTITION p_list_1 VALUES (-1,-2,-3,-4,-5,-6,-7,-8,-9,-10)
    (
        SUBPARTITION p_hash_1_1,
        SUBPARTITION p_hash_1_2,
        SUBPARTITION p_hash_1_3
    ),
    PARTITION p_list_2 VALUES (1,2,3,4,5,6,7,8,9,10),
    PARTITION p_list_3 VALUES (11,12,13,14,15,16,17,18,19,20)
    (
        SUBPARTITION p_hash_3_1,
        SUBPARTITION p_hash_3_2
    ),
    PARTITION p_list_4 VALUES (21,22,23,24,25,26,27,28,29,30)
    (
        SUBPARTITION p_hash_4_1,
        SUBPARTITION p_hash_4_2,
        SUBPARTITION p_hash_4_3,
        SUBPARTITION p_hash_4_4,
        SUBPARTITION p_hash_4_5
    ),
    PARTITION p_list_5 VALUES (31,32,33,34,35,36,37,38,39,40),
    PARTITION p_list_6 VALUES (41,42,43,44,45,46,47,48,49,50)
    (
        SUBPARTITION p_hash_6_1,
        SUBPARTITION p_hash_6_2,
        SUBPARTITION p_hash_6_3,
        SUBPARTITION p_hash_6_4,
        SUBPARTITION p_hash_6_5
    ),
    PARTITION p_list_7 VALUES (DEFAULT)
);

alter table list_hash_2 add partition p_list_8 values (51,52,53,54,55,56,57,58,59,60) (subpartition p_hash_8_1, subpartition p_hash_8_2, subpartition p_hash_8_3);

-- drop table table_list3;		
create table table_list3 (id int, a date, b varchar)
partition by list (id, b)
(
    partition table_list3_p1 values ((1, 'a'), (2,'b'), (3,'c'), (4,'d')) ,
    partition table_list3_p2 values ((5,'a'), (6,'b'), (7,'NULL'), (8,NULL))
);

alter table table_list3 add partition table_list3_p3 values ((15, 'a'), (16,'default'), (17,'NULL'), (18,NULL));

alter table table_list3 add partition table_list3_p4 values (default);	

CREATE TABLE range_range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) 
PARTITION BY RANGE (customer_id) SUBPARTITION BY RANGE (time_id)
(
    PARTITION customer1 VALUES LESS THAN (200)
    (
        SUBPARTITION customer1_2008 VALUES LESS THAN ('2009-01-01'),
        SUBPARTITION customer1_2009 VALUES LESS THAN ('2010-01-01'),
        SUBPARTITION customer1_2010 VALUES LESS THAN ('2011-01-01'),
        SUBPARTITION customer1_2011 VALUES LESS THAN ('2012-01-01')
    ),
    PARTITION customer2 VALUES LESS THAN (500)
    (
        SUBPARTITION customer2_2008 VALUES LESS THAN ('2009-01-01'),
        SUBPARTITION customer2_2009 VALUES LESS THAN ('2010-01-01'),
        SUBPARTITION customer2_2010 VALUES LESS THAN ('2011-01-01'),
        SUBPARTITION customer2_2011 VALUES LESS THAN ('2012-01-01')
    ),
    PARTITION customer3 VALUES LESS THAN (800),
    PARTITION customer4 VALUES LESS THAN (1200)
    (
        SUBPARTITION customer4_all VALUES LESS THAN ('2012-01-01')
    )
);

INSERT INTO range_range_sales SELECT generate_series(1,1000),
                                     generate_series(1,1000),
                                     date_pli('2008-01-01', generate_series(1,1000)),
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%1000,
                                     generate_series(1,1000);
CREATE INDEX range_range_sales_idx ON range_range_sales(product_id) LOCAL;
ALTER TABLE range_range_sales ADD PARTITION customer5 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer5_2008 VALUES LESS THAN ('2009-01-01'),
        SUBPARTITION customer5_2009 VALUES LESS THAN ('2010-01-01'),
        SUBPARTITION customer5_2010 VALUES LESS THAN ('2011-01-01'),
        SUBPARTITION customer5_2011 VALUES LESS THAN ('2012-01-01')
    );
ALTER TABLE range_range_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_2012 VALUES LESS THAN ('2013-01-01');

CREATE TABLE range2_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id, product_id)
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01', 200),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01', 500),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01', 800),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01', 1200)
);

INSERT INTO range2_sales SELECT generate_series(1,1000),
                                generate_series(1,1000),
                                date_pli('2008-01-01', generate_series(1,1000)),
                                generate_series(1,1000)%10,
                                generate_series(1,1000)%10,
                                generate_series(1,1000)%1000,
                                generate_series(1,1000);
CREATE INDEX range2_sales_idx ON range2_sales(product_id) LOCAL;

ALTER TABLE range2_sales TRUNCATE PARTITION time_2008;
ALTER TABLE range2_sales TRUNCATE PARTITION FOR VALUES('2011-04-01', 700) ;

ALTER TABLE range2_sales DROP PARTITION time_2009;
ALTER TABLE range2_sales DROP PARTITION FOR ('2011-06-01', 600);

CREATE TABLE range_list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(100),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) 
PARTITION BY RANGE (customer_id) SUBPARTITION BY LIST (channel_id)
(
    PARTITION customer1 VALUES LESS THAN (200)
    (
        SUBPARTITION customer1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer1_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION customer1_channel4 VALUES ('9')
    ),
    PARTITION customer2 VALUES LESS THAN (500)
    (
        SUBPARTITION customer2_channel1 VALUES ('0', '1', '2', '3', '4'),
        SUBPARTITION customer2_channel2 VALUES (DEFAULT)
    ),
    PARTITION customer3 VALUES LESS THAN (800),
    PARTITION customer4 VALUES LESS THAN (1200)
    (
        SUBPARTITION customer4_channel1 VALUES ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    )
);

INSERT INTO range_list_sales SELECT generate_series(1,1000),
                                    generate_series(1,1000),
                                    date_pli('2008-01-01', generate_series(1,1000)),
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%1000,
                                    generate_series(1,1000);
CREATE INDEX range_list_sales_idx ON range_list_sales(product_id) LOCAL;
ALTER TABLE range_list_sales ADD PARTITION customer5 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer5_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer5_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer5_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION customer5_channel4 VALUES ('9')
    );        
ALTER TABLE range_list_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_channel5 VALUES ('X', 'A', 'bbb');   
ALTER TABLE range_list_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_channel6 VALUES ('NULL', 'asdasd', 'hahaha');   
ALTER TABLE range_list_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_channel7 VALUES (NULL);   
ALTER TABLE range_list_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_channel8 VALUES ('DEFAULT', 'wawawa');  
ALTER TABLE range_list_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_channel9 VALUES (DEFAULT);  
ALTER TABLE range_list_sales DROP SUBPARTITION customer1_channel9;  

ALTER TABLE range_list_sales SPLIT partition customer4 INTO (
  partition customer4_p1 values less than (900)
  (
    subpartition customer4_p1_s1 VALUES ('11'),
    subpartition customer4_p1_s2 VALUES ('12')
  ),
  partition customer4_p2 values less than (1000)
  (
    subpartition customer4_p2_s1 VALUES ('11'),
    subpartition customer4_p2_s2 VALUES ('12')
  ) 
);  

ALTER TABLE range_list_sales truncate partition customer2 update global index;
ALTER TABLE range_list_sales truncate partition for (300);
ALTER TABLE range_list_sales truncate partition customer5_channel3;

ALTER TABLE range_list_sales DROP PARTITION customer2;      
ALTER TABLE range_list_sales DROP SUBPARTITION customer1_channel1;        


create table test_list (col1 int, col2 int)
partition by list(col1)
(
partition p1 values (2000),
partition p2 values (3000),
partition p3 values (4000),
partition p4 values (5000)
);

INSERT INTO test_list VALUES(2000, 2000);
INSERT INTO test_list VALUES(3000, 3000);
alter table test_list add partition p5 values (6000);
INSERT INTO test_list VALUES(6000, 6000);

create table t1 (col1 int, col2 int);

alter table test_list exchange partition (p1) with table t1 VERBOSE;
alter table test_list truncate partition p2;
alter table test_list drop partition p5;


create table test_hash (col1 int, col2 int)
partition by hash(col1)
(
partition p1,
partition p2
);

INSERT INTO test_hash VALUES(1, 1);
INSERT INTO test_hash VALUES(2, 2);
INSERT INTO test_hash VALUES(3, 3);
INSERT INTO test_hash VALUES(4, 4);

alter table test_hash exchange partition (p1) with table t1 WITHOUT VALIDATION;

alter table test_hash truncate partition p2;


CREATE TABLE interval_sales
(
    prod_id       NUMBER(6),
    cust_id       NUMBER,
    time_id       DATE,
    channel_id    CHAR(1),
    promo_id      NUMBER(6),
    quantity_sold NUMBER(3),
    amount_sold   NUMBER(10, 2)
)
    PARTITION BY RANGE (time_id)
    INTERVAL
    ('1 MONTH')
(
  PARTITION p0 VALUES LESS THAN (TO_DATE('1-1-2008', 'DD-MM-YYYY')),
  PARTITION p1 VALUES LESS THAN (TO_DATE('6-5-2008', 'DD-MM-YYYY'))
);

alter table interval_sales split partition p0 at (to_date('2007-02-10', 'YYYY-MM-DD')) into (partition p0_1, partition p0_2);

alter table interval_sales split partition p0_1 into (partition p0_1_1 values less than (TO_DATE('1-1-2005', 'DD-MM-YYYY')), partition p0_1_2 values less than(TO_DATE('1-1-2006', 'DD-MM-YYYY')) );

alter table interval_sales split partition p0_2 into (partition p0_2_1 START (TO_DATE('8-5-2007', 'DD-MM-YYYY'), partition p0_2_2 START (TO_DATE('9-5-2007', 'DD-MM-YYYY'));


insert into interval_sales
values (1, 1, to_date('9-2-2007', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('11-2-2007', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('11-2-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('20-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('05-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('08-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('05-4-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('05-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-9-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-11-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-12-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-01-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-5-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-6-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-7-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales
values (1, 1, to_date('04-9-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);

alter table interval_sales merge partitions p0_1, p0_2, p1 into partition p01;
alter table interval_sales merge partitions sys_p6, sys_p7, sys_p8 into partition sys_p6_p7_p8;
ALTER TABLE interval_sales RESET PARTITION;

CREATE TABLE interval_sales1
(
    prod_id       NUMBER(6),
    cust_id       NUMBER,
    time_id       DATE,
    channel_id    CHAR(1),
    promo_id      NUMBER(6),
    quantity_sold NUMBER(3),
    amount_sold   NUMBER(10, 2)
)
    PARTITION BY RANGE (time_id)
    INTERVAL
('1 MONTH')
(PARTITION p0 VALUES LESS THAN (TO_DATE('1-1-2008', 'DD-MM-YYYY')),
    PARTITION p1 VALUES LESS THAN (TO_DATE('6-5-2008', 'DD-MM-YYYY'))
);
create index interval_sales1_time_id_idx on interval_sales1 (time_id) local;
create index interval_sales1_quantity_sold_idx on interval_sales1 (quantity_sold) local;
alter table interval_sales1 split partition p0 at (to_date('2007-02-10', 'YYYY-MM-DD')) into (partition p0_1, partition p0_2);

insert into interval_sales1
values (1, 1, to_date('9-2-2007', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('11-2-2007', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('11-2-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('20-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('05-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('08-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('05-4-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('05-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-9-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-11-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-12-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-01-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-5-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-6-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-7-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
insert into interval_sales1
values (1, 1, to_date('04-9-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);

alter table interval_sales1 merge partitions p0_1, p0_2, p1 into partition p01 UPDATE GLOBAL INDEX;


CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( MAXVALUE )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '6' )
  )
);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('201902', '2', '1', 1);
insert into range_range values('201902', '3', '1', 1);
insert into range_range values('201903', '1', '1', 1);
insert into range_range values('201903', '2', '1', 1);
insert into range_range values('201903', '5', '1', 1);

alter table range_range split subpartition p_201901_b at (3) into
(
	subpartition p_201901_c,
	subpartition p_201901_d
);

alter table range_range split subpartition p_201902_b at (3) into
(
	subpartition p_201902_c,
	subpartition p_201902_d
);

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
    SUBPARTITION p_201902_b VALUES ( default )
  )
);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201902', '2', '1', 1);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201903', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
insert into list_list values('201903', '3', '1', 1);

alter table list_list split subpartition p_201901_b values (2) into
(
	subpartition p_201901_b,
	subpartition p_201901_c
);

alter table list_list split subpartition p_201902_b values (2, 3) into
(
	subpartition p_201902_b,
	subpartition p_201902_c
);


CREATE TABLE range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id)
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01'),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01')
);
CREATE INDEX range_sales_idx1 ON range_sales(product_id) LOCAL;
CREATE INDEX range_sales_idx2 ON range_sales(time_id) GLOBAL;
EXECUTE partition_get_partitionno('range_sales');
ALTER TABLE range_sales ADD PARTITION time_default VALUES LESS THAN (MAXVALUE);
ALTER TABLE range_sales DROP PARTITION time_2008;
ALTER TABLE range_sales SPLIT PARTITION time_default AT ('2013-01-01') INTO (PARTITION time_2012, PARTITION time_default_temp);
ALTER TABLE range_sales RENAME PARTITION time_default_temp TO time_default;
ALTER TABLE range_sales MERGE PARTITIONS time_2009, time_2010 INTO PARTITION time_2010_old UPDATE GLOBAL INDEX;
ALTER TABLE range_sales TRUNCATE PARTITION time_2011 UPDATE GLOBAL INDEX;
ALTER TABLE range_sales disable row movement;
ALTER TABLE range_sales enable row movement;

CREATE TABLE interval_sales2
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)DEFAULT CHARACTER SET
)
PARTITION BY RANGE (time_id) INTERVAL ('1 year')
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01')
);
CREATE INDEX interval_sales2_idx1 ON interval_sales2(product_id) LOCAL;
CREATE INDEX interval_sales2_idx2 ON interval_sales2(time_id) GLOBAL;

-- add/drop partition
INSERT INTO interval_sales2 VALUES (1,1,'2013-01-01','A',1,1,1);
INSERT INTO interval_sales2 VALUES (2,2,'2012-01-01','B',2,2,2);
ALTER TABLE interval_sales2 DROP PARTITION time_2008;


-- merge/split partition
ALTER TABLE interval_sales2 SPLIT PARTITION time_2009 AT ('2009-01-01') INTO (PARTITION time_2008, PARTITION time_2009_temp);
ALTER TABLE interval_sales2 RENAME PARTITION time_2009_temp TO time_2009;
ALTER TABLE interval_sales2 MERGE PARTITIONS time_2009, time_2010 INTO PARTITION time_2010_old UPDATE GLOBAL INDEX;


-- truncate partition with gpi
ALTER TABLE interval_sales2 TRUNCATE PARTITION time_2008 UPDATE GLOBAL INDEX;


--reset
ALTER TABLE interval_sales2 RESET PARTITION;
ALTER TABLE interval_sales2 disable row movement;

create table unit_varchar(a1 varchar default '1', a2 varchar(2), a3 varchar(2 byte) default 'ye', a4 varchar(2 character) default '你好', a5 varchar(2 char) default '默认');
create table unit_varchar2(a1 varchar2 default '1', a2 varchar2(2) default 'ha', a3 varchar2(2 byte), a4 varchar2(2 character) default '你好', a5 varchar2(2 char) default '默认');
create table unit_char(a1 char default '1', a2 char(2) default 'ha', a3 char(2 byte) default 'ye', a4 char(2 character), a5 char(2 char) default '默认');
create table unit_nchar(a1 nchar default '1', a2 nchar(2) default 'ha', a3 nchar(2) default 'ye', a4 nchar(2) default '你好', a5 nchar(2));
create table unit_nvarchar2(a1 nvarchar2 default '1', a2 nvarchar2(2) default 'ha', a3 nvarchar2(2) default 'ye', a4 nvarchar2(2) default '你好', a5 nvarchar2(2));

insert into unit_varchar (a1) values ('1111111111123大苏打撒旦11111111111111111111111111111111阿三大苏打实打实1');
insert into unit_varchar (a2) values ('12    ');
-- exceed
insert into unit_varchar (a2) values ('啊');
insert into unit_varchar (a3) values ('12    ');
-- exceed
insert into unit_varchar (a3) values ('啊');
insert into unit_varchar (a4) values ('啊2    ');
-- exceed
insert into unit_varchar (a4) values ('啊23    ');
-- exceed
insert into unit_varchar (a4) values ('223    ');
insert into unit_varchar (a5) values ('啊2    ');
-- exceed
insert into unit_varchar (a5) values ('啊23    ');
-- exceed
insert into unit_varchar (a5) values ('223    ');
-- exceed
update unit_varchar set a2='啊  ';
update unit_varchar set a3='啊a   ';
-- exceed
update unit_varchar set a5='啊啊啊';
update unit_varchar set a5='啊啊';
select * from unit_varchar;

insert into unit_varchar2 (a1) values ('啊111111111123大苏打撒旦11111111111111111111111111111111阿三大苏打实打实1');
insert into unit_varchar2 (a2) values ('12    ');
-- exceed
insert into unit_varchar2 (a2) values ('啊');
insert into unit_varchar2 (a3) values ('12    ');
-- exceed
insert into unit_varchar2 (a3) values ('啊');
insert into unit_varchar2 (a4) values ('啊2    ');
-- exceed
insert into unit_varchar2 (a4) values ('啊23    ');
-- exceed
insert into unit_varchar2 (a4) values ('223    ');
insert into unit_varchar2 (a5) values ('啊2    ');
-- exceed
insert into unit_varchar2 (a5) values ('啊23    ');
-- exceed
insert into unit_varchar2 (a5) values ('223    ');
ALTER TABLE unit_varchar2 ALTER COLUMN a2 SET data TYPE char(1 char) USING a2::char(1 char);
insert into unit_varchar2 (a2) values ('一  ');
alter table unit_varchar2 modify column a3 varchar2(2 char) default '黑黑';
-- exceed
insert into unit_varchar2 (a2) values ('一e');
insert into unit_varchar2 (a1) values(default);
select * from unit_varchar2;

-- exceed
insert into unit_char (a1) values ('1111111111123大苏打撒旦11111111111111111111111111111111阿三大苏打实打实1');
-- exceed
insert into unit_nchar (a1) values ('啊 ');
insert into unit_nchar (a1) values ('1 ');
insert into unit_char (a2) values ('12    ');
-- exceed
insert into unit_char (a2) values ('啊');
insert into unit_char (a3) values ('12    ');
-- exceed
insert into unit_char (a3) values ('啊');
insert into unit_char (a4) values ('啊2    ');
-- exceed
insert into unit_char (a4) values ('啊23    ');
-- exceed
insert into unit_char (a4) values ('223    ');
insert into unit_char (a5) values ('啊2    ');
-- exceed
insert into unit_char (a5) values ('啊23    ');
-- exceed
insert into unit_char (a5) values ('223    ');
ALTER table unit_char ADD COLUMN a6 varchar(3 char) default '默认值';
insert into unit_char (a6) values ('啊1a   ');
-- exceed
insert into unit_char (a6) values ('1234');
update unit_char set a4='啊';
-- execeed
update unit_char set a5='一二3';
select * from unit_char;

-- exceed
insert into unit_nchar (a1) values ('1111111111123大苏打撒旦11111111111111111111111111111111阿三大苏打实打实1');
insert into unit_nchar (a1) values ('啊 ');
insert into unit_nchar (a2) values ('啊啊    ');
-- exceed
insert into unit_nchar (a2) values ('123    ');
-- exceed
insert into unit_nchar (a2) values ('啊');
insert into unit_nchar (a3) values ('12    ');
insert into unit_nchar (a3) values ('啊');
insert into unit_nchar (a4) values ('啊2    ');
-- exceed
insert into unit_nchar (a4) values ('啊23    ');
-- exceed
insert into unit_nchar (a4) values ('223    ');
insert into unit_nchar (a5) values ('啊2    ');
-- exceed
insert into unit_nchar (a5) values ('啊23    ');
-- exceed
insert into unit_nchar (a5) values ('223    ');

-- exceed
insert into unit_nvarchar2 (a1) values ('1111111111123大苏打撒旦11111111111111111111111111111111阿三大苏打实打实1');
insert into unit_nvarchar2 (a1) values ('啊 ');
insert into unit_nvarchar2 (a2) values ('啊啊    ');
-- exceed
insert into unit_nvarchar2 (a2) values ('123    ');
-- exceed
insert into unit_nvarchar2 (a2) values ('啊');
insert into unit_nvarchar2 (a3) values ('12    ');
insert into unit_nvarchar2 (a3) values ('啊');
insert into unit_nvarchar2 (a4) values ('啊2    ');
insert into unit_nvarchar2 (a5) values ('啊2    ');
-- exceed
insert into unit_nvarchar2 (a5) values ('啊23    ');
-- exceed
insert into unit_nvarchar2 (a5) values ('223    ');



create table test_char(col char(20 char));
insert into test_char values ('这是一个测试'), ('asd一二三四五bsd'), ('一二三四五六七八九十一二三四五六七八九十                              '), ('一2  ');
select col, length(col), lengthb(col) from test_char;

create table test_varchar(col varchar(20 char));
insert into test_varchar values ('这是一个测试'), ('asd一二三四五bsd'), ('一二三四五六七八九十一二三四五六七八九十                              '), ('一2  ');
select col, length(col), lengthb(col) from test_varchar;

create table test_charb(col char(20));
insert into test_charb values ('这是一个测试'), ('asd一二三四五bs     '), ('一二三四五六                              '), ('一2  ');
select col, length(col), lengthb(col) from test_charb;

create table test_varcharb(col varchar(20));
insert into test_varcharb values ('这是一个测试'), ('asd一二三四五bs      '), ('一二三四五六                              '), ('一2  ');
select col, length(col), lengthb(col) from test_varcharb;