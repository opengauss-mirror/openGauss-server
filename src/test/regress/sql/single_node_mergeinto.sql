drop table if exists row_ddl;
drop table if exists col_ddl;
create table row_ddl 
(c1 int not null, 
c2 varchar(200) not null, 
c3 date default '2018-06-14', 
c4 varchar(200) default '空的就用这个default', 
c5 numeric(18,9) default 123456.000000009 check (c5>0), 
c6 text default 'comments for row');

create table col_ddl
(c1 int not null, 
c2 varchar(200) not null, 
c3 date default '2018-06-14', 
c4 varchar(200) default '空的就用这个default', 
c5 numeric(18,9) default 123456.000000009 , 
c6 text default 'comments for col');


insert into row_ddl values(generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), 'c'||generate_series(1,10)||'我的２００４');
insert into col_ddl values(generate_series(11,20),'A'||(generate_series(11,20))||'Z', date'2000-03-01'+generate_series(11,20), 'c'||generate_series(11,20)||'我的２００４');

alter table row_ddl drop column c5;
alter table row_ddl add column c5 int default 10;

alter table col_ddl drop column c2;
alter table col_ddl add column c2 int;

merge into col_ddl t1 using row_ddl t2 on t1.c1=t2.c1
when matched then update set t1.c6=t1.c6||t2.c6,c3=t2.c3+interval '1' day
when not matched then insert values(t2.c1,t2.c3,t2.c4,t2.c5,t2.c6,length(t2.c2));

drop table if exists row_ddl;
drop table if exists col_ddl;
create table row_ddl 
(c1 int not null, 
c2 varchar(200) not null, 
c3 date default '2018-06-14', 
c4 varchar(200) default '空的就用这个default', 
c5 numeric(18,9) default 123456.000000009 check (c5>0), 
c6 text default 'comments for row');

create table col_ddl
(c1 int not null, 
c2 varchar(200) not null, 
c3 date default '2018-06-14', 
c4 varchar(200) default '空的就用这个default', 
c5 numeric(18,9) default 123456.000000009 , 
c6 text default 'comments for col');

--表上建立索引
create index i_row_ddl on row_ddl(c4,c2)local;
create index i_col_ddl on col_ddl(c1,c3)local;

insert into row_ddl values(generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), 'c'||generate_series(1,10)||'我的２００４');
insert into col_ddl values(generate_series(11,20),'A'||(generate_series(11,20))||'Z', date'2000-03-01'+generate_series(11,20), 'c'||generate_series(11,20)||'我的２００４');

alter table row_ddl drop column c5;
alter table row_ddl add column c5 int default 10;

alter table col_ddl drop column c2;
alter table col_ddl add column c2 int;

alter table row_ddl drop column c3;
alter table row_ddl add column c3 bool default 't';

merge into row_ddl t1 using col_ddl t2 on t1.c1=t2.c1
when matched then update set t1.c6=t2.c6, t1.c3=case when t2.c3> date'2000-03-02' then 0 else 1 end
when not matched then insert(c1,c4,c5,c6) values(t2.c1,t2.c4,t2.c5,t2.c6);


drop table if exists row_dml;
drop table if exists col_dml;
create temp table row_dml 
(c1 int not null, 
c2 varchar(200) not null, 
c3 date default '2018-06-14', 
c4 varchar(200) default '空的就用这个default', 
c5 numeric(18,9) default 123456.000000009 check (c5>0), 
c6 text default 'comments',
unique(c2,c4));

create unlogged table col_dml
(c1 int not null, 
c2 varchar(200) not null, 
c3 date default '2018-06-14', 
c4 varchar(200) default '空的就用这个default', 
c5 numeric(18,9) default 123456.000000009 , 
c6 text default 'comments');

create table web_page
(
wp_web_page_sk            integer               not null,
wp_web_page_id            char(16)              not null,
wp_rec_start_date         date                          ,
wp_rec_end_date           date                          ,
wp_creation_date_sk       integer                       ,
wp_access_date_sk         integer                       ,
wp_autogen_flag           char(1)                       ,
wp_customer_sk            integer                       ,
wp_url                    varchar(100)                  ,
wp_type                   char(50)                      ,
wp_char_count             integer                       ,
wp_link_count             integer                       ,
wp_image_count            integer                       ,
wp_max_ad_count           integer                       
 );
 
--表上建立索引
insert into row_dml values(generate_series(1,100),'A'||(generate_series(1,100))||'Z', date'2000-03-01'+generate_series(1,100), 'c'||generate_series(1,100)||'我的２００４');
insert into col_dml select * from row_dml;

copy web_page from STDIN (DELIMITER ',');
1,AAAAAAABAAAAAAA,1997-09-03 00:00:00,,2450810,2452620,Y,98539,http://www.foo.com,welcome                                           ,2531,8,3,4
2,AAAAAAAACAAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450814,2452580,N,,http://www.foo.com,protected                                         ,1564,4,3,1
4,AAAAAAAAEAAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450812,2452579,N,,http://www.foo.com,general                                           ,3732,18,7,1
5,AAAAAAAAEAAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450812,2452597,N,,http://www.foo.com,welcome                                           ,3732,18,3,1
7,AAAAAAAAHAAAAAAA,1997-09-03 00:00:00,,2450815,2452574,N,,http://www.foo.com,feedback                                          ,3034,18,7,4
8,AAAAAAAAIAAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450815,2452646,Y,1898,http://www.foo.com,protected                                         ,3128,12,2,4
10,AAAAAAAAKAAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,,2452623,N,,http://www.foo.com,,,,,
11,AAAAAAAAKAAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450814,2452611,N,,http://www.foo.com,welcome                                           ,7046,23,4,4
13,AAAAAAAANAAAAAAA,1997-09-03 00:00:00,,2450807,2452629,N,,http://www.foo.com,protected                                         ,2281,6,4,1
14,AAAAAAAAOAAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450810,2452639,N,,http://www.foo.com,dynamic                                           ,5676,19,6,0
16,AAAAAAAAABAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450814,2452601,Y,33463,http://www.foo.com,feedback                                          ,701,2,1,4
17,AAAAAAAAABAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450812,2452645,N,,http://www.foo.com,general                                           ,701,11,1,3
19,AAAAAAAADBAAAAAA,1997-09-03 00:00:00,,2450808,2452648,Y,57610,http://www.foo.com,general                                           ,2347,9,7,4
20,AAAAAAAAEBAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450809,2452555,Y,46487,http://www.foo.com,ad                                                ,1147,3,6,0
22,AAAAAAAAGBAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450812,2452565,Y,20213,http://www.foo.com,general                                           ,5663,25,3,4
23,AAAAAAAAGBAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450812,2452623,Y,20213,http://www.foo.com,order                                             ,4729,23,6,4
25,AAAAAAAAJBAAAAAA,1997-09-03 00:00:00,,2450811,2452620,N,,http://www.foo.com,feedback                                          ,1526,9,4,2
26,AAAAAAAAKBAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450812,2452636,Y,98376,http://www.foo.com,ad                                                ,1826,9,3,1
28,AAAAAAAAMBAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450807,2452572,N,,http://www.foo.com,protected                                         ,1308,4,1,2
29,AAAAAAAAMBAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450808,2452611,N,,http://www.foo.com,order                                             ,1308,4,1,2
31,AAAAAAAAPBAAAAAA,1997-09-03 00:00:00,,2450810,2452596,N,,http://www.foo.com,general                                           ,1732,3,6,0
32,AAAAAAAAACAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450808,2452585,N,,http://www.foo.com,welcome                                           ,5104,20,7,4
34,AAAAAAAACCAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450808,2452616,N,,http://www.foo.com,welcome                                           ,2726,12,5,2
35,AAAAAAAACCAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450808,2452591,N,,http://www.foo.com,protected                                         ,2726,12,1,2
37,AAAAAAAAFCAAAAAA,1997-09-03 00:00:00,,2450809,2452556,N,,http://www.foo.com,ad                                                ,3076,15,3,0
38,AAAAAAAAGCAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450811,2452583,Y,37285,http://www.foo.com,general                                           ,3096,18,3,0
40,AAAAAAAAICAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450813,2452576,N,,http://www.foo.com,general                                           ,4402,18,4,2
41,AAAAAAAAICAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450813,2452579,Y,16769,http://www.foo.com,welcome                                           ,784,3,4,4
43,AAAAAAAALCAAAAAA,1997-09-03 00:00:00,,2450814,2452580,Y,64793,http://www.foo.com,ad                                                ,3760,12,3,2
44,AAAAAAAAMCAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450811,2452602,Y,92078,http://www.foo.com,ad                                                ,4179,19,7,1
46,AAAAAAAAOCAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450809,2452574,N,,http://www.foo.com,protected                                         ,1711,4,5,1
47,AAAAAAAAOCAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450815,2452574,N,,http://www.foo.com,welcome                                           ,1711,4,5,1
49,AAAAAAAABDAAAAAA,1997-09-03 00:00:00,,2450809,2452618,N,,http://www.foo.com,order                                             ,4894,20,3,2
50,AAAAAAAACDAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450808,2452615,N,,http://www.foo.com,welcome                                           ,5262,16,5,2
52,AAAAAAAAEDAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450815,2452606,N,,http://www.foo.com,welcome                                           ,3306,21,7,1
53,AAAAAAAAEDAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450808,2452636,N,,http://www.foo.com,dynamic                                           ,3306,21,7,1
55,AAAAAAAAHDAAAAAA,1997-09-03 00:00:00,,2450811,2452549,N,,http://www.foo.com,order                                             ,3788,19,1,0
56,AAAAAAAAIDAAAAAA,1997-09-03 00:00:00,2000-09-02 00:00:00,2450815,2452554,N,,http://www.foo.com,protected                                         ,5733,24,2,2
58,AAAAAAAAKDAAAAAA,1997-09-03 00:00:00,1999-09-03 00:00:00,2450813,2452619,Y,7625,http://www.foo.com,ad                                                ,6577,24,4,3
59,AAAAAAAAKDAAAAAA,1999-09-04 00:00:00,2001-09-02 00:00:00,2450813,2452624,Y,80555,http://www.foo.com,general                                           ,6577,24,2,3
3,AAAAAAAACAAAAAAA,2000-09-03 00:00:00,,2450814,2452611,N,,http://www.foo.com,feedback                                          ,1564,4,3,4
6,AAAAAAAAEAAAAAAA,2001-09-03 00:00:00,,2450814,2452597,N,,http://www.foo.com,ad                                                ,3732,18,7,4
9,AAAAAAAAIAAAAAAA,2000-09-03 00:00:00,,2450807,2452579,Y,84146,http://www.foo.com,welcome                                           ,3128,13,5,3
12,AAAAAAAAKAAAAAAA,2001-09-03 00:00:00,,2450815,2452611,N,,http://www.foo.com,protected                                         ,7046,17,4,4
15,AAAAAAAAOAAAAAAA,2000-09-03 00:00:00,,2450810,2452639,N,,http://www.foo.com,dynamic                                           ,2469,10,5,2
18,AAAAAAAAABAAAAAA,2001-09-03 00:00:00,,2450812,2452608,N,,http://www.foo.com,ad                                                ,4080,11,6,3
21,AAAAAAAAEBAAAAAA,2000-09-03 00:00:00,,2450809,2452555,Y,10897,http://www.foo.com,general                                           ,1147,3,6,4
24,AAAAAAAAGBAAAAAA,2001-09-03 00:00:00,,2450812,2452646,Y,20213,http://www.foo.com,dynamic                                           ,5918,23,6,1
27,AAAAAAAAKBAAAAAA,2000-09-03 00:00:00,,2450812,2452607,Y,98376,http://www.foo.com,protected                                         ,1553,9,1,1
30,AAAAAAAAMBAAAAAA,2001-09-03 00:00:00,,2450808,2452611,N,,http://www.foo.com,general                                           ,3872,18,1,4
33,AAAAAAAAACAAAAAA,2000-09-03 00:00:00,,2450808,2452585,N,,http://www.foo.com,protected                                         ,2129,7,1,0
36,AAAAAAAACCAAAAAA,2001-09-03 00:00:00,,2450812,2452613,N,,http://www.foo.com,dynamic                                           ,2726,3,1,2
39,AAAAAAAAGCAAAAAA,2000-09-03 00:00:00,,2450815,2452583,N,,http://www.foo.com,general                                           ,3096,18,3,0
42,AAAAAAAAICAAAAAA,2001-09-03 00:00:00,,2450813,2452579,Y,60150,http://www.foo.com,dynamic                                           ,1451,3,4,4
45,AAAAAAAAMCAAAAAA,2000-09-03 00:00:00,,2450811,2452575,Y,98633,http://www.foo.com,feedback                                          ,4584,19,7,4
48,AAAAAAAAOCAAAAAA,2001-09-03 00:00:00,,2450815,2452622,N,,http://www.foo.com,ad                                                ,1732,9,5,1
51,AAAAAAAACDAAAAAA,2000-09-03 00:00:00,,2450811,2452564,N,,http://www.foo.com,general                                           ,3423,19,7,1
54,AAAAAAAAEDAAAAAA,2001-09-03 00:00:00,,2450808,2452629,N,,http://www.foo.com,protected                                         ,1931,7,2,2
57,AAAAAAAAIDAAAAAA,2000-09-03 00:00:00,,2450811,2452568,N,,http://www.foo.com,ad                                                ,5733,16,2,2
60,AAAAAAAAKDAAAAAA,2001-09-03 00:00:00,,2450813,2452566,Y,80555,http://www.foo.com,welcome                                           ,6577,24,2,3
\.

merge into row_dml t1 using
(
select coalesce(wp_type, wp_web_page_id) c6,
wp_type c1,
nvl(wp_rec_start_date, wp_rec_end_date) c2 ,
length(nullif(wp_type,wp_web_page_id)) c5
from web_page ) t2
on ( t1.c1=t2.c5+50 )
when matched then update set t1.c6 = t2.c6
when not matched then insert values(t2.c5, t2.c6, t2.c2);

-------------------------------------------------------
-- Verify foreign key validity
-- Notice: merge into  when matched then update [FK] when not matched then insert values(value, [FK]);
--         we must take attention about column in "[]";
-------------------------------------------------------
create table pkt(a int primary key);
create table fkt(a int primary key, b int references pkt);
create table dtt(a int, b int);
insert into pkt values(1),(2),(3);
insert into dtt values(1,1),(2,2);
merge into fkt using dtt on (dtt.a=fkt.a) when matched then update set fkt.b = 3 when not matched then insert values(dtt.a, dtt.b);
select * from fkt;
merge into fkt using dtt on (dtt.a=fkt.a) when matched then update set fkt.b = 3 when not matched then insert values(dtt.a, dtt.b);
select * from fkt;
merge into fkt using dtt on (dtt.a=fkt.a) when matched then update set fkt.b = 5 when not matched then insert values(dtt.a, dtt.b);
select * from fkt;
truncate fkt;
insert into dtt values(5,5);
merge into fkt using dtt on (dtt.a=fkt.a) when matched then update set fkt.b = 3 when not matched then insert values(dtt.a, dtt.b);
select * from fkt;


----------------------------------------------------
-- trigger
----------------------------------------------------
CREATE FUNCTION mgit_before_func()
  RETURNS TRIGGER language plpgsql AS
$$
BEGIN
  IF (TG_OP = 'UPDATE') THEN
    RAISE warning 'before update (old): %', old.*::TEXT;
    RAISE warning 'before update (new): %', new.*::TEXT;
  elsIF (TG_OP = 'INSERT') THEN
    RAISE warning 'before insert (new): %', new.*::TEXT;
  END IF;
  RETURN new;
END;
$$;
CREATE TRIGGER mgit_before_trig BEFORE INSERT OR UPDATE ON fkt
  FOR EACH ROW EXECUTE procedure mgit_before_func();

CREATE FUNCTION mgit_after_func()
  RETURNS TRIGGER language plpgsql AS
$$
BEGIN
  IF (TG_OP = 'UPDATE') THEN
    RAISE warning 'after update (old): %', old.*::TEXT;
    RAISE warning 'after update (new): %', new.*::TEXT;
  elsIF (TG_OP = 'INSERT') THEN
    RAISE warning 'after insert (new): %', new.*::TEXT;
  END IF;
  RETURN null;
END;
$$;
CREATE TRIGGER mgit_after_trig AFTER INSERT OR UPDATE ON fkt
  FOR EACH ROW EXECUTE procedure mgit_after_func();

insert into fkt values(1,1);
delete from dtt where a = 5; -- now dtt: (1,1),(2,2)  fkt:(1,1)
merge into fkt using dtt on (dtt.a=fkt.a) when matched then update set fkt.b = 3 when not matched then insert values(dtt.a, dtt.b);
select * from fkt;

-- test for merge with where clauses
create table explain_t1 (a int, b int);
create table explain_t2 (f1 int, f2 int);
explain (verbose on, costs off) merge into explain_t1
    using explain_t2 tt2 on explain_t1.a = tt2.f1
when not matched then
    insert values(1,3) where tt2.f1 = 1;

explain (verbose on, costs off) merge into explain_t1
    using explain_t2 tt2 on explain_t1.a = tt2.f1
when matched then
    update set b = 10 where explain_t1.a = 1;

explain (verbose on, costs off) merge into explain_t1
    using explain_t2 tt2 on explain_t1.a = tt2.f1
when matched then
    update set b = 10 where explain_t1.a = 1
when not matched then
    insert values(1,3) where tt2.f1 = 1;

explain (verbose on, costs off) merge into explain_t1
    using explain_t2 tt2 on explain_t1.a = tt2.f1
when matched then
    update set b = 10 where tt2.f2 = 1;

-- duplicate alias on source table
explain (verbose on, costs off) merge into explain_t2 t2 using (
  select
    t1.a,
    t1.b,
    t1.a aa,
    t1.b bb
  from
    explain_t1 t1
) tmp on (t2.f1 = tmp.b)
when matched THEN
    update
    set
      t2.f2 = tmp.aa
    where
      t2.f1 = tmp.bb;

explain (verbose on, costs off) merge /*+ leading((t2 t1)) */ into explain_t2 t2 using (
  select
    t1.a,
    t1.b,
    t1.a aa,
    t1.b bb
  from
    explain_t1 t1
) tmp on (t2.f1 = tmp.b)
when not matched THEN
  insert values(1,3) where tmp.bb = 1;

explain (verbose on, costs off) merge /*+ leading((t1 t2)) */ into explain_t2 t2 using (
  select
    t1.a,
    t1.b,
    t1.a aa,
    t1.b bb
  from
    explain_t1 t1
) tmp on (t2.f1 = tmp.b)
when not matched THEN
  insert values(1,3) where tmp.bb = 1;

--test using caluse is one time fiter result
create schema bmsql_schema;
set current_schema to bmsql_schema;
CREATE TABLE bmsql_history (
    hist_id int4,
    h_c_id int4,
    h_c_d_id int4,
    h_c_w_id int4,
    h_d_id int4,
    h_w_id int4,
    h_date timestamp(6),
    h_amount numeric(6, 2),
    h_data varchar(24)
);

CREATE TABLE bmsql_customer (
    c_w_id int4 NOT NULL default 1,
    c_d_id int4 NOT NULL default 2,
    c_id int4 NOT NULL default 3,
    c_discount numeric(4, 4),
    c_credit bpchar(2),
    c_last varchar(16),
    c_first varchar(16),
    c_credit_lim numeric(12, 2),
    c_balance numeric(12, 2),
    c_ytd_payment numeric(12, 2),
    c_payment_cnt int4,
    c_delivery_cnt int4,
    c_street_1 varchar(20),
    c_street_2 varchar(20),
    c_city varchar(20),
    c_state bpchar(2),
    c_zip bpchar(9),
    c_phone bpchar(16),
    c_since timestamp(6),
    lc_middle bpchar(2),
    c_data varchar(500),
    CONSTRAINT bmsql_customer_pkey PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE INDEX bmsql_customer_idx1 ON bmsql_customer (c_w_id, c_d_id, c_last, c_first);

CREATE TABLE bmsql_district (
    d_w_id int4 NOT NULL,
    d_id int4 NOT NULL,
    d_ytd numeric(12, 2),
    d_tax numeric(4, 4),
    d_next_o_id int4,
    d_name varchar(10),
    d_street_1 varchar(20),
    d_street_2 varchar(20),
    d_city varchar(20),
    d_state bpchar(2),
    d_zip bpchar(9),
    cONSTRAINT bmsq1_district_pkey PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE bmsql_item (
    i_id int4 NoT NULL,
    i_name varchar(24),
    i_price numeric(5, 2),
    i_data varchar(50),
    i_im_id int4
);

create view bmsql_b_view as (
    select
        c_w_id,
        c_balance,
        c_last
    from
        bmsql_customer
        inner join bmsql_district on bmsql_customer.c_w_id != bmsql_district.d_id
    union
    select
        *
    from
        (
            select
                count(*),
                i_price,
                i_name
            from
                bmsql_item
            group by
                i_price,
                i_name
        ) alias1
);

CREATE TABLE bmsql_new_order (
    no_w_id int4,
    no_d_id int4,
    no_o_id int4
);

CREATE TABLE bmsql_oorder (
    o_w_id int4 NOT NULL default 1,
    o_d_id int4 NOT NULL default 2,
    o_id int4 NOT NULL default 3,
    o_c_id int4,
    o_carrier_id int4,
    o_ol_cnt int4,
    o_ali_local int4,
    o_entry_d timestamp(6),
    CONSTRAINT bmsql_oorder_pkey PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE bmsql_config (
    cfg_name varchar(30) NOT NULL,
    cfg_value varchar(50)
);

explain (costs off) MERGE INTO bmsql_history AS alias1 USING (
    select d_w_id alias3 from bmsql_district where 1<1
) AS alias2 ON alias1.h_c_w_id <= alias2.alias3
WHEN MATCHED THEN
UPDATE
SET alias1.h_w_id = 1
WHERE alias3 != 30002;

MERGE INTO bmsql_history AS alias1 USING (
    select d_w_id alias3 from bmsql_district where 1<1
) AS alias2 ON alias1.h_c_w_id <= alias2.alias3
WHEN MATCHED THEN
UPDATE
SET alias1.h_w_id = 1
WHERE alias3 != 30002;

explain(costs off) MERGE INTO bmsql_history AS alias1 USING (
    SELECT
        count(bmsql_b_view.c_last) AS alias1,
        CEIL(count(bmsql_district.d_zip)) alias2,
        min(bmsql_district.d_w_id) alias3,
        count(bmsql_district.d_zip) AS alias4,
        pg_client_encoding() AS alias5
    FROM
        bmsql_b_view
        RIGHT JOIN ONLY bmsql_new_order ON bmsql_b_view.c_balance >= bmsql_new_order.no_d_id,
        ONLY bmsql_district
    WHERE
        bmsql_district.d_name <= substring(
            'xx'
            FROM
                1 FOR 10
        )
        AND rownum < 3
    GROUP BY
        bmsql_new_order.no_w_id
    HAVING
        pg_client_encoding() < pg_client_encoding()
    ORDER BY
        alias3 DESC NULLS FIRST offset 2
    FETCH FIRST
        ROW ONLY
) AS alias2 ON alias1.h_c_w_id <= alias2.alias3
WHEN MATCHED THEN
UPDATE
SET
    alias1.h_w_id =(
        SELECT
            octet_length(quote_literal(bmsql_config.cfg_value)) alias2
        FROM
            bmsql_new_order alias1,
            bmsql_history
            LEFT JOIN bmsql_config ON initcap(bmsql_history.h_data) <= to_char(
                nvl(bmsql_config.cfg_name, bmsql_config.cfg_value)
            )
        GROUP BY
            bmsql_config.cfg_value
        ORDER BY
            alias2 NULLS FIRST
        LIMIT
            1
    )
WHERE
    alias1.h_c_w_id >= alias1.h_amount
    OR alias3 != 30002;

MERGE INTO bmsql_history AS alias1 USING (
    SELECT
        count(bmsql_b_view.c_last) AS alias1,
        CEIL(count(bmsql_district.d_zip)) alias2,
        min(bmsql_district.d_w_id) alias3,
        count(bmsql_district.d_zip) AS alias4,
        pg_client_encoding() AS alias5
    FROM
        bmsql_b_view
        RIGHT JOIN ONLY bmsql_new_order ON bmsql_b_view.c_balance >= bmsql_new_order.no_d_id,
        ONLY bmsql_district
    WHERE
        bmsql_district.d_name <= substring(
            'xx'
            FROM
                1 FOR 10
        )
        AND rownum < 3
    GROUP BY
        bmsql_new_order.no_w_id
    HAVING
        pg_client_encoding() < pg_client_encoding()
    ORDER BY
        alias3 DESC NULLS FIRST offset 2
    FETCH FIRST
        ROW ONLY
) AS alias2 ON alias1.h_c_w_id <= alias2.alias3
WHEN MATCHED THEN
UPDATE
SET
    alias1.h_w_id =(
        SELECT
            octet_length(quote_literal(bmsql_config.cfg_value)) alias2
        FROM
            bmsql_new_order alias1,
            bmsql_history
            LEFT JOIN bmsql_config ON initcap(bmsql_history.h_data) <= to_char(
                nvl(bmsql_config.cfg_name, bmsql_config.cfg_value)
            )
        GROUP BY
            bmsql_config.cfg_value
        ORDER BY
            alias2 NULLS FIRST
        LIMIT
            1
    )
WHERE
    alias1.h_c_w_id >= alias1.h_amount
    OR alias3 != 30002;

CREATE TABLE products
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60)
);
INSERT INTO products VALUES (1501, 'vivitar 35mm', 'electrncs');
INSERT INTO products VALUES (1502, 'olympus is50', 'electrncs');
INSERT INTO products VALUES (1600, 'play gym', 'toys');
INSERT INTO products VALUES (1601, 'lamaze', 'toys');
INSERT INTO products VALUES (1666, 'harry potter', 'dvd');
MERGE INTO products vp
USING products np
ON (vp.product_id = np.product_id)
WHEN MATCHED THEN
UPDATE SET vp.product_name = np.product_name, vp.category = np.category WHERE vp.product_name != 'play gym'
WHEN NOT MATCHED THEN
INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
select * from products order by 1;
MERGE INTO products vp
USING products np
ON (vp.product_id = np.product_id)
WHEN MATCHED THEN
UPDATE SET vp.product_name = np.category, vp.category = np.product_name WHERE vp.product_name != 'play gym'
WHEN NOT MATCHED THEN
INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
select * from products order by 1;

drop table products;
reset current_schema;
------------------------------------------------
-- clean up
------------------------------------------------
drop schema bmsql_schema cascade;
drop trigger mgit_after_trig on fkt;
drop trigger mgit_before_trig on fkt;
drop function mgit_before_func;
drop function mgit_after_func;
drop table dtt;
drop table fkt;
drop table pkt;

