create schema hashfilter;
set current_schema = hashfilter;

START TRANSACTION;
--create row table
create table hashfilter_t1(a int not null, b varchar(10), c regproc);
create table hashfilter_t2(a int not null, b varchar(10), c regproc);
create table hashfilter_t3(a int not null, b varchar(10), c regproc);
--create column table
create table hashfilter_t4(a int not null, b varchar(10)) WITH (ORIENTATION = COLUMN);
create table hashfilter_t5(a int not null, b varchar(10)) WITH (ORIENTATION = COLUMN);
create table hashfilter_t6(a int not null, b varchar(10)) WITH (ORIENTATION = COLUMN);
--mutiple distribute key
create table hf_t1(a int, b int, c int, d varchar(10)) WITH (ORIENTATION = COLUMN);
create table hf_t2(a int, b int, c int, d varchar(10)) WITH (ORIENTATION = COLUMN);
create table hf_t3(a int, b int, c int, d varchar(10));
create table hf_t4(a int, b int, c int, d varchar(10));
create table hf_t5(a int, b int, c int, d varchar(10)) WITH (ORIENTATION = COLUMN);
create table hf_t6(a int, b int, c int, d varchar(10)) WITH (ORIENTATION = COLUMN);
--partition table
create table hashfilter_t7(a int, b int) WITH (ORIENTATION=COLUMN)
partition by range (a) 
(
partition p0 values less than (50), 
partition p1 values less than (100), 
partition p2 values less than (150)
) ;
create table hashfilter_t8(a int, b int);
--
-- Data for Name: hashfilter_t1; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t1 (a, b, c) FROM stdin;
111	111	bool
111	\N	ascii
222	222	any_in
222	333	array_eq
222	\N	bool
333	333	ascii
333	444	any_in
333	111	array_eq
333	\N	bit_in
\.
;

--
-- Data for Name: hashfilter_t2; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t2 (a, b, c) FROM stdin;
333	222	any_in
333	111	any_in
333	\N	array_out
222	222	bit_in
222	111	bool
222	444	ascii
111	111	bit_in
\.
;

--
-- Data for Name: hashfilter_t3; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t3 (a, b, c) FROM stdin;
333	\N	array_out
333	111	any_in
222	111	bool
111	111	bit_in
222	444	ascii
333	222	any_in
222	222	bit_in
\.
;

--
-- Data for Name: hashfilter_t4; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t4 (a, b) FROM stdin;
111	111
111	\N
222	222
222	333
222	\N
333	333
333	444
333	111
333	\N
\.
;

--
-- Data for Name: hashfilter_t5; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t5 (a, b) FROM stdin;
333	222
333	111
333	\N
222	222
222	111
222	444
111	111
\.
;

--
-- Data for Name: hashfilter_t6; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t6 (a, b) FROM stdin;
333	\N
333	111
222	111
111	111
222	444
333	222
222	222
\.
;

--
-- Data for Name: hashfilter_t7; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t7 (a, b) FROM stdin;
\.
;

--
-- Data for Name: hashfilter_t8; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hashfilter_t8 (a, b) FROM stdin;
\.
;

--
-- Data for Name: hf_t1; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hf_t1 (a, b, c, d) FROM stdin;
4	3	3	4
4	4	3	5
8	3	4	3
7	6	5	8
4	7	3	7
7	6	6	7
7	6	5	5
5	9	6	9
5	7	4	8
\.
;

--
-- Data for Name: hf_t2; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hf_t2 (a, b, c, d) FROM stdin;
6	9	5	9
\N	2	3	4
5	9	6	9
4	5	6	6
4	5	7	7
4	8	4	8
4	5	9	7
\N	3	3	5
\.
;

--
-- Data for Name: hf_t3; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hf_t3 (a, b, c, d) FROM stdin;
4	3	3	4
4	4	3	5
8	3	4	3
7	6	5	8
4	7	3	7
7	6	6	7
7	6	5	5
5	9	6	9
5	7	4	8
\.
;

--
-- Data for Name: hf_t4; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hf_t4 (a, b, c, d) FROM stdin;
6	9	5	9
\N	2	3	4
5	9	6	9
4	5	6	6
4	5	7	7
4	8	4	8
4	5	9	7
\N	3	3	5
\.
;

--
-- Data for Name: hf_t5; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hf_t5 (a, b, c, d) FROM stdin;
\N	\N	\N	1
1	\N	1	1
1	1	1	1
1	\N	\N	1
\N	1	\N	1
\N	\N	1	1
\.
;

--
-- Data for Name: hf_t6; Type: TABLE DATA; Schema: hashfilter; Owner: jyh
--

COPY hf_t6 (a, b, c, d) FROM stdin;
\N	\N	\N	1
1	\N	1	1
1	\N	\N	1
\N	1	\N	1
\N	\N	1	1
1	1	1	1
\.
;
COMMIT;
--analyze
analyze hashfilter_t1;
analyze hashfilter_t2;
analyze hashfilter_t3;
analyze hashfilter_t4;
analyze hashfilter_t5;
analyze hashfilter_t6;
analyze hf_t1;
analyze hf_t2;
analyze hf_t3;
analyze hf_t4;
analyze hf_t5;
analyze hf_t6;
--SetOp for append between replication and hash
explain (costs off) select b from hashfilter_t1 union all (select b from hashfilter_t2) order by b;
select b from hashfilter_t1 union all (select b from hashfilter_t2) order by b;

explain (costs off) select b from hashfilter_t1 union all (select b from hashfilter_t1 union all (select b from hashfilter_t2)) order by b;
select b from hashfilter_t1 union all (select b from hashfilter_t1 union all (select b from hashfilter_t2)) order by b;

explain (costs off) select b from hashfilter_t4 union all (select b from hashfilter_t5) order by b;
select b from hashfilter_t4 union all (select b from hashfilter_t5) order by b;

explain (costs off) select b from hashfilter_t1 union (select b from hashfilter_t2) order by b;
select b from hashfilter_t1 union (select b from hashfilter_t2) order by b;

explain (costs off) select b from hashfilter_t4 union (select b from hashfilter_t5) order by b;
select b from hashfilter_t4 union (select b from hashfilter_t5) order by b;

explain (costs off) select a from hashfilter_t1 intersect all (select a from hashfilter_t3) order by a;
select a from hashfilter_t1 intersect all (select a from hashfilter_t3) order by a;

explain (costs off) select a from hashfilter_t4 intersect all (select a from hashfilter_t6) order by a;
select a from hashfilter_t4 intersect all (select a from hashfilter_t6) order by a;

explain (costs off) select a from hashfilter_t1 except all (select a from hashfilter_t3) order by a;
select a from hashfilter_t1 except all (select a from hashfilter_t3) order by a;

explain (costs off) select a from hashfilter_t4 except all (select a from hashfilter_t6) order by a;
select a from hashfilter_t4 except all (select a from hashfilter_t6) order by a;

explain (costs off) (select a from hashfilter_t1 order by 1) intersect all (select a from hashfilter_t1 order by 1) except all select a from hashfilter_t2 order by 1;
(select a from hashfilter_t1 order by 1) intersect all (select a from hashfilter_t1 order by 1) except all select a from hashfilter_t2 order by 1;

explain (costs off) (select a from hashfilter_t1 order by 1) intersect all (select a from hashfilter_t1 order by 1 limit 6) except all (select a from hashfilter_t2 order by 1 limit 3) order by 1;
(select a from hashfilter_t1 order by 1) intersect all (select a from hashfilter_t1 order by 1 limit 6) except all (select a from hashfilter_t2 order by 1 limit 3) order by 1;

create view v1 as (select a from hashfilter_t1 order by 1) intersect all (select a from hashfilter_t1 order by 1 limit 6);
explain (costs off) select v1.a from v1 left join (select a from hashfilter_t2 order by 1 limit 3) t2 on v1.a=t2.a order by 1;
select v1.a from v1 left join (select a from hashfilter_t2 order by 1 limit 3) t2 on v1.a=t2.a order by 1;

--two table join between replication and hash
explain (costs off) select t1.a from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.a=t2.b order by t1.a;
select t1.a from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.a=t2.b order by t1.a;

explain (costs off) select t1.a from hashfilter_t4 t1 left join hashfilter_t6 t2 on t1.a=t2.b order by t1.a;
select t1.a from hashfilter_t4 t1 left join hashfilter_t6 t2 on t1.a=t2.b order by t1.a;

explain (costs off) select t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.b=t2.a order by t1.b;
select t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.b=t2.a order by t1.b;

explain (costs off) select t1.b from hashfilter_t4 t1 left join hashfilter_t6 t2 on t1.b=t2.a order by t1.b;
select t1.b from hashfilter_t4 t1 left join hashfilter_t6 t2 on t1.b=t2.a order by t1.b;

explain (costs off) select t1.b from hashfilter_t1 t1 full join hashfilter_t2 t2 on t1.b=t2.b order by t1.b;
select t1.b from hashfilter_t1 t1 full join hashfilter_t2 t2 on t1.b=t2.b order by t1.b;

explain (costs off) select t1.b from hashfilter_t4 t1 full join hashfilter_t6 t2 on t1.b=t2.b order by t1.b;
select t1.b from hashfilter_t4 t1 full join hashfilter_t6 t2 on t1.b=t2.b order by t1.b;

explain (costs off) select t2.b from hashfilter_t2 t2 full join hashfilter_t1 t1 on t1.b=t2.b order by t2.b; 
select t2.b from hashfilter_t2 t2 full join hashfilter_t1 t1 on t1.b=t2.b order by t2.b; 

explain (costs off) select t2.a from hashfilter_t2 t2 right join hashfilter_t1 t1 on t1.a=t2.b order by t2.a;
select t2.a from hashfilter_t2 t2 right join hashfilter_t1 t1 on t1.a=t2.b order by t2.a;

explain (costs off) select t1.a from hashfilter_t6 t1 right join hashfilter_t4 t2 on t1.a=t2.b order by t1.a;
select t1.a from hashfilter_t4 t1 left join hashfilter_t6 t2 on t1.a=t2.b order by t1.a;

explain (costs off) select t1.* from hashfilter_t1 t1 where t1.b in (select t2.b from hashfilter_t2 t2) order by t1.b; 
select t1.b from hashfilter_t1 t1 where t1.b in (select t2.b from hashfilter_t2 t2) order by t1.b;

explain (costs off) select t1.b from hashfilter_t4 t1 where t1.b in (select t2.b from hashfilter_t6 t2) order by t1.b;  
select t1.b from hashfilter_t4 t1 where t1.b in (select t2.b from hashfilter_t6 t2) order by t1.b;

explain (costs off) select t1.* from hashfilter_t1 t1 where t1.a not in (select t2.a from hashfilter_t2 t2) order by t1.a;
select t1.* from hashfilter_t1 t1 where t1.a not in (select t2.a from hashfilter_t2 t2) order by t1.a;

explain (costs off) select t1.* from hashfilter_t1 t1 where not exists (select * from hashfilter_t2 t2 where t2.a=t1.b) order by t1.b; 
select t1.* from hashfilter_t1 t1 where not exists (select * from hashfilter_t2 t2 where t2.a=t1.b) order by 1, 2, 3; 

explain (costs off) select t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.b=t2.a and t1.a=t2.b order by t1.a,t1.b; 
select t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.b=t2.a and t1.a=t2.b order by t1.a,t1.b; 

explain (costs off) select t1.a,t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.a+222=t2.b order by t1.a,t1.b; 
select t1.a,t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.a+222=t2.b order by t1.a,t1.b;

--unsupport data type redistribute
explain (costs off) select t1.c from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.c=t2.c order by t1.c;
select t1.c from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.c=t2.c order by t1.c;
--multiply table join between replication and hash
explain (costs off) select t1.a from hashfilter_t1 t1 where t1.b in (select t3.b from hashfilter_t1 t3 left join hashfilter_t2 t2 on t3.a=t2.b) order by t1.a;
select t1.a from hashfilter_t1 t1 where t1.b in (select t3.b from hashfilter_t1 t3 left join hashfilter_t2 t2 on t3.a=t2.b) order by t1.a;

--setop and join combination
explain (costs off) select b from hashfilter_t1 intersect (select t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.a=t2.a) order by b;
select b from hashfilter_t1 intersect (select t1.b from hashfilter_t1 t1 left join hashfilter_t2 t2 on t1.a=t2.a) order by b;

--partition replication table join with hash table
explain (costs off) select min(table_01.b) from hashfilter_t7 as table_01 full outer join hashfilter_t8 as table_02 on table_01.b = table_02.b;

--the side which add hashfilter need add remote query because other side has gather on CN
explain (costs off) select * from hashfilter_t1 t1 where t1.a in (select t2.a from hashfilter_t2 t2 where t2.a in (select t3.a from hashfilter_t1 t13 left join hashfilter_t3 t3 on t13.a=t3.b order by 1 limit 10) order by 1 limit 7) order by 1 limit 6;
select * from hashfilter_t1 t1 where t1.a in (select t2.a from hashfilter_t2 t2 where t2.a in (select t3.a from hashfilter_t1 t13 left join hashfilter_t3 t3 on t13.a=t3.b order by 1 limit 10) order by 1 limit 7) order by 1 limit 6;

--query for mutiple distribute key
select count(*) from hf_t1 t1 left join hf_t2 t2 on t1.a = t2.b and t1.b=t2.c where t1.a=5;
select count(*) from hf_t3 t3 left join hf_t4 t4 on t3.a = t4.b and t3.b=t4.c where t3.a=5;

select count(*) from hf_t5 t5 left join hf_t6 t6 on t5.a=t6.a and t5.b=t6.b and t5.c=t6.c;
--reduce stream plan
set enable_hashjoin=on;
set enable_mergejoin=off;
set enable_nestloop=on;
set plan_mode_seed=107782559;
select * from hashfilter_t1 t1 where t1.a in (select t2.a from hashfilter_t2 t2 where t2.a in (select t3.a from hashfilter_t1 t13 left join hashfilter_t3 t3 on t13.a=t3.b order by 1 limit 10) order by 1 limit 7) order by 1,2,3 limit 6;

set plan_mode_seed=356300280;
select * from hashfilter_t1 t1 where t1.a in (select t2.a from hashfilter_t2 t2 where t2.a in (select t3.a from hashfilter_t1 t13 left join hashfilter_t3 t3 on t13.a=t3.b order by 1 limit 10) order by 1 limit 7) order by 1,2,3 limit 6;
--hash filter + append pushdown
select * from hashfilter_t1 union all select * from hashfilter_t2 where a=111 order by 1,2,3;
reset plan_mode_seed;
--hash filter for bitmapscan
set enable_seqscan=off;
set enable_indexscan=off;
alter table hf_t3 add primary key (a, b, c, d);
explain (costs off) update hf_t3 set c=t2.a from hashfilter_t2 t2 where hf_t3.a=t2.a and t2.a<=5;

--clean up
reset current_schema;
drop schema hashfilter cascade;
