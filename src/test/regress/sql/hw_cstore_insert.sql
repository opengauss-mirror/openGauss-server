--case 1: before insert, do coerce process in quallist and targetlist
create schema colstore_engine;
create table  colstore_engine.target(
        c_id    varchar,
        c_street_1      varchar(20),
        c_city  text,
        c_zip   varchar(9),
        c_d_id  numeric,
        c_w_id  text)
with(orientation=column);

create table  colstore_engine.source(
        c_id    integer,
        c_street_1      varchar(20),
        c_city  character varying(20),
        c_zip   varchar(9),
        c_d_id  integer,
        c_w_id  integer)
with(orientation=column);

COPY colstore_engine.source(c_id, c_street_1, c_city, c_zip, c_d_id, c_w_id) FROM stdin;
1	lawlipzfcxcle	dyfaoptppzjcgjrvyqa	480211111	1	7
1	cedlipzfcxcle	lawvuhqcck	480211111	21	8
3	cyxkjkgdn	ceddkjgakdgkjg	480211111	1	1
4	dgakkjjkdnjd	dyfaoptppzjcgjra	480211111	4	5
5	ftyjkjkdjkdjkgd	dyfaoptppzdfgjrvyqa	480211111	11	3
6	jkdnnmk	dyfaoptppzjcgjrvyqa	480211111	20	2
7	cedjkjgdakj	frgfptppzjcgjrvyqa	480211111	2	3
8	cdkdngnnnnn	dyfaopjcgjrvyqa	480211111	6	3
9	cyxjkjkgaj	creoptppzjcgjrvyqa	480211111	1	1
12	bkilipzfcxcle	bghaoptppzjcgjrvyqa	480211111	1	15
\N	\N	\N	\N	\N	\N
15	ftyjkjkdjkdjkgd	\N	\N	11	3
\.

explain (verbose, costs off) insert into colstore_engine.target select c_id,c_street_1,c_city,c_zip,c_d_id,c_w_id from colstore_engine.source where c_d_id != 9 and c_w_id != 8 and (c_street_1  like '%cyx%' or c_street_1 like '%ced%' or c_street_1 like '%fty%');
insert into colstore_engine.target select c_id,c_street_1,c_city,c_zip,c_d_id,c_w_id from colstore_engine.source where c_d_id != 9 and c_w_id != 8 and (c_street_1  like '%cyx%' or c_street_1 like '%ced%' or c_street_1 like '%fty%');
insert into colstore_engine.target select c_id,c_street_1,c_city,c_zip,c_d_id,c_w_id from colstore_engine.source where c_d_id != 9 and c_w_id != 8 and (c_street_1  like '%cyx%' or c_street_1 like '%ced%' or c_street_1 like '%fty%') returning *;
insert into colstore_engine.target select c_id,c_street_1,c_city,c_zip,c_d_id,c_w_id from colstore_engine.source where c_d_id != 9 and c_w_id != 8 and (c_street_1  like '%cyx%' or c_street_1 like '%ced%' or c_street_1 like '%fty%') returning c_id;
select * from colstore_engine.target order by c_id;

DROP TABLE IF EXISTS colstore_engine.ct;
create table colstore_engine.ct(a int) with(orientation = column);
insert into colstore_engine.ct values (1);
begin;
alter table colstore_engine.ct add c1 boolean default true;
select * from colstore_engine.ct;
rollback;
begin;
alter table colstore_engine.ct add c1 boolean default true;
select * from colstore_engine.ct;
rollback;

drop schema colstore_engine cascade;

