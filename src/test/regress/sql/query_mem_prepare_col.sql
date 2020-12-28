CREATE TABLE pg_class_stat AS
SELECT relname::text, relpages, reltuples
FROM pg_class c JOIN pg_namespace n ON(c.relnamespace = n.oid AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema'))
WHERE false;

CREATE TABLE pg_statistic_stat AS
SELECT relname::text, staattnum, stadistinct, s.stadndistinct
FROM pg_statistic s JOIN pg_class c ON(c.oid = s.starelid AND c.relnamespace <>11)
WHERE false;

COPY pg_class_stat FROM STDIN;
nation	798	25
supplier	206842	10000000
part	3385740	200000000
partsupp	15164676	800000000
region	710	5
lineitem	106206606	5.99998976e+09
orders	22501294	1.5e+09
customer	3009835	150000000
\.

COPY pg_statistic_stat FROM STDIN;
lineitem	1	54595000	1134450
lineitem	2	171319008	-.782242
lineitem	3	9296500	7667370
lineitem	4	7	7
lineitem	5	50	50
lineitem	6	2483250	2850340
lineitem	7	11	11
lineitem	8	9	9
lineitem	9	3	3
lineitem	10	2	2
lineitem	11	2514	2504
lineitem	12	2459	2460
lineitem	13	2523	2514
lineitem	14	4	4
lineitem	15	7	7
lineitem	16	1193540	1057570
orders	1	-1	-1
orders	2	102680000	-.782297015
orders	3	3	3
orders	4	23443800	-.384443015
orders	5	2406	2406
orders	6	5	5
orders	7	989708	1087000
orders	8	1	1
orders	9	50126600	-.61491102
customer	1	-1	-1
customer	2	-1	-1
customer	3	-1	-1
customer	4	25	25
customer	5	-1	-1
customer	6	1119340	-.259864986
customer	7	5	5
customer	8	-.710779011	-.993120015
part	1	-1	-1
part	2	-1	-1
part	3	5	5
part	4	25	25
part	5	150	150
part	6	50	50
part	7	40	40
part	8	110090	106180
part	9	119143	105068
partsupp	1	-.34217599	-.220521003
partsupp	2	9767220	-.359304011
partsupp	3	9997	9999
partsupp	4	100213	99881
partsupp	5	-1	-.964236021
supplier	1	-1	-1
supplier	2	-1	-1
supplier	3	-1	-1
supplier	4	25	25
supplier	5	-1	-1
supplier	6	919790	-.846526027
supplier	7	-.983283997	-.999135971
region	1	-1	-1
region	2	-1	-1
region	3	-1	-1
nation	1	-1	-1
nation	2	-1	-1
nation	3	-.200000003	-.200000003
nation	4	-1	-1
\.

UPDATE pg_class SET (relpages, reltuples) = (t.relpages, t.reltuples) FROM pg_class_stat t
WHERE pg_class.relname = t.relname and relnamespace=(select oid from pg_namespace where nspname='vector_engine');

UPDATE pg_statistic SET (stadistinct, stadndistinct) = (t.stadistinct, t.stadndistinct)
FROM pg_statistic_stat t, pg_class c
WHERE starelid=c.oid and c.relname = t.relname and pg_statistic.staattnum = t.staattnum
and relnamespace=(select oid from pg_namespace where nspname='vector_engine');

DROP TABLE pg_class_stat;
DROP TABLE pg_statistic_stat;
