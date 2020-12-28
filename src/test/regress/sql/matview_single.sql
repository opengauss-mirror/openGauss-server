-- prepare
create table test(c1 int);
insert into test values(1);
-- base op
drop Materialized view mvtest_tm;

CREATE MATERIALIZED VIEW mvtest_tm AS select *from test;
select *From mvtest_tm;

insert into test values(1);
REFRESH MATERIALIZED VIEW mvtest_tm;
select *from mvtest_tm;

drop Materialized view mvtest_tm;

-- error
create incremental MATERIALIZED VIEW mvtest_inc AS select *from test;
REFRESH incremental MATERIALIZED VIEW mvtest_tm;
