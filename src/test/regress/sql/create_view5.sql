--
-- CREATE_VIEW3
--
-- Enforce use of COMMIT instead of 2PC for temporary objects

--test COMMENT ON view's column
create table test_comment_normal_view_column_t(id1 int,id2 int);
create or replace view test_comment_normal_view_column_v as select * from test_comment_normal_view_column_t;
create temp table test_comment_temp_view_column_t(id1 int,id2 int);
create or replace temp view test_comment_temp_view_column_v as select * from test_comment_temp_view_column_t;
comment on column test_comment_normal_view_column_t.id1 is 'this is normal table';
comment on column test_comment_normal_view_column_v.id1 is 'this is normal view';
comment on column test_comment_temp_view_column_t.id1 is 'this is temp table';
comment on column test_comment_temp_view_column_v.id1 is 'this is temp view';
\d+ test_comment_normal_view_column_t
\d+ test_comment_normal_view_column_v
\d+ test_comment_temp_view_column_t
\d+ test_comment_temp_view_column_v
comment on column test_comment_normal_view_column_t.id1 is 'this is normal table too';
comment on column test_comment_normal_view_column_v.id1 is 'this is normal view too';
comment on column test_comment_temp_view_column_t.id1 is 'this is temp table too';
comment on column test_comment_temp_view_column_v.id1 is 'this is temp view too';
\d+ test_comment_normal_view_column_t
\d+ test_comment_normal_view_column_v
\d+ test_comment_temp_view_column_t
\d+ test_comment_temp_view_column_v
drop view test_comment_normal_view_column_v;
drop table test_comment_normal_view_column_t;

-- check display of ScalarArrayOp with a sub-select
select 'foo'::text = any(array['abc','def','foo']::text[]);
select 'foo'::text = any((select array['abc','def','foo']::text[]));  -- fail
select 'foo'::text = any((select array['abc','def','foo']::text[])::text[]);

create view tt19v as
select 'foo'::text = any(array['abc','def','foo']::text[]) c1,
       'foo'::text = any((select array['abc','def','foo']::text[])::text[]) c2;
select pg_get_viewdef('tt19v', true);
drop view tt19v;

-- This test checks that proper typmods are assigned in a multi-row VALUES

CREATE VIEW tt1 AS
  SELECT * FROM (
    VALUES
       ('abc'::varchar(3), '0123456789', 42, 'abcd'::varchar(4)),
       ('0123456789', 'abc'::varchar(3), 42.12, 'abc'::varchar(4))
  ) vv(a,b,c,d);
\d+ tt1
SELECT * FROM tt1;
SELECT a::varchar(3) FROM tt1;
DROP VIEW tt1;

-- check handling of views with immediately-renamed columns

create view tt23v (col_a, col_b) as
select q1 as other_name1, q2 as other_name2 from int8_tbl
union
select 42, 43;

select pg_get_viewdef('tt23v', true);
select pg_get_ruledef(oid, true) from pg_rewrite
    where ev_class = 'tt23v'::regclass and ev_type = '1';
DROP VIEW tt23v;
-- check display of assorted RTE_FUNCTION expressions

create view tt20v as
select * from
  coalesce(1,2) as c,
  collation for ('x'::text) col,
  current_date as d,
  cast(1+2 as int4) as i4,
  cast(1+2 as int8) as i8;
select pg_get_viewdef('tt20v', true);
drop view tt20v;
