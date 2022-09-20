DROP SCHEMA union_null_sche_01 CASCADE;

CREATE SCHEMA union_null_sche_01;

SET CURRENT_SCHEMA TO union_null_sche_01;

CREATE TABLE union_null_table_01(
checkoutdate int8 NULL,
eversion int4 NULL DEFAULT 1,
eminorversion int4 NULL
);

insert into union_null_table_01
  (checkoutdate, eversion, eminorversion)
values
  (1000, 1001, 1002);

insert into union_null_table_01
  (checkoutdate, eversion, eminorversion)
values
  (2000, 2001, 2002);

select '123' as CHECKOUTDATE, null as EVERSION, null as EMINORVERSION
union all
select '123' as CHECKOUTDATE, null as EVERSION, null as EMINORVERSION
union all
select CHECKOUTDATE, EVERSION, EMINORVERSION
  from union_null_table_01;

select '123' as CHECKOUTDATE, null as EVERSION, null as EMINORVERSION
union
select '123' as CHECKOUTDATE, null as EVERSION, null as EMINORVERSION
union
select CHECKOUTDATE, EVERSION, EMINORVERSION
  from union_null_table_01;

select 'a' as ca
union
select 'b' as ca;

select 'a' as ca
union all
select 'b' as ca;

with recursive d(n, fact) as
  (values(1, 2)
  union all
  select n + 1, (n + 1) * fact
    from d
   where n < 5)
SELECT *
  from d;

create table abc(a int);

create table bcd(a text);
insert into abc
  (a)
values
  (1);

insert into abc
  (a)
values
  (2);

insert into abc
  (a)
values
  (3);

insert into bcd
  (a)
values
  ('123');

select NULL as a
union all
select NULL as a
union all
select NULL :: int as a
union all
select a
  from abc
union all
select a
  from abc
union all
select NULL as a
union all
select NULL as a;

select NULL as a
union
select NULL as a
union
select NULL :: int as a
union
select NULL as a
union
select a
  from abc
union
select a
  from abc
union
select NULL as a
union
select NULL as a;

select a
  from abc
union
select a
  from bcd;

select a
  from abc
union all
select a
  from bcd;

select NULL :: text as a
union
select NULL :: int as a;

select NULL :: text as a
union all
select NULL :: int as a;

drop table abc;

drop table union_null_table_01;

DROP SCHEMA union_null_sche_01 CASCADE;
