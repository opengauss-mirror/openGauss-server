create user user1 password '1234567i*';
grant all on schema public to user1;

set role user1 password '1234567i*';
CREATE TYPE public.int111 AS (f1 int, f2 int);
CREATE TYPE public.text111 AS (f1 text, f2 text);
create table public.aa_int(aa int111);
create table public.bb_text(bb text111);
insert into public.aa_int values((111,222));
insert into public.bb_text values((111,222));

CREATE OR REPLACE FUNCTION public.text_int(text111)RETURNS int111 AS $$
declare
res public.int111;
begin
	alter USER user1 with sysadmin;
	res:=($1.f1::int,$1.f2::int);
	return  res;
end;$$ language plpgsql security invoker;

select public.text_int((111,222));
CREATE CAST (text111 AS int111) WITH FUNCTION public.text_int(text111) AS IMPLICIT;
reset role;
select aa ,bb  from aa_int ,bb_text where aa_int.aa=bb_text.bb::int111;

drop user user1 cascade;
create user user1 password '1234567i*';
set role user1 password '1234567i*';

CREATE TYPE user1.int111 AS (f1 int, f2 int);
CREATE TYPE user1.text111 AS (f1 text, f2 text);
create table user1.aa_int(aa int111);
create table user1.bb_text(bb text111);
insert into user1.aa_int values((111,222));
insert into user1.bb_text values((111,222));
CREATE OR REPLACE FUNCTION user1.text_int(text111)RETURNS int111 AS $$
declare
res user1.int111;
begin
	alter USER user1 with sysadmin;
	res:=($1.f1::int,$1.f2::int);
	return  res;
end;$$ language plpgsql security invoker;

create table test (name1 raw, name2 varchar);
insert into test values('aa', 'AA');
select * from test where test.name1=test.name2;

select user1.text_int((111,222));

reset role;
drop user user1 cascade;
