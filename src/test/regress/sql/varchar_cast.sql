-- test varchar char semantic cast
SET client_min_messages = warning;
drop table if exists test_varchar_cast;
create table test_varchar_cast (id int, val varchar (30 char));
insert into test_varchar_cast values(1, 'test_varchar_cast');
select val::regclass from test_varchar_cast where id = 1;
insert into test_varchar_cast values(2, '2'::text);
insert into test_varchar_cast values(3, '3'::clob);
insert into test_varchar_cast values(4, '4'::char(10));
insert into test_varchar_cast values(5, '5'::char(10 char));
select val::char(10) from test_varchar_cast where id = 1;
select val::char(10 char) from test_varchar_cast where id = 1;
select val::text from test_varchar_cast where id = 1;
select val::clob from test_varchar_cast where id = 1;
insert into test_varchar_cast values(6, '6'::nvarchar2);
select val::nvarchar2 from test_varchar_cast where id = 1;
insert into test_varchar_cast values(7, '77'::char);
insert into test_varchar_cast values(8, '8'::name);
select val::char from test_varchar_cast where id = 7;
select val::name from test_varchar_cast where id = 8;
insert into test_varchar_cast values(9, '9'::cidr);
select val::cidr from test_varchar_cast where id = 9;
insert into test_varchar_cast values(10, '10.0.0.0'::inet);
select val::inet from test_varchar_cast where id = 10;
insert into test_varchar_cast values(11, 't'::bool);
select val::bool from test_varchar_cast where id = 11;
insert into test_varchar_cast values(12, '12'::xml);
select val::xml from test_varchar_cast where id = 12;
insert into test_varchar_cast values(13, '2020-1-1'::date);
select val::date from test_varchar_cast where id = 13;
insert into test_varchar_cast values(14, '14'::numeric);
select val::numeric from test_varchar_cast where id = 14;
insert into test_varchar_cast values(15, '15'::int4);
select val::int4 from test_varchar_cast where id = 15;
insert into test_varchar_cast values(16, '16'::int8);
select val::int8 from test_varchar_cast where id = 16;
insert into test_varchar_cast values(17, '2020-1-1 00:00:00'::timestamp);
select val::timestamp from test_varchar_cast where id = 17;
insert into test_varchar_cast values(18, '18'::int2);
select val::int2 from test_varchar_cast where id = 18;
insert into test_varchar_cast values(19, '19'::float4);
select val::float4 from test_varchar_cast where id = 19;
insert into test_varchar_cast values(20, '20'::float8);
select val::float8 from test_varchar_cast where id = 20;
insert into test_varchar_cast values(21, '21'::int1);
select val::int1 from test_varchar_cast where id = 21;
insert into test_varchar_cast values(22, '22'::char(10 char));
select val::char(10 char) from test_varchar_cast where id = 22;




