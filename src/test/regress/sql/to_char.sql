-- to_char overflow case test
select to_char(  127::int4, '999');
select to_char(  126::int8, '999');
select to_char(  125.7::float4, '999D9');
select to_char(  125.9::float8, '999D9');
select to_char(  125.9::numeric, '999D9');
select to_char(  8e2, '999D9');
select to_char(  125.9::float8, '999.999');
select to_char(  125.9::numeric, '999.999');
select to_char(  8e2, '999.999');

select to_char(  -127::int4, '999');
select to_char(  -126::int8, '999');
select to_char(  -125.7::float4, '999D9');
select to_char(  -125.9::float8, '999D9');
select to_char(  -125.9::numeric, '999D9');
select to_char(  -8e2, '999D9');
select to_char(  -125.9::float8, '999.999');
select to_char(  -125.9::numeric, '999.999');
select to_char(  -8e2, '999.999');

select to_char(  1287::int4, '999');
select to_char(  1286::int8, '999');
select to_char(  1285.7888::float4, '999D9');
select to_char(  1285.9888::float8, '999D9');
select to_char(  1285.8889::numeric, '999D9');
select to_char(  8e99, '999D9');
select to_char(  1285.9888::float8, '999.999');
select to_char(  1285.8889::numeric, '999.999');
select to_char(  8e99, '999.999');


select to_char(  -1287::int4, '999');
select to_char(  -1286::int8, '999');
select to_char(  -1285.7888::float4, '999D9');
select to_char(  -1285.9888::float8, '999D9');
select to_char(  -1285.8889::numeric, '999D9');
select to_char(  -8e99, '999D9');
select to_char(  -1285.9888::float8, '999.999');
select to_char(  -1285.8889::numeric, '999.999');
select to_char(  -8e99, '999.999');
