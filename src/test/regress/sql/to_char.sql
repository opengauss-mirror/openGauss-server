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

-- 保留原始 numeric demo 用例：覆盖正常输出、四舍五入、负数、零值、
-- 以及目标格式 999,999.99 下的溢出表现。
create table t_to_char_numeric(id serial, b numeric);

insert into t_to_char_numeric(b) values(123.45);
insert into t_to_char_numeric(b) values(123.123);
insert into t_to_char_numeric(b) values(123.126);
insert into t_to_char_numeric(b) values(123.454);
insert into t_to_char_numeric(b) values(123.455);
insert into t_to_char_numeric(b) values(123.999);
insert into t_to_char_numeric(b) values(0.999);
insert into t_to_char_numeric(b) values(9.999);
insert into t_to_char_numeric(b) values(99.999);
insert into t_to_char_numeric(b) values(999.999);
insert into t_to_char_numeric(b) values(1999.999);
insert into t_to_char_numeric(b) values(0.12);
insert into t_to_char_numeric(b) values(0.0);
insert into t_to_char_numeric(b) values(0.9);
insert into t_to_char_numeric(b) values(1.0);
insert into t_to_char_numeric(b) values(100.45);
insert into t_to_char_numeric(b) values(1200.56);
insert into t_to_char_numeric(b) values(100123.78);
insert into t_to_char_numeric(b) values(123000.99);
insert into t_to_char_numeric(b) values(123.5);
insert into t_to_char_numeric(b) values(123);
insert into t_to_char_numeric(b) values(123.0);
insert into t_to_char_numeric(b) values(1234.56);
insert into t_to_char_numeric(b) values(12345.67);
insert into t_to_char_numeric(b) values(123456.78);
insert into t_to_char_numeric(b) values(100000.99);
insert into t_to_char_numeric(b) values(-123.45);
insert into t_to_char_numeric(b) values(-123.456);
insert into t_to_char_numeric(b) values(-0.999);
insert into t_to_char_numeric(b) values(-999.999);
insert into t_to_char_numeric(b) values(0);
insert into t_to_char_numeric(b) values(1);
insert into t_to_char_numeric(b) values(99999.999);
insert into t_to_char_numeric(b) values(100000.000);
insert into t_to_char_numeric(b) values(0.001);
insert into t_to_char_numeric(b) values(0.009);
insert into t_to_char_numeric(b) values(99999.999);
insert into t_to_char_numeric(b) values(100000.999);
insert into t_to_char_numeric(b) values(999999.999);
insert into t_to_char_numeric(b) values(1111111.9);

-- 默认参数关闭时的基准输出。
select id, to_char(b, '999,999.99') from t_to_char_numeric order by id;

-- 同时开启 display_leading_zero 和 hide_tailing_zero 时，应保持与原始通用路径一致。
set behavior_compat_options='display_leading_zero,hide_tailing_zero';
select id, to_char(b, '999,999.99') from t_to_char_numeric order by id;
reset behavior_compat_options;

drop table t_to_char_numeric;

-- 两个精确目标格式的 fast path 边界检查。
-- 覆盖整数位长度上限、round 后进位、scale 为 0 的 number 值、
-- 正负号，以及未开启 hide_tailing_zero 时的固定宽度溢出。
reset behavior_compat_options;
select case when
    to_char(100000.000::numeric, '999,999.99') = ' 100,000.00' and
    to_char(99999.999::numeric, '999,999.99') = ' 100,000.00' and
    to_char(0.004::numeric, '999,999.99') = '        .00' and
    to_char(0.005::numeric, '999,999.99') = '        .01' and
    to_char(-0.005::numeric, '999,999.99') = '       -.01' and
    to_char(100000000.000::numeric, '999,999,999.99') = ' 100,000,000.00' and
    to_char(99999999.999::numeric, '999,999,999.99') = ' 100,000,000.00' and
    to_char(0.005::numeric, '999,999,999.99') = '            .01' and
    to_char(cast(1 as number(20,0)), '999,999,999.99') = '           1.00' and
    to_char(cast(-10 as number(20,0)), '999,999,999.99') = '         -10.00' and
    to_char(cast(100000000 as number(20,0)), '999,999,999.99') = ' 100,000,000.00' and
    to_char(cast(999999 as number(20,0)), '999,999.99') = ' 999,999.00' and
    to_char(123456::numeric, '999,999.99') = ' 123,456.00' and
    to_char((-123456)::numeric, '999,999.99') = '-123,456.00' and
    to_char(999999.999::numeric, '999,999.99') = '###########' and
    to_char(-999999.999::numeric, '999,999.99') = '###########' and
    to_char(1000000.000::numeric, '999,999.99') = '###########' and
    to_char(-1000000.000::numeric, '999,999.99') = '###########' and
    to_char(99999999999999999.999::numeric, '999,999.99') = '###########' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '###########' and
    to_char(999999999.999::numeric, '999,999,999.99') = '###############' and
    to_char(1000000000.000::numeric, '999,999,999.99') = '###############' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = '###############' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '###############'
then 'ok' else 'failed' end as fast_path_edges;

-- 默认行为：全 9 格式不会打印小数点前的前导 0，
-- 同时保留固定两位小数。
set behavior_compat_options='';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    to_char(0::numeric, '999,999.99') = '        .00' and
    to_char(0.001::numeric, '999,999.99') = '        .00' and
    btrim(to_char(123::numeric, '999,999.99')) = '123.00' and
    btrim(to_char(-0.12::numeric, '999,999.99')) = '-.12' and
    btrim(to_char(-123::numeric, '999,999.99')) = '-123.00' and
    btrim(to_char(123456::numeric, '999,999.99')) = '123,456.00' and
    btrim(to_char(123456.1::numeric, '999,999.99')) = '123,456.10' and
    btrim(to_char(123456.12::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234567::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456789::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678901234567890::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.125000000::numeric, '999,999.99')) = '123,456.13' and
    btrim(to_char(-123456.125000000::numeric, '999,999.99')) = '-123,456.13' and
    btrim(to_char(0.12::numeric, '999,999,999.99')) = '.12' and
    btrim(to_char(-0.12::numeric, '999,999,999.99')) = '-.12' and
    btrim(to_char(123456789.123456789::numeric, '999,999,999.99')) = '123,456,789.12' and
    btrim(to_char(-123456789.125000000::numeric, '999,999,999.99')) = '-123,456,789.13' and
    to_char(0.001::numeric, '999,999,999.99') = '            .00'
then 'ok' else 'failed' end as fast_path_default_options;

-- 单独开启 display_leading_zero 不应影响这类全 9 格式。
-- 原始通用路径这里仍输出 .12/.00，而不是 0.12/0.00。
set behavior_compat_options='display_leading_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    to_char(0::numeric, '999,999.99') = '        .00' and
    to_char(0.001::numeric, '999,999.99') = '        .00' and
    btrim(to_char(123::numeric, '999,999.99')) = '123.00' and
    btrim(to_char(-0.12::numeric, '999,999.99')) = '-.12' and
    btrim(to_char(-123::numeric, '999,999.99')) = '-123.00' and
    btrim(to_char(0.12::numeric, '999,999,999.99')) = '.12' and
    btrim(to_char(-0.12::numeric, '999,999,999.99')) = '-.12' and
    to_char(0.001::numeric, '999,999,999.99') = '            .00'
then 'ok' else 'failed' end as fast_path_display_leading_zero;

-- hide_tailing_zero 用于裁剪小数部分末尾 0；
-- 当显示值等价于 0 时，原始通用路径返回短字符串 "0"。
set behavior_compat_options='hide_tailing_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    to_char(0::numeric, '999,999.99') = '0' and
    to_char(0.001::numeric, '999,999.99') = '0' and
    btrim(to_char(123::numeric, '999,999.99')) = '123' and
    btrim(to_char(-0.90::numeric, '999,999.99')) = '-.9' and
    btrim(to_char(-123::numeric, '999,999.99')) = '-123' and
    btrim(to_char(0.12::numeric, '999,999,999.99')) = '.12' and
    btrim(to_char(-0.90::numeric, '999,999,999.99')) = '-.9' and
    to_char(0.001::numeric, '999,999,999.99') = '0'
then 'ok' else 'failed' end as fast_path_hide_tailing_zero;

-- 两个兼容参数同时开启：全 9 格式仍不补小数点前的 0，
-- 同时 hide_tailing_zero 保持短字符串 "0" 的行为。
set behavior_compat_options='display_leading_zero,hide_tailing_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    to_char(0::numeric, '999,999.99') = '0' and
    to_char(0.001::numeric, '999,999.99') = '0' and
    btrim(to_char(123::numeric, '999,999.99')) = '123' and
    btrim(to_char(-0.90::numeric, '999,999.99')) = '-.9' and
    btrim(to_char(-123::numeric, '999,999.99')) = '-123' and
    btrim(to_char(0.12::numeric, '999,999,999.99')) = '.12' and
    btrim(to_char(-0.90::numeric, '999,999,999.99')) = '-.9' and
    to_char(0.001::numeric, '999,999,999.99') = '0'
then 'ok' else 'failed' end as fast_path_both_options;

-- truncate_numeric_tail_zero 作用在 numeric_out 上，不应单独影响 numeric_to_char。
-- 因此该参数单独开启时，固定格式 to_char 输出不应变化。
set behavior_compat_options='truncate_numeric_tail_zero';
select case when
    to_char(0.001::numeric, '999,999.99') = '        .00' and
    to_char(0.001::numeric, '999,999,999.99') = '            .00'
then 'ok' else 'failed' end as fast_path_truncate_only;

-- 即使同时开启 truncate_numeric_tail_zero，
-- 控制 to_char 小数尾部裁剪的参数仍应是 hide_tailing_zero。
set behavior_compat_options='hide_tailing_zero,truncate_numeric_tail_zero';
select case when
    to_char(0::numeric, '999,999.99') = '0' and
    to_char(0.001::numeric, '999,999.99') = '0' and
    to_char(0.001::numeric, '999,999,999.99') = '0'
then 'ok' else 'failed' end as fast_path_hide_and_truncate;

-- A_FORMAT + hide_tailing_zero 下的溢出 # 数量，
-- 应按小数尾部 0 裁剪后的最终显示宽度决定，而不总是 fmt_len + 1。
set behavior_compat_options='display_leading_zero,hide_tailing_zero';
select case when
    to_char(999999.999::numeric, '999,999.99') = '#########' and
    to_char(-999999.999::numeric, '999,999.99') = '#########' and
    to_char(1000000.00::numeric, '999,999.99') = '#########' and
    to_char(1000000.01::numeric, '999,999.99') = '###########' and
    to_char(1000000.90::numeric, '999,999.99') = '###########' and
    to_char(1000000.995::numeric, '999,999.99') = '#########' and
    to_char(9999999.995::numeric, '999,999.99') = '##########' and
    to_char(99999999999999999.999::numeric, '999,999.99') = '###########' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '###########' and
    to_char(999999999.999::numeric, '999,999,999.99') = '#############' and
    to_char(-999999999.999::numeric, '999,999,999.99') = '#############' and
    to_char(1000000000.01::numeric, '999,999,999.99') = '###############' and
    to_char(1000000000.995::numeric, '999,999,999.99') = '#############' and
    to_char(9999999999.995::numeric, '999,999,999.99') = '##############' and
    to_char(10000000000.00::numeric, '999,999,999.99') = '##############' and
    to_char(100000000000.90::numeric, '999,999,999.99') = '###############' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = '###############' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '###############'
then 'ok' else 'failed' end as fast_path_a_hide_overflow;

-- 额外 numeric 边界：NULL 透传、接近溢出但未溢出的值、
-- 中间 0000 数字组被省略的 base-10000 存储场景，以及 number(20,0) 输入。
reset behavior_compat_options;
select case when
    to_char(NULL::numeric, '999,999.99') is null and
    btrim(to_char(999999.994::numeric, '999,999.99')) = '999,999.99' and
    btrim(to_char(-999999.994::numeric, '999,999.99')) = '-999,999.99' and
    btrim(to_char(99999999.994::numeric, '999,999,999.99')) = '99,999,999.99' and
    btrim(to_char(-99999999.994::numeric, '999,999,999.99')) = '-99,999,999.99' and
    btrim(to_char(100000.01::numeric, '999,999.99')) = '100,000.01' and
    btrim(to_char(100000.10::numeric, '999,999.99')) = '100,000.10' and
    btrim(to_char(100000000.01::numeric, '999,999,999.99')) = '100,000,000.01' and
    btrim(to_char(100000000.10::numeric, '999,999,999.99')) = '100,000,000.10' and
    btrim(to_char(cast(0 as number(20,0)), '999,999.99')) = '.00' and
    btrim(to_char(cast(999999 as number(20,0)), '999,999.99')) = '999,999.00' and
    btrim(to_char(cast(-999999 as number(20,0)), '999,999.99')) = '-999,999.00'
then 'ok' else 'failed' end as fast_path_numeric_boundaries;

-- dscale > 2 的 round 和数字组场景。
-- 覆盖本地拆位 + round 路径，包括中间 0000 的 NBASE 数字组和负数 round。
reset behavior_compat_options;
select case when
    btrim(to_char(-0.12::numeric, '999,999.99')) = '-.12' and
    btrim(to_char(-0.10::numeric, '999,999.99')) = '-.10' and
    btrim(to_char(100000.007::numeric, '999,999.99')) = '100,000.01' and
    btrim(to_char(100000.012::numeric, '999,999.99')) = '100,000.01' and
    btrim(to_char(100000.015::numeric, '999,999.99')) = '100,000.02' and
    btrim(to_char(10000000.0063::numeric, '999,999,999.99')) = '10,000,000.01' and
    btrim(to_char(1000000.101::numeric, '999,999,999.99')) = '1,000,000.10' and
    btrim(to_char(100000000.00000::numeric, '999,999,999.99')) = '100,000,000.00' and
    btrim(to_char(100000000.1211::numeric, '999,999,999.99')) = '100,000,000.12' and
    btrim(to_char(100000000.1251::numeric, '999,999,999.99')) = '100,000,000.13' and
    btrim(to_char(-100000000.1251::numeric, '999,999,999.99')) = '-100,000,000.13'
then 'ok' else 'failed' end as fast_path_rounding_and_digit_groups;

-- 固定两位小数输出后，以及 round 后的小数尾部裁剪边界。
-- 等价于 0 的结果输出短字符串 "0"；非 0 结果保持原始通用路径的对齐表现。
set behavior_compat_options='hide_tailing_zero';
select case when
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    btrim(to_char(10.00::numeric, '999,999.99')) = '10' and
    btrim(to_char(100000.10::numeric, '999,999.99')) = '100,000.1' and
    btrim(to_char(100000.00::numeric, '999,999.99')) = '100,000' and
    to_char(0.004::numeric, '999,999.99') = '0' and
    btrim(to_char(0.005::numeric, '999,999.99')) = '.01' and
    btrim(to_char(-0.005::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(10.10::numeric, '999,999,999.99')) = '10.1' and
    btrim(to_char(100000000.00::numeric, '999,999,999.99')) = '100,000,000'
then 'ok' else 'failed' end as fast_path_hide_tail_boundaries;

-- A_FORMAT + hide_tailing_zero 下的负数溢出应与原始通用路径保持一致；
-- 这里负号不会额外增加一个 #。
set behavior_compat_options='display_leading_zero,hide_tailing_zero';
select case when
    to_char(-1000000.00::numeric, '999,999.99') = '#########' and
    to_char(-1000000.90::numeric, '999,999.99') = '###########' and
    to_char(-10000000000.00::numeric, '999,999,999.99') = '##############' and
    to_char(-100000000000.90::numeric, '999,999,999.99') = '###############'
then 'ok' else 'failed' end as fast_path_a_hide_negative_overflow;

-- 接近基准测试表结构的 typmod 表字段：
-- price number(19,10) 覆盖 dscale > 2；
-- volume number(20,0) 覆盖整数 numeric 按两位小数格式输出的场景。
reset behavior_compat_options;
create table t_to_char_fast_path_typmod(id serial, price number(19,10), volume number(20,0));
insert into t_to_char_fast_path_typmod(price, volume) values(4000.1234567890, 1);
insert into t_to_char_fast_path_typmod(price, volume) values(0.0000000000, 0);
insert into t_to_char_fast_path_typmod(price, volume) values(999999.9940000000, 999999);
insert into t_to_char_fast_path_typmod(price, volume) values(-0.0050000000, -999999);

select case when
    (select btrim(to_char(price, '999,999.99')) from t_to_char_fast_path_typmod where id = 1) = '4,000.12' and
    (select btrim(to_char(volume, '999,999.99')) from t_to_char_fast_path_typmod where id = 1) = '1.00' and
    (select btrim(to_char(price, '999,999.99')) from t_to_char_fast_path_typmod where id = 2) = '.00' and
    (select btrim(to_char(volume, '999,999.99')) from t_to_char_fast_path_typmod where id = 2) = '.00' and
    (select btrim(to_char(price, '999,999.99')) from t_to_char_fast_path_typmod where id = 3) = '999,999.99' and
    (select btrim(to_char(volume, '999,999.99')) from t_to_char_fast_path_typmod where id = 3) = '999,999.00' and
    (select btrim(to_char(price, '999,999.99')) from t_to_char_fast_path_typmod where id = 4) = '-.01' and
    (select btrim(to_char(volume, '999,999.99')) from t_to_char_fast_path_typmod where id = 4) = '-999,999.00'
then 'ok' else 'failed' end as fast_path_typmod_table_values;

drop table t_to_char_fast_path_typmod;

-- 非目标格式必须快速失败并回退原始通用路径。
-- 这些格式刻意接近目标格式，但不应命中 fast path。
reset behavior_compat_options;
select case when
    btrim(to_char(123.45::numeric, '999,999.999')) = '123.450' and
    btrim(to_char(123.45::numeric, '999,999.9')) = '123.5' and
    btrim(to_char(123.45::numeric, 'FM999,999.99')) = '123.45' and
    btrim(to_char(123.45::numeric, '999,999.90')) = '123.45' and
    btrim(to_char(123456789.45::numeric, '999,999,999.999')) = '123,456,789.450' and
    btrim(to_char(123456789.45::numeric, '999,999,999.9')) = '123,456,789.5' and
    btrim(to_char(123456789.45::numeric, 'FM999,999,999.99')) = '123,456,789.45' and
    btrim(to_char(123456789.45::numeric, '999,999,999.90')) = '123,456,789.45'
then 'ok' else 'failed' end as fast_path_non_target_formats;

-- 更多非目标格式：这些格式和目标格式相近，但包含 FM、D/G、S、EEEE
-- 或小数位不同，必须回退通用 NUM_processor 路径。
reset behavior_compat_options;
select case when
    btrim(to_char(123456789.45::numeric, 'FM999,999,999.99')) = '123,456,789.45' and
    btrim(to_char(-123456789.45::numeric, 'FM999,999,999.99')) = '-123,456,789.45' and
    btrim(to_char(123456789.45::numeric, '999G999G999D99')) = '123,456,789.45' and
    btrim(to_char(-123456789.45::numeric, '999G999G999D99')) = '-123,456,789.45' and
    btrim(to_char(123456789.45::numeric, 'S999,999,999.99')) = '+123,456,789.45' and
    btrim(to_char(-123456789.45::numeric, 'S999,999,999.99')) = '-123,456,789.45' and
    btrim(to_char(123456.78::numeric, '9.99EEEE')) = '1.23e+05' and
    btrim(to_char(-123456.78::numeric, '9.99EEEE')) = '-1.23e+05' and
    btrim(to_char(1::numeric, '999TH')) = '1ST' and
    btrim(to_char(2::numeric, '999TH')) = '2ND' and
    btrim(to_char(3::numeric, '999TH')) = '3RD' and
    btrim(to_char(4::numeric, '999TH')) = '4TH' and
    btrim(to_char(1::numeric, '999th')) = '1st' and
    btrim(to_char(2::numeric, '999th')) = '2nd' and
    btrim(to_char(123.45::numeric, '999,999.999')) = '123.450' and
    btrim(to_char(-123.45::numeric, '999,999.999')) = '-123.450' and
    btrim(to_char(123.45::numeric, 'FM999,999.99')) = '123.45' and
    btrim(to_char(-123.45::numeric, 'FM999,999.99')) = '-123.45'
then 'ok' else 'failed' end as fast_path_more_non_target_formats;

-- 72068f72 之后的补充用例保持正负相邻，覆盖 A_FORMAT 的 round、
-- hide_tailing_zero、溢出宽度和非目标格式回退。
reset behavior_compat_options;
select case when
    btrim(to_char(100000.007::numeric, '999,999.99')) = '100,000.01' and
    btrim(to_char(-100000.007::numeric, '999,999.99')) = '-100,000.01' and
    btrim(to_char(100000.015::numeric, '999,999.99')) = '100,000.02' and
    btrim(to_char(-100000.015::numeric, '999,999.99')) = '-100,000.02' and
    btrim(to_char(999999.994::numeric, '999,999.99')) = '999,999.99' and
    btrim(to_char(-999999.994::numeric, '999,999.99')) = '-999,999.99' and
    to_char(999999.995::numeric, '999,999.99') = '###########' and
    to_char(-999999.995::numeric, '999,999.99') = '###########' and
    btrim(to_char(100000000.1251::numeric, '999,999,999.99')) = '100,000,000.13' and
    btrim(to_char(-100000000.1251::numeric, '999,999,999.99')) = '-100,000,000.13' and
    to_char(999999999.995::numeric, '999,999,999.99') = '###############' and
    to_char(-999999999.995::numeric, '999,999,999.99') = '###############'
then 'ok' else 'failed' end as fast_path_a_paired_rounding;

set behavior_compat_options='hide_tailing_zero';
select case when
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    btrim(to_char(-10.10::numeric, '999,999.99')) = '-10.1' and
    btrim(to_char(100000.00::numeric, '999,999.99')) = '100,000' and
    btrim(to_char(-100000.00::numeric, '999,999.99')) = '-100,000' and
    to_char(999999.999::numeric, '999,999.99') = '#########' and
    to_char(-999999.999::numeric, '999,999.99') = '#########' and
    to_char(1000000.90::numeric, '999,999.99') = '###########' and
    to_char(-1000000.90::numeric, '999,999.99') = '###########' and
    btrim(to_char(100000000.00::numeric, '999,999,999.99')) = '100,000,000' and
    btrim(to_char(-100000000.00::numeric, '999,999,999.99')) = '-100,000,000' and
    to_char(10000000000.00::numeric, '999,999,999.99') = '##############' and
    to_char(-10000000000.00::numeric, '999,999,999.99') = '##############'
then 'ok' else 'failed' end as fast_path_a_paired_hide_tail;

reset behavior_compat_options;
select case when
    btrim(to_char(123.45::numeric, 'FM999,999.99')) = '123.45' and
    btrim(to_char(-123.45::numeric, 'FM999,999.99')) = '-123.45' and
    btrim(to_char(123.45::numeric, '999,999.90')) = '123.45' and
    btrim(to_char(-123.45::numeric, '999,999.90')) = '-123.45' and
    btrim(to_char(123456789.45::numeric, 'FM999,999,999.99')) = '123,456,789.45' and
    btrim(to_char(-123456789.45::numeric, 'FM999,999,999.99')) = '-123,456,789.45' and
    btrim(to_char(123456789.45::numeric, '999,999,999.90')) = '123,456,789.45' and
    btrim(to_char(-123456789.45::numeric, '999,999,999.90')) = '-123,456,789.45'
then 'ok' else 'failed' end as fast_path_paired_non_target_formats;

-- 特殊 numeric 边界：NaN 回退、BI/超大整数、负零 round、
-- 以及 0.0049/0.0050、999999.9949/999999.9950 这类贴边值。
reset behavior_compat_options;
select case when
    to_char('NaN'::numeric, '999,999.99') is not null and
    to_char('NaN'::numeric, '999,999,999.99') is not null and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999.99') = '###########' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999.99') = '###########' and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999,999.99') = '###############' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999,999.99') = '###############' and
    to_char(-0.0049::numeric, '999,999.99') = '        .00' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(0.9949::numeric, '999,999.99')) = '.99' and
    btrim(to_char(0.9950::numeric, '999,999.99')) = '1.00' and
    btrim(to_char(999999.9949::numeric, '999,999.99')) = '999,999.99' and
    to_char(999999.9950::numeric, '999,999.99') = '###########' and
    btrim(to_char(-999999.9949::numeric, '999,999.99')) = '-999,999.99' and
    to_char(-999999.9950::numeric, '999,999.99') = '###########' and
    btrim(to_char(999999999.9949::numeric, '999,999,999.99')) = '999,999,999.99' and
    to_char(999999999.9950::numeric, '999,999,999.99') = '###############' and
    btrim(to_char(-999999999.9949::numeric, '999,999,999.99')) = '-999,999,999.99' and
    to_char(-999999999.9950::numeric, '999,999,999.99') = '###############'
then 'ok' else 'failed' end as fast_path_a_special_numeric_edges;

set behavior_compat_options='hide_tailing_zero';
select case when
    to_char(-0.0049::numeric, '999,999.99') = '0' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(0.9950::numeric, '999,999.99')) = '1' and
    btrim(to_char(999999.9949::numeric, '999,999.99')) = '999,999.99' and
    to_char(999999.9950::numeric, '999,999.99') = '#########' and
    to_char(-999999.9950::numeric, '999,999.99') = '#########' and
    btrim(to_char(999999999.9949::numeric, '999,999,999.99')) = '999,999,999.99' and
    to_char(999999999.9950::numeric, '999,999,999.99') = '#############' and
    to_char(-999999999.9950::numeric, '999,999,999.99') = '#############'
then 'ok' else 'failed' end as fast_path_a_hide_tail_special_edges;

reset behavior_compat_options;

-- 非 A_FORMAT 兼容库验证：B/C/PG 库下溢出应走非 A 固定模板。
-- 用例执行后切回 regression 并删除临时库。
CREATE DATABASE to_char_fast_b DBCOMPATIBILITY 'B';
CREATE DATABASE to_char_fast_c DBCOMPATIBILITY 'C';
CREATE DATABASE to_char_fast_pg DBCOMPATIBILITY 'PG';

\c to_char_fast_b
reset behavior_compat_options;
select case when
    btrim(to_char(123.45::numeric, '999,999.99')) = '123.45' and
    btrim(to_char(99999.999::numeric, '999,999.99')) = '100,000.00' and
    btrim(to_char(123456::numeric, '999,999.99')) = '123,456.00' and
    btrim(to_char(123456.1::numeric, '999,999.99')) = '123,456.10' and
    btrim(to_char(123456.12::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234567::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456789::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678901234567890::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.125000000::numeric, '999,999.99')) = '123,456.13' and
    btrim(to_char(-123456.125000000::numeric, '999,999.99')) = '-123,456.13' and
    btrim(to_char(123456789.123456789::numeric, '999,999,999.99')) = '123,456,789.12' and
    btrim(to_char(-123456789.125000000::numeric, '999,999,999.99')) = '-123,456,789.13' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(1000000.00::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-1000000.00::numeric, '999,999.99') = '-###,###.##' and
    to_char(999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-999999999.999::numeric, '999,999,999.99') = '-###,###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_b_default;

set behavior_compat_options='display_leading_zero,hide_tailing_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    to_char(0::numeric, '999,999.99') = '0' and
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_b_options;

set behavior_compat_options='display_leading_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(-0.12::numeric, '999,999.99')) = '-.12' and
    btrim(to_char(999999.994::numeric, '999,999.99')) = '999,999.99' and
    btrim(to_char(-999999.994::numeric, '999,999.99')) = '-999,999.99' and
    to_char(999999.995::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.995::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(100000000.1251::numeric, '999,999,999.99')) = '100,000,000.13' and
    btrim(to_char(-100000000.1251::numeric, '999,999,999.99')) = '-100,000,000.13' and
    to_char(999999999.995::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-999999999.995::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_b_display_leading_zero;

set behavior_compat_options='hide_tailing_zero';
select case when
    to_char(-0.0049::numeric, '999,999.99') = '0' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(0.9950::numeric, '999,999.99')) = '1' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    btrim(to_char(-0.90::numeric, '999,999.99')) = '-.9' and
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    btrim(to_char(-10.10::numeric, '999,999.99')) = '-10.1' and
    btrim(to_char(100000.00::numeric, '999,999.99')) = '100,000' and
    btrim(to_char(-100000.00::numeric, '999,999.99')) = '-100,000' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(100000000.00::numeric, '999,999,999.99')) = '100,000,000' and
    btrim(to_char(-100000000.00::numeric, '999,999,999.99')) = '-100,000,000' and
    to_char(10000000000.00::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-10000000000.00::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_b_hide_tailing_zero;

reset behavior_compat_options;
select case when
    to_char('NaN'::numeric, '999,999.99') is not null and
    to_char('NaN'::numeric, '999,999,999.99') is not null and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999.99') = ' ###,###.##' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999.99') = '-###,###.##' and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999,999.99') = ' ###,###,###.##' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999,999.99') = '-###,###,###.##' and
    to_char(-0.0049::numeric, '999,999.99') = '        .00' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(999999.9949::numeric, '999,999.99')) = '999,999.99' and
    to_char(999999.9950::numeric, '999,999.99') = ' ###,###.##' and
    btrim(to_char(-999999.9949::numeric, '999,999.99')) = '-999,999.99' and
    to_char(-999999.9950::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(999999999.9949::numeric, '999,999,999.99')) = '999,999,999.99' and
    to_char(999999999.9950::numeric, '999,999,999.99') = ' ###,###,###.##' and
    btrim(to_char(-999999999.9949::numeric, '999,999,999.99')) = '-999,999,999.99' and
    to_char(-999999999.9950::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_b_special_edges;

\c to_char_fast_c
reset behavior_compat_options;
select case when
    btrim(to_char(123.45::numeric, '999,999.99')) = '123.45' and
    btrim(to_char(99999.999::numeric, '999,999.99')) = '100,000.00' and
    btrim(to_char(123456::numeric, '999,999.99')) = '123,456.00' and
    btrim(to_char(123456.1::numeric, '999,999.99')) = '123,456.10' and
    btrim(to_char(123456.12::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234567::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456789::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678901234567890::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.125000000::numeric, '999,999.99')) = '123,456.13' and
    btrim(to_char(-123456.125000000::numeric, '999,999.99')) = '-123,456.13' and
    btrim(to_char(123456789.123456789::numeric, '999,999,999.99')) = '123,456,789.12' and
    btrim(to_char(-123456789.125000000::numeric, '999,999,999.99')) = '-123,456,789.13' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(1000000.00::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-1000000.00::numeric, '999,999.99') = '-###,###.##' and
    to_char(999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-999999999.999::numeric, '999,999,999.99') = '-###,###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_c_default;

set behavior_compat_options='display_leading_zero,hide_tailing_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    to_char(0::numeric, '999,999.99') = '0' and
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_c_options;

set behavior_compat_options='display_leading_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(-0.12::numeric, '999,999.99')) = '-.12' and
    btrim(to_char(999999.994::numeric, '999,999.99')) = '999,999.99' and
    btrim(to_char(-999999.994::numeric, '999,999.99')) = '-999,999.99' and
    to_char(999999.995::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.995::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(100000000.1251::numeric, '999,999,999.99')) = '100,000,000.13' and
    btrim(to_char(-100000000.1251::numeric, '999,999,999.99')) = '-100,000,000.13' and
    to_char(999999999.995::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-999999999.995::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_c_display_leading_zero;

set behavior_compat_options='hide_tailing_zero';
select case when
    to_char(-0.0049::numeric, '999,999.99') = '0' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(0.9950::numeric, '999,999.99')) = '1' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    btrim(to_char(-0.90::numeric, '999,999.99')) = '-.9' and
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    btrim(to_char(-10.10::numeric, '999,999.99')) = '-10.1' and
    btrim(to_char(100000.00::numeric, '999,999.99')) = '100,000' and
    btrim(to_char(-100000.00::numeric, '999,999.99')) = '-100,000' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(100000000.00::numeric, '999,999,999.99')) = '100,000,000' and
    btrim(to_char(-100000000.00::numeric, '999,999,999.99')) = '-100,000,000' and
    to_char(10000000000.00::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-10000000000.00::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_c_hide_tailing_zero;

reset behavior_compat_options;
select case when
    to_char('NaN'::numeric, '999,999.99') is not null and
    to_char('NaN'::numeric, '999,999,999.99') is not null and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999.99') = ' ###,###.##' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999.99') = '-###,###.##' and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999,999.99') = ' ###,###,###.##' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999,999.99') = '-###,###,###.##' and
    to_char(-0.0049::numeric, '999,999.99') = '        .00' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(999999.9949::numeric, '999,999.99')) = '999,999.99' and
    to_char(999999.9950::numeric, '999,999.99') = ' ###,###.##' and
    btrim(to_char(-999999.9949::numeric, '999,999.99')) = '-999,999.99' and
    to_char(-999999.9950::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(999999999.9949::numeric, '999,999,999.99')) = '999,999,999.99' and
    to_char(999999999.9950::numeric, '999,999,999.99') = ' ###,###,###.##' and
    btrim(to_char(-999999999.9949::numeric, '999,999,999.99')) = '-999,999,999.99' and
    to_char(-999999999.9950::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_c_special_edges;

\c to_char_fast_pg
reset behavior_compat_options;
select case when
    btrim(to_char(123.45::numeric, '999,999.99')) = '123.45' and
    btrim(to_char(99999.999::numeric, '999,999.99')) = '100,000.00' and
    btrim(to_char(123456::numeric, '999,999.99')) = '123,456.00' and
    btrim(to_char(123456.1::numeric, '999,999.99')) = '123,456.10' and
    btrim(to_char(123456.12::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.1234567::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.123456789::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.12345678901234567890::numeric, '999,999.99')) = '123,456.12' and
    btrim(to_char(123456.125000000::numeric, '999,999.99')) = '123,456.13' and
    btrim(to_char(-123456.125000000::numeric, '999,999.99')) = '-123,456.13' and
    btrim(to_char(123456789.123456789::numeric, '999,999,999.99')) = '123,456,789.12' and
    btrim(to_char(-123456789.125000000::numeric, '999,999,999.99')) = '-123,456,789.13' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(1000000.00::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-1000000.00::numeric, '999,999.99') = '-###,###.##' and
    to_char(999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-999999999.999::numeric, '999,999,999.99') = '-###,###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_pg_default;

set behavior_compat_options='display_leading_zero,hide_tailing_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    to_char(0::numeric, '999,999.99') = '0' and
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999.99') = '-###,###.##' and
    to_char(99999999999999999.999::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-99999999999999999.999::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_pg_options;

set behavior_compat_options='display_leading_zero';
select case when
    btrim(to_char(0.12::numeric, '999,999.99')) = '.12' and
    btrim(to_char(-0.12::numeric, '999,999.99')) = '-.12' and
    btrim(to_char(999999.994::numeric, '999,999.99')) = '999,999.99' and
    btrim(to_char(-999999.994::numeric, '999,999.99')) = '-999,999.99' and
    to_char(999999.995::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.995::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(100000000.1251::numeric, '999,999,999.99')) = '100,000,000.13' and
    btrim(to_char(-100000000.1251::numeric, '999,999,999.99')) = '-100,000,000.13' and
    to_char(999999999.995::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-999999999.995::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_pg_display_leading_zero;

set behavior_compat_options='hide_tailing_zero';
select case when
    to_char(-0.0049::numeric, '999,999.99') = '0' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(0.9950::numeric, '999,999.99')) = '1' and
    btrim(to_char(0.90::numeric, '999,999.99')) = '.9' and
    btrim(to_char(-0.90::numeric, '999,999.99')) = '-.9' and
    btrim(to_char(10.10::numeric, '999,999.99')) = '10.1' and
    btrim(to_char(-10.10::numeric, '999,999.99')) = '-10.1' and
    btrim(to_char(100000.00::numeric, '999,999.99')) = '100,000' and
    btrim(to_char(-100000.00::numeric, '999,999.99')) = '-100,000' and
    to_char(999999.999::numeric, '999,999.99') = ' ###,###.##' and
    to_char(-999999.999::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(100000000.00::numeric, '999,999,999.99')) = '100,000,000' and
    btrim(to_char(-100000000.00::numeric, '999,999,999.99')) = '-100,000,000' and
    to_char(10000000000.00::numeric, '999,999,999.99') = ' ###,###,###.##' and
    to_char(-10000000000.00::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_pg_hide_tailing_zero;

reset behavior_compat_options;
select case when
    to_char('NaN'::numeric, '999,999.99') is not null and
    to_char('NaN'::numeric, '999,999,999.99') is not null and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999.99') = ' ###,###.##' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999.99') = '-###,###.##' and
    to_char(cast('9223372036854775807' as number(20,0)), '999,999,999.99') = ' ###,###,###.##' and
    to_char(cast('-9223372036854775808' as number(20,0)), '999,999,999.99') = '-###,###,###.##' and
    to_char(-0.0049::numeric, '999,999.99') = '        .00' and
    btrim(to_char(-0.0050::numeric, '999,999.99')) = '-.01' and
    btrim(to_char(999999.9949::numeric, '999,999.99')) = '999,999.99' and
    to_char(999999.9950::numeric, '999,999.99') = ' ###,###.##' and
    btrim(to_char(-999999.9949::numeric, '999,999.99')) = '-999,999.99' and
    to_char(-999999.9950::numeric, '999,999.99') = '-###,###.##' and
    btrim(to_char(999999999.9949::numeric, '999,999,999.99')) = '999,999,999.99' and
    to_char(999999999.9950::numeric, '999,999,999.99') = ' ###,###,###.##' and
    btrim(to_char(-999999999.9949::numeric, '999,999,999.99')) = '-999,999,999.99' and
    to_char(-999999999.9950::numeric, '999,999,999.99') = '-###,###,###.##'
then 'ok' else 'failed' end as to_char_fast_pg_special_edges;

\c postgres
DROP DATABASE to_char_fast_b;
DROP DATABASE to_char_fast_c;
DROP DATABASE to_char_fast_pg;
