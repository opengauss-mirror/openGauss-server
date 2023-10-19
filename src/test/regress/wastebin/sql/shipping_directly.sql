set current_schema='shipping_schema';
--convert
--convert_to
--convert_from
--age(timestamptz)
--age(timestamp)
--overlaps(timestamptz,interval,timestamptz,interval)
--overlaps(timestamptz, timestamptz,timestamptz,interval)
--overlaps(timestamptz,interval,timestamptz, timestamptz)
--to_char(float4,text)
--to_char(float8,text)
--to_char(interval,text)
--to_number(text,text)
--to_timestamp(text)
--timezone(text,timetz)
--generate_series(timestamptz, timestamptz, interval)
--regexp_like
--regex_like_m
--pg_encoding_to_char
--pg_char_to_encoding

---------------convert/convert_to/convert_from-----------------
---case1 shipping---
explain (costs off) select t4.a,convert_from(convert(convert_to(t5.c, 'GBK'),'GBK','LATIN1'),'LATIN1') from t4, t5 where t4.b = t5.a order by 1 limit 3; 

select t4.a,convert_from(convert(convert_to(t5.c, 'GBK'),'GBK','LATIN1'),'LATIN1') from t4, t5 where t4.b = t5.a order by 1,2 limit 3;

---case2 shipping---
explain (costs off) select t4.a from t4, t5 where t4.b = t5.a and t4.c = convert_from(convert(convert_to(t5.c, 'GBK'),'GBK','LATIN1'),'LATIN1') order by 1 limit 3;

select t4.a from t4, t5 where t4.b = t5.a and t4.c = convert_from(convert(convert_to(t5.c, 'GBK'),'GBK','LATIN1'),'LATIN1') order by 1 limit 3;

---------------age(timestamptz)/age(timestamp)-----------------
---case1 shipping---
explain (costs off) 
select t4.a, age(timestamptz'1757-06-13+08') > interval '1 year', age(timestamp'9999-06-13') > interval '1 year' from t4, t5 where t4.b = t5.a order by 1,2,3 limit 1;
select t4.a, age(timestamptz'1757-06-13+08') > interval '1 year', age(timestamp'9999-06-13') > interval '1 year' from t4, t5 where t4.b = t5.a order by 1,2,3 limit 1;

---case2 shipping---
explain (costs off) 
select t4.a from t4, t5 where t4.b = t5.a and age(timestamptz'1757-06-13+08') > interval '1 year' and age(timestamp'9999-06-13') < interval '1 year' order by 1 limit 1;
select t4.a from t4, t5 where t4.b = t5.a and age(timestamptz'1757-06-13+08') > interval '1 year' and age(timestamp'9999-06-13') < interval '1 year' order by 1 limit 1;

----1305 overlaps(timestamptz,interval,timestamptz,interval)/1306 overlaps(timestamptz, timestamptz,timestamptz,interval)/1307 overlaps(timestamptz,interval,timestamptz, timestamptz)---
---case1 shipping---
explain (costs off)
select overlaps(t6.b,t6.c,t7.b,t7.c), overlaps(t6.b,t7.b,t7.b,t7.c),overlaps(t6.b,t6.c,t7.b,t6.b) from t6, t7 where t6.d = t7.d;

select overlaps(t6.b,t6.c,t7.b,t7.c), overlaps(t6.b,t7.b,t7.b,t7.c),overlaps(t6.b,t6.c,t7.b,t6.b) from t6, t7 where t6.d = t7.d;

---case2 shipping---
explain (costs off)
select t6.e, t7.e from t6,t7 where overlaps(t6.b,t7.b,t7.b,t7.c) and overlaps(t6.b,t6.c,t7.b,t7.c) and overlaps(t6.b,t6.c,t7.b,t6.b) order by 1,2;

select t6.e, t7.e from t6,t7 where overlaps(t6.b,t7.b,t7.b,t7.c) and overlaps(t6.b,t6.c,t7.b,t7.c) and overlaps(t6.b,t6.c,t7.b,t6.b) order by 1,2;

------to_char(float4,text)/to_char(float8,text)/to_char(interval,text)------
---case1 shipping---
explain (costs off)
select to_char(t8.a,'00999.00')from t8 ;

select to_char(t8.a,'00999.00')from t8 order by 1;
---case2 shipping---
explain (costs off)
select t8.a,t8.b from t8 ,t9 where t8.c=t9.c and to_char(t8.a,'00999.9900') = to_char(t9.b,'00999.9900');

select t8.a,t8.b from t8 ,t9 where t8.c=t9.c and to_char(t8.a,'00999.9900') = to_char(t9.b,'00999.9900');

--------------to_number(text,text)/to_timestamp(text)/timezone(text,timetz)------------
---case1 shipping---
explain (costs off)
select to_number(t8.a::text,'9999.00') from t8 ,t9 where t8.c=t9.c;

select to_number(t8.a::text,'9999.00') from t8 ,t9 where t8.c=t9.c order by 1;

---case2 shipping---
explain (costs off)
select t8.a,t8.b from t8 ,t9 where t8.c=t9.c and to_number(t8.a::text,'00999999.999900') = to_number(t9.b,'00999999.9999900');

select t8.a,t8.b from t8 ,t9 where t8.c=t9.c and to_number(t8.a::text,'00999999.999900') = to_number(t9.b,'00999999.9999900');

---case3 shipping---

explain (costs off)
select timezone('hkt',current_time),count(*) from t4, t9 where t4.b = t9.c order by 1;


--------------pg_char_to_encoding/pg_encoding_to_char/regexp_like/regex_like_m------------
---case1 shipping---
explain (costs off)
select pg_char_to_encoding('UTF-8'),count(*) from t4, t9 where t4.b = t9.c order by 1;
---case2 shipping---
explain (costs off)
select pg_encoding_to_char(7),count(*) from t4, t9 where t4.b = t9.c order by 1;
---case3 shipping---
explain (costs off)
select regexp_like(t10.a, t10.a, 'i'),count(*) from t10, t9 where t10.b = t9.c group by t10.a order by 1;

---case4 shipping---
explain (costs off)
select regexp_like(t10.a, t10.a, 'm'),count(*) from t10, t9 where t10.b = t9.c group by t10.a order by 1;

---case4 shipping---
explain (costs off)
select regexp_like(t10.a, t10.a, 'c'),count(*) from t10, t9 where t10.b = t9.c group by t10.a order by 1;


---case5 shipping---
explain select count(*) from t10, 
(select *  from generate_series('2018-10-25 00:00:00+08'::timestamptz,'2018-10-26 00:00:00+08'::timestamptz,'1 hour'))tmp  
where t10.a = tmp.generate_series order by 1;
