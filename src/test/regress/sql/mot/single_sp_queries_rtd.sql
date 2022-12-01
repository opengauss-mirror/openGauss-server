create foreign table customer (
c_w_id         integer        not null,
c_d_id         integer        not null,
c_id           integer        not null,
c_discount     decimal(4,4),
c_credit       char(2),
c_last         varchar(16) not null,
c_first        varchar(16) not null,
c_credit_lim   decimal(12,2),
c_balance      decimal(12,2),
c_ytd_payment  decimal(12,2),
c_payment_cnt  integer,
c_delivery_cnt integer,
c_street_1     varchar(20),
c_street_2     varchar(20),
c_city         varchar(20),
c_state        char(2),
c_zip          char(9),
c_phone        char(16),
c_since        timestamp,
c_middle       char(2),
c_data         varchar(500),
primary key (c_w_id, c_d_id, c_id)
);



insert into customer values(1,1,1,.4819,'GC','BARBARBAR','K1qXJYUx8',50000.00,-10.00,10.00,1,1,'GzRriVYf8PkgzfDWHo','hw5XQWK43NYR','S4qUZfkIBvp7HIgdYEPc','RF',637211111,3140408377069331,'2021-05-03 18:17:11.558','OE','qJ4LHCniHIiv5dnReoE36YzJnndwpSTXp2VWCPcVQYJ2SZxJ6RfQAt6zs3wwyl3rEDeoo5UEAzZKndJaqaU5I27X1u0jNUUXGtnM24Q9i4L3rY9ysXzokgB0eReIBPHmcizS7qtULcIy8cyD3FrmsvVgABPeocvNSwltSxLkXm0G9yI2Z9lTmWQomiDWoYDGQtCKxxqwRFGz3KeKf8mh1HicQSPL57wP3cC5SK3JPjhRMlldWrUzthBa6orKIJhpHkz4UrJCvce71o9lWE0DezPuMQZvVs3vHey3E9D8my9ZhvnphCROC8qJ2pIQ7Jv0asc5kUCFPkOrLz9KOmYnsP80KFVy377AMjuFTnTtXCExxDPjCJaJf1exzgfcBGLMxDjcw1HX7f4beI10GyX2P2KOuE');
insert into customer values(1,1,2,.1120,'GC','BARBAROUGHT','KhZaT7ZZgZoiU',50000.00,-10.00,10.00,1,1,'EhbkYYG1wX8CJ1sqFQn','ibBtwABlISHbmIS36Nq','zrj4DcMHmbwQF','FZ',735311111,2391423312654894,'2021-05-03 18:17:11.558','OE','Su57rgcoXVY8gVIYH68miuTOIsiMXMNuTh0AwX5lc3CVm7U5x1UJS6cqVypHanbxLmwbf9ru5tt2g2GzORMhojJW4CchtVlSCBor6D3ZO32vIVUmwpOtgV7ZfHsapanWZHlXPTf3it5fkUCctrAZZ5bCuxFCFq1HjDuGGQ3YfAap8NKyPGWdlZI4dexOjuQGHSGwaOV97WR1YhaahDG4Y4z3EdmLPkIutg5ATjDFFAYBIJmWPfx1scIEZJANfO2LMsJuORNA1pY5RcBgIE528LO4nji8QrGPuiIUgsftepu2YHLSD5OjEo3xiNotRJAwkB');
insert into customer values(1,1,3,.4651,'BC','BARBARABLE','IeE0f3Mp',50000.00,-10.00,10.00,1,1,'QTCUXZXNkbyJE','ix2pRoBHnO','pk7FNxuq9mc','OG',818211111,9505990784961970,'2021-05-03 18:17:11.558','OE','YYp3gkM6KDTE9DjLmWoxUqYQzBYV9YGQx8OzAGd0JjAvujjvzdrA7jfpYRJHuIqGKHuNlzXT1Tl7wZfJknuUXfZUfohIJ3lxPMQxzt2xMKlNXweJ28bbfR5nWbLGnQviVAotqce8KTGrqyVCN38M7hibGRbogKa55t3Wu7XePi1Mghllws62kAKgvC9DaWojjOcFXXnKI1bZgMjoDeVxZFATn7tCBVIVDh8kUR7SkJ1ZYjykbH9Iyi62vL497aOeqjRRa8dSoYNdgwxcUv891p1KyVgSGAMU8IfBnMyfG5wB0Ow8Rfkr9XedEk4B7R03bNhFUAfVF7L81z9EjFSc44SKwpovXBMGubxQz7fnEc9JDbCd2KIGpetqoeqwkmkAHyCbGfJP8GZGZoioTmLNNasvsotbRh7S1sqM2mI4qtRHOoJ9mKfY1WW5Ui2wMxWKQNI10s7VovXfSfgO');
insert into customer values(1,1,4,.0141,'GC','BARBARPRI','XmyH4CJm3GTVRan',50000.00,-10.00,10.00,1,1,'DDgvLy5wBLRh5zX7ru2','LxqcrPJyQO8U','ogPkYtXVquW0N','FU',552811111,9444908480678354,'2021-05-03 18:17:11.558','OE','pF8XO8imui000TS00FSydSJzZAxPzJXSpTFjnJLlXF1C8rYY8QXtNyNN9ouRQScd80tbuAtcSYTEQPgxUgd9u79e69qBlPDDYGTISqBwG90OTEzFrakWyzySTASQeIRYbe3VPgiuRcMwybGoipZLbDHNixlGSZZCeXkFEempaPDaDBIqtrdnq6LAZAaDN18wCgZjMAv2B7EGteg2UtdQswOFOwGbnwG4VsMbp7axJsM74ZlS4E92MllOZnWD0cHyGk6KG7wRqzQlQNmi1cQix9YiG6j813WqktJw629RmwwmpqTNJ7TAERKuRUItksVl3Hvx1lgloX7V5WfbHyaHd57i2kIqMY8zeoAacD0DEICzugH7yynutInaTpZovuPUADs7NdVy393HDLaKQpq1ZzYnP9hxLmDhU9zjs7a');
insert into customer values(1,1,5,.1251,'GC','BARBARPRES','WEHHK7ZdvakFPwa',50000.00,-10.00,10.00,1,1,'WnrnoQKSED','GTFxzPEcua66HQVmeuWp','cxsKcj4zQEb3xnurFcz','EY',581311111,0980067796148934,'2021-05-03 18:17:11.558','OE','ckgfjVSUJgXdkTjOmAhfnDlUI3TWt3AMnxTu9BUlPan1c5GCRo4NQKDX9H1lCX312ZKGqjCHKWtPKgL1uDDw1xnbrK2xePuaLBd92w44ZrWX2F6PvnDyR0yPZqaAmuFNLxd7KKbWJp57RWQSZ1qrTfB1FXECeHCPWtpSZX2bL9ziWqXS1LQ7OUGo1PmG9CnlRRVK93Qh6wvCY9ruONB2mf1q0CH0kOL9IDdvNBrN5kfFND2ivFfaZbEGD8SaIaDmj41y9B2cOzy9rALeNMUURnhEnu7A8XhRpQcW0lzBdSishHC4iHMaFrgnYw3cYUJnPjBt9n6JxIKOQx4u3qHWytXIco4PLoD4aN7z4ERYRxXDayTqgg4Yj0cpP1EmkstnXcYj6JDzgkCCM4Nofn5g5BS1vxVGXBAyYwooQ2XagDXfDdjhLXB5dL379BUFOlNMIatk');
insert into customer values(1,1,6,.2541,'GC','BARBARESE','ZoeeBnnU1Zop6ML',50000.00,-10.00,10.00,1,1,'tGTUqTtjP1P0W','yUty9WfLIrslZUX','XpkIRU7D1rOt9ev95V6','NJ',093311111,0381242010918520,'2021-05-03 18:17:11.558','OE','KBRveK8nbwgb87VuhY9T2YsQoBEemGvuwQisgsXk8iJuVFwvF75aL0SOQi0iGIOHIDUAvznb5QVmzq4sY7cAG5gElx8DXvYa1lA3qMVDUSBtdgTYNxWchbjGbMuXOQdqOylH3Bo9CvDyoaLvyjg2KvkzRheGpTgaJFCEq4eKBVxsIt7biiqzoPvDXIuqcMpuYjcT86UtwOuuJRRWuEliL7ehviiY6WhqEc7TX6zORSpJ3hCwsaMRLnurwZFcFGHxUxfboDB91nm7N5ydJROzUz5qmafczmw5SPEONkkuiPnEeffMtKCgyXqjR2FIjSaXXWEyHGdkoDUd3yNX3PgQxFK8vIiLEIGNl5CrHnoDZE41v9WEAVi4MqesWdqcbguxdohuAXQhH4aZRNJLoQb2o8X5bQJccTLqzEVZVa58ZePzeTG6NUvDxVMSQPp7JAYqGtzPO0PpsBvpXj9ixIY0vbEjIJV5nPBLuiondBvfFPa');
insert into customer values(1,1,7,.2211,'GC','BARBARANTI','yP3UO14nDcKVWT',50000.00,-10.00,10.00,1,1,'CsRfGWBSiHsM48m','nQcWfwSnQ8rx','s3BiVOmYlUVDULfUd5Sh','NJ',986511111,9852899873047153,'2021-05-03 18:17:11.558','OE','D2HYFitKPl71BaupGFL7dpmvCZBF9sVeKrCZtQCq3tjuHRPAiRUolinxKqGqygM9ifnwULISJR9U2l5CwpELoyFetm5HJnAo9MM7sdcpyZyrb60hk5lZhTcjvlqTCsA2jcn7b4Q6aI20tyxwZLwD8G5lWvRFR9VpKbQZyVNwlR1yUPn5iXTYJemslhS35jNfVympGSPtt42RZz3pl3Y7iLAg3naB4E9tX0rLCyix0pjNJ1pEJVZkE0lUdLB4BSv7qBTRgl6NytO5Pvvvvq9vY13lHH8uBcK5Snv8pcakmtWNqxFUMkVIkQmpQywWKWQbvzvN9v9MKCT6y6O2x7vRADDmLC5QvA4pMwmR5dshCVUoqLYAS1YxWo2kkGdGLYKooLjXjLCMt');
insert into customer values(1,1,8,.4275,'GC','BARBARCALLY','AmIEo5hro',50000.00,-10.00,10.00,1,1,'T38BcTmUfrfwlVE','k7fJJVbI5PEH8dwv3zdd','MSAOPNhZbRlT2jMJG','BH',312011111,9652687324079719,'2021-05-03 18:17:11.558','OE','BEQM7mOJK9pDfNqnLF7ppP36SiV0widFPOT6DU3YY0VFnxP5veTUngwPtlxVgF7t2G8a8PTxHRw3GNb1RrYRiR13uBd49Mqzmtee37LUyCPGyyze5ZQtTvRdhun2Vucfwv1R4esQ6bPoAgnteYLyWzBXBod2ZNOruLNNqfm3r9OUOyDDkzz69wpEKxa6MXISoSe9OrfkBKiza76N09YowaM1s10IsIrqSDW6HhW91hszYemTjJRiUUSo72tW9mTTZiVrtGYkrDyYbBBKhqtdPasJ7I6SUkEtFyM77WFYSsPrVGG2Dd9AWvCN6eby8Cpvbwpo6egBqQq2ckZkTlbOPmde2r4XwAr5lmOOVo63ebUOJD7l5LQp4tNUYMn6UxLH5MaUizguaISlgjYqykKfx5sz5Fh5LPNmY5HiVGcc9JYEPpT');
insert into customer values(1,1,9,.0230,'BC','BARBARATION','o7S4i3Fyzj7CE',50000.00,-10.00,10.00,1,1,'uew980V3LMjRWlaMkr','DbXnfpT5t9gh','L2QLwQzOZkX','EQ',741911111,1093277057883824,'2021-05-03 18:17:11.558','OE','dZrWWxARghTi7R3cF8CwnOUkZtmVVQLZvvZpPFOaZnqgPA9G2FLG2Vr7F6h7o2wlkOUiv67xxS1TiZ1HNhFBe8H0w1xNTXyG2Hj0pXJZ1ArSXufD0KTpJmcIQqRshwEIa8lHG34LA0mnexakbU0nkMgRSam8EZyES2o9GFcDmmSVtM3hmrC7jOuI2ggJo3uMd1QrViWh9FltJHiENTyRfV880oRBtcKevmG1S3CTedq4XPQQwBKK6Bm3uBiPfiby6dIeYNeMEPcoM7C0W5y4llCR0nWwFhpOFi3SrmWZ5JJhanfLgSQTjdLrfBZo9yjrPSFQjLF2cbTQAgWf');


drop procedure if exists functions;

-- max, min, avg, count, left, right, update, select, substring, coalesce, extract, epoch, concat, rtrim, abs, position, order by
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='query_count';
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='function_count';

CREATE OR REPLACE PROCEDURE functions(OUT out1 integer, OUT out2 integer, OUT out3 integer, OUT out4 timestamp, OUT out5 timestamp,
OUT out6 decimal, OUT out7 decimal, OUT out8 character varying, OUT out9 character varying, OUT out10 integer, OUT out11 character varying, 
OUT out12 integer, OUT err_msg character varying)
AS
DECLARE
    v_count1 integer;
	v_count2 integer;
BEGIN

    SELECT COUNT(*) INTO v_count1 from customer where c_credit like 'G%';
    out1 := v_count1;
	
	SELECT COUNT(*) INTO v_count2 FROM customer
			where c_w_id = 1
			and c_d_id < 10
			and c_id <> 100
			and c_credit like 'G%'
		GROUP BY c_id 
		ORDER BY c_id
		LIMIT 1;
	out2 := v_count2;
	
	select count( distinct c_id) into out3 from customer where c_d_id <10;
	select max( c_since) into out4 from customer;
	select min( c_since) into out5 from customer;
	select avg( c_discount) into out6 from customer;
	update customer set c_credit_lim = c_balance - c_ytd_payment where c_d_id <> 5;
	select count(*) into out7 from customer where c_d_id BETWEEN 6 AND 9;
	
	select SUBSTRING( c_first, 1, 5 ) into out8 from customer where c_balance = (select max(c_balance) from customer) order by 1 limit 1;
	select COALESCE (c_first, LEFT(c_data, 15)) into out9 from customer order by c_first desc limit 1;
	select extract(epoch from c_since) - extract(epoch from c_since - 10) into out10 from customer limit 1;
	select concat((select left(c_data,5) from customer order by c_first desc limit 1), (select right(c_data,5) from customer order by c_first asc limit 1)) into out11 from customer limit 1;
	select count (rtrim (c_phone, '260944')) into v_count2 from customer;
	select max(abs(POSITION('XXX' IN c_data))) into out12 from customer;
		
EXCEPTION
    WHEN OTHERS THEN
        err_msg := SQLERRM;

END;
/

select functions();
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='query_count';
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='function_count';


--------------------------------------------------------------------------
-- sum, ceiling, char_length, length, cos, lower, upper, mod
drop procedure if exists functions;

CREATE OR REPLACE PROCEDURE functions(OUT out1 integer, OUT out2 integer, OUT out3 integer, OUT out4 numeric, OUT out5 integer,
OUT out6 character varying, OUT out7 character varying, OUT out8 integer, OUT err_msg character varying)
AS
DECLARE
    v_count1 integer;
	v_count2 integer;
BEGIN
	select sum(c_id) into out1 from customer;
	SELECT CEILING(c_discount + 9.76) into out2 from customer order by c_discount desc limit 1;
	SELECT char_length(c_data) into out3 from customer order by c_data desc limit 1;
	SELECT length(c_data) into out4 from customer order by c_data desc limit 1;
	select cos(c_balance) into out5 from customer limit 1; 
	select lower(c_first) into out6 from customer where c_id = 1 order by c_first limit 1;
	select upper(c_first) into out7 from  customer where c_id = 1 order by c_first limit 1;
	select mod((select sum(c_id) from customer), 3) into out8 from customer limit 1;
	
EXCEPTION
    WHEN OTHERS THEN
        err_msg := SQLERRM;

END;
/

select functions();
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='query_count';
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='function_count';

-------------------------------------------------------------------------
-- REGEXP_MATCHES, sqrt, 
drop procedure if exists functions;

CREATE OR REPLACE PROCEDURE functions(OUT out1 character varying, OUT out2 integer, OUT err_msg character varying)
AS
DECLARE
    v_count1 integer;
	v_count2 integer;
BEGIN
	SELECT REGEXP_MATCHES(c_data, '([A-Za-z0-9_]+)','g') into out1 from customer order by c_data desc limit 1;
	select sqrt((select sum(c_id) from customer)) into out2 from customer limit 1;
	
EXCEPTION
    WHEN OTHERS THEN
        err_msg := SQLERRM;

END;
/

select functions();
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='query_count';
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='function_count';

------------------------------------------------------------------------

-- create, update, delete, loop, extract, day, month, year, left join, right join, inner join, IN
drop procedure if exists functions;

CREATE OR REPLACE PROCEDURE functions(OUT out1 integer, OUT out2 character varying, OUT out3 character varying, OUT out4 character varying, OUT out5 integer, OUT out6 integer, 
OUT out7 integer, OUT out8 integer, OUT out9 integer, OUT out10 integer, OUT err_msg character varying)
AS
DECLARE
    counter integer := 0;
	v_count2 integer := 0;
	vsum integer := 0;
BEGIN
	create foreign table if not exists tmp (like customer);
	insert into tmp select * from customer;
	update tmp set c_balance = c_balance + c_credit_lim where (c_d_id = 2 or c_d_id = 3) and c_credit like 'G%';
	delete from tmp where (c_d_id = 2 or c_d_id = 3) and c_credit not like 'G%';
	
	select count (*) into v_count2 from tmp where (c_d_id = 1 or c_d_id = 2) and c_credit not like 'G%';
	
	WHILE TRUE LOOP
		counter := counter + 1;
		IF counter = v_count2 THEN
			update tmp set c_balance = c_balance + c_credit_lim where (c_w_id = 1 and c_d_id = counter and c_id = vsum);
			delete from tmp where (c_d_id = 5 or c_id = 3) and c_credit like 'G%';
			EXIT;
		END IF;
		
		vsum := vsum + counter;
	END LOOP;
	out1 = vsum;
	
	
	SELECT EXTRACT(YEAR FROM c_since) into out2 from customer where (c_d_id = 1 or c_d_id = 2) and c_credit not like 'G%' order by c_data limit 1;
	SELECT EXTRACT(MONTH FROM c_since) into out3 from customer where (c_d_id = 1 or c_d_id = 2) and c_credit not like 'G%' order by c_data limit 1;
	SELECT EXTRACT(DAY FROM c_since) into out4 from customer where (c_d_id = 1 or c_d_id = 2) and c_credit not like 'G%' order by c_data limit 1;
	
	select count(*) into out5 from customer left join tmp on customer.c_w_id = tmp.c_id and customer.c_d_id = tmp.c_w_id and customer.c_id = tmp.c_id and customer.c_credit = tmp.c_credit and tmp.c_credit not like 'G%' ;
	select count(*) into out6 from customer right join tmp on customer.c_w_id = tmp.c_id and customer.c_d_id = tmp.c_w_id and customer.c_id = tmp.c_id and customer.c_credit = tmp.c_credit and tmp.c_credit not like 'G%' ;
	select count(*) into out7 from customer inner join tmp on customer.c_w_id = tmp.c_id and customer.c_d_id = tmp.c_w_id and customer.c_id = tmp.c_id and customer.c_credit = tmp.c_credit and tmp.c_credit not like 'G%' ;
	select count(*) into out8 from customer left outer join tmp on customer.c_w_id = tmp.c_id and customer.c_d_id = tmp.c_w_id and customer.c_id = tmp.c_id and customer.c_credit = tmp.c_credit and tmp.c_credit not like 'G%' ;
	select count(*) into out9 from customer right outer join tmp on customer.c_w_id = tmp.c_id and customer.c_d_id = tmp.c_w_id and customer.c_id = tmp.c_id and customer.c_credit = tmp.c_credit and tmp.c_credit not like 'G%' ;
	
	select count(*) into out10 from tmp where c_w_id IN (1,2,3) and c_d_id IN (3,2,1) and c_id IN (4,5,6);
	drop foreign table tmp;
	
EXCEPTION
    WHEN OTHERS THEN
        err_msg := SQLERRM;

END;
/

select functions();
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='query_count';
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='function_count';

------------------------------------------------------------------------
drop procedure if exists functions;

-- for loop, while loop, cast, delete using, 
CREATE OR REPLACE PROCEDURE functions(OUT out1 integer, OUT out2 integer, OUT out3 character varying, OUT out4 integer, OUT out5 integer, OUT err_msg character varying)
AS
DECLARE
	counter integer := 0;
	total_balance integer := 0;
	balance integer := 0;
	genre_rec character varying;
	v_loop integer := 0;
	v_out integer;
BEGIN

	create foreign table if not exists tmp (like customer);
	create index on tmp(c_w_id, c_d_id, c_id);
	insert into tmp select * from customer;
	select count(*) into counter from customer;
	out1 := counter / 100;
	
	FOR i IN 1..out1 LOOP
        	delete from tmp using customer where tmp.c_w_id = customer.c_w_id and tmp.c_d_id = customer.c_d_id and tmp.c_id = customer.c_id limit 1;
	END LOOP;
	select count (*) into out2 from tmp;
	
	WHILE TRUE LOOP
		IF v_loop >= out1 THEN
			EXIT;
		END IF;
		
		IF ((v_loop % 2) = 1) THEN
			update tmp set c_balance = c_balance + 1 where c_w_id % 2 = 1 and c_d_id % 2 = 1 and c_id = 1;
		ELSE
			insert into tmp select * from customer where c_w_id % 2 = 1 and c_d_id % 2 = 1 and c_id % 2 = 1 limit 1;
		END IF;
		
		v_loop := v_loop + 1;
	END LOOP;
	
	select count (*) into v_out from tmp;
	
	select CAST (v_out AS character varying) into out3;
	out4 := out3::integer;

	update customer set c_delivery_cnt = c_delivery_cnt + c_payment_cnt + c_discount where c_balance != 0 and c_city like '%y' and c_first like '%a%' and c_middle not like '%w%' and c_w_id <> 13 and c_id in (131,295.665);
	
	select count (*) into out5 from customer where c_balance != 0 and c_city like '%y%' and c_first like '%a%' and c_middle not like '%w%' and c_w_id <> 13 and c_id in (select c_id from customer where c_city like '%y%' and c_first like '%a%' and c_middle not like '%w%' and c_w_id <> 13);
	drop foreign table tmp;
	
EXCEPTION
    WHEN OTHERS THEN
        err_msg := SQLERRM;

END;
/

select functions();
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='query_count';
--select jittable_status from mot_jit_detail() where namespace='TEST' and proc_id=9999 and proc_name='function_count';


drop foreign table customer;
drop procedure if exists functions;
