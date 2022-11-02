CREATE OR REPLACE FUNCTION tomarrow()
RETURNS timestamptz  
AS $$
BEGIN
 RETURN now() + interval '1 day';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION remove_all_job()
RETURNS void  
AS $$
DECLARE
    sql                  text;
    job_id               int;
BEGIN
    sql := 'select job_id from pg_job;';
    FOR job_id IN EXECUTE(sql) LOOP
        sql := 'select dbms_job.remove('||job_id||')';
        EXECUTE (sql);
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
select remove_all_job();

/*成功 : 时序开关打开
1. 标准时序创建表方式*/
CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

select reloptions from pg_class where relname ='cpu';
select attname,attkvtype from pg_attribute where attrelid = (select oid from pg_class where relname = 'cpu')  and attnum > 0 order by attnum;
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu%' order by pg_job.job_id;

/*2.   标准时序修改表增加列方式*/
ALTER TABLE CPU ADD COLUMN memory real TSFIELD;
select attname,attkvtype from pg_attribute where attrelid = (select oid from pg_class where relname = 'cpu')  and attnum > 0 order by attnum;

/*3. 修改时序表删除列*/
select attname,attkvtype from pg_attribute where attrelid = (select oid from pg_class where relname = 'cpu')  and attnum > 0 order by attnum;
ALTER TABLE CPU DROP COLUMN idle;
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
/*4.修改时序表PERIOD, TTL*/
ALTER TABLE CPU SET (PERIOD = '0.5 day');
ALTER TABLE CPU SET (TTL = '1 week');
DO $$ BEGIN PERFORM pg_sleep(5); END $$;
/*5. 删除时序表*/
DROP TABLE CPU;

/*6. 不指定PERIOD， 默认PERIOD为1天。 不指定TTL，则不创建分区删除任务*/
CREATE TABLE CPU2(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (orientation=TIMESERIES)
distribute by hash(IP);
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu2%' order by pg_job.job_id;
DROP TABLE CPU2;

/*7. 除时间列外所有KVType指定为TSTAG或TSFIELD*/

CREATE TABLE CPU3(
idle real TSFIELD,
IO real TSFIELD,
scope text TSFIELD,
IP text TSFIELD,
time timestamp TSTIME
) with (orientation=TIMESERIES)
distribute by hash(IP)
PARTITION BY RANGE(time)
(
partition p1 values less than(now()),
partition p2 values less than(tomarrow())
);

select reloptions from pg_class where relname = 'cpu3'; 
DROP TABLE CPU3;

/*9. 创建分区表并且指定了TTL或者PERIOD，但ORIENTATION非TIMESERIES*/
CREATE TABLE CPU4(
idle real,
IO real,
scope text,
IP text,
time timestamp 
) with (TTL='7 days', PERIOD = '1 day', orientation=COLUMN)
distribute by hash(IP)
PARTITION BY RANGE(time)
(
partition p1 values less than(now()),
partition p2 values less than(tomarrow())
);DO $$ BEGIN PERFORM pg_sleep(3); END $$;
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu4%' order by pg_job.job_id;
DROP TABLE CPU4;

/*9. 删除分区策略，删除表成功*/
CREATE TABLE CPU5(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu5%' order by pg_job.job_id;
SELECT remove_create_partition_policy_v2('cpu5');
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu5%' order by pg_job.job_id;
SELECT remove_drop_partition_policy_v2 ('cpu5');
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu5%' order by pg_job.job_id;

DROP TABLE CPU5;


/*10. 非时序表，无论是否包含分区策略，成功删除*/

CREATE TABLE CPU6(
idle real,
IO real,
scope text,
IP text,
time timestamp 
) with (TTL='7 days', PERIOD = '1 day', orientation=COLUMN)
distribute by hash(IP)
PARTITION BY RANGE(time)
(
partition p1 values less than(now()),
partition p2 values less than(tomarrow())
);
DO $$ BEGIN PERFORM pg_sleep(5); END $$;
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu6%' order by pg_job.job_id;
DROP TABLE CPU6;
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu6%' order by pg_job.job_id;

/*11. 只有一個屬性 */
CREATE TABLE CPU7(
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(time)
PARTITION BY RANGE(time)
(
partition p1 values less than(now()),
partition p2 values less than(tomarrow())
);
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
select interval,what from pg_job join pg_job_proc on pg_job.job_id = pg_job_proc.job_id where what like '%cpu7%' order by pg_job.job_id;
DROP TABLE CPU7;

/*失败 :*/ 
/* 1.时序开关未打开 */

/* 2.两个TSDDBTIME 定义 或者未定义TSTIME */
CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME,
timetz timestamptz TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTAG
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

/* 4. PERIOD间隔大于TTL */
CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 month', orientation=TIMESERIES)
distribute by hash(IP);

/* 5.TTL超过一年 */
CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='100 year 1 month', PERIOD = '1 month', orientation=TIMESERIES)
distribute by hash(IP);

/* 6.PERIOD小于1小时 */
CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='1 year ', PERIOD = '59 min', orientation=TIMESERIES)
distribute by hash(IP);


/* 7.增加列 定义为 TSTIME或不指定kvtype */
CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
ALTER TABLE CPU ADD COLUMN time2 timestamp TSTIME;
ALTER TABLE CPU ADD COLUMN time2 timestamp;

DROP TABLE CPU;

/* 8. 时序表未定义或部分kvtype */
CREATE TABLE CPU(
idle real TSFIELD,
IO real ,
scope text ,
IP text ,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

/* 9. 非时序表不指定period,period默认为1天*/

CREATE TABLE CPU(
idle real,
IO real,
scope text,
IP text,
time timestamp 
) with (TTL='1 hour', orientation=COLUMN)
distribute by hash(IP)
PARTITION BY RANGE(time)
(
partition p1 values less than(now()),
partition p2 values less than(tomarrow())
);

/* 10. 时序表指定的partition非TSTIME类型 */

CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
recordtime timestamp TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP)
PARTITION BY RANGE(recordtime)
(
partition p1 values less than(now()),
partition p2 values less than(tomarrow())
);

/* 11. 时序修改表ttl < period  */

CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

ALTER TABLE CPU SET (PERIOD = '2 week');
ALTER TABLE CPU SET (TTL = '1 hour');
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
DROP TABLE CPU;

/* 12. 时序表暂不支持unlogged table限制  */
CREATE UNLOGGED TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

/* 13. 不支持 [LOCAL/GLOBAL] + TEMP/TEMPORAY 限制   */
CREATE TEMP TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE LOCAL TEMP TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE LOCAL TEMPORAY TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE GLOBAL TEMPORAY TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

/* 13. 不支持 table constraint 限制   */

CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME,
PRIMARY KEY(idle) 
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME,
CONSTRAINT CONSTR_KEY UNIQUE(idle) 
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE TABLE CPU(
idle real TSFIELD,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME,
CONSTRAINT CONSTR_KEY PARTIAL CLUSTER KEY(idle) 
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

/* 14. 不支持 column constraint 限制   */
CREATE TABLE CPU(
idle real TSFIELD PRIMARY KEY,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE TABLE CPU(
idle real TSFIELD UNIQUE USING INDEX,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

CREATE TABLE CPU(
idle real TSFIELD NOT NULL,
IO real TSFIELD,
scope text TSTAG,
IP text TSTAG,
time timestamp TSTIME
) with (TTL='7 days', PERIOD = '1 day', orientation=TIMESERIES)
distribute by hash(IP);

create unique index uindex on cpu(io);
DO $$ BEGIN PERFORM pg_sleep(3); END $$;
DROP TABLE CPU;
