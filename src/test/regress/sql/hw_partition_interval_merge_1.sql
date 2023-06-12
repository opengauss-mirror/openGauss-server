-- 1 init environment
create schema partition_interval_merge_1;
set current_schema to partition_interval_merge_1;
set DateStyle = 'Postgres';
prepare pg_partition_sql2(char) as
    select relname,
           parttype,
           rangenum,
           intervalnum,
           partstrategy,
           interval,
           to_char(to_date(boundaries[1], 'Dy Mon DD hh24:mi:ss YYYY'), 'YYYY-MM-DD') boundary
    from pg_partition
    where parentid = (select oid from pg_class where relname = $1)
    order by to_date(boundaries[1], 'Dy Mon DD hh24:mi:ss YYYY');
-- B. test with index
    CREATE TABLE interval_sales_1
    (
        prod_id       NUMBER(6),
        cust_id       NUMBER,
        time_id       DATE,
        channel_id    CHAR(1),
        promo_id      NUMBER(6),
        quantity_sold NUMBER(3),
        amount_sold   NUMBER(10, 2)
    )
        PARTITION BY RANGE (time_id)
        INTERVAL
    ('1 MONTH')
    (PARTITION p0 VALUES LESS THAN (TO_DATE('1-1-2008', 'DD-MM-YYYY')),
        PARTITION p1 VALUES LESS THAN (TO_DATE('6-5-2008', 'DD-MM-YYYY'))
    );
    create index interval_sales_1_time_id_idx on interval_sales_1 (time_id) local;
    create index interval_sales_1_quantity_sold_idx on interval_sales_1 (quantity_sold) local;
    alter table interval_sales_1 split partition p0 at (to_date('2007-02-10', 'YYYY-MM-DD')) into (partition p0_1, partition p0_2);
    execute pg_partition_sql2('interval_sales_1');
    insert into interval_sales_1
    values (1, 1, to_date('9-2-2007', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('11-2-2007', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('11-2-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('20-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('05-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('08-2-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('05-4-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('05-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-9-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-11-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-12-2008', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-01-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-5-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-6-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-7-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-8-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    insert into interval_sales_1
    values (1, 1, to_date('04-9-2009', 'DD-MM-YYYY'), 'a', 1, 1, 1);
    -- 2 cases
    -- 2.1.0 merge normal range partitions in bad order
    select *
    from interval_sales_1 partition (p0_1);
    select *
    from interval_sales_1 partition (p0_2);
    select *
    from interval_sales_1 partition (p1);
    alter table interval_sales_1 merge partitions p1, p0_1, p0_2 into partition p01;
    -- 2.1.1 merge normal range partitions in right order
    execute pg_partition_sql2('interval_sales_1');

    alter table interval_sales_1 merge partitions p0_1, p0_2, p1 into partition p01;
    execute pg_partition_sql2('interval_sales_1');

    select *
    from interval_sales_1 partition (p0_1);
    select *
    from interval_sales_1 partition (p0_2);
    select *
    from interval_sales_1 partition (p1);
    select *
    from interval_sales_1 partition (p01);

    -- 2.2.0 merge interval partitions in wrong order
    select *
    from interval_sales_1 partition (sys_p6);
    select *
    from interval_sales_1 partition (sys_p5);
    alter table interval_sales_1 merge partitions sys_p6, sys_p5 into partition sys_p6_p5;
    -- 2.2.1 merge interval partitions in right order, but they are not continuous
    alter table interval_sales_1 merge partitions sys_p5, sys_p6 into partition sys_p5_p6;
    -- 2.2.2 merge interval partitions in right order, and they are continuous.
    select *
    from interval_sales_1 partition (sys_p6);
    select *
    from interval_sales_1 partition (sys_p7);
    select *
    from interval_sales_1 partition (sys_p8);
    execute pg_partition_sql2('interval_sales_1');

    alter table interval_sales_1 merge partitions sys_p6, sys_p7, sys_p8 into partition sys_p6_p7_p8;
    select *
    from interval_sales_1 partition (sys_p6);
    select *
    from interval_sales_1 partition (sys_p7);
    select *
    from interval_sales_1 partition (sys_p8);
    select *
    from interval_sales_1 partition (sys_p6_p7_p8);
    execute pg_partition_sql2('interval_sales_1');

    -- 2.3 merge interval partition and range partition
    -- FIRST, build a range partition which is continuous with a interval partition
    alter table interval_sales_1 merge partitions sys_p2, sys_p1 into partition sys_p2_p1;

    -- 2.3.1 merge sys_p2_p1 with sys_p3 in wrong order
    alter table interval_sales_1 merge partitions sys_p3, sys_p2_p1 into partition sys_p2_p1_p3;

    -- 2.3.2 merge sys_p2_p1 with sys_p3 in right order
    select *
    from interval_sales_1 partition (sys_p2_p1);
    select *
    from interval_sales_1 partition (sys_p3);
    alter table interval_sales_1 merge partitions sys_p2_p1, sys_p3 into partition sys_p2_p1_p3;
    select *
    from interval_sales_1 partition (sys_p2_p1_p3);
    execute pg_partition_sql2('interval_sales_1');

    -- 2.4.0 merge interval partition and range partition into one in wrong order
    alter table interval_sales_1 merge partitions sys_p9, sys_p2_p1_p3 into partition sys_p9_p2_p1_p3;
    -- 2.4.1 merge interval partition and range partition into one in right order
    alter table interval_sales_1 merge partitions sys_p2_p1_p3, sys_p9 into partition sys_p9_p2_p1_p3;
    select *
    from interval_sales_1 partition (sys_p9_p2_p1_p3);
    execute pg_partition_sql2('interval_sales_1');

    -- 2.5 merge interval partitions, which is divided by several interval partitions, will failed
    alter table interval_sales_1 merge partitions sys_p10, sys_p12 into partition sys_p10_p12;

    -- 2.6 case that failed to update new partition's type
    drop table if exists partiton_table_001;
    create table partiton_table_001(
        COL_4 date
    )
        PARTITION BY RANGE (COL_4)
        INTERVAL ('1 month')
    (
        PARTITION partiton_table_001_p1 VALUES LESS THAN (date'2020-03-01'),
        PARTITION partiton_table_001_p2 VALUES LESS THAN (date'2020-04-01'),
        PARTITION partiton_table_001_p3 VALUES LESS THAN (date'2020-05-01')
    );

    -- @插入的分区键值
    insert into partiton_table_001 values (date'2020-02-23');
    insert into partiton_table_001 values (date'2020-03-23');
    insert into partiton_table_001 values (date'2020-04-23');
    insert into partiton_table_001 values (date'2020-05-23');
    insert into partiton_table_001 values (date'2020-06-23');
    insert into partiton_table_001 values (date'2020-07-23');

    -- @查看分区表、分区表索引信息
    execute pg_partition_sql2('partiton_table_001');
    alter table partiton_table_001 merge partitions sys_p1, sys_p2 into PARTITION sys_p4;
    execute pg_partition_sql2('partiton_table_001');
    drop table if exists partiton_table_001;

-- 3 drop indexes and table
    drop index interval_sales_1_time_id_idx;
    drop index interval_sales_1_quantity_sold_idx;
    drop table interval_sales_1;

    drop schema partition_interval_merge_1;