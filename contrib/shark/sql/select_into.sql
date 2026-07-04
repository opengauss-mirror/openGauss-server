drop table if exists tab;
create table tab(id int,name varchar(50));
insert into tab values(1,'a');

DO $$
DECLARE
    v_name varchar(50);
    v_sSample varchar(20) := 'a';
BEGIN
    DROP TABLE IF EXISTS test_table_1;

    SELECT id INTO test_table_1
    FROM tab 
    WHERE name = v_sSample;

    GET DIAGNOSTICS v_name = ROW_COUNT;
    RAISE NOTICE 'Success: Created table with % rows.', v_name;

END $$;

SELECT * FROM test_table_1;

DO $$
DECLARE
    v_name varchar(50);
    v_sSample varchar(20) := 'a';
BEGIN
    DROP TABLE IF EXISTS test_table_2;

    SELECT id INTO TABLE test_table_2
    FROM tab 
    WHERE name = v_sSample;

    GET DIAGNOSTICS v_name = ROW_COUNT;
    RAISE NOTICE 'Success: Created table with % rows.', v_name;

END $$;

SELECT * FROM test_table_2;

drop table if exists test_table_3;
select id into test_table_3 from tab where name='a';
select * from test_table_3;

drop table if exists test_table_4;
select id into table test_table_4 from tab where name='a';
select * from test_table_4;

drop table test_table_1;
drop table test_table_2;
drop table test_table_3;
drop table test_table_4;

drop table tab;
