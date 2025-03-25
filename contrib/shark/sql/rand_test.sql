
DO $$
DECLARE
    counter SMALLINT := 1;
BEGIN
    WHILE counter < 5 LOOP
        RAISE NOTICE '%', RAND();
        counter := counter + 1;
    END LOOP;
END $$;

SELECT rand(1);
SELECT rand(-1);
SELECT rand(1::SMALLINT);
SELECT rand(-1::SMALLINT);
SELECT rand(1::tinyint);
SELECT rand(-1::tinyint);

drop table if exists t1;
create table t1(a1 int);
insert into t1 values(floor((100 + RAND() * 100)));
select * from t1;

select RAND(null);
select RAND(2147483647);
select RAND(2147483648);
select RAND(-2147483648);
select RAND(-2147483649);