CREATE TABLE area_example1(id SERIAL primary key, somedata int, text varchar(120))with(storage_type = ustore);
CREATE TABLE area_example2(a int primary key,b int,c int);
CREATE TABLE area_example3(a int,b int,c int);
CREATE OR REPLACE function decode_area_proc(plugin text) returns SETOF text
LANGUAGE plpgsql
AS
$$
declare o_ret text;
        my_sql text;
        param1 text;
begin
truncate table area_example1;
truncate table area_example2;
truncate table area_example3;
EXECUTE('SELECT pg_current_xlog_location();') into o_ret;
INSERT INTO area_example1(somedata, text) VALUES (1, 1);
INSERT INTO area_example1(somedata, text) VALUES (1, 2);
update area_example1 set somedata=somedata*10 where somedata=1;
delete from area_example1 where somedata=10;

INSERT INTO area_example2 VALUES (1, 1, 1);
INSERT INTO area_example2 VALUES (2, 2, 2);
update area_example2 set b=b*10 where a=1;
delete from area_example2 where c=1;

INSERT INTO area_example3 VALUES (1, 1, 1);
INSERT INTO area_example3 VALUES (2, 2, 2);
update area_example3 set b=b*10 where a=1;
delete from area_example3 where c=1;
my_sql = 'select data from pg_logical_get_area_changes(''' || o_ret || ''',NULL,NULL,''' || plugin || ''',NULL);';
return query EXECUTE(my_sql);
END;
$$
;
call decode_area_proc('mppdb_decoding');
call decode_area_proc('sql_decoding');
drop table area_example1;
drop table area_example2;
drop table area_example3;
