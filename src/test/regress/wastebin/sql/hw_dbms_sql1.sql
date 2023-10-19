create database pl_test_cursor_part1 DBCOMPATIBILITY 'pg';
\c pl_test_cursor_part1;
---bind_variable int
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,3,1,11);
query := 'select * from pro_dbe_sql_all_tb1_02 where a > y and a < z order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_variable(context_id, 'z', 10);
dbe_sql.sql_bind_variable(context_id, 'y', 1);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

---bind_variable clob
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b clob,c clob,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,HEXTORAW('DEADBEEF'),HEXTORAW('D'),11);
insert into pro_dbe_sql_all_tb1_02 values(6,HEXTORAW('DEADBEEF'),HEXTORAW('DE'),11);
query := 'select * from pro_dbe_sql_all_tb1_02 where b = y and c = z';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_variable(context_id, 'y', HEXTORAW('DEADBEEF'));
dbe_sql.sql_bind_variable(context_id, 'z', HEXTORAW('D'));
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

---bind_array int\char\bytea\text\raw
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
v_id1 int[];
v_id4 char[];
v_id5 bytea[];
v_id6 text[];
v_id7 raw[];
begin
v_id1[1] := 3;
v_id1[2] := 4;

v_id5[1] := '2';
v_id5[2] := '2';
v_id5[3] := '3';

v_id4[1] := '3';
v_id4[2] := '3';
v_id4[3] := '3';

v_id6[1] := '11';
v_id6[2] := '11';

v_id7[1] := '1';
v_id7[2] := '1';
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b char,c bytea,d text,e raw);
insert into pro_dbe_sql_all_tb1_02 values(4,'3','2','11','1');
insert into pro_dbe_sql_all_tb1_02 values(6,'3','1','11','1');
query := 'select * from pro_dbe_sql_all_tb1_02 where a > y and b = f and c = i and d = j and e = k';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_array(context_id, 'y', v_id1,1,1);
dbe_sql.sql_bind_array(context_id, 'i', v_id5,2,3);
dbe_sql.sql_bind_array(context_id, 'f', v_id4,2,3);
dbe_sql.sql_bind_array(context_id, 'j', v_id6,2,2);
dbe_sql.sql_bind_array(context_id, 'k', v_id7,2,2);

--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

---bind_array error
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
v_id1 int[];
begin
v_id1[1] := 3;
v_id1[2] := 4;
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,3,1,11);
query := 'select * from pro_dbe_sql_all_tb1_02 where a > y order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_array(context_id, 'y', v_id1,-1,2);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

---bind_array error
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
v_id1 int[];
begin
v_id1[1] := 3;
v_id1[2] := 4;
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,3,1,11);
query := 'select * from pro_dbe_sql_all_tb1_02 where a > y order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_array(context_id, 'y', v_id1);
dbe_sql.sql_bind_array(context_id, 'y', v_id1,1,2);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;


---bind_array error
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
v_id1 int[];
begin
v_id1[1] := 3;
v_id1[2] := 4;
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,3,1,11);
query := 'select * from pro_dbe_sql_all_tb1_02 where a > y and b =z order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_array(context_id, 'y', v_id1);
dbe_sql.sql_bind_array(context_id, 'y', v_id1,1,2);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

---bind_array error
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
v_id1 int[];
begin
v_id1[1] := 3;
v_id1[2] := 4;
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,3,1,11);
query := 'select * from pro_dbe_sql_all_tb1_02 where a > y order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_array(context_id, 'y', v_id1);
dbe_sql.sql_bind_variable(context_id, 'y', 1);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

---set_results_type
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int :=3;
--test
v_id1 int[];
v_id4 character[];
v_id5 bytea[];
v_id6 text[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a text ,b int, c char, d text);
insert into pro_dbe_sql_all_tb1_02 values('1',9,'5','13');
insert into pro_dbe_sql_all_tb1_02 values('2',10,'6','14');
insert into pro_dbe_sql_all_tb1_02 values('3',11,'7','15');
insert into pro_dbe_sql_all_tb1_02 values('4',12,'8','16');
query := ' select * from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--定义列
dbe_sql.set_results_type(context_id,1,v_id6,v_id,v_id2);
dbe_sql.set_results_type(context_id,2,v_id1,v_id,v_id2);
dbe_sql.set_results_type(context_id,3,v_id4,v_id,v_id2);
dbe_sql.set_results_type(context_id,4,v_id5,v_id,v_id2);

--执行
execute_ret := dbe_sql.sql_run(context_id);
loop
v_id3 := dbe_sql.next_row(context_id);
v_id6 := dbe_sql.get_results(context_id,1,v_id6);
v_id1 := dbe_sql.get_results(context_id,2,v_id1);
v_id4 := dbe_sql.get_results(context_id,3,v_id4);
v_id5 := dbe_sql.get_results(context_id,4,v_id5);
exit when(v_id3 != 3);
end loop;

FOR i IN v_id1.FIRST .. v_id1.LAST  LOOP
	    dbe_output.print_line('int' || i || ' = ' || v_id1[i]);
END LOOP;
FOR j IN v_id4.FIRST .. v_id4.LAST  LOOP
	    dbe_output.print_line('char' || j || ' = ' || v_id4[j]);
END LOOP;
FOR j IN v_id6.FIRST .. v_id6.LAST  LOOP
	    dbe_output.print_line('text' || j || ' = ' || v_id6[j]);
END LOOP;
FOR j IN v_id5.FIRST .. v_id5.LAST  LOOP
	    dbe_output.print_line('bytea' || j || ' = ' || v_id5[j]);
END LOOP;

--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;


---set_results_type
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int :=3;
--test
v_id1 int[];
v_id4 character[];
v_id5 bytea[];
v_id6 text[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a text ,b int, c char, d text);
insert into pro_dbe_sql_all_tb1_02 values('1',9,'5','13');
insert into pro_dbe_sql_all_tb1_02 values('2',10,'6','14');
insert into pro_dbe_sql_all_tb1_02 values('3',11,'7','15');
insert into pro_dbe_sql_all_tb1_02 values('4',12,'8','16');
query := ' select * from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--定义列
dbe_sql.set_results_type(context_id,1,v_id6,v_id,-1);
dbe_sql.set_results_type(context_id,2,v_id1,v_id,v_id2);
dbe_sql.set_results_type(context_id,3,v_id4,v_id,v_id2);
dbe_sql.set_results_type(context_id,4,v_id5,v_id,v_id2);

--执行
execute_ret := dbe_sql.sql_run(context_id);
loop
v_id3 := dbe_sql.next_row(context_id);
v_id6 := dbe_sql.get_results(context_id,1,v_id6);
v_id1 := dbe_sql.get_results(context_id,2,v_id1);
v_id4 := dbe_sql.get_results(context_id,3,v_id4);
v_id5 := dbe_sql.get_results(context_id,4,v_id5);
exit when(v_id3 != 3);
end loop;

FOR i IN v_id1.FIRST .. v_id1.LAST  LOOP
	    dbe_output.print_line('int' || i || ' = ' || v_id1[i]);
END LOOP;
FOR j IN v_id4.FIRST .. v_id4.LAST  LOOP
	    dbe_output.print_line('char' || j || ' = ' || v_id4[j]);
END LOOP;
FOR j IN v_id6.FIRST .. v_id6.LAST  LOOP
	    dbe_output.print_line('text' || j || ' = ' || v_id6[j]);
END LOOP;
FOR j IN v_id5.FIRST .. v_id5.LAST  LOOP
	    dbe_output.print_line('bytea' || j || ' = ' || v_id5[j]);
END LOOP;

--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

--type
create or replace procedure pro_dbe_sql_all_02()
as 
bb dbe_sql.date_table;
cc dbe_sql.number_table;
dd dbe_sql.varchar2_table;
ee dbe_sql.desc_tab;
begin

bb(1) :=to_date('2016-11-24 10:30:10','yyyy-mm-dd hh24:mi:ss');
cc(2) := 300;
dd(1) := 'gasdf';
ee(1):= (111,1,'1',1,'1',1,1,1,1,1,false);
ee(2):= (222,1,'1',1,'1',1,1,1,1,1,false);
ee(3):= (333,1,'1',1,'1',1,1,1,1,1,false);

RAISE INFO 'date_table: %' ,bb(1);
RAISE INFO 'number_table: %' ,cc(2);
RAISE INFO 'varchar2_table: %' ,dd(1);
RAISE INFO 'desc_tab: %' ,ee(1).col_type;
RAISE INFO 'desc_tab: %' ,ee(2).col_type;
RAISE INFO 'desc_tab: %' ,ee(3).col_type;
RAISE INFO 'desc_tab: %' ,ee(3).col_name;
end;
/
--调用存储过程
call pro_dbe_sql_all_02();
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;


--describe columns
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as
context_id int;
type re_rssc is record (col_num int, desc_col dbe_sql.desc_tab);
employer re_rssc;
res re_rssc;
d int;
dd dbe_sql.desc_tab;
query varchar(2000);
begin
drop table if exists pro_dbe_sql_all_tb1_02;
create table pro_dbe_sql_all_tb1_02(a int ,b int);
insert into pro_dbe_sql_all_tb1_02 values(1,3);
insert into pro_dbe_sql_all_tb1_02 values(2,3);
query := 'select * from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--执行
res := dbe_sql.sql_describe_columns(context_id, d,dd);

--输出结果
dbe_output.print_line('col_num:' || res.col_num);

dbe_output.print_line('col_type:' || res.desc_col[1].col_type);
dbe_output.print_line('col_max_len:' || res.desc_col[1].col_max_len);
dbe_output.print_line('col_name:' || res.desc_col[1].col_name);
dbe_output.print_line('col_name_len:' || res.desc_col[1].col_name_len);
dbe_output.print_line('col_schema_name:' || res.desc_col[1].col_schema_name);
dbe_output.print_line('col_schema_name_len:' || res.desc_col[1].col_schema_name_len);
dbe_output.print_line('col_precision:' || res.desc_col[1].col_precision);
dbe_output.print_line('col_scale:' || res.desc_col[1].col_scale);
dbe_output.print_line('col_charsetid:' || res.desc_col[1].col_charsetid);
dbe_output.print_line('col_charsetform:' || res.desc_col[1].col_charsetform);
dbe_output.print_line('col_null_ok:' || res.desc_col[1].col_null_ok);

dbe_output.print_line('col_type:' || res.desc_col[2].col_type);
dbe_output.print_line('col_max_len:' || res.desc_col[2].col_max_len);
dbe_output.print_line('col_name:' || res.desc_col[2].col_name);
dbe_output.print_line('col_name_len:' || res.desc_col[2].col_name_len);
dbe_output.print_line('col_schema_name:' || res.desc_col[2].col_schema_name);
dbe_output.print_line('col_schema_name_len:' || res.desc_col[2].col_schema_name_len);
dbe_output.print_line('col_precision:' || res.desc_col[2].col_precision);
dbe_output.print_line('col_scale:' || res.desc_col[2].col_scale);
dbe_output.print_line('col_charsetid:' || res.desc_col[2].col_charsetid);
dbe_output.print_line('col_charsetform:' || res.desc_col[2].col_charsetform);
dbe_output.print_line('col_null_ok:' || res.desc_col[2].col_null_ok);

--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;


create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as
context_id int;
type re_rssc is record (col_num int, desc_col dbe_sql.desc_tab);
employer re_rssc;
d int;
dd dbe_sql.desc_tab;
res re_rssc;
query varchar(2000);
begin
drop table if exists pro_dbe_sql_all_tb1_02;
create table pro_dbe_sql_all_tb1_02(a int ,b int);
insert into pro_dbe_sql_all_tb1_02 values(1,3);
insert into pro_dbe_sql_all_tb1_02 values(2,3);
query := 'select a,b from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--执行
res := dbe_sql.sql_describe_columns(context_id,d,dd);

--输出结果
dbe_output.print_line('col_num:' || res.col_num);

dbe_output.print_line('col_type:' || res.desc_col[1].col_type);
dbe_output.print_line('col_max_len:' || res.desc_col[1].col_max_len);
dbe_output.print_line('col_name:' || res.desc_col[1].col_name);
dbe_output.print_line('col_name_len:' || res.desc_col[1].col_name_len);
dbe_output.print_line('col_schema_name:' || res.desc_col[1].col_schema_name);
dbe_output.print_line('col_schema_name_len:' || res.desc_col[1].col_schema_name_len);
dbe_output.print_line('col_precision:' || res.desc_col[1].col_precision);
dbe_output.print_line('col_scale:' || res.desc_col[1].col_scale);
dbe_output.print_line('col_charsetid:' || res.desc_col[1].col_charsetid);
dbe_output.print_line('col_charsetform:' || res.desc_col[1].col_charsetform);
dbe_output.print_line('col_null_ok:' || res.desc_col[1].col_null_ok);

dbe_output.print_line('col_type:' || res.desc_col[2].col_type);
dbe_output.print_line('col_max_len:' || res.desc_col[2].col_max_len);
dbe_output.print_line('col_name:' || res.desc_col[2].col_name);
dbe_output.print_line('col_name_len:' || res.desc_col[2].col_name_len);
dbe_output.print_line('col_schema_name:' || res.desc_col[2].col_schema_name);
dbe_output.print_line('col_schema_name_len:' || res.desc_col[2].col_schema_name_len);
dbe_output.print_line('col_precision:' || res.desc_col[2].col_precision);
dbe_output.print_line('col_scale:' || res.desc_col[2].col_scale);
dbe_output.print_line('col_charsetid:' || res.desc_col[2].col_charsetid);
dbe_output.print_line('col_charsetform:' || res.desc_col[2].col_charsetform);
dbe_output.print_line('col_null_ok:' || res.desc_col[2].col_null_ok);

--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;




------------------------------------------------anyelement
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out int,o_ret2 out int) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret:=10;
o_ret2:=30;
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 int;
v3 int;
v2 int;
o_ret int;
o_retw int;
begin
query := 'call proc_test(i_Col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_Col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw,100);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
end;
/


CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out text,o_ret2 out text) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret:='10';
o_ret2:='30';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 text;
v3 text;
v2 int;
o_ret3 text;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_ret3,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_ret3,100);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
end;
/


CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out bytea,o_ret2 out bytea) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret:='1';
o_ret2:='3';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 bytea;
v3 bytea;
o_retw bytea;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw,100);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
end;
/

CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out character,o_ret2 out character) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret:='1';
o_ret2:='3';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 character;
v3 character;
o_retw character;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw,100);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
end;
/

--------------------------------------------------------
------------------------------------------------anyarray
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out text[],o_ret2 out text[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret(0):='10';
o_ret(1):='20';
o_ret2(0):='30';
o_ret2(1):='40';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 text[];
v3 text[];
v2 int;
o_ret text[];
o_retw text[];
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);
dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);
dbe_sql.sql_unregister_context(context_id);
--输出结果
dbe_output.print_line('v1: '|| v1(0));
dbe_output.print_line('v1: '|| v1(1));
dbe_output.print_line('v1: '|| v3(0));
dbe_output.print_line('v1: '|| v3(1));
end;
/

CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out int[],o_ret2 out int[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret(0):='10';
o_ret(1):='20';
o_ret2(0):='30';
o_ret2(1):='40';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 int[];
v3 int[];
v2 int;
o_ret int[];
o_retw int[];
begin
query := 'call proc_test(i_col1,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
RAISE INFO 'v1: %' ,v1(0);
RAISE INFO 'v1: %' ,v1(1);
RAISE INFO 'v3: %' ,v3(0);
RAISE INFO 'v3: %' ,v3(1);
end;
/


CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out bytea[],o_ret2 out bytea[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret(0):='1';
o_ret(1):='1';
o_ret2(0):='1';
o_ret2(1):='1';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 bytea[];
v3 bytea[];
v2 int;
o_ret bytea[];
o_retw bytea[];
begin
query := 'call proc_test(i_col1,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
RAISE INFO 'v1: %' ,v1(0);
RAISE INFO 'v1: %' ,v1(1);
RAISE INFO 'v3: %' ,v3(0);
RAISE INFO 'v3: %' ,v3(1);
end;
/

CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out text[],o_ret2 out text[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret(0):='100';
o_ret(1):='100';
o_ret2(0):='40';
o_ret2(1):='30';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 text[];
v3 text[];
v2 int;
o_ret text[];
o_retw text[];
begin
query := 'call proc_test(i_col1,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
dbe_output.print_line('v1: '|| v1(0));
dbe_output.print_line('v1: '|| v1(1));
dbe_output.print_line('v1: '|| v3(0));
dbe_output.print_line('v1: '|| v3(1));
end;
/

CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out clob[],o_ret2 out clob[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret(0):='100';
o_ret(1):='100';
o_ret2(0):='40';
o_ret2(1):='30';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 clob[];
v3 clob[];
v2 int;
o_ret clob[];
o_retw clob[];
begin
query := 'call proc_test(i_col1,o_ret,o_ret);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
--输出结果
dbe_output.print_line('v1: '|| v1(0));
dbe_output.print_line('v1: '|| v1(1));
dbe_output.print_line('v1: '|| v3(0));
dbe_output.print_line('v1: '|| v3(1));
end;
/


CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out character[],o_ret2 out character[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret(0):='1';
o_ret(1):='2';
o_ret2(0):='3';
o_ret2(1):='4';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 character[];
v3 character[];
v2 int;
o_retw character[];
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

dbe_sql.sql_unregister_context(context_id);
RAISE INFO 'v1: : %' ,v1(0);
RAISE INFO 'v1: : %' ,v1(1);

RAISE INFO 'v1: : %' ,v3(0);

RAISE INFO 'v1: : %' ,v3(1);

end;
/

drop PROCEDURE proc_test;

--------------------------支持同时-bind-----array-和-variable-------------------------
---------------------------bind-----array-----variable-------------------------
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
exe int[];
dddd char;
begin
exe[1] := 4;
exe[2] := 6;
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,5,1,11);
insert into pro_dbe_sql_all_tb1_02 values(6,10,1,11);
insert into pro_dbe_sql_all_tb1_02 values(6,20,1,11);
query := 'select * from pro_dbe_sql_all_tb1_02 where a = y and b < 20 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
--dbe_sql.sql_bind_variable(context_id, 'z', 20);
dbe_sql.sql_bind_array(context_id, 'y', exe);
--定义列
dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

---------------------bind-----array-----variable---------------insert
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
exe int[];
dddd char;
begin
exe[1] := 4;
exe[2] := 6;
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,5,1,11);
insert into pro_dbe_sql_all_tb1_02 values(6,10,1,11);
insert into pro_dbe_sql_all_tb1_02 values(6,20,1,11);
query := 'insert into pro_dbe_sql_all_tb1_02 values(y,z)';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_variable(context_id, 'z', 20);
dbe_sql.sql_bind_array(context_id, 'y', exe);
--定义列
dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;




---------------------------支持---bind_variable----的列大小限制-------------------------
----------------------bind_variable--------------in-------------------------------------
----------------------------------------------------------------------------------------
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id text;
v_id1 text;
--v_id int;
query varchar(2000);
execute_ret int;
begin
v_id1 := 'abc';
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a text ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values('ab',3,2,11);
insert into pro_dbe_sql_all_tb1_02 values('abc',3,1,11);
query := 'select * from pro_dbe_sql_all_tb1_02 where a = y order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--绑定参数
dbe_sql.sql_bind_variable(context_id, 'y', v_id1);
--dbe_sql.sql_bind_variable(context_id, 'y', 3);
--定义列
dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

------------------------------------------------------------------------
---------------------------bind_variable--------------inout-------------

----##########################------------------------text:
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out text,o_ret2 out text) as
v_a varchar2;
begin
o_ret:='123';
o_ret2:='34567';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 text;
v3 text;
v2 int;
--o_ret character[];
o_retw text;
o_retw1 text;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,1);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw1,3);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

RAISE INFO 'v1: : %' ,v1;
RAISE INFO 'v1: : %' ,v3;
end;
/


------##########################------------------------text:
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out text,o_ret2 out text) as
v_a varchar2;
begin
o_ret:='123';
o_ret2:='34567';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 text;
v3 text;
v2 int;
--o_ret character[];
o_retw text;
o_retw1 text;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,1);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw1,3);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

RAISE INFO 'v1: : %' ,v1;
RAISE INFO 'v1: : %' ,v3;
end;
/


-----##########################------------------------bytea:
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out bytea,o_ret2 out bytea) as
v_a varchar2;
begin
o_ret:='123';
o_ret2:='34567';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 bytea;
v3 bytea;
o_retw bytea;
o_retw1 bytea;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,1);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw1,3);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

RAISE INFO 'v1: : %' ,v1;
RAISE INFO 'v1: : %' ,v3;
end;
/



-----##########################------------------------bpchar:
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out bpchar,o_ret2 out bpchar) as
v_a varchar2;
begin
o_ret:='123';
o_ret2:='34567';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 bpchar;
v3 bpchar;
o_retw bpchar;
o_retw1 bpchar;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw1,4);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);

RAISE INFO 'v1: : %' ,v1;
RAISE INFO 'v1: : %' ,v3;
end;
/

-----------------------支持---set_result_type-----的列大小限制----------------------------
---------------------------------------set_result_type-------column_value-------------text
------------------------------------------------------------------------------------------
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info1 text :=1;

query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a text ,b clob);
insert into pro_dbe_sql_all_tb1_02 values('asbdrdgg',HEXTORAW('DEADBEEE'));
insert into pro_dbe_sql_all_tb1_02 values(2,in_raw);
query := 'select a from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_info1,10);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_info1);
--输出结果
dbe_output.print_line('info:'|| 1 || ' info:' ||v_info1);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;


---------------set_result_type-------column_value------------------------------bytea
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
nnn text;
v_info1 bytea;
query varchar(2000);
execute_ret int;
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a bytea ,b clob);
insert into pro_dbe_sql_all_tb1_02 values('646464',HEXTORAW('DEADBEEE'));
insert into pro_dbe_sql_all_tb1_02 values('646464',in_raw);
query := 'select a from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_info1,10);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_result_raw(context_id,1,v_info1);

--输出结果
dbe_output.print_line('info:'|| 1 || ' info:' ||v_info1);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;

-----------set_result_type-------column_value----------------------------bpchar

create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
nnn text;
v_info1 bpchar;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a bpchar ,b clob);
insert into pro_dbe_sql_all_tb1_02 values('646464',HEXTORAW('DEADBEEE'));
insert into pro_dbe_sql_all_tb1_02 values('646464',in_raw);
query := 'select a from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_info1,3);

--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_info1);

--nnn := pkg_util.lob_rawtotext(v_info1);
--输出结果
dbe_output.print_line('info:'|| 1 || ' info:' ||v_info1);
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;
---========================test  raw /clob/blob function
create or replace procedure pro_get_variable_07(in_raw int,v_in out bigint,v_offset out bigint,ary1 out bigint[],ary2 out bigint[])
as
context_id int;
v_id int :=3;

v_id1 int[];

v_id5 bytea[];
v_id6 text[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
begin
v_in:=10;
v_offset:=30;
ary1(0):='1';
ary1(1):='2';
ary1(2):='3';
ary2(0):='12';
ary2(1):='13';
ary2(2):='14';
end;
/

create or replace procedure call_get_variable_07()
as
context_id number;
query text;
define_column_ret int;
v1 bigint;
v3 bigint;
v2 bigint;
v4 bigint[];
v5 bigint[];
v_in bigint;
v_offset bigint;
ary1 bigint[];
ary2 bigint[];
o_retw bigint;
o_retw1 bigint[];
begin
query := 'call pro_get_variable_07(in_raw,NULL,NULL,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'in_raw',1,10);
dbe_sql.sql_bind_variable(context_id, 'v_in',o_retw,100);
dbe_sql.sql_bind_variable(context_id, 'v_offset',o_retw,100);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);

define_column_ret := dbe_sql.sql_run(context_id);
dbe_sql.get_variable_result(context_id,'v_in',v1);
dbe_sql.get_variable_result(context_id,'v_offset',v3);
dbe_sql.get_array_result_int(context_id,'ary1',v4);
dbe_sql.get_array_result_int(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v5: %' ,v5(0);
RAISE INFO 'v5: %' ,v5(1);
end;
/
call call_get_variable_07();

----==================================================
create or replace procedure pro_get_variable_result_text_02(in_raw int,v_in out clob,v_offset out clob,ary1 out clob[],ary2 out clob[])
as
context_id int;
v_id int :=3;
--test
v_id1 int[];
v_id4 character[];
v_id5 bytea[];
v_id6 clob[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
begin
v_in:='abcdnfdfdfdafds';
v_offset:='ccccccccccccccccccccccc';
ary1(0):='aa';
ary1(1):='bb';
ary2(0):='cc';
ary2(1):='dd';
end;
/

create or replace procedure call_get_variable_text_02()
as
context_id number;
query clob;
define_column_ret int;
v1 clob;
v3 clob;
v2 clob;
v4 clob[];
v5 clob[];
v_in clob;
v_offset clob;
ary1 clob[];
ary2 clob[];
o_retw clob;
o_retw1 clob[];
begin
query := 'call pro_get_variable_result_text_02(in_raw,NULL,NULL,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'in_raw',1,10);
dbe_sql.sql_bind_variable(context_id, 'v_in',o_retw,100);
dbe_sql.sql_bind_variable(context_id, 'v_offset',o_retw,100);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);
define_column_ret := dbe_sql.sql_run(context_id);

v1:=dbe_sql.get_variable_result_text(context_id,'v_in');
v2:=dbe_sql.get_variable_result_text(context_id,'v_offset');
dbe_sql.get_array_result_text(context_id,'ary1',v4);
dbe_sql.get_array_result_text(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v2: %' ,v2;
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v5: %' ,v5(0);
RAISE INFO 'v5: %' ,v5(1);
dbe_sql.sql_unregister_context(context_id);
end;
/

call call_get_variable_07();

----==================================================
CREATE OR REPLACE PROCEDURE proc_get_variable_arr_result_text_03(i_col1 in int,o_ret out character varying,o_ret2 out character varying, ary1 out character varying[],ary2 out character varying[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret:=1;
o_ret2:=2;
ary1(0):='a';
ary1(1):='d';
ary1(2):='f';
ary2(0):='f';
ary2(1):='d';
ary2(2):='f';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 character varying;
v3 character varying;
v2 character varying;
v4 character varying[];
v5 character varying[];
o_ret character varying;

ary1 character varying[];
ary2 character varying[];
o_retw character varying;
o_retw1 character varying[];
begin
query := 'call proc_get_variable_arr_result_text_03(i_col1,o_ret,o_ret2,ary1,ary2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);

dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,1);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw,1);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);
define_column_ret := dbe_sql.sql_run(context_id);
v1:=dbe_sql.get_variable_result_text(context_id,'o_ret');
v2:=dbe_sql.get_variable_result_text(context_id,'o_ret2');
dbe_sql.get_array_result_text(context_id,'ary1',v4);
dbe_sql.get_array_result_text(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v2: %' ,v2;
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v5: %' ,v5(2);
RAISE INFO 'v5: %' ,v5(0);
end;
/

----=============================================

create or replace procedure proc_get_variable_result_raw_01(in_raw int,v_in out raw,v_offset out raw,ary1 out raw[],ary2 out raw[])
as
context_id int;
v_id int :=3;
begin
v_in:=HEXTORAW('DEADBEEF');
v_offset:=HEXTORAW('DEADBEEF');
ary1(0):=HEXTORAW('DEADBEEF');
ary1(1):=HEXTORAW('DEADBEEF');
ary2(0):=HEXTORAW('DEADBEEF');
ary2(1):=HEXTORAW('DEADBEEF');
end;
/

create or replace procedure call_get_variable_arr_raw_01()
as
context_id number;
query text;
define_column_ret int;
v1 raw;
v3 raw;
v2 raw;
v4 raw[];
v5 raw[];
v_in raw;
v_offset raw;
ary2 raw[];
ary1 raw[];
o_retw raw;
o_retw1 raw[];
begin
query := 'call proc_get_variable_result_raw_01(in_raw,v_in,v_offset,ary1,ary2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'in_raw',1,10);
dbe_sql.sql_bind_variable(context_id, 'v_in',o_retw,100);
dbe_sql.sql_bind_variable(context_id, 'v_offset',o_retw,100);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result_raw(context_id,'v_in',v1);
dbe_sql.get_variable_result_raw(context_id,'v_offset',v3);
dbe_sql.get_array_result_raw(context_id,'ary1',v4);
dbe_sql.get_array_result_raw(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v5: %' ,v5(0);
RAISE INFO 'v5: %' ,v5(1);
end;
/
call call_get_variable_arr_raw_01();


---============================================

create or replace procedure pro_get_variable_06(in_raw int,v_in out clob,v_offset out clob,ary1 out clob[],ary2 out clob[])
as
context_id int;
v_id int :=3;

v_id1 int[];
v_id4 clob[];
v_id5 bytea[];
v_id6 text[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
begin
v_in:='aaa36';
v_offset:='fdf5';
ary1(0):='aafd';
ary1(1):='fdsf';
ary2(0):='fa';
ary2(1):='fsafdasf';
end;
/

create or replace procedure call_get_variable_06()
as
context_id number;
query text;
define_column_ret int;
v1 clob;
v3 clob;
v2 clob;
v4 clob[];
v5 clob[];
v_in clob;
v_offset clob;
ary1 clob[];
ary2 clob[];
o_retw clob;
o_retw1 clob[];
begin
query := 'call pro_get_variable_06(in_raw,NULL,NULL,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'in_raw',1,10);
dbe_sql.sql_bind_variable(context_id, 'v_in',o_retw,100);
dbe_sql.sql_bind_variable(context_id, 'v_offset',o_retw,100);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'v_in',v1);
dbe_sql.get_variable_result(context_id,'v_offset',v3);
dbe_sql.get_variable_result(context_id,'ary1',v4);
dbe_sql.get_variable_result(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v5: %' ,v5(0);
RAISE INFO 'v5: %' ,v5(1);
end;
/
call call_get_variable_06();

----=================================test 直接获取第n列======================
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(b int, c char, d text);
insert into pro_dbe_sql_all_tb1_02 values(9,'5','13');
insert into pro_dbe_sql_all_tb1_02 values(10,'6','14');
insert into pro_dbe_sql_all_tb1_02 values(11,'7','15');
insert into pro_dbe_sql_all_tb1_02 values(12,'8','16');
create or replace procedure pro_dbe_sql_all_01()
as
context_id int;
v_id int :=3;
--test
v_id1 int[];
v_id4 character[];
v_id5 bytea[];
v_id6 text[];
v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
col_type1 int;
col_type2 char;
col_type3 text;
col_type4 bytea;
begin
query := ' select * from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--定义列
DBE_SQL.set_result_type_ints(context_id,2,v_id1,v_id,v_id2);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop
v_id3 := dbe_sql.next_row(context_id);
v_id1 := DBE_SQL.get_results_int(context_id,2,v_id1);
exit when(v_id3 != 3);
end loop;
FOR i IN v_id1.FIRST .. v_id1.LAST LOOP
dbe_output.print_line('int' || i || ' = ' || v_id1[i]);
END LOOP;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/

call pro_dbe_sql_all_01();
drop table if exists pro_dbe_sql_all_tb1_02;

----==============================test  raw set_results_type== ====
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(b raw, c raw, d clob);
insert into pro_dbe_sql_all_tb1_02 values('9','5','13');
insert into pro_dbe_sql_all_tb1_02 values('10','6','14');
insert into pro_dbe_sql_all_tb1_02 values('11','7','15');
insert into pro_dbe_sql_all_tb1_02 values('12','8','16');
create or replace procedure pro_dbe_sql_all_01()
as
context_id int;
v_id int :=3;
--test
v_id1 raw[];
v_id4 character[];
v_id5 bytea[];
v_id6 text[];
v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
col_type1 int;
col_type2 char;
col_type3 text;
col_type4 bytea;
begin
query := ' select * from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--定义列
DBE_SQL.set_results_type(context_id,1,v_id1,v_id,v_id2);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop
v_id3 := dbe_sql.next_row(context_id);
DBE_SQL.get_results(context_id,1,v_id1);
exit when(v_id3 != 3);
end loop;
FOR i IN v_id1.FIRST .. v_id1.LAST LOOP
dbe_output.print_line('int' || i || ' = ' || v_id1[i]);
END LOOP;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/

call pro_dbe_sql_all_01();
drop table if exists pro_dbe_sql_all_tb1_02 ;

----------------check NULL for is_active and sql_unregister_context

create or replace procedure call_get_variable_06()
as
context_id int := NULL;
begin
raise notice '11111';
if dbe_sql.is_active(context_id) then
    raise notice '2222';
    dbe_sql.sql_unregister_context(context_id);
end if;
end;
/
select * from call_get_variable_06();

create or replace procedure call_get_variable_06()
as
context_id int := NULL;
begin
dbe_sql.sql_unregister_context(context_id);
end;
/
select * from call_get_variable_06();
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info bytea :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
type re_rssc is record (col_num int, desc_col dbe_sql.desc_tab);
employer re_rssc;
res re_rssc;
d int;
dd dbe_sql.desc_tab;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a int ,b int,c int,d int);
insert into pro_dbe_sql_all_tb1_02 values(4,3,2,11);
insert into pro_dbe_sql_all_tb1_02 values(6,3,1,11);
query := 'select *,1,ss from pro_dbe_sql_all_tb1_02 where a > y and a < z order by s';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
dbe_sql.sql_set_sql(context_id, query, 1);
--描述
res := dbe_sql.sql_describe_columns(context_id, d,dd);

--绑定参数
dbe_sql.sql_bind_variable(context_id, 'z', 10);
dbe_sql.sql_bind_variable(context_id, 'y', 1);
dbe_sql.sql_bind_variable(context_id, 's', 1);
dbe_sql.sql_bind_variable(context_id, 'ss', 1);
--定义列
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_id);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_id);
--输出结果
dbe_output.print_line('id:'|| v_id);
end loop;
dbe_output.print_line('col_num:' || res.col_num);
dbe_output.print_line('col_type:' || res.desc_col[1].col_type);
dbe_output.print_line('col_type:' || res.desc_col[2].col_type);
dbe_output.print_line('col_type:' || res.desc_col[3].col_type);
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);
--删除存储过程
DROP PROCEDURE pro_dbe_sql_all_02;
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out text[],o_ret2 out text[]) as
v_a varchar2;
begin
if i_col1=1 then
select 2 into v_a;
end if;
o_ret(0):='10';
o_ret(1):='20';
o_ret2(0):='30';
o_ret2(1):='40';
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 text[];
v3 text[];
v2 int;
o_ret text[];
o_retw text[];
v4 int[];

begin
v4(0):=1;
v4(1):=2;
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_array(context_id, 'i_col1',v4);
commit;
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);
dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);
dbe_sql.sql_unregister_context(context_id);
--输出结果
dbe_output.print_line('v1: '|| v1(0));
dbe_output.print_line('v1: '|| v1(1));
dbe_output.print_line('v1: '|| v3(0));
dbe_output.print_line('v1: '|| v3(1));
end;
/
----===============================================
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,i_col2 in int,o_ret out text[],o_ret2 out text[]) as
v_a varchar2;
begin
if i_col1=1 then
o_ret(0):='10';
o_ret(1):='20';
o_ret2(0):='30';
o_ret2(1):='40';
end if;
if i_col1=2 and i_col2=1 then
o_ret(0):='100';
o_ret(1):='200';
o_ret2(0):='300';
o_ret2(1):='400';
end if;

end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 text[];
v3 text[];
v2 int;
o_ret text[];
o_retw text[];
v4 int[];

begin
v4(0):=1;
v4(1):=2;
query := 'call proc_test(i_col1,i_col2,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_array(context_id, 'i_col1',v4);
dbe_sql.sql_bind_variable(context_id, 'i_col2',1);
dbe_sql.sql_bind_array(context_id, 'o_ret',o_retw);
dbe_sql.sql_bind_array(context_id, 'o_ret2',o_retw);
define_column_ret := dbe_sql.sql_run(context_id);
dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);
dbe_sql.sql_unregister_context(context_id);
--输出结果
dbe_output.print_line('v1: '|| v1(0));
dbe_output.print_line('v1: '|| v1(1));
dbe_output.print_line('v1: '|| v3(0));
dbe_output.print_line('v1: '|| v3(1));
end;
/

-----===========================
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out int,o_ret2 out int) as
v_a varchar2;
begin
if i_col1=1 then
o_ret:=10;
o_ret2:=30;
end if;
if i_col1=2 then
o_ret:=20;
o_ret2:=40;
else
o_ret:=100;
o_ret2:=200;
end if;
end;
/

declare
context_id number;
query text;
define_column_ret int;
v1 int;
v3 int;
v2 int;
o_ret int;
o_retw int;
begin
query := 'call proc_test(i_col1,NULL,NULL);';
context_id := dbe_sql.register_context();

for i in 1..3 loop
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',i,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw,100);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
end loop;
dbe_sql.sql_unregister_context(context_id);
--输出结果

end;
/


-----====================================================
create or replace procedure pro_dbe_sql_all_02(in_raw raw,v_in int,v_offset int)
as 
context_id int;
v_id int;
v_info1 text :=1;

query varchar(2000);
execute_ret int;
define_column_ret_raw bytea :='1';
define_column_ret int;
begin
drop table if exists pro_dbe_sql_all_tb1_02 ;
create table pro_dbe_sql_all_tb1_02(a text ,b clob);
insert into pro_dbe_sql_all_tb1_02 values('asbdrdgg',HEXTORAW('DEADBEEE'));
insert into pro_dbe_sql_all_tb1_02 values(2,in_raw);
query := 'select a from pro_dbe_sql_all_tb1_02 order by 1';
--打开游标
context_id := dbe_sql.register_context();
--编译游标
--定义列
for i in 1..20 loop
dbe_sql.sql_set_sql(context_id, query, 1);
define_column_ret:= dbe_sql.set_result_type(context_id,1,v_info1,10);
--执行
execute_ret := dbe_sql.sql_run(context_id);
loop 
exit when (dbe_sql.next_row(context_id) <= 0);
--获取值
dbe_sql.get_results(context_id,1,v_info1);
--输出结果
dbe_output.print_line('info:'|| 1 || ' info:' ||v_info1);
end loop;
end loop;
--关闭游标
dbe_sql.sql_unregister_context(context_id);
end;
/
--调用存储过程
call pro_dbe_sql_all_02(HEXTORAW('DEADBEEF'),0,1);

---============================6.自治事物================
--建表
create table t2(a int, b int);
insert into t2 values(1,2);
select * from t2;
--创建包含自治事务的存储过程
CREATE OR REPLACE PROCEDURE autonomous_4(a int, b int) AS 
DECLARE 
 num3 int := a;
 num4 int := b;
 PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
 insert into t2 values(num3, num4); 
 dbe_output.print_line('just use call.');
END;
/
--创建调用自治事务存储过程的普通存储过程
CREATE OR REPLACE PROCEDURE autonomous_5(a int, b int) AS 
DECLARE 
BEGIN
 dbe_output.print_line('just no use call.');
 insert into t2 values(666, 666);
 autonomous_4(a,b);
 rollback;
END;
/

create or replace procedure proc_test3() as
context_id number;
query text;
define_column_ret int;
v1 varchar2;
proc_name varchar2;
begin
proc_name:='autonomous_5';
query := 'call '||proc_name||'(o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'o_ret',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',1,10);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.sql_unregister_context(context_id);

--输出结果
RAISE INFO 'v1: %' ,v1;
end;
/
CREATE OR REPLACE PACKAGE package_002 IS
PROCEDURE testpro1(var3 int);
END package_002;
/

--调用普通存储过程
select autonomous_5(11,22);
--查看表结果
select * from t2 order by a;

------------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_test(i_col1 in int,o_ret out text,o_ret2 out text) as
v_a varchar2;
i int;
begin
i =1/0;
exception 
when others then
    raise info '%', 'exception';
end;
/

CREATE OR REPLACE PROCEDURE q(mm out int) as
declare
context_id number;
query text;
define_column_ret int;
v1 text;
v3 text;
v2 int;
o_retw text;
o_retw1 text;
begin
query := 'call proc_test(i_col1,o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'i_col1',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret',o_retw,1);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',o_retw1,3);
define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.get_variable_result(context_id,'o_ret2',v3);
dbe_sql.sql_unregister_context(context_id);

RAISE INFO 'v1: : %' ,v1;
RAISE INFO 'v1: : %' ,v3;
mm = 1;
end;
/
select * from q();

create or replace procedure proc_get_variable_result_raw_01(in_raw int,v_in out blob,v_offset out blob,ary1 out blob[],ary2 out blob[])
as
context_id int;
v_id int :=3;
--test
v_id1 int[];
v_id4 blob[];
v_id5 blob[];
v_id6 text[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
i int := 1;
begin
v_in:=HEXTORAW('DEADBEEF');
v_offset:=HEXTORAW('DEADBEEF');
ary1(0):=HEXTORAW('DEADBEEF');
ary1(1):=HEXTORAW('DEADBEEF');
ary2(0):=HEXTORAW('DEADBEEF');
ary2(1):=HEXTORAW('DEADBEEF');
end;
/

create or replace procedure call_get_variable_raw_01()
as
context_id number;
query text;
define_column_ret int;
v1 blob;
v3 blob;
v2 blob;
v4 blob[];
v5 blob[];
v_in blob;
v_offset blob;
ary2 blob[];
ary1 blob[];
o_retw blob;
o_retw1 blob[];
i int := 1;
begin
query := 'call proc_get_variable_result_raw_01(in_raw,NULL,NULL,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
--while i < 4 loop
dbe_sql.sql_bind_variable(context_id, 'in_raw',1,10);
dbe_sql.sql_bind_variable(context_id, 'v_in',o_retw,100);
dbe_sql.sql_bind_variable(context_id, 'v_offset',o_retw,100);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result_raw(context_id,'v_in',v1);
dbe_sql.get_variable_result_raw(context_id,'v_offset',v3);
dbe_sql.get_array_result_raw(context_id,'ary1',v4);
dbe_sql.get_array_result_raw(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v5: %' ,v5(0);
RAISE INFO 'v5: %' ,v5(1);
dbe_sql.sql_unregister_context(context_id);
end;
/

call call_get_variable_raw_01();

-----------------------------------
create or replace procedure proc_get_variable_result_raw_01(in_raw int,v_in out bytea,v_offset out bytea,ary1 out bytea[],ary2 out bytea[])
as
context_id int;
v_id int :=3;
--test
v_id1 int[];
v_id4 bytea[];
v_id5 bytea[];
v_id6 text[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
i int := 1;
begin
v_in:=HEXTORAW('DEADBEEF');
v_offset:=HEXTORAW('DEADBEEF');
ary1(0):=HEXTORAW('DEADBEEF');
ary1(1):=HEXTORAW('DEADBEEF');
ary2(0):=HEXTORAW('DEADBEEF');
ary2(1):=HEXTORAW('DEADBEEF');
end;
/

create or replace procedure call_get_variable_raw_01()
as
context_id number;
query text;
define_column_ret int;
v1 bytea;
v3 bytea;
v2 bytea;
v4 bytea[];
v5 bytea[];
v_in bytea;
v_offset bytea;
ary2 bytea[];
ary1 bytea[];
o_retw bytea;
o_retw1 bytea[];
i int := 1;
begin
query := 'call proc_get_variable_result_raw_01(in_raw,NULL,NULL,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
--while i < 4 loop
dbe_sql.sql_bind_variable(context_id, 'in_raw',1,10);
dbe_sql.sql_bind_variable(context_id, 'v_in',o_retw,100);
dbe_sql.sql_bind_variable(context_id, 'v_offset',o_retw,100);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result_raw(context_id,'v_in',v1);
dbe_sql.get_variable_result_raw(context_id,'v_offset',v3);
dbe_sql.get_array_result_raw(context_id,'ary1',v4);
dbe_sql.get_array_result_raw(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v5: %' ,v5(0);
RAISE INFO 'v5: %' ,v5(1);
dbe_sql.sql_unregister_context(context_id);
end;
/

call call_get_variable_raw_01();

-------------------------------------
create or replace procedure proc_get_variable_result_raw_01(in_raw int,v_in out raw,v_offset out raw,ary1 out raw[],ary2 out raw[])
as
context_id int;
v_id int :=3;
--test
v_id1 int[];
v_id4 raw[];
v_id5 raw[];
v_id6 text[];

v_id2 int := 1;
v_id3 int;
query varchar(2000);
execute_ret int;
define_column_ret int;
i int := 1;
begin
v_in:=HEXTORAW('DEADBEEF');
v_offset:=HEXTORAW('DEADBEEF');
ary1(0):=HEXTORAW('DEADBEEF');
ary1(1):=HEXTORAW('DEADBEEF');
ary2(0):=HEXTORAW('DEADBEEF');
ary2(1):=HEXTORAW('DEADBEEF');
end;
/

create or replace procedure call_get_variable_raw_01()
as
context_id number;
query text;
define_column_ret int;
v1 raw;
v3 raw;
v2 raw;
v4 raw[];
v5 raw[];
v_in raw;
v_offset raw;
ary2 raw[];
ary1 raw[];
o_retw raw;
o_retw1 raw[];
i int := 1;
begin
query := 'call proc_get_variable_result_raw_01(in_raw,NULL,NULL,NULL,NULL);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'in_raw',1,10);
dbe_sql.sql_bind_variable(context_id, 'v_in',o_retw,100);
dbe_sql.sql_bind_variable(context_id, 'v_offset',o_retw,100);
dbe_sql.sql_bind_array(context_id, 'ary1',o_retw1);
dbe_sql.sql_bind_array(context_id, 'ary2',o_retw1);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result_raw(context_id,'v_in',v1);
dbe_sql.get_variable_result_raw(context_id,'v_offset',v3);
dbe_sql.get_array_result_raw(context_id,'ary1',v4);
dbe_sql.get_array_result_raw(context_id,'ary2',v5);
--输出结果
RAISE INFO 'v1: %' ,v1;
RAISE INFO 'v3: %' ,v3;
RAISE INFO 'v4: %' ,v4(0);
RAISE INFO 'v4: %' ,v4(1);
RAISE INFO 'v5: %' ,v5(0);
RAISE INFO 'v5: %' ,v5(1);
dbe_sql.sql_unregister_context(context_id);
end;
/

call call_get_variable_raw_01();

CREATE OR REPLACE FUNCTION x1(a in int)
RETURNS int
AS $$
DECLARE
BEGIN
    a:=11;
    commit;
    return 12;
END;
$$ LANGUAGE plpgsql;
create or replace procedure y(a in int)
as 
declare
begin
savepoint aa;
a:= x1(1);
rollback to aa;
end;
/
call y(1);
drop FUNCTION x1();
drop procedure y();

-- test commit/rollback with /
create or replace procedure pro(numt out int)
as
begin
numt:=12;
rollback;
numt:=10;
commit;
numt:=10;
commit;
end;
/
create or replace procedure pro2(numtin in int,numtout out int)
as
begin
numtout:=pro()/numtin;
exception
when others then
raise info 'numtout : %', numtout;
end;
/

call pro2(2,1);
call pro2(2,1);

drop procedure pro2;
drop procedure pro;

drop table if exists tab_1;
drop procedure if exists test();

create table tab_1(a int);
insert into tab_1 values (1),(2),(3),(4);

select * from tab_1 order by 1;

create or replace procedure test()
as
query varchar(2000);
context_id int;
cnt int :=3;
col int[];
col1 int[];
col2 int[];
num int;
begin
query := 'select * from tab_1 order by 1;';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1); 
dbe_sql.set_results_type(context_id,1,col,cnt,1);
dbe_sql.sql_run(context_id);
loop 
num := dbe_sql.next_row(context_id);
dbe_sql.get_results(context_id,1,col1); 
dbe_sql.get_results(context_id,1,col2);
exit when(num != 3); 
end loop; 
FOR j IN col1.FIRST .. col1.LAST LOOP 
dbe_output.print_line(col1[j]); 
END LOOP; 
FOR j IN col2.FIRST .. col2.LAST LOOP 
dbe_output.print_line(col2[j]); 
END LOOP;
dbe_sql.sql_unregister_context(context_id);
end;
/

call test();

drop procedure if exists test();
drop table if exists tab_1;


create or replace procedure pro_dbms_sql_all_03(in_raw raw,in_int int,in_long bigint,in_text text,in_char char(30),in_varchar varchar(30))
as
declare
cursorid int;
v_id int;
v_info bytea :=1;
v_info_new raw;
v_long bigint;
v_text text:=2;
v_char char(30):=1;
v_varchar text :=1;
query varchar(2000);
execute_ret int;
define_column_ret_raw raw;
define_column_ret int;
define_column_ret_long bigint;
define_column_ret_text text;
define_column_ret_char char(30);
define_column_ret_varchar text;
err numeric;
act_len int;
begin
drop table if exists pro_dbms_sql_all_tb1_03 ;
create table pro_dbms_sql_all_tb1_03(a int ,b raw,c bigint,d text,e char(30),f varchar(30));
insert into pro_dbms_sql_all_tb1_03 values(1,HEXTORAW('DEADBEEF'),in_long,in_text,in_char,in_varchar);
insert into pro_dbms_sql_all_tb1_03 values(in_int,in_raw,-9223372036854775808,'5','十里春风','845injnj');
insert into pro_dbms_sql_all_tb1_03 values(3,HEXTORAW('DEADBEEF'),9223372036854775807,'4','十全十美','芝麻街');
insert into pro_dbms_sql_all_tb1_03 values(-2147483648,HEXTORAW('1'),'3','3','暖暖','weicn');
insert into pro_dbms_sql_all_tb1_03 values(2147483647,HEXTORAW('2'),'4','2',in_varchar,in_char);
query := 'select * from pro_dbms_sql_all_tb1_03 order by 1';
--打开游标
cursorid := DBE_SQL.register_context();
--编译游标
DBE_SQL.sql_set_sql(cursorid, query, 1);
--定义列
define_column_ret:= DBE_SQL.set_result_type(cursorid,1,v_id);
define_column_ret_raw := DBE_SQL.set_result_type_raw(cursorid,2,v_info_new,20);
define_column_ret_long := DBE_SQL.set_result_type_long(cursorid,3);
define_column_ret_text := DBE_SQL.set_result_type(cursorid,4,v_text);
define_column_ret_char := DBE_SQL.set_result_type_char(cursorid,5,v_char,30);
define_column_ret_varchar:= DBE_SQL.set_result_type(cursorid,6,v_varchar);
--执行
execute_ret := DBE_SQL.sql_run(cursorid);
loop
exit when (DBE_SQL.next_row(cursorid) <= 0);
--获取值
DBE_SQL.get_result(cursorid,1,v_id);
DBE_SQL.get_result_raw(cursorid,2,v_info_new);
DBE_SQL.get_result_long(cursorid,3,21,1,v_long,act_len);
DBE_SQL.get_result(cursorid,4,v_text);
DBE_SQL.get_result_char(cursorid,5,v_char,err,act_len);
DBE_SQL.get_result(cursorid,6,v_varchar);
--输出结果
raise notice 'result is: %', v_id || ' , ' || v_info|| ' , ' || v_long|| ' , ' || v_text|| ' , ' || v_char|| ' , ' || v_varchar;
end loop;
--关闭游标
DBE_SQL.sql_unregister_context(cursorid);
end;
/

call pro_dbms_sql_all_03(HEXTORAW('DEADBEEF'),2,4,'十里桃花','sdj是','东城街道shj23');

----==========================bigint
create or replace procedure pro_dbms_sql_all_03()
as
declare
	context_id int;
	execute_ret int;
	fetch_ret int;
	col_value bigint[];  
	cnt int := 2;  
	query varchar(2000);
BEGIN
drop table if exists t;
create table t (a int, b bigint, c text, d raw, e bytea, f clob, g blob, h varchar);
insert into t values (1, 1, '1', '1', '1', '1', '1', '1');
insert into t values (2, 2, '2', '2', '2', '2', '2', '2');
insert into t values (3, 3, '3', '3', '3', '3', '3', '3');
query := 'select b from t where b < x;';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'x', 10);
dbe_sql.set_results_type(context_id, 1, col_value, cnt, 1);
execute_ret := dbe_sql.sql_run(context_id);
fetch_ret := dbe_sql.next_row(context_id);
dbe_sql.get_results(context_id, 1, col_value);
dbe_sql.sql_unregister_context(context_id);
raise info 'execute_ret: %', execute_ret;
raise info 'fetch_ret: %', fetch_ret;
raise info 'col_value: %', col_value;
END;
/
call pro_dbms_sql_all_03();
