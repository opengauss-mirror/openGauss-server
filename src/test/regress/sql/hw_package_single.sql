create database pl_test_pkg_single DBCOMPATIBILITY 'pg';
\c pl_test_pkg_single;
--test dbe_utility
CREATE OR REPLACE PROCEDURE p0()
AS
declare
    a  integer;
    c  integer;
    b  integer;
BEGIN
    a:=1;
    c:=0;
    b := a / c;
    dbe_output.print_line('result is: '||to_char(b));
END;
/

CREATE OR REPLACE PROCEDURE p1() 
AS
BEGIN
    p0();
END;
/

CREATE OR REPLACE PROCEDURE p2() 
AS
BEGIN
    p1();
END;
/

--test dbe_utility.format_error_backtrack
CREATE OR REPLACE PROCEDURE p3_error() 
AS
BEGIN
    p2();
EXCEPTION
    WHEN OTHERS THEN
        dbe_output.print_line(dbe_utility.format_error_backtrace());
END;
/
call p3_error();

--test dbe_utility.format_error_stack
CREATE OR REPLACE PROCEDURE p3_error_stack() 
AS
BEGIN
    p2();
EXCEPTION
    WHEN OTHERS THEN
        dbe_output.print_line(dbe_utility.format_error_stack());
END;
/
call p3_error_stack();

CREATE OR REPLACE PROCEDURE p0()
AS
declare
    a  integer;
    c  integer;
    b  integer;
BEGIN
    a:=1;
    c:=1;
    b := a / c;
    dbe_output.print_line('result is: '||to_char(b));
END;
/

--test dbe_utility.format_error_backtrace
CREATE OR REPLACE PROCEDURE p3_noError() 
AS
BEGIN
    p2();
EXCEPTION
    WHEN OTHERS THEN
        dbe_output.print_line(utility.format_error_backtrace());
END;
/
call p3_noError();

--test dbe_utility.format_error_stack
CREATE OR REPLACE PROCEDURE p3_noError_stack() 
AS
BEGIN
    p2();
EXCEPTION
    WHEN OTHERS THEN
        dbe_output.print_line(utility.format_error_stack());
END;
/
call p3_noError_stack();

--test dbe_utility.format_call_stack
CREATE OR REPLACE PROCEDURE p0()
AS
declare
    a  integer;
    c  integer;
    b  integer;
BEGIN
    a:=1;
    c:=1;
    b := a / c;
    dbe_output.print_line('result is: '||to_char(b));
    dbe_output.print_line(dbe_utility.format_call_stack());
END;
/

CREATE OR REPLACE PROCEDURE p3_call_stack() 
AS
BEGIN
	p2();	
END;
/
call p3_call_stack();

--test dbe_utility.get_time
CREATE OR REPLACE PROCEDURE test_get_time1() 
AS
declare
    start_time  bigint;
    end_time  bigint;
BEGIN
    start_time:= dbe_utility.get_time ();
    pg_sleep(1);
    end_time:=dbe_utility.get_time ();
    dbe_output.print_line(end_time - start_time);	
END;
/
call test_get_time1();

CREATE OR REPLACE PROCEDURE test_get_time5() 
AS
declare
    start_time  bigint;
    end_time  bigint;
BEGIN
    start_time:= dbe_utility.get_time ();
    pg_sleep(5);
    end_time:=dbe_utility.get_time ();
    dbe_output.print_line(end_time - start_time);	
END;
/
call test_get_time5();

--test dbe_match.edit_distance_similarity
select dbe_match.edit_distance_similarity('abcd', 'abcd');
select dbe_match.edit_distance_similarity('aaaa', 'a');
select dbe_match.edit_distance_similarity('aaaa', 'aaa');

--test dbe_raw
select dbe_raw.bit_or('a1234', '12');
select dbe_raw.bit_or('0000', '1111');
select dbe_raw.bit_or('0000', '11');
select dbe_raw.bit_or('baf234', '11');
select dbe_raw.bit_or('baf234', '00');

CREATE OR REPLACE PROCEDURE test_bitor() 
AS
declare
    a  raw;
	b  raw;
BEGIN
    a:= 'abc123';
    b:= '12';
    dbe_output.print_line(dbe_raw.bit_or(a, b));	
END;
/
call test_bitor();

select DBE_RAW.cast_from_varchar2_to_raw('aaa');
select dbe_raw.cast_to_varchar2('616161');
select DBE_RAW.cast_from_varchar2_to_raw('cf12');
select dbe_raw.cast_to_varchar2('63663132');
select DBE_RAW.cast_from_varchar2_to_raw('341');
select dbe_raw.cast_to_varchar2('333431');


select dbe_raw.substr('aba', 1, 2);
CREATE OR REPLACE PROCEDURE test_substr() 
AS
declare
    a  raw;
BEGIN
    a:= 'abc123';
    dbe_output.print_line(dbe_raw.substr(a, 3, 2));	
END;
/
call test_substr();

--test dbe_session
select DBE_SESSION.set_context('test', 'gaussdb', 'one');
select DBE_SESSION.search_context('test', 'gaussdb');
select DBE_SESSION.set_context('test', 'gaussdb', 'two');
select DBE_SESSION.search_context('test', 'gaussdb');
select DBE_SESSION.set_context('test', 'gaussdb', 'two');
select DBE_SESSION.search_context('test', 'gaussdb');
select DBE_SESSION.clear_context('test', 'test','gaussdb');
select DBE_SESSION.search_context('test', 'gaussdb');

create or replace function test_set_context (
    namespace text,
    attribute text,
    value text
)
returns void AS $$
BEGIN
    DBE_SESSION.set_context(namespace, attribute, value);
END;
$$ LANGUAGE plpgsql;

call test_set_context('test', 'name', 'tony');

create or replace function test_sys_context (
    namespace text,
    attribute text
)
returns text AS $$
BEGIN
    return DBE_SESSION.search_context(namespace, attribute);
END;
$$ LANGUAGE plpgsql;

call test_sys_context('test', 'name'); 

create or replace function test_clear_context2 (
    namespace text,
    attribute text,
    value text
)
returns void AS $$
BEGIN
    DBE_SESSION.clear_context(namespace, attribute, value);
END;
$$ LANGUAGE plpgsql;

call test_clear_context('test', 'text', 'name');
call test_sys_context('test', 'name');

create or replace function test_set_context2 (
    namespace text,
    attribute text,
    value text
)
returns void AS $$
BEGIN
    DBE_SESSION.set_context(namespace, attribute, value);
END;
$$ LANGUAGE plpgsql;

call test_set_context2('CTX_P_GCMS_BIND_PKG', 'type', 'AAA');

create or replace function test_sys_context2 (
    namespace text,
    attribute text
)
returns text AS $$
BEGIN
    return DBE_SESSION.search_context(namespace, attribute);
END;
$$ LANGUAGE plpgsql;

call test_sys_context2('CTX_P_GCMS_BIND_PKG ', 'type',); 

create or replace function test_clear_context2 (
    namespace text,
    attribute text,
    value text
)
returns void AS $$
BEGIN
    DBE_SESSION.clear_context(namespace, attribute, value);
END;
$$ LANGUAGE plpgsql;

call test_clear_context2('test', 'text', 'name');
call test_sys_context2('test', 'name');

create or replace function test_set_context3 (
    namespace text,
    attribute text,
    value text
)
returns void AS $$
BEGIN
    DBE_SESSION.set_context(namespace, attribute, value);
END;
$$ LANGUAGE plpgsql;

call test_set_context('test1', 'name1', 'tony1');

create or replace function test_sys_context3 (
    namespace text,
    attribute text
)
returns text AS $$
BEGIN
    return DBE_SESSION.search_context(namespace, attribute);
END;
$$ LANGUAGE plpgsql;

call test_sys_context('test1', 'name1'); 

create or replace function test_clear_context3 (
    namespace text,
    attribute text,
    value text
)
returns void AS $$
BEGIN
    DBE_SESSION.clear_context(namespace, attribute, value);
END;
$$ LANGUAGE plpgsql;

call test_clear_context('test1', 'text1', 'name1');
call test_sys_context('test', 'name');

create or replace procedure proc_test1(i_col1 in varchar2, o_ret out varchar2) as
begin
null;
end;
/
create or replace procedure proc_test1(i_col1 in varchar2, o_ret out varchar2) as
v_cursor_id number;
o_ret1 varchar2;
v_execute number;
v_sql text;
begin
o_ret:='1';
o_ret1 := '0';
v_sql:='begin proc_test(i_col1,o_ret1); end;';
v_cursor_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(v_cursor_id,v_sql,1);
perform dbe_sql.sql_bind_variable(v_cursor_id,'i_col1',i_col1,10);
perform dbe_sql.sql_bind_variable(v_cursor_id,'o_col1',o_ret1,10);
v_execute:=dbe_sql.sql_run(v_cursor_id);
exception
when others then
if dbe_sql.is_active(v_cursor_id) then
dbe_sql.sql_unregister_context(v_cursor_id);
end if;
end;
/
select proc_test1('1','');

drop procedure proc_test1;

--test write+update
create table test_clob(a int, b clob);
create table test_blob(a int, b blob);

insert into test_clob values(1, 'abc');
insert into test_clob values(2, 'zzz');

insert into test_blob values(1, 'abc');
insert into test_blob values(2, 'fffffff');

create or replace procedure test_clob_write
as
    dest_clob clob;
	PSV_SQL varchar(100);
begin
    PSV_SQL := 'select b from test_clob where a = 1';
    EXECUTE IMMEDIATE PSV_SQL into dest_clob;
    dbe_lob.write(dest_clob, 1, 1, 'ddd');
    return;
end;
/
call test_clob_write();
select * from test_clob;

create or replace procedure test_lob_write_nodyna
as
declare
    dest_clob clob;
begin
    select b from test_clob where a = 2 into dest_clob;
    dbe_lob.write(dest_clob, 1, 1, 'eeee'); 
    DBE_OUTPUT.print_line(dest_clob);
    return;
end;
/

call test_lob_write_nodyna();
select * from test_clob;

create or replace procedure test_blob_write
as
    dest_blob blob;
	PSV_SQL varchar(100);
begin
    PSV_SQL := 'select b from test_blob where a = 1';
    EXECUTE IMMEDIATE PSV_SQL into dest_blob;
    dbe_lob.write(dest_blob, 1, 1, 'ddd');
    return;
end;
/
call test_blob_write();
select * from test_blob;

--test append+update
create or replace procedure test_clob_append
as
    dest_clob clob;
	PSV_SQL varchar(100);
begin
    PSV_SQL := 'select b from test_clob where a = 1';
    EXECUTE IMMEDIATE PSV_SQL into dest_clob;
    dbe_lob.append(dest_clob, 'ddd');
    return;
end;
/

call test_clob_append();
select * from test_clob;

create or replace procedure test_blob_append
as
    dest_blob blob;
	PSV_SQL varchar(100);
begin
    PSV_SQL := 'select b from test_blob where a = 1';
    EXECUTE IMMEDIATE PSV_SQL into dest_blob;
    dbe_lob.append(dest_blob, 'ddd');
    return;
end;
/

call test_blob_append();
select * from test_blob;

--test read
create or replace procedure test_clob_read
as
    dest_clob clob;
	buf clob;
	PSV_SQL varchar(100);
begin
    PSV_SQL := 'select b from test_clob where a = 1';
	EXECUTE IMMEDIATE PSV_SQL into dest_clob;
	dbe_lob.read(dest_clob, 1, 1, buf);
    DBE_OUTPUT.print_line(buf);
	return;
end;
/

call test_clob_read();

drop table if exists test_clob;

\c regression;
drop database IF EXISTS pl_test_pkg_single;

