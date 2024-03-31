SET client_min_messages = WARNING;
\set VERBOSITY terse
\set ECHO all

drop procedure if exists do_something;
drop procedure if exists do_wrapper;
drop procedure if exists test_profiler_start;
drop procedure if exists test_profiler_flush;
drop procedure if exists test_profiler_version;
drop procedure if exists test_profiler_pause;
drop procedure if exists test_profiler_resume;
drop procedure if exists test_profiler_resume2;
drop procedure if exists test_profiler_version_check;
drop procedure if exists test_profiler_start_ext;

create extension gms_profiler;

create or replace procedure do_something (p_times in number) as
      l_dummy number;
begin
for i in 1 .. p_times loop
select l_dummy +1
into l_dummy;
end loop;
end;
/

create or replace procedure do_wrapper (p_times in number) as
begin
for i in 1 .. p_times loop
        do_something(p_times);
end loop;
end;
/

create or replace procedure test_profiler_start () as
declare
l_result binary_integer;
begin
    l_result := gms_profiler.start_profiler('test_profiler', 'simple');
    do_wrapper(p_times => 2);
    l_result := gms_profiler.stop_profiler();
end;
/

create or replace procedure test_profiler_flush () as
declare
l_result binary_integer;
begin
    l_result := gms_profiler.start_profiler('test_profiler_flush', 'flush');
    do_wrapper(p_times => 2);
    l_result := gms_profiler.flush_data();
    do_wrapper(p_times => 2);
    l_result := gms_profiler.stop_profiler();
end;
/

create or replace procedure test_profiler_version () as
declare
major_v binary_integer;
   minor_v binary_integer;
begin
select major, minor into major_v, minor_v from gms_profiler.get_version();
end;
/

create or replace procedure test_profiler_pause () as
declare
l_result binary_integer;
begin
    l_result := gms_profiler.start_profiler('test_profiler_pause', 'pause');
    do_wrapper(p_times => 2);
    l_result := gms_profiler.pause_profiler();
    do_wrapper(p_times => 2);
    l_result := gms_profiler.stop_profiler();
end;
/

create or replace procedure test_profiler_resume () as
declare
l_result binary_integer;
begin
    l_result := gms_profiler.start_profiler('test_profiler_resume', 'resume1');
    do_wrapper(p_times => 2);
    l_result := gms_profiler.pause_profiler();
    l_result := gms_profiler.resume_profiler();
    do_wrapper(p_times => 2);
    l_result := gms_profiler.stop_profiler();
end;
/

create or replace procedure test_profiler_resume2 () as
declare
l_result binary_integer;
begin
    l_result := gms_profiler.resume_profiler();
    do_wrapper(p_times => 2);
    l_result := gms_profiler.start_profiler('test_profiler_resume2', 'resume2');
    l_result := gms_profiler.pause_profiler();
    do_wrapper(p_times => 2);
    l_result := gms_profiler.pause_profiler();
    l_result := gms_profiler.resume_profiler();
    do_wrapper(p_times => 2);
    l_result := gms_profiler.stop_profiler();
end;
/

create or replace procedure test_profiler_version_check () as
declare
l_result binary_integer;
begin
    l_result := gms_profiler.internal_version_check();
end;
/

create or replace procedure test_profiler_start_ext () as
declare
l_result binary_integer;
   runid binary_integer;
begin
select run_number, run_result into runid, l_result from gms_profiler.start_profiler_ext('start_profiler_ext');
do_wrapper(p_times => 2);
    l_result := gms_profiler.stop_profiler();
end;
/

call test_profiler_start();
call test_profiler_flush();
call test_profiler_version();
call test_profiler_pause();
call test_profiler_resume();
call test_profiler_resume2();
call test_profiler_version_check();
call test_profiler_start_ext();

select runid, run_comment, run_comment1 from gms_profiler.plsql_profiler_runs order by runid;
select runid, unit_number, unit_type, unit_name from gms_profiler.plsql_profiler_units order by runid, unit_number;
select runid, unit_number, line#, total_occur from gms_profiler.plsql_profiler_data order by runid, unit_number, line#;

drop procedure if exists do_something;
drop procedure if exists do_wrapper;
drop procedure if exists test_profiler_start;
drop procedure if exists test_profiler_flush;
drop procedure if exists test_profiler_version;
drop procedure if exists test_profiler_pause;
drop procedure if exists test_profiler_resume;
drop procedure if exists test_profiler_resume2;
drop procedure if exists test_profiler_version_check;
drop procedure if exists test_profiler_start_ext;

reset client_min_messages;
