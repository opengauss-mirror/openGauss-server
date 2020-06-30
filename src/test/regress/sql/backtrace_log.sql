-- test view
select count(*) from pg_node_env;

select count(*) from pg_os_threads;


-- test backtrace output to log
set backtrace_min_messages=error;
select * from aaa;
reset backtrace_min_messages;
