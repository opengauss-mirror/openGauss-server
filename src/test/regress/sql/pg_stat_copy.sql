create database pg_stat_copy_a dbcompatibility 'A';
\c pg_stat_copy_a

\d+ pg_stat_progress_copy

create table tab_progress_reporting (
    name text,
    age int4,
    location point,
    salary int4,
    manager name
);

create function notice_after_tab_progress_reporting() returns trigger AS
$$
declare report record;
begin
  with progress_data as (
    select
       relid::regclass::text as relname,
       command,
       type,
       bytes_processed,
       bytes_total, 
       bytes_processed > 0 as has_bytes_processed,
       bytes_total > 0 as has_bytes_total,
       tuples_processed,
       tuples_excluded
      from pg_stat_progress_copy)
select into report (row_to_json(r))::text as value
from progress_data r;

  raise info 'progress: %', report.value::text;
  return new;
end;
$$ language plpgsql;

create trigger check_after_tab_progress_reporting
   after insert on tab_progress_reporting
   for each statement
   execute function notice_after_tab_progress_reporting();

copy tab_progress_reporting from stdin;
sharon	25	(15,12)	1000	sam
sam	30	(10,5)	2000	bill
bill	20	(11,10)	1000	sharon
\.

\copy tab_progress_reporting from stdin;
sharon	25	(15,12)	1000	sam
sam	30	(10,5)	2000	bill
bill	20	(11,10)	1000	sharon
\.

\c regression
drop database pg_stat_copy_a;

