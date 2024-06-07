create schema ignore_keyword_list;
set current_schema to 'ignore_keyword_list';
select 1 interval; --error
set disable_keyword_options = 'interval';
select 1 interval; --ok
set disable_keyword_options = 'intervalxx';
set disable_keyword_options = 'interval,interval';
select 1 interval; --ok
reset disable_keyword_options;
select 1 interval; --error
drop schema ignore_keyword_list cascade;