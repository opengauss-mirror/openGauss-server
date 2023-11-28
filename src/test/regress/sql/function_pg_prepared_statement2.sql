drop table if exists table_test_prepared_statement2;

select pg_sleep(1);

create table table_test_prepared_statement2 (a int);

prepare stmt2_in_session2 as insert into table_test_prepared_statement2 values (2);

select name, statement, parameter_types, from_sql from pg_prepared_statement(
 (
    select sessionid
    from pg_stat_activity
    where application_name='gsql' and query like 'select pg_sleep%' 
    limit 1
 )
) order by prepare_time;

select name, statement, parameter_types, from_sql  from pg_prepared_statement(0) order by prepare_time;

drop table table_test_prepared_statement2;

