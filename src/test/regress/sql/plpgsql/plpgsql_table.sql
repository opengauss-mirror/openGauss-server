create schema plpgsql_table;
set search_path to plpgsql_table;

CREATE TYPE t_tf_row AS
(
    id          NUMBER,
    description VARCHAR2(50)
);
CREATE TYPE t_tf_tab IS TABLE OF t_tf_row;

CREATE OR REPLACE FUNCTION get_tab_ptf() returns t_tf_tab pipelined LANGUAGE plpgsql AS
$BODY$
declare result t_tf_row;
begin
    for i in 1 .. 4 loop
        result.id = i;
        result.description = 'Descrption for ' || i;
        pipe row(null);
        pipe row(result);
    end loop;
end;
$BODY$;

CREATE OR REPLACE FUNCTION get_tab_ptf(IN p_row number) returns t_tf_tab pipelined LANGUAGE plpgsql AS
$BODY$
declare result t_tf_row;
begin
    for i in 1 .. p_row loop
        result.id = i;
        result.description = 'Descrption for ' || i;
        pipe row(null);
        pipe row(result);
    end loop;
end;
$BODY$;

create table test(id int);
insert into test values(3);

-- syntax error at table
select table(get_tab_ptf);
-- syntax error at test
update table test set id = 1;
-- syntax error at set
update table(test) set id = 1;
-- syntax error at set
update table(get_tab_ptf) set id = 1;
-- success
select count(*) from table(get_tab_ptf) limit 1;
select count(*) from table(get_tab_ptf()) limit 1;
select count(*) from test t1 left join table(get_tab_ptf) t2 on t1.id = t2.id;
select count(*) from test t1 cross join table(get_tab_ptf) t2;
select count(*) from test t1 join table(get_tab_ptf) t2 on t1.id = t2.id;
select count(*) from test t1 natural join table(get_tab_ptf) t2;
select count(*) from table(get_tab_ptf) t2 left join test t1 on t1.id = t2.id;
select count(*) from table(get_tab_ptf) t2 cross join test t1;
select count(*) from table(get_tab_ptf) t2 join test t1 on t1.id = t2.id;
select count(*) from table(get_tab_ptf) t2 natural join test t1;

-- syntax error at table
delete from table(get_tab_ptf(3));
-- success
delete from test  using table(get_tab_ptf(2)) t2 where t2.id=1;
-- success
merge into test t1
using table(get_tab_ptf(2)) t2 on (1=1) 
when matched then update set t1.id = t2.id 
when not matched then insert values (9);

reset search_path;
drop schema plpgsql_table cascade;