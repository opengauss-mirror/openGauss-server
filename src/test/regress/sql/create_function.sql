create function create_function_test(integer,integer) RETURNS integer
AS 'SELECT $1 + $2;'
LANGUAGE SQL
IMMUTABLE SHIPPABLE
RETURNS NULL ON NULL INPUT;
create or replace procedure proc_commit
is
begin
commit;
end;
/
DROP SYNONYM proc_test;
create or replace procedure proc_test
IMMUTABLE
is
begin
proc_commit();
end;
/
CREATE OR REPLACE FUNCTION public.func_jbpm_createtime( i_businessid IN VARCHAR2 )
 RETURN timestamp without time zone NOT SHIPPABLE NOT FENCED
AS  DECLARE  v_tm TIMESTAMP ;
BEGIN
     BEGIN
          SELECT
                    t.start_time INTO v_tm
               FROM
                    dams_wf_process t
               WHERE
                    t.business_id = i_businessid ;

               EXCEPTION
                    WHEN no_data_found THEN
                    SELECT
                         t.start_time INTO v_tm
                    FROM
                         dams_wf_hist_process t
                    WHERE
                         t.business_id = i_businessid ;

     END ;

     RETURN v_tm ;

END ;
/

call proc_test();
create or replace procedure p1(a out varchar2,b int) is
begin
a:='aa'||b;
raise info 'a:%',a;
end;
/

declare
var varchar2;
begin
var=p1(:var,3);
raise info 'var:%',var;
end;
/

create or replace function test_create_function_ex_1()
returns text
language plpgsql
as $function$
declare
xx varchar(10) default null;
pp varchar(10) default null;
begin
     select into xx (case when pp IS NULL or pp = 'A' then 'A' else NULL end);
     return xx;
end;
$function$
;

select test_create_function_ex_1();

create or replace function test_create_function_ex_2()
returns text
language plpgsql
as $function$
declare
xx varchar(10) default null;
pp varchar(10) default null;
begin
     select into xx,xx (case when pp IS NULL or pp = 'A' then 'A' else NULL end), 1;
     return xx;
end;
$function$
;

select test_create_function_ex_2();

create or replace function test_create_function_ex_3(idname text)
returns text as
$body$
declare
t text[];
pgver text;
begin
     select into t array_agg(quote_ident(term)) from 
     (select unnest(string_to_array(idname, '.', '')) as term) as foo;
     return array_to_string(t, '.');
end;
$body$
language plpgsql immutable
;

select test_create_function_ex_3('111.222');

drop function test_create_function_ex_1;
drop function test_create_function_ex_2;
drop function test_create_function_ex_3;

create or replace procedure "p_'''_1"() is
begin
null;
end;
/
drop procedure "p_'''_1";
create or replace function "p_'''_1"() return void
as
begin
null;
end;
drop function "p_'''_1";
