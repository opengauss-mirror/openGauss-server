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

