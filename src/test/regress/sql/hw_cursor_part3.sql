---------------------------------------------------------------------------------
----- test the mixed use of implicit and explicit cursor attributes -------------
----- test the effect of the explicit cursor use to implicit cursor attributes --
---------------------------------------------------------------------------------
create database pl_test_cursor_part3 DBCOMPATIBILITY 'pg';
\c pl_test_cursor_part3;
CREATE schema hw_cursor_part3;
set current_schema = hw_cursor_part3; 
set behavior_compat_options = 'skip_insert_gs_source';
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp_select
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --select    
    select v1 into v from t1 where v1=1;    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then   
        raise notice '%','test select: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test select: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test select: sql%notfound=true';
    end if;    
    raise notice 'test select: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_select();
drop procedure sp_testsp_select;
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_insert
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --insert
    insert into t1 values (4,'abc4');    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test insert: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test insert: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test insert: sql%notfound=true';
    end if;    
    raise notice 'test insert: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_insert();
drop procedure sp_testsp_insert;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_update
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --update
    update t1 set v1=v1+100 where v1>1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test update: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test update: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test update: sql%notfound=true';
    end if;    
    raise notice 'test update: sql%%rowcount=%',sql%rowcount;
    
    update t1 set v1=v1+100 where v1<1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test update: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test update: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test update: sql%notfound=true';
    end if;    
    raise notice 'test update: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_update();
drop procedure sp_testsp_update;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_delete
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --delete
    delete from t1 where v1>1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test delete: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test delete: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test delete: sql%notfound=true';
    end if;    
    raise notice 'test delete: sql%%rowcount=%',sql%rowcount;
    
    delete from t1 where v1<1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
        raise notice '%','test delete: sql%isopen=false';
    end if;
    if sql%found then 
        raise notice '%','test delete: sql%found=true';
    end if;
    if sql%notfound then 
        raise notice '%','test delete: sql%notfound=true';
    end if;    
    raise notice 'test delete: sql%%rowcount=%',sql%rowcount;
    close cur;
end;
/
call sp_testsp_delete();

create table JOINT_DEBUG_CURSOR_FUNC_TABLE_012(C_INT integer,C_CHAR char(10))  ;

insert into JOINT_DEBUG_CURSOR_FUNC_TABLE_012 values(10,'hello');
insert into JOINT_DEBUG_CURSOR_FUNC_TABLE_012 values(10,'nihao');

create or replace procedure JOINT_DEBUG_CURSOR_FUNC_PROCEDURE_012
as
I_INDEX   INTEGER;
a   INTEGER;
I_NAME    char(10);
begin
  declare
 CURSOR aaaaabbbbb_cccccddddd_eeeeefffff_ggggghhhhh_iiiiijjjjj_kkkkklll(C_VARCHAR  varchar) IS  select C_INT from JOINT_DEBUG_CURSOR_FUNC_TABLE_012 where C_INT = 10 and C_CHAR = C_VARCHAR;

  begin
 OPEN  aaaaabbbbb_cccccddddd_eeeeefffff_ggggghhhhh_iiiiijjjjj_kkkkklll('hello');
    FETCH aaaaabbbbb_cccccddddd_eeeeefffff_ggggghhhhh_iiiiijjjjj_kkkkklll  INTO  I_INDEX;
   --%isopen
    if   aaaaabbbbb_cccccddddd_eeeeefffff_ggggghhhhh_iiiiijjjjj_kkkkklll%ISOPEN  then
    update JOINT_DEBUG_CURSOR_FUNC_TABLE_012 set C_INT =2 where C_CHAR ='nihao';
    end if;
    end ;
end;
/

call     JOINT_DEBUG_CURSOR_FUNC_PROCEDURE_012();


drop table if exists cursor_vl_tb10;
create unlogged table cursor_vl_tb10(c1 int,c2 varchar,c3 numeric,c4 date,c5 text);

create or replace function i_refcursor10_1 (a1 int) returns void as $$
declare
a int;
b varchar;
sql_srt varchar(100);
cursor c1 is  select c1,c2 from cursor_vl_tb10 order by 1,2;
begin 
sql_srt :='insert into cursor_vl_tb10(c1,c2) select :1,:1+1;';
while a1 > 0  loop
execute immediate sql_srt using IN a1;
a1:=a1-1;
end loop;

open c1;
loop
fetch c1 into a,b ;
EXIT WHEN c1%NOTFOUND;
raise notice '%,%,%',a,b,c1%rowcount;
end loop;
end;
$$ language plpgsql;

select * from i_refcursor10_1(11);
select * from i_refcursor10_1('');
select * from i_refcursor10_1(0);



create or replace function i_refcursor10_2 (a1 int) returns void as $$
declare
a int;
b varchar;
sql_srt varchar(100);
cursor c1 is  select c1,c2 from cursor_vl_tb10 order by 1,2;
begin 
sql_srt :='update  cursor_vl_tb10 set c2= :1+3 where c1 =:1;';
while a1 > 0  loop
execute immediate sql_srt using IN a1;
a1:=a1-1;
end loop;

open c1;
loop
fetch c1 into a,b ;
EXIT WHEN c1%NOTFOUND;
raise notice '%,%,%',a,b,c1%rowcount;
end loop;
end;
$$ language plpgsql;

select * from i_refcursor10_2(5);
select * from i_refcursor10_2('5');


create or replace function i_refcursor10_3 () returns void as $$
 DECLARE
 a int;
 b varchar;
 cursor c1 is  select c1,c2 from cursor_vl_tb10 order by 1,2;
 BEGIN
 EXECUTE IMMEDIATE 'select * from i_refcursor10_2(5.0);';

open c1;
loop
fetch c1 into a,b ;
EXIT WHEN c1%NOTFOUND;
raise notice '%,%,%',a,b,c1%rowcount;
end loop;
 END;
$$ language plpgsql;

call i_refcursor10_3();



create or replace function i_refcursor10_4 (a1 int) returns setof refcursor as $$
declare
a int;
b varchar;
sql_srt varchar(100);
cursor c1 is  select c1,c2 from cursor_vl_tb10 order by 1,2;
begin 
sql_srt :='delete from  cursor_vl_tb10 where c1 =:1;';
while a1 > 0  loop
execute immediate sql_srt using IN a1;
a1:=a1-1;
end loop;

open c1;
loop
fetch c1 into a,b ;
EXIT WHEN c1%NOTFOUND;
raise notice '%,%,%',a,b,c1%rowcount;
end loop;
end;
$$ language plpgsql;

create or replace function i_refcursor10_5 () returns void as $$
 DECLARE
 a int;
 b varchar;
 c1 sys_refcursor;
 BEGIN
 c1 := i_refcursor10_4(5.0);
 EXECUTE IMMEDIATE 'select * from i_refcursor10_4(5.0);';
 END;
$$ language plpgsql;

call i_refcursor10_5();



create or replace function i_refcursor10_6 () returns void as $$
 BEGIN
 EXECUTE IMMEDIATE 'truncate cursor_vl_tb10;';
 END;
$$ language plpgsql;

call i_refcursor10_6();


select * from cursor_vl_tb10;




create or replace function i_refcursor10_7 () returns void as $$
 BEGIN
 EXECUTE IMMEDIATE 'drop table cursor_vl_tb10;';
 END;
$$ language plpgsql;

call i_refcursor10_7();


select * from cursor_vl_tb10;

DROP SCHEMA hw_cursor_part3 CASCADE;
\c regression;
drop database IF EXISTS pl_test_cursor_part3;
