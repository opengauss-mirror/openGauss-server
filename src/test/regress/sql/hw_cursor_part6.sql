CREATE schema hw_cursor_part6;
set current_schema = hw_cursor_part6;
set behavior_compat_options = 'skip_insert_gs_source';
create table company(name varchar(100), loc varchar(100), no integer);

insert into company values ('macrosoft',    'usa',          001);
insert into company values ('oracle',       'usa',          002);
insert into company values ('backberry',    'canada',       003);
insert into company values ('sumsung',      'south korea',  004);
insert into company values ('tencent',      'china',        005);
insert into company values ('ibm',          'usa',          006);
insert into company values ('nokia',        'finland',      007);
insert into company values ('apple',        'usa',          008);
insert into company values ('sony',         'japan',        009);
insert into company values ('baidu',        'china',        010);
insert into company values ('kingsoft',     'china',        011);

--test explicit cursor without args
create or replace procedure test_cursor_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        raise notice '%','c1_all%isopen=false: c1_all closed';
        open c1_all;
        raise notice '%','c1_all opened';
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        exit when c1_all%notfound;
        raise notice '% : % : %',company_name,company_loc,company_no;
    end loop;
    if c1_all%isopen then
        raise notice '%','c1_all%isopen=true: c1_all opened';
        close c1_all;
        raise notice '%','c1_all closed';
    end if;
end;
/
call test_cursor_1();
drop procedure test_cursor_1;

--test explicit cursor with args
create or replace procedure test_cursor_2 
as 
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c2_no_range(no_1 integer, no_2 integer) is --cursor with args 
        select name, loc, no from company where no >=no_1 and no <= no_2 order by 1, 2, 3;
begin 
    if not c2_no_range%isopen then
        raise notice '%','c2_no_range%isopen=false: c2_no_range closed';
        open c2_no_range(5,10);
        raise notice '%','c2_no_range opened';
    end if;
    loop
        fetch c2_no_range into company_name, company_loc, company_no;
        exit when c2_no_range%notfound;
        raise notice '% : % : %',company_name,company_loc,company_no;
    end loop;
    if c2_no_range%isopen then
        raise notice '%','c2_no_range%isopen=true: c2_no_range opened';
        close c2_no_range;
        raise notice '%','c2_no_range closed';
    end if;
end;
/
call test_cursor_2();
drop procedure test_cursor_2;

--test explicit cursor attributes
create or replace procedure test_cursor_3
as
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        raise notice '%','c1_all%isopen=false: c1_all closed';
        open c1_all;
        raise notice '%','c1_all opened';
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        if c1_all%notfound then
            raise notice '%','c1_all%notfound=true: no rows selected';
            exit;
        end if;
        if c1_all%found then
            raise notice 'rows_% : % % %',c1_all%rowcount,company_name,company_loc,company_no;
        end if;
    end loop;
    if c1_all%isopen then
        raise notice '%','c1_all%isopen=true: c1_all opened';
        close c1_all;
        raise notice '%','c1_all closed';
    end if;
end;
/
call test_cursor_3();
drop procedure test_cursor_3;

--test implicit cursor attributes: (sql%)%found,%notfound,%isopen,%rowcount
create or replace procedure test_cursor_4
as
begin 
    delete from company where loc='china';
    if sql%isopen then --sql%isopen always false, as closed after the execution of sql.
        raise notice '%','sql%isopen=true: implicit opened!';
    end if;
    if not sql%isopen then 
        raise notice '%','sql%isopen=false: implicit closed!';
    end if;
    if sql%found then
        raise notice 'sql%%found=true:% rows',sql%rowcount;
    end if;
    if sql%notfound then
        raise notice '%','sql%found=false: no rows found!';
    end if;
end;
/
call test_cursor_4();
drop procedure test_cursor_4;

--test dynamic cursor: (weak type)without return
create or replace procedure test_cursor_5()
as
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    type ref_cur_type is ref cursor; --declare cursor type
        my_cur ref_cur_type; --declare cursor var
    sqlstr varchar(100);
begin 
    sqlstr := 'select name,loc,no from company where loc=:1 order by 1, 2, 3';
    open my_cur for 'select name,loc,no from company order by 1, 2, 3';
    fetch my_cur into company_name,company_loc,company_no;
    while my_cur%found loop
        raise notice '%:%:%',company_name,company_loc,company_no;
        fetch my_cur into company_name,company_loc,company_no;
    end loop;
    close my_cur;
end;
/
call test_cursor_5();
drop procedure test_cursor_5;

----test more than one cursors access	
create or replace procedure test_cursor_6
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;		
    cursor c2_no_range(no_1 integer, no_2 integer) is --cursor with args 
        select name, loc, no from company where no >=no_1 and no <= no_2 order by 1, 2, 3;
begin 
    open c1_all; 
    open c2_no_range(50,100); --result null 	
    fetch c1_all into company_name, company_loc, company_no;
    fetch c2_no_range into company_name, company_loc, company_no;
    if c1_all%found then
        raise notice '%','c1_all%found=true';
    end if;
    if c1_all%notfound then
        raise notice '%','c1_all%notfound=true';
    end if;
    if c2_no_range%found then
        raise notice '%','c2_no_range%found=true';
    end if;
    if c2_no_range%notfound then
        raise notice '%','c2_no_range%notfound=true';
    end if;
    raise notice 'c1_all%%rowcount=% rows',c1_all%rowcount;
    raise notice 'c2_no_range%%rowcount=% rows',c2_no_range%rowcount;
end;
/
call test_cursor_6();
drop procedure test_cursor_6;

drop table company;

create table tbl (id int);
insert into tbl values  (1);
insert into tbl values  (2);
insert into tbl values  (3);
insert into tbl values  (4);

create or replace procedure sp_testsp
as
    temp1       integer;
    temp2       integer;
    sql_str     varchar2(200);
begin
    declare
        type crs_recode_type is ref cursor;
        c1 crs_recode_type;
    begin
        temp1   := 4;
        temp2   := 0;
        sql_str := 'select id from tbl where id < :a and id > :b order by 1';
        open c1 for sql_str using in temp1, in temp2;
        loop
            fetch c1 into temp1;
            raise notice '%',temp1;
            raise notice '%',temp1;
            exit when c1%notfound; 
        end loop;
        close c1;
--test implicit cursor rowcount attribute
        select id into temp1 from tbl where id=2 order by 1;
        raise notice '%',sql%rowcount;
        update tbl set id=100 where id<3;
        raise notice '%',sql%rowcount;
        insert into tbl select * from tbl;
        raise notice '%',sql%rowcount;
        delete from tbl;
        raise notice '%',sql%rowcount;
    end;  
end;
/

call sp_testsp();

DROP schema hw_cursor_part6 CASCADE;
