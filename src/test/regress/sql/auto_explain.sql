set enable_auto_explain = false;
create or replace function data_table  returns int as $$
begin
drop table if exists course;
drop table if exists stu;
drop table if exists teacher;
create table course(cno int,name varchar);
insert into course values(1,'test1');
insert into course values(2,'test2');
insert into course values(3,'test2');
create table stu(sno int, name varchar,sex varchar,cno int);
insert into stu values(1,'zhang','M',1);
insert into stu values(1,'zhang','M',2);
insert into stu values(2,'wangwei','M',2);
insert into stu values(3,'liu','F',3);
create table teacher(tno int,name varchar,sex varchar,cno int);
insert into teacher values(1,'Yang','F',1);
insert into teacher values(2,'zhang','F',2);
insert into teacher values(3,'liu','F',3);
return 1;
end;
$$
LANGUAGE plpgsql;

select data_table();

CREATE OR REPLACE FUNCTION course_delete_trigger()
RETURNS TRIGGER AS $$
BEGIN
   DELETE FROM teacher where teacher.cno = OLD.cno;
    RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER delete_trigger 
    AFTER DELETE ON course
    FOR EACH ROW EXECUTE PROCEDURE course_delete_trigger();
    
CREATE OR REPLACE FUNCTION courseUpdate()
  RETURNS trigger AS $$
   BEGIN
      UPDATE teacher SET teacher.cno = NEW.cno  where teacher.cno = NEW.cno;
      UPDATE student set student.cno = NEW.cno  where student.cno = NEW.cno;
   END;
$$
LANGUAGE plpgsql VOLATILE;

CREATE TRIGGER course_Update AFTER UPDATE OF "cno" ON "public"."course"
FOR EACH ROW
EXECUTE PROCEDURE "public".courseUpdate();

create or replace function process_test() returns int as $$
declare status  int;
begin
select complicate_process() into status;
return status;
END
$$
LANGUAGE plpgsql;

prepare get_stu_lesson(varchar) as select stu.name,course.name from stu left  join course on course.cno = stu.cno where stu.name = $1;
execute get_stu_lesson('liu');
prepare get_stu_info(varchar)  as select stu.name,course.name,teacher.name from stu left  join course on course.cno =stu.cno left join teacher on course.cno = teacher.cno where stu.name = $1;

set auto_explain_level = notice;
set enable_auto_explain = true;
execute get_stu_info('');
set enable_auto_explain = false;

create or replace function open_cursor(myCursor OUT REFCURSOR) as $$
begin
open myCursor for select teacher.name,stu.name from teacher left join course on course.cno = teacher.cno left join stu on stu.cno = course.cno;
END
$$
LANGUAGE plpgsql;

create or replace function complicate_process(status out int)  as $$
declare sql  varchar;
numbers int;
declare docType varchar:='REISSUE';
declare v_count1 int;
declare v_count2 int;
declare tt  REFCURSOR;
declare teacher_name varchar;
declare stu_name varchar;
begin
status:=0;
if docType = 'REISSUE' then
     select count(1) into v_count1 from stu;
     select count(2) into v_count2 from teacher;
     if v_count1>0 and v_count2>0 then
         insert into stu values(4,'liu','F',1);
         insert into teacher values(4,'li',4);
     end if;
end if;
update teacher set tno =100 where tno = 3;
select open_cursor() into tt;
fetch next from tt into teacher_name,stu_name;
While true
loop  
     fetch next from tt into teacher_name,stu_name;  
     if found then 
     else
     Exit ;
     end if;
end loop;
status:=1;

END
$$
LANGUAGE plpgsql;

set auto_explain_level = notice;
set enable_auto_explain = true;
select process_test();
set enable_auto_explain = false;
drop table if exists test1;
create table test1(id number, val number);
insert into test1 values(generate_series(1,1000), generate_series(1,1000));
create  OR REPLACE function test_merge_into() returns int as $$
declare tt  REFCURSOR;
id_val int;
begin
id_val:=103;
merge into test1 t1 using (select count(*) cnt from test1 where id = id_val) t2 on (cnt <> 0)
when matched then update set val = val + 1 where id = id_val when not matched then insert values(id_val, 1);
return 1;
end;
$$
LANGUAGE plpgsql;
set enable_auto_explain = true;
set auto_explain_level = notice;
select test_merge_into();
set enable_auto_explain = false;
drop table if exists course;
drop table if exists stu;
drop table if exists teacher;
drop table if exists test1;
