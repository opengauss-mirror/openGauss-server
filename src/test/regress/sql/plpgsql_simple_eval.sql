drop function if exists tg_syn;
CREATE FUNCTION tg_syn () RETURNS TRIGGER AS '
BEGIN
update des_tab set c1=new.c1 where c1=old.c1;
return NEW;
END;
' LANGUAGE plpgsql;

--update des_tab set c1=2 where c1=1;
--update des_tab set c1=3 where c1=2;
drop table if exists syn_tab;
drop table if exists des_tab;
create table syn_tab(a serial, c1 int, c2 char(100), c3 date);
create table des_tab(a serial, c1 int, c2 char(100), c3 date) with(orientation=column);

--create view syn_view as select * from syn_tab;
drop trigger if exists tg_test on syn_tab;
CREATE TRIGGER tg_test BEFORE UPDATE ON syn_tab
FOR EACH ROW WHEN (OLD.c1 IS DISTINCT FROM NEW.c1)
EXECUTE PROCEDURE tg_syn();

insert into syn_tab(c1,c2,c3) values(1,'test','2019-01-01');
insert into syn_tab(c1,c2,c3) values(2,'upda','2019-01-02');

insert into des_tab select * from syn_tab;
update syn_tab set c1=c1+1;
select c1 from syn_tab order by 1;
select c1 from des_tab order by 1;

drop trigger if exists tg_test on syn_tab;
drop table if exists syn_tab;
drop table if exists des_tab;
drop function if exists tg_syn;
