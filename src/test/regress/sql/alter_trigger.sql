create table trigtest(i serial primary key);
create function trigtestfunc() returns trigger as $$
    begin
        raise notice '% % % %', TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL;
        return new;
    end;$$ language plpgsql;
create trigger trigtest_b_row_tg before insert or update or delete on trigtest 
    for each row execute procedure trigtestfunc();
create trigger trigtest_a_row_tg after insert or update or delete on trigtest 
    for each row execute procedure trigtestfunc();

create table trigtest2 (i serial primary key);
create trigger trigtest_a_row_tg after insert or update or delete on trigtest2 
    for each row execute procedure trigtestfunc();
    
select tgname, tgtype, tgenabled from pg_trigger where tgname like 'trigtest%';

alter trigger trigtest_b_row_tg disable;
select tgname, tgtype, tgenabled from pg_trigger where tgname = 'trigtest_b_row_tg';
insert into trigtest values(1);

alter trigger trigtest_b_row_tg enable;
select tgname, tgtype, tgenabled from pg_trigger where tgname = 'trigtest_b_row_tg';
insert into trigtest values(2);

alter trigger trigtest_a_row_tg disable;
select tgname, tgtype, tgenabled from pg_trigger where tgname like 'trigtest%';

alter trigger trigtest_err disable;
alter trigger trigtest_err enable;

drop table trigtest cascade;
drop table trigtest2 cascade;