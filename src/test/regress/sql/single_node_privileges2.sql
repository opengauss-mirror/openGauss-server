-- test use trigger to promote authority
create user user01 password 'test-1234';
SET SESSION AUTHORIZATION user01 password 'test-1234';

create table user01.tbl01(a int);
-- function to promote authority
create or replace function user01.tg1() returns trigger as $$
declare
issysadmin boolean;
begin
  select rolsystemadmin into issysadmin from pg_roles where rolname = current_user;
  CASE issysadmin
  WHEN 1 THEN
    alter USER user01 with sysadmin; 
  else
  null;
  end case;
  return new;
end;
$$ language plpgsql security invoker; 

-- trigger for table
create trigger insert_tg1 
before insert on user01.tbl01
for each row
execute procedure user01.tg1();

grant all on user01.tbl01 to public;

-- try to trigger the trigger, should fail
insert into user01.tbl01 values(1);

RESET SESSION AUTHORIZATION;

-- use root user to trigger, should fail
insert into user01.tbl01 values(1);

-- cleanup
drop schema user01 cascade;
drop role user01;