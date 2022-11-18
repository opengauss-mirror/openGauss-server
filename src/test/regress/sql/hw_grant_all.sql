CREATE USER user1 with PASSWORD  'wang@123';
CREATE USER user2 with PASSWORD  'wang@123';
SET ROLE user1 PASSWORD 'wang@123';
--can not drop schema user2 in General user
DROP SCHEMA user2;
--can not drop user user2 in General user
DROP USER user2;
--can not grant user1 to sysadmin user
GRANT ALL PRIVILEGES TO user1;
RESET ROLE;
--grant user1 to sysadmin user
GRANT ALL PRIVILEGES TO user1;
SET ROLE user1 PASSWORD 'wang@123';
--success drop schema 
DROP SCHEMA user2;
--success drop user;
DROP USER user2;
RESET ROLE;
REVOKE ALL PRIVILEGES FROM user1;
DROP USER user1 ;

create user non_superuser password 'Gauss@123';
GRANT CREATE ON SCHEMA public TO non_superuser;
set role non_superuser password 'Gauss@123';
 
create or replace function myfunc(a varchar, b varchar) return varchar
as
begin
    return 'myfunc';
end;
/

create operator public.|| (procedure = non_superuser.myfunc, LEFTARG = varchar, RIGHTARG = varchar);
create aggregate public.myagg(varchar)(sfunc = non_superuser.myfunc, stype = varchar);

create operator non_superuser.|| (procedure = non_superuser.myfunc, LEFTARG = varchar, RIGHTARG = varchar);
alter operator non_superuser.|| (varchar,varchar) set schema public;
drop operator non_superuser.||(varchar,varchar);
  
reset role;
   
create operator public.|| (procedure = non_superuser.myfunc, LEFTARG = varchar, RIGHTARG = varchar);
create aggregate public.myagg(varchar)(sfunc = non_superuser.myfunc, stype = varchar);

drop operator public.||(varchar,varchar);
drop aggregate public.myagg(varchar);

REVOKE CREATE ON SCHEMA public FROM non_superuser;
DROP USER non_superuser CASCADE;
