create role test_myrole001 with password "gauss@123";
create role test_myrole002 with sysadmin password "gauss@123";

set role test_myrole001  password "gauss@123";
alter role test_myrole002 SET maintenance_work_mem = 100000; 
alter role test_myrole002 rename to temp_myrole;
reset role;
drop role test_myrole001;
drop role test_myrole002;
