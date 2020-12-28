create database "Db_1";
create database "1_db";
create database "3_db";
create database "$_db";
create database "f分x，。、}‘’{“'”《'";
drop database "Db_1";
-- quote identifier issue when sending 'CLEAN CONNECTION ...' before dropdb
create database "all";
drop database "all";

create user "f分x，。、}‘’{“'”《'" password 'Gauss@123';
create user "1_db" password 'Gauss@123';
create user "$_db" password 'Gauss@123';
create user "_Uname1" password 'Gauss@123';
create user "uname_1" password 'Gauss@123';
drop user "_Uname1";
drop user "uname_1";

create role "f分x，。、}‘’{“'”《'" password 'Gauss@123';
create role "1_db" password 'Gauss@123';
create role "$_db" password 'Gauss@123';
create role "_Uname1" password 'Gauss@123';
create role "UNAME_1" password 'Gauss@123';
drop role "_Uname1";
drop role "UNAME_1";
