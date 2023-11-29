--
--Create Foreign table in Normal mode.
--
create foreign table foreign_t1 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'text', location 'gsfs://127.0.0.1:12345/t1.data', delimiter '|');

create foreign table foreign_t2 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'csv', location 'gsfs://127.0.0.1:12345/t1.data', delimiter '|', mode 'normal');

--
--Create Foreign table in Shared mode 
--
create foreign table foreign_t3 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'text', location 'gsfs://127.0.0.1:12345/t1.data', delimiter '|', mode 'shared');

create foreign table foreign_t4 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'csv', location '/data/t1.data', delimiter '|', mode 'shared');

create foreign table foreign_t5 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'text', location '/data/t1.data', delimiter '|', mode 'shared');

--
--Create Foreign table in Private mode
--
create foreign table foreign_t6 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'text', location '/data/t1.data', delimiter '|', mode 'private');

create foreign table foreign_t7 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'csv', location '/data/t1.data', delimiter '|', mode 'private');

create foreign table foreign_t8 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'csv', location 'gsfs://127.0.0.1:12345/t1.data', delimiter '|', mode 'private');


-----
create foreign table foreign_t9 (
	c1 int,
	c2 varchar(17)
)server gsmpp_server options(format 'csv', location '/data/t1.data', delimiter '|', mode 'private', fill_missing_fields 'true');

drop foreign table foreign_t1;
drop foreign table foreign_t2;
drop foreign table foreign_t3;
drop foreign table foreign_t4;
drop foreign table foreign_t5;
drop foreign table foreign_t6;
drop foreign table foreign_t7;
drop foreign table foreign_t8;
drop foreign table foreign_t9;
