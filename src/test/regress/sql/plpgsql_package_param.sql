create or replace package pkg1 as 
	function func_add_sql(a integer, b integer) return integer immutable;
end pkg1;
create or replace package body pkg1 as data1 integer; 
	function func_add_sql(a integer, b integer) return integer stable as 
		begin select a+b into data1;
		return data1;
	end;
end pkg1;
/

create or replace package pkg1 as 
	function func_add_sql(a integer, b integer) return integer immutable;
end pkg1;
create or replace package body pkg1 as data1 integer; 
	function func_add_sql(a integer, b integer) return integer immutable as 
		begin select a+b into data1;
		return data1;
	end;
end pkg1;
/

drop package if exists pkg1;