create table table_function_export_def_base (
	id integer primary key,
	name varchar(100)
);
create table table_function_export_def (
	id integer primary key,
	fid integer,
	constraint table_export_base_fkey foreign key (fid) references table_function_export_def_base(id)
);
select * from pg_get_tabledef('table_function_export_def');
