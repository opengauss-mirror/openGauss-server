create table t1 (c1 int) with (storage_type=USTORE);
insert into t1 values (1);

\parallel on 2

begin
	update t1 set c1 = 2;
	perform pg_sleep(1.5);
end;
/

begin
	perform pg_sleep(1);
	delete from t1;
end;
/

\parallel off

select * from t1;

drop table t1;
