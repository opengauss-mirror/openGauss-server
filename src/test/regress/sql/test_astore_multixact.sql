drop table if exists astore_mult1;
drop table if exists astore_mult2;
create table astore_mult1 (a int primary key, b int);
create table astore_mult2 (c int , d int);
alter table astore_mult2 add foreign key (c) references astore_mult1 (a);
insert into astore_mult1 values (1, 1);
insert into astore_mult2 values (1, 1);

\parallel on 2
begin
PERFORM * from astore_mult1 where a = 1 for key share; 
perform pg_sleep(2);
end;
/

begin
perform pg_sleep(0.5);
PERFORM * from astore_mult1 where a = 1 for no key update;

end;
/
\parallel off


\parallel on 2
begin
PERFORM * from astore_mult1 where a = 1 for key share; 
perform pg_sleep(2);
end;
/

begin
perform pg_sleep(0.5);
PERFORM * from astore_mult1 where a = 1 for key share;
end;
/
\parallel off

\parallel on 2
begin
PERFORM * from astore_mult1 where a = 1 for key share; 
perform pg_sleep(2);
end;
/

begin
perform pg_sleep(0.5);
PERFORM * from astore_mult1 where a = 1 for share;
end;
/
\parallel off

\parallel on 2
begin
PERFORM * from astore_mult1 where a = 1 for share; 
perform pg_sleep(2);
end;
/

begin
perform pg_sleep(0.5);
PERFORM * from astore_mult1 where a = 1 for share;
end;
/
\parallel off

\parallel on 2
begin
update astore_mult1 set b = 2 where a = 1;
perform pg_sleep(3);
end;
/

begin
update astore_mult2 set d = 2 where c = 1;
update astore_mult2 set d = 2 where c = 1;
end;
/
\parallel off

\parallel on 2
begin
update astore_mult1 set b = 2 where a = 1;
perform pg_sleep(3);
end;
/

begin
PERFORM * from astore_mult1 where a = 1 for key share;
end;
/
\parallel off

insert into astore_mult1 values (2, 2);
\parallel on 2
begin
perform * from astore_mult1 where a = 2 for key share;
perform pg_sleep(2);
delete from astore_mult1 where a = 2;
end;
/
begin
update astore_mult1 set b = 2 where a = 2;
perform pg_sleep(3);
end;
/
\parallel off

vacuum freeze astore_mult1;
vacuum freeze astore_mult2;

drop table astore_mult2;
drop table astore_mult1;