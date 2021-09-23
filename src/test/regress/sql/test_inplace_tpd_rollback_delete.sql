-- for tpd delete
-- test case: concurrency = 3 (tx_slots = 4)
create table z1 (c1 int) with (storage_type=USTORE);

insert into z1 values (1);
insert into z1 values (2);
insert into z1 values (3);
insert into z1 values (11);
insert into z1 values (12);
insert into z1 values (13);
insert into z1 values (21);
insert into z1 values (22);
insert into z1 values (23);

\parallel on 3

begin
	delete from z1 where c1=1;
	delete from z1 where c1=2;
	delete from z1 where c1=3;
	perform pg_sleep(3.1);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=11;
	delete from z1 where c1=12;
	delete from z1 where c1=13;
	perform pg_sleep(3.2);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=21;
	delete from z1 where c1=22;
	delete from z1 where c1=23;
	perform pg_sleep(3.3);
	RAISE EXCEPTION '';
end;
/

\parallel off

select * from z1 order by c1;			-- should be (rows: 1, 2, 3, 11, 12, 13, 21, 22, 23)

drop table z1;


-- test case: concurrency = 5 (tx_slots = 4)
create table z1 (c1 int) with (storage_type=USTORE);

insert into z1 values (1);
insert into z1 values (2);
insert into z1 values (3);
insert into z1 values (11);
insert into z1 values (12);
insert into z1 values (13);
insert into z1 values (21);
insert into z1 values (22);
insert into z1 values (23);
insert into z1 values (31);
insert into z1 values (32);
insert into z1 values (33);
insert into z1 values (41);
insert into z1 values (42);
insert into z1 values (43);

\parallel on 5

begin
	delete from z1 where c1=1;
	delete from z1 where c1=2;
	delete from z1 where c1=3;
	perform pg_sleep(3.1);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=11;
	delete from z1 where c1=12;
	delete from z1 where c1=13;
	perform pg_sleep(3.2);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=21;
	delete from z1 where c1=22;
	delete from z1 where c1=23;
	perform pg_sleep(3.3);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=31;
	delete from z1 where c1=32;
	delete from z1 where c1=33;
	perform pg_sleep(3.4);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=41;
	delete from z1 where c1=42;
	delete from z1 where c1=43;
	perform pg_sleep(3.5);
	RAISE EXCEPTION '';
end;
/

\parallel off

select * from z1 order by c1;			-- should be (rows: 1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43)

drop table z1;


-- test case: concurrency = 10 (tx_slots = 4)
create table z1 (c1 int) with (storage_type=USTORE);

insert into z1 values (1);
insert into z1 values (2);
insert into z1 values (3);
insert into z1 values (11);
insert into z1 values (12);
insert into z1 values (13);
insert into z1 values (21);
insert into z1 values (22);
insert into z1 values (23);
insert into z1 values (31);
insert into z1 values (32);
insert into z1 values (33);
insert into z1 values (41);
insert into z1 values (42);
insert into z1 values (43);
insert into z1 values (51);
insert into z1 values (52);
insert into z1 values (53);
insert into z1 values (61);
insert into z1 values (62);
insert into z1 values (63);
insert into z1 values (71);
insert into z1 values (72);
insert into z1 values (73);
insert into z1 values (9);
insert into z1 values (10);

\parallel on 10

begin
	delete from z1 where c1=1;
	delete from z1 where c1=2;
	delete from z1 where c1=3;
	perform pg_sleep(3.1);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=11;
	delete from z1 where c1=12;
	delete from z1 where c1=13;
	perform pg_sleep(3.2);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=21;
	delete from z1 where c1=22;
	delete from z1 where c1=23;
	perform pg_sleep(3.3);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=31;
	delete from z1 where c1=32;
	delete from z1 where c1=33;
	perform pg_sleep(3.4);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=41;
	delete from z1 where c1=42;
	delete from z1 where c1=43;
	perform pg_sleep(3.5);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=51;
	delete from z1 where c1=52;
	delete from z1 where c1=53;
	perform pg_sleep(3.6);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=61;
	delete from z1 where c1=62;
	delete from z1 where c1=63;
	perform pg_sleep(3.7);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=71;
	delete from z1 where c1=72;
	delete from z1 where c1=73;
	perform pg_sleep(3.8);
	RAISE EXCEPTION '';
end;
/

begin
	delete from z1 where c1=9;
	perform pg_sleep(3.9);
end;
/

begin
	delete from z1 where c1=10;
	perform pg_sleep(4.0);
end;
/

\parallel off

select * from z1 order by c1;			-- should be (rows: 1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53, 61, 62, 63, 71, 72, 73)

drop table z1;


