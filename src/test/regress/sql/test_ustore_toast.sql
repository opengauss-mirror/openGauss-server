create table toasttest (col1 int4 primary key, col2 int4 NOT NULL, col3 text default 'testing', col4 text, b integer GENERATED ALWAYS AS (length(col3)) STORED) with (storage_type=USTORE);

-- test insert
-- insert short row
insert into toasttest values(30, 50, repeat('x', 10), 'XXXXX');
select col1, col2, length(col3), col4 from toasttest;

-- insert toast row
insert into toasttest values(40, 60, repeat('y', 1000000), 'ZZZZZZZZZZ');
begin;
insert into toasttest values(generate_series(0, 9), 60, repeat('y', 300000), 'ZZZZZZZZZZ');
insert into toasttest values(generate_series(10, 20), 50, repeat('x', 100), 'XXXXX');
end;
create index idx1 on toasttest(col2);
select col1, col2, length(col3), col4 from toasttest where col1 = 40;
select /*+ indexonlyscan(toasttest) */ sum(col1), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col1),count(*) from toasttest;
select /*+ indexonlyscan(toasttest) */ sum(col2), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col2),count(*) from toasttest;
select b, length(col3), ctid from toasttest where b != length(col3) ;

begin;
update toasttest set col3 = col3 || col3;
rollback;
select /*+ indexonlyscan(toasttest) */ sum(col1), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col1),count(*) from toasttest;
select /*+ indexonlyscan(toasttest) */ sum(col2), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col2),count(*) from toasttest;
select b, length(col3), ctid from toasttest where b != length(col3) ;

begin;
update toasttest set col3 = col3 || col3;
end;
select /*+ indexonlyscan(toasttest) */ sum(col1), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col1),count(*) from toasttest;
select /*+ indexonlyscan(toasttest) */ sum(col2), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col2),count(*) from toasttest;
select b, length(col3), ctid from toasttest where b != length(col3) ;

-- test update
-- update short row to toast row
update toasttest set col3 = repeat('x', 2000000) where col1 = 30;
select col1, col2, length(col3), col4 from toasttest where col1 = 30;

-- update toast row
update toasttest set col3 = col3 ||col3 where col1 = 40;
select col1, col2, length(col3), col4 from toasttest where col1 = 40;

update toasttest set col4 = repeat('z', 10000) where col1 = 40;
select col1, col2, length(col3), length(col4) from toasttest where col1 = 40;

update toasttest set col4 = col4 || '123' where col1 = 40;
select col1, col2, length(col3), length(col4) from toasttest where col1 = 40;

-- insert toast row with 2 long fields
insert into toasttest values(70, 80, repeat('a', 200000), repeat('B', 200000));
select col1, col2, length(col3), length(col4) from toasttest where col1 = 70;

-- test delete
delete from toasttest where col1 = 30;
select col1, col2,  length(col3), length(col4) from toasttest order by col1;

insert into toasttest values(90, 100, 'cccccccccc', repeat('D', 1000000));
select col1, col2, length(col3), length(col4) from toasttest where col1 = 90;

delete from toasttest where col1 = 40;
delete from toasttest where col1 = 70;
update toasttest set col3 = repeat('x', 2000000) where col1 = 30;
update toasttest set col3 = repeat('x', 2000) where col1 < 20;
select /*+ indexonlyscan(toasttest) */ sum(col1), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col1),count(*) from toasttest;
select /*+ indexonlyscan(toasttest) */ sum(col2), count(*) from toasttest union select /*+ tablescan(toasttest) */ sum(col2),count(*) from toasttest;
select b, length(col3), ctid from toasttest where b != length(col3) ;

drop table toasttest;