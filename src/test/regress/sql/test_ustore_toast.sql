create table toasttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing') with (storage_type=USTORE);

begin;
insert into toasttest values(30, 50, repeat('x', 10000));
select col1, col2,  length(col3) from toasttest;
rollback;

select col1, col2,  length(col3) from toasttest;

insert into toasttest values(30, 50, repeat('x', 10000));

select col1, col2,  length(col3) from toasttest;

-- test update
begin;
update toasttest set col3 = col3 || col3;
select col1, col2,  length(col3) from toasttest;
rollback;

select col1, col2,  length(col3) from toasttest;

update toasttest set col3 = col3 || '123';
select col1, col2,  length(col3) from toasttest;

-- test delete
begin;
delete from toasttest where col1 = 30;
select col1, col2,  length(col3) from toasttest;
rollback;

select col1, col2,  length(col3) from toasttest;

delete from toasttest where col1 = 30;

select col1, col2,  length(col3) from toasttest;

drop table toasttest;


create table toasttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing', col4 text) with (storage_type=USTORE);

-- test insert
-- insert short row
insert into toasttest values(30, 50, repeat('x', 10), 'XXXXX');
select col1, col2, length(col3), col4 from toasttest;

-- insert toast row
insert into toasttest values(40, 60, repeat('y', 10000), 'ZZZZZZZZZZ');
select col1, col2, length(col3), col4 from toasttest where col1 = 40;

-- test update
-- update short row to toast row
update toasttest set col3 = repeat('x', 10000) where col1 = 30;
select col1, col2, length(col3), col4 from toasttest where col1 = 30;

-- update toast row
update toasttest set col3 = col3 ||col3 where col1 = 40;
select col1, col2, length(col3), col4 from toasttest where col1 = 40;

update toasttest set col4 = repeat('z', 10000) where col1 = 40;
select col1, col2, length(col3), length(col4) from toasttest where col1 = 40;

update toasttest set col4 = col4 || '123' where col1 = 40;
select col1, col2, length(col3), length(col4) from toasttest where col1 = 40;

-- insert toast row with 2 long fields
insert into toasttest values(70, 80, repeat('a', 10000), repeat('B', 10000));
select col1, col2, length(col3), length(col4) from toasttest where col1 = 70;

-- test delete
delete from toasttest where col1 = 30;
select col1, col2,  length(col3), length(col4) from toasttest order by col1;

insert into toasttest values(90, 100, 'cccccccccc', repeat('D', 10000));
select col1, col2, length(col3), length(col4) from toasttest where col1 = 90;

delete from toasttest where col1 = 40;
select col1, col2,  length(col3), length(col4) from toasttest order by col1;

delete from toasttest where col1 = 70;
select col1, col2,  length(col3), length(col4) from toasttest order by col1;

drop table toasttest;
