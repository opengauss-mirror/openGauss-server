
create table IF NOT EXISTS skiplocked_t1(
    id int,
    info text,
    primary key (id)
);

insert into skiplocked_t1 values (1, 'one');
insert into skiplocked_t1 values (2, 'two');
insert into skiplocked_t1 values (3, 'three');
insert into skiplocked_t1 values (4, 'four');
insert into skiplocked_t1 values (5, 'five');

create table IF NOT EXISTS skiplocked_t2(
    id int,
    info text,
    primary key (id)
)with (STORAGE_TYPE=USTORE);
insert into skiplocked_t2 values (1, 'one');

create table IF NOT EXISTS skiplocked_t3(
    id int,
    info text,
    primary key (id)
)with (ORIENTATION=COLUMN);
insert into skiplocked_t3 values (1, 'one');

-- test skiplocked with inherited table
drop table if exists skiplocked_inherits_1,skiplocked_inherits_2;
create table skiplocked_inherits_1(
    id int unique,
    a1 jsonb check(a1!='{}')
);
CREATE TABLE skiplocked_inherits_2 (
    a2 jsonb default '{"name": "John", "age": 30}',
    a3 jsonb not null
) INHERITS (skiplocked_inherits_1);
insert into skiplocked_inherits_2 values(1,'{"name":"test1"}','{"id":1001}','[null,"aaa"]');
insert into skiplocked_inherits_2 values(2,'{"name":"test2"}',default,'["true"]');
insert into skiplocked_inherits_2 values(3,'{"name":"test3"}','{"id":1003}','["a", {"b":1,"name": "John", "age": 30}]');
insert into skiplocked_inherits_2 values(4,'{"name":"test"}',default,'["null","T"]');
select * from skiplocked_inherits_1 order by id;
select * from skiplocked_inherits_2 order by id;
