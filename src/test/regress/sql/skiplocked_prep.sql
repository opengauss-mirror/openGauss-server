
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