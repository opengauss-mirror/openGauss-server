create schema create_sequence_if_not_exists;
set current_schema = create_sequence_if_not_exists;

create sequence seq start with 1;
create sequence seq start with 1;

create sequence if not exists seq start with 1;
drop sequence seq;
create sequence if not exists seq start with 1;
create sequence if not exists seq start with 2;
drop sequence seq;
drop schema create_sequence_if_not_exists;