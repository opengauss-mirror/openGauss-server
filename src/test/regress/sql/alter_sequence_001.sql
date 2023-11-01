DROP SCHEMA IF EXISTS test_alter_seq_sche_01 CASCADE;

CREATE SCHEMA test_alter_seq_sche_01;

SET CURRENT_SCHEMA TO test_alter_seq_sche_01;

create sequence seqa;

select nextval('seqa');

alter sequence seqa increment 10;

select nextval('seqa');

select nextval('seqa');

alter sequence seqa restart 200;

select nextval('seqa');

select nextval('seqa');

alter sequence seqa cycle;

alter sequence seqa start 10;

alter sequence seqa maxvalue 1000;

alter sequence seqa minvalue 5;

select sequence_name , last_value , start_value , increment_by , max_value , min_value , cache_value , is_cycled , is_called from seqa;

DROP SCHEMA test_alter_seq_sche_01 CASCADE;
