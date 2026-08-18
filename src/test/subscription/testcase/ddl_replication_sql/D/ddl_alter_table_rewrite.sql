set search_path=rewrite;
insert into t1_full values (4,'d');
alter table t1_full add column c timestamp default now() not null first;
alter table t1_full add column d timestamp;

alter table t1_full add column e int unique;
alter table t1_full alter column b set data type timestamp using now();

alter table t1_full rename to t1_repl_index;
alter table t1_repl_index add constraint t1_pkey_a primary key (a);
alter table t1_repl_index replica identity default;
alter table t1_repl_index add column f int auto_increment unique;
alter table t1_repl_index alter column b set data type timestamp using now();
alter table t1_repl_index add column e timestamp default now() not null;
alter table t1_repl_index alter column e set data type float using random();
alter table t1_repl_index add column h int default random();
alter table t1_repl_index alter column h set data type float;
update t1_repl_index set h=random();
alter table t1_repl_index add column g timestamp generated always as (b + '1 year');

create table t1 (a int, b timestamp without time zone);
alter table t1 alter column b set default now();
alter table t1 modify column b timestamp on update current_timestamp;
insert into t1 (a,b) values  (1,default), (2,default),(3,'1900-01-01 1:00:00');
alter table t1 replica identity full;
alter table t1 alter column b set data type timestamp using now() - a;
