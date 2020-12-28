drop table if exists dfs_insert_append_mode;
create table dfs_insert_append_mode(a int, b int) tablespace hdfs_ts distribute by hash(a);

insert into dfs_insert_append_mode values(1,2);
update dfs_insert_append_mode set b=3;
delete dfs_insert_append_mode where b=4;

-- expecting an error
alter table dfs_insert_append_mode set (append_mode=read_only);

alter table dfs_insert_append_mode set (append_mode=read_only);

\d+ dfs_insert_append_mode

-- read only case
insert into dfs_insert_append_mode values(1,2);
update dfs_insert_append_mode set b=3;
delete dfs_insert_append_mode where b=4;

-- read only case
insert into dfs_insert_append_mode values(1,2);
update dfs_insert_append_mode set b=3;
delete dfs_insert_append_mode where b=4;

alter table dfs_insert_append_mode set (append_mode=off);
\d+ dfs_insert_append_mode

-- clean up
drop table dfs_insert_append_mode;
