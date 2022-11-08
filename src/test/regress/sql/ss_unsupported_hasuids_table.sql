drop table if exists hasuids_standby_t1;
CREATE TABLE hasuids_standby_t1 (id int,num int) with (segment=on);
alter table hasuids_standby_t1 set (hasuids=on);

drop table if exists hasuids_standby_t1;
CREATE TABLE hasuids_standby_t1 (id int,num int) with (segment=on,hasuids=on);
drop table if exists hasuids_standby_t1;