
CREATE SCHEMA gtt;

set search_path=gtt,sys;

create global temp table gtt1(a int primary key, b text) on commit delete rows;

create global temp table gtt2(a int primary key, b text) on commit delete rows;

create global temp table gtt3(a int primary key, b text);

create global temp table gtt_t_kenyon(id int,vname varchar(48),remark text) on commit PRESERVE rows;
create index idx_gtt_t_kenyon_1 on gtt_t_kenyon(id);
create index idx_gtt_t_kenyon_2 on gtt_t_kenyon(remark);

CREATE GLOBAL TEMPORARY TABLE gtt_with_seq(c1 bigint, c2 bigserial) on commit PRESERVE rows;

reset search_path;

