--test duplicate keys can be created successfully
create table test_index(id int);
create index index_01 on test_index(id,id,id);
create index index_01 on test_index(id);

drop index if exists index_01;
create index index_01 on test_index using gin(to_tsvector('english', id),
	to_tsvector('english', id));
create index index_01 on test_index using gin(to_tsvector('english', id));


drop index if exists index_01;
drop table if exists test_index;
create table test_index(id int) with (orientation=column);
create index index_01 on test_index using btree(id,id,id);
create index index_01 on test_index using btree(id);

drop index if exists index_01;
create index index_01 on test_index using psort(id,id,id);
create index index_01 on test_index using psort(id);

drop index if exists index_01;
create index index_01 on test_index using gin(to_tsvector('english', id),
	to_tsvector('english', id),to_tsvector('english', id));
create index index_01 on test_index using gin(to_tsvector('english', id));

--test whether duplicate expression can be created successfully
drop table if exists test_index;
create table test_index(id int, id_1 int);

drop index if exists index_01;
create index index_01 on test_index using btree(sin(id), sin(id));
create index index_01 on test_index using btree(sin(id), sin(id_1));
drop index if exists index_01;
create index index_01 on test_index using btree(sin(id), cos(id));

drop index if exists index_01;
create index index_01 on test_index using gin(to_tsvector('english', sin(id)),
	to_tsvector('english', sin(id)));
drop index if exists index_01;
create index index_01 on test_index using gin(to_tsvector('english', sin(id)),
	to_tsvector('english', sin(id_1)));
drop index if exists index_01;
create index index_01 on test_index using gin(to_tsvector('english', sin(id)),
	to_tsvector('english', cos(id)));
