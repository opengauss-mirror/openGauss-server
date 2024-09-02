--
-- VACUUM
--

CREATE TABLE vactst (i INT);
INSERT INTO vactst VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT * FROM vactst;
VACUUM FULL vactst;
UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
VACUUM (FULL) vactst;
DELETE FROM vactst;
SELECT * FROM vactst;

VACUUM (FULL, FREEZE) vactst;
VACUUM (ANALYZE, FULL) vactst;

CREATE TABLE vaccluster (i INT PRIMARY KEY);
ALTER TABLE vaccluster CLUSTER ON vaccluster_pkey;
CLUSTER vaccluster;

CREATE FUNCTION do_analyze() RETURNS VOID VOLATILE LANGUAGE SQL
	AS 'ANALYZE pg_am';
CREATE FUNCTION wrap_do_analyze(c INT) RETURNS INT IMMUTABLE LANGUAGE SQL
	AS 'SELECT $1 FROM do_analyze()';
CREATE INDEX ON vaccluster(wrap_do_analyze(i));
INSERT INTO vaccluster VALUES (1), (2);
ANALYZE vaccluster;

set xc_maintenance_mode = on;
VACUUM FULL pg_am;
VACUUM FULL pg_class;
VACUUM FULL pg_database;
set xc_maintenance_mode = off;
VACUUM FULL vaccluster;
VACUUM FULL vactst;

-- check behavior with duplicate column mentions
VACUUM ANALYZE vaccluster(i,i);
ANALYZE vaccluster(i,i);

DROP TABLE vaccluster;
DROP TABLE vactst;

-- test vacuum opt
set enable_vacuum_extreme_xmin=off; -- should error
set enable_vacuum_extreme_xmin=on; -- should error

alter system set enable_vacuum_extreme_xmin=on;
select pg_sleep(1);
show enable_vacuum_extreme_xmin;
drop table if exists vac_opt_t;
drop table if exists vac_opt_t2;
create table vac_opt_t(c1 int);
insert into vac_opt_t values(generate_series(1,10));

vacuum vac_opt_t;
insert into vac_opt_t values(generate_series(1,10));
vacuum analyze vac_opt_t;
delete from vac_opt_t where c1 < 5;
vacuum full vac_opt_t ;
select * from vac_opt_t order by c1;
update vac_opt_t set c1 = -1;
create table vac_opt_t2(c1 int, c2 varchar(20));
insert into vac_opt_t2 values(generate_series(1,1000), 'hello');
Delete from vac_opt_t2 where c1 in (8, 9, 10);

select * from vac_opt_t order by c1;
select * from vac_opt_t2 order by c1 limit 10;

alter system set enable_vacuum_extreme_xmin=off;
drop table vac_opt_t;
drop table vac_opt_t2;
