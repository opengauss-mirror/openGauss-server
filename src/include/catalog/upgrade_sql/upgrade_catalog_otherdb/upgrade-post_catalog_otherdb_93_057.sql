SET search_path TO information_schema;

DROP TABLE IF EXISTS ENGINES;
CREATE TABLE ENGINES (
  ENGINE varchar(64) NOT NULL DEFAULT '',
  SUPPORT varchar(8) NOT NULL DEFAULT '',
  COMMENT varchar(80) NOT NULL DEFAULT '',
  TRANSACTIONS varchar(3) DEFAULT NULL,
  XA varchar(3) DEFAULT NULL,
  SAVEPOINTS varchar(3) DEFAULT NULL
) WITHOUT OIDS;

insert into ENGINES values ('MEMORY', 'YES', 'Hash based, stored in memory, useful for temporary tables', 'NO', 'NO', 'NO');
insert into ENGINES values ('InnoDB', 'DEFAULT', 'Supports transactions, row-level locking, and foreign keys', 'YES', 'YES', 'YES');
insert into ENGINES values ('PERFORMANCE_SCHEMA', 'YES', 'Performance Schema', 'NO', 'NO', 'NO');
insert into ENGINES values ('MyISAM', 'YES', 'MyISAM storage engine', 'NO', 'NO', 'NO');
insert into ENGINES values ('ndbinfo', 'NO', 'MySQL Cluster system information storage engine', NULL, NULL, NULL);
insert into ENGINES values ('MRG_MYISAM', 'YES', 'Collection of identical MyISAM tables', 'NO', 'NO', 'NO');
insert into ENGINES values ('BLACKHOLE', 'YES', '/dev/null storage engine (anything you write to it disappears)', 'NO', 'NO', 'NO');
insert into ENGINES values ('CSV', 'YES', 'CSV storage engine', 'NO', 'NO', 'NO');
insert into ENGINES values ('ARCHIVE', 'YES', 'Archive storage engine', 'NO', 'NO', 'NO');
insert into ENGINES values ('ndbcluster', 'NO', 'Clustered, fault-tolerant tables', NULL, NULL, NULL);

GRANT SELECT ON ENGINES TO PUBLIC;

RESET search_path;
