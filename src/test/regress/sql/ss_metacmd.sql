DROP VIEW IF EXISTS SS_VIEW1 CASCADE;
DROP TABLE IF EXISTS SS_METACMD_TAB1 CASCADE;
CREATE TABLE SS_METACMD_TAB1 (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(128)) WITH(SEGMENT = ON);

CREATE VIEW SS_VIEW1 AS
    SELECT * from SS_METACMD_TAB1;
    
INSERT INTO SS_METACMD_TAB1 VALUES (1, 'PAIR');
COMMIT;

\dS+ SS_METACMD_TAB1
\dS+ SS_VIEW1
select pg_catalog.pg_table_size('SS_VIEW1');
select pg_catalog.pg_table_size('SS_METACMD_TAB1');

select gs_xlogdump_xid('171794');
select gs_xlogdump_lsn('0/70230830', '0/7023AB80');
select gs_xlogdump_tablepath('+data/base/15114/4600', 0, 'heap');
select gs_xlogdump_parsepage_tablepath('+data/base/15114/4600', 0, 'heap', false);

drop TABLE if exists ss_range_range_ddl_001;
CREATE TABLE ss_range_range_ddl_001
(
    col_1 int primary key USING INDEX TABLESPACE startend_tbs4, -- expected error
    col_2 bigint NOT NULL ,
    col_3 VARCHAR2 ( 30 ) NOT NULL ,
    col_4 int generated always as(2*col_2) stored ,
    col_5 bigint,
    col_6 bool,
    col_7 text,
    col_8 decimal,
    col_9 numeric(12,6),
    col_10 date,
    check (col_4 >= col_2)
)
with(FILLFACTOR=80,segment=on)
PARTITION BY range (col_1) SUBPARTITION BY range (col_2)
(
    PARTITION p_range_1 values less than (-10 )
    (
        SUBPARTITION p_range_1_1 values less than ( 0),
        SUBPARTITION p_range_1_2 values less than ( MAXVALUE )
    )
) ENABLE ROW MOVEMENT;

drop TABLE if exists ss_range_range_ddl_001;
DROP VIEW IF EXISTS SS_VIEW1 CASCADE;
DROP TABLE IF EXISTS SS_METACMD_TAB1 CASCADE;