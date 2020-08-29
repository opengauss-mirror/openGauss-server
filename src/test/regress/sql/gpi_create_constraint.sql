
DROP TABLE if exists gpi_table1;
DROP TABLE if exists gpi_table2;
CREATE TABLE gpi_table1
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_gpi_table1 VALUES less than (10000),
    partition p1_gpi_table1 VALUES less than (20000),
    partition p2_gpi_table1 VALUES less than (30000),
    partition p3_gpi_table1 VALUES less than (maxvalue)
);
--ok
INSERT INTO gpi_table1 SELECT r, r, r FROM generate_series(0,10000) AS r;
--ok

CREATE INDEX ON gpi_table1 (c1) GLOBAL;
--error
CREATE INDEX idx1_gpi_table1 ON gpi_table1 (c1) GLOBAL;
--ok
CREATE INDEX idx2_gpi_table1 ON gpi_table1 (c1) LOCAL;
--error
CREATE INDEX idx3_gpi_table1 ON gpi_table1 (c2) LOCAL;
--ok
CREATE INDEX idx4_gpi_table1 ON gpi_table1 ((c1+10)) LOCAL;
--ok
CREATE INDEX idx5_gpi_table1 ON gpi_table1 (c1, c2) LOCAL;
--ok
CREATE INDEX idx6_gpi_table1 ON gpi_table1 (c1, c2) GLOBAL;
--error
CREATE INDEX idx7_gpi_table1 ON gpi_table1 ((c1+10)) GLOBAL;
--error
CREATE INDEX idx8_gpi_table1 ON gpi_table1 ((c1+10), c2) GLOBAL;
--error
CREATE INDEX idx9_gpi_table1 ON gpi_table1 USING hash (c1, c2) GLOBAL;
--error
CREATE INDEX idx10_gpi_table1 ON gpi_table1 (c1);
--ok
\d gpi_table1;


CREATE TABLE gpi_table2
(
    c1 int,
    c2 int,
    c3 int
) WITH (orientation = column)
partition by range (c1)
(
    partition p0_gpi_table2 VALUES less than (10000),
    partition p1_gpi_table2 VALUES less than (20000),
    partition p2_gpi_table2 VALUES less than (30000),
    partition p3_gpi_table2 VALUES less than (maxvalue)
);
--ok
CREATE INDEX idx1_gpi_table2 ON gpi_table2 USING btree (c1)  GLOBAL;
--error
CREATE INDEX idx1_gpi_table2 ON gpi_table2 (c1) GLOBAL;
--error
CREATE INDEX idx1_gpi_table2 ON gpi_table2 (c2) GLOBAL;

drop table gpi_table2;

CREATE TABLE gpi_table2
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_gpi_table2 VALUES less than (10000),
    partition p1_gpi_table2 VALUES less than (20000),
    partition p2_gpi_table2 VALUES less than (30000),
    partition p3_gpi_table2 VALUES less than (maxvalue)
);
--ok
CREATE INDEX idx1_gpi_table2 ON gpi_table2 USING btree (c1)  GLOBAL;
--error
CREATE INDEX idx1_gpi_table2 ON gpi_table2 (c1) GLOBAL;
--error
CREATE INDEX idx2_gpi_table2_local_c1 ON gpi_table2 (c1) LOCAL;
--ok
CREATE INDEX idx1_gpi_table2_global_c2 ON gpi_table2 (c2) GLOBAL;

\d gpi_table2
alter table gpi_table2 drop column c2;
\d gpi_table2

create table gpi_table2_inherits (c4 int) INHERITS (gpi_table2);

-- error
alter table gpi_table2 set (wait_clean_gpi=y);

drop table gpi_table2;