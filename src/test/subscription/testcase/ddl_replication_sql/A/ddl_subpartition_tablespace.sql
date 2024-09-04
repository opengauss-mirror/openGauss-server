--DROP SCHEMA hw_subpartition_tablespace CASCADE;
CREATE SCHEMA hw_subpartition_tablespace;
SET CURRENT_SCHEMA TO hw_subpartition_tablespace;

--
----test create subpartition with tablespace----
--
--range-range
CREATE TABLE t_range_range1(c1 int, c2 int, c3 int)
PARTITION BY RANGE (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 VALUES LESS THAN (15)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 VALUES LESS THAN (15)
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15)
    (
        SUBPARTITION P_RANGE3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 VALUES LESS THAN (15)
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20) TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_RANGE5 VALUES LESS THAN (25)
);
SELECT pg_get_tabledef('t_range_range1');
-- DROP TABLEt_range_range1;

CREATE TABLE t_range_range2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY RANGE (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE1_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE2_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_RANGE3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE3_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20)
    (
        SUBPARTITION P_RANGE4_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE4_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE4_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE4_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE5 VALUES LESS THAN (25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_RANGE6 VALUES LESS THAN (30)
);
SELECT pg_get_tabledef('t_range_range2');
-- DROP TABLEt_range_range2;

--range-list
CREATE TABLE t_range_list1(c1 int, c2 int, c3 int)
PARTITION BY RANGE (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15)
    (
        SUBPARTITION P_RANGE3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20) TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_RANGE5 VALUES LESS THAN (25)
);
SELECT pg_get_tabledef('t_range_list1');
-- DROP TABLEt_range_list1;

CREATE TABLE t_range_list2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY RANGE (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE1_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE2_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_RANGE3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE3_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20)
    (
        SUBPARTITION P_RANGE4_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE4_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE4_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE4_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_RANGE5 VALUES LESS THAN (25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_RANGE6 VALUES LESS THAN (30)
);
SELECT pg_get_tabledef('t_range_list2');
-- DROP TABLEt_range_list2;

--range-hash
CREATE TABLE t_range_hash1(c1 int, c2 int, c3 int)
PARTITION BY RANGE (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15)
    (
        SUBPARTITION P_RANGE3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20) TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_RANGE5 VALUES LESS THAN (25)
);
SELECT pg_get_tabledef('t_range_hash1');
-- DROP TABLEt_range_hash1;

CREATE TABLE t_range_hash2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY RANGE (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE1_4
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE2_4
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_RANGE3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE3_4
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20)
    (
        SUBPARTITION P_RANGE4_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE4_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE4_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE4_4
    ),
    PARTITION P_RANGE5 VALUES LESS THAN (25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_RANGE6 VALUES LESS THAN (30)
);
SELECT pg_get_tabledef('t_range_hash2');
-- DROP TABLEt_range_hash2;

--list-range
CREATE TABLE t_list_range1(c1 int, c2 int, c3 int)
PARTITION BY LIST (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 VALUES LESS THAN (15)
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 VALUES LESS THAN (15)
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15)
    (
        SUBPARTITION P_LIST3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 VALUES LESS THAN (15)
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20) TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_LIST5 VALUES (21,22,23,24,25)
);
SELECT pg_get_tabledef('t_list_range1');
-- DROP TABLEt_list_range1;

CREATE TABLE t_list_range2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY LIST (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST1_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST2_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_LIST3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST3_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20)
    (
        SUBPARTITION P_LIST4_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST4_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST4_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST4_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST5 VALUES (21,22,23,24,25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_LIST6 VALUES (26,27,28,29,30)
);
SELECT pg_get_tabledef('t_list_range2');
-- DROP TABLEt_list_range2;

--list-list
CREATE TABLE t_list_list1(c1 int, c2 int, c3 int)
PARTITION BY LIST (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15)
    (
        SUBPARTITION P_LIST3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20) TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_LIST5 VALUES (21,22,23,24,25)
);
SELECT pg_get_tabledef('t_list_list1');
-- DROP TABLEt_list_list1;

CREATE TABLE t_list_list2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY LIST (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST1_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST2_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_LIST3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST3_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20)
    (
        SUBPARTITION P_LIST4_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST4_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST4_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST4_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_LIST5 VALUES (21,22,23,24,25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_LIST6 VALUES (26,27,28,29,30)
);
SELECT pg_get_tabledef('t_list_list2');
-- DROP TABLEt_list_list2;

--list-hash
CREATE TABLE t_list_hash1(c1 int, c2 int, c3 int)
PARTITION BY LIST (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15)
    (
        SUBPARTITION P_LIST3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20) TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_LIST5 VALUES (21,22,23,24,25)
);
SELECT pg_get_tabledef('t_list_hash1');
-- DROP TABLEt_list_hash1;

CREATE TABLE t_list_hash2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY LIST (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST1_4
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST2_4
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_LIST3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST3_4
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20)
    (
        SUBPARTITION P_LIST4_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST4_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST4_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST4_4
    ),
    PARTITION P_LIST5 VALUES (21,22,23,24,25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_LIST6 VALUES (26,27,28,29,30)
);
SELECT pg_get_tabledef('t_list_hash2');
-- DROP TABLEt_list_hash2;

--hash-range
CREATE TABLE t_hash_range1(c1 int, c2 int, c3 int)
PARTITION BY HASH (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3 VALUES LESS THAN (15)
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3 VALUES LESS THAN (15)
    ),
    PARTITION P_HASH3
    (
        SUBPARTITION P_HASH3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3 VALUES LESS THAN (15)
    ),
    PARTITION P_HASH4 TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_HASH5
);
SELECT pg_get_tabledef('t_hash_range1');
-- DROP TABLEt_hash_range1;

CREATE TABLE t_hash_range2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY HASH (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH1_4 VALUES LESS THAN (20)
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH2_4 VALUES LESS THAN (20)
    ),
    PARTITION P_HASH3 TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_HASH3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH3_4 VALUES LESS THAN (20)
    ),
    PARTITION P_HASH4
    (
        SUBPARTITION P_HASH4_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH4_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH4_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH4_4 VALUES LESS THAN (20)
    ),
    PARTITION P_HASH5 TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_HASH6
);
SELECT pg_get_tabledef('t_hash_range2');
-- DROP TABLEt_hash_range2;

--hash-list
CREATE TABLE t_hash_list1(c1 int, c2 int, c3 int)
PARTITION BY HASH (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_HASH3
    (
        SUBPARTITION P_HASH3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_HASH4 TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_HASH5
);
SELECT pg_get_tabledef('t_hash_list1');
-- DROP TABLEt_hash_list1;

CREATE TABLE t_hash_list2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY HASH (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH1_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH2_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH3 TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_HASH3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH3_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH4
    (
        SUBPARTITION P_HASH4_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH4_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH4_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH4_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH5 TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_HASH6
);
SELECT pg_get_tabledef('t_hash_list2');
-- DROP TABLEt_hash_list2;

--hash-hash
CREATE TABLE t_hash_hash1(c1 int, c2 int, c3 int)
PARTITION BY HASH (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3
    ),
    PARTITION P_HASH3
    (
        SUBPARTITION P_HASH3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3
    ),
    PARTITION P_HASH4 TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_HASH5
);
SELECT pg_get_tabledef('t_hash_hash1');
-- DROP TABLEt_hash_hash1;

CREATE TABLE t_hash_hash2(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY HASH (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH1_4
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH2_4
    ),
    PARTITION P_HASH3 TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_HASH3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH3_4
    ),
    PARTITION P_HASH4
    (
        SUBPARTITION P_HASH4_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH4_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH4_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH4_4
    ),
    PARTITION P_HASH5 TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_HASH6
);
SELECT pg_get_tabledef('t_hash_hash2');
-- DROP TABLEt_hash_hash2;

--
----test add partition with tablespace----
--
--since the add subpartition define use the same code, we only test different partition type: range/list
--range-list
CREATE TABLE t_range_list3(c1 int, c2 int, c3 int)
PARTITION BY RANGE (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 VALUES (11,12,13,14,15)
    )
);
ALTER TABLE t_range_list3 ADD PARTITION P_RANGE3 VALUES LESS THAN (15)
    (
        SUBPARTITION P_RANGE3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 VALUES (11,12,13,14,15)
    );
ALTER TABLE t_range_list3 ADD PARTITION P_RANGE4 VALUES LESS THAN (20) TABLESPACE hw_subpartition_tablespace_ts1;
ALTER TABLE t_range_list3 ADD PARTITION P_RANGE5 VALUES LESS THAN (25);
SELECT pg_get_tabledef('t_range_list3');
-- DROP TABLEt_range_list3;


CREATE TABLE t_range_list4(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY RANGE (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE1_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE2_4 VALUES (16,17,18,19,20)
    )
);
ALTER TABLE t_range_list4 ADD PARTITION P_RANGE3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_RANGE3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE3_4 VALUES (16,17,18,19,20)
    );
ALTER TABLE t_range_list4 ADD PARTITION P_RANGE4 VALUES LESS THAN (20)
    (
        SUBPARTITION P_RANGE4_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE4_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE4_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE4_4 VALUES (16,17,18,19,20)
    );
ALTER TABLE t_range_list4 ADD PARTITION P_RANGE5 VALUES LESS THAN (25) TABLESPACE hw_subpartition_tablespace_ts3;
ALTER TABLE t_range_list4 ADD PARTITION P_RANGE6 VALUES LESS THAN (30);
SELECT pg_get_tabledef('t_range_list4');
-- DROP TABLEt_range_list4;

--list-hash
CREATE TABLE t_list_hash3(c1 int, c2 int, c3 int)
PARTITION BY LIST (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3
    )
);

ALTER TABLE t_list_hash3 ADD PARTITION P_LIST3 VALUES (11,12,13,14,15)
    (
        SUBPARTITION P_LIST3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3
    );
ALTER TABLE t_list_hash3 ADD PARTITION P_LIST4 VALUES (16,17,18,19,20) TABLESPACE hw_subpartition_tablespace_ts1;
ALTER TABLE t_list_hash3 ADD PARTITION P_LIST5 VALUES (21,22,23,24,25);
SELECT pg_get_tabledef('t_list_hash3');
-- DROP TABLEt_list_hash3;

CREATE TABLE t_list_hash4(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY LIST (c1) SUBPARTITION BY HASH (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST1_4
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST2_4
    )
);
ALTER TABLE t_list_hash4 ADD PARTITION P_LIST3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_LIST3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST3_4
    );
ALTER TABLE t_list_hash4 ADD PARTITION P_LIST4 VALUES (16,17,18,19,20)
    (
        SUBPARTITION P_LIST4_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST4_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST4_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST4_4
    );
ALTER TABLE t_list_hash4 ADD PARTITION P_LIST5 VALUES (21,22,23,24,25) TABLESPACE hw_subpartition_tablespace_ts3;
ALTER TABLE t_list_hash4 ADD PARTITION P_LIST6 VALUES (26,27,28,29,30);
SELECT pg_get_tabledef('t_list_hash4');
-- DROP TABLEt_list_hash4;

--
----test add subpartition with tablespace----
--
--list-range
CREATE TABLE t_list_range3(c1 int, c2 int, c3 int)
PARTITION BY LIST (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 VALUES LESS THAN (15)
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 VALUES LESS THAN (15)
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15)
    (
        SUBPARTITION P_LIST3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 VALUES LESS THAN (15)
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20) TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_LIST5 VALUES (21,22,23,24,25)
);
ALTER TABLE t_list_range3 MODIFY PARTITION P_LIST1 ADD SUBPARTITION P_LIST1_4 VALUES LESS THAN (20) TABLESPACE hw_subpartition_tablespace_ts3;
ALTER TABLE t_list_range3 MODIFY PARTITION P_LIST2 ADD SUBPARTITION P_LIST2_4 VALUES LESS THAN (20);
ALTER TABLE t_list_range3 MODIFY PARTITION P_LIST3 ADD SUBPARTITION P_LIST3_4 VALUES LESS THAN (20);
SELECT pg_get_tabledef('t_list_range3');
-- DROP TABLEt_list_range3;

CREATE TABLE t_list_range4(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY LIST (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_LIST1 VALUES (1,2,3,4,5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_LIST1_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST1_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST1_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST1_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST2 VALUES (6,7,8,9,10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_LIST2_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST2_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST2_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST2_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_LIST3_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST3_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST3_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST3_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST4 VALUES (16,17,18,19,20)
    (
        SUBPARTITION P_LIST4_1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_LIST4_2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_LIST4_3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_LIST4_4 VALUES LESS THAN (20)
    ),
    PARTITION P_LIST5 VALUES (21,22,23,24,25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_LIST6 VALUES (26,27,28,29,30)
);
ALTER TABLE t_list_range4 MODIFY PARTITION P_LIST1 ADD SUBPARTITION P_LIST1_5 VALUES LESS THAN (25) TABLESPACE hw_subpartition_tablespace_ts3;
ALTER TABLE t_list_range4 MODIFY PARTITION P_LIST2 ADD SUBPARTITION P_LIST2_5 VALUES LESS THAN (25) TABLESPACE hw_subpartition_tablespace_ts2;
ALTER TABLE t_list_range4 MODIFY PARTITION P_LIST3 ADD SUBPARTITION P_LIST3_5 VALUES LESS THAN (25);
ALTER TABLE t_list_range4 MODIFY PARTITION P_LIST4 ADD SUBPARTITION P_LIST4_5 VALUES LESS THAN (25);
SELECT pg_get_tabledef('t_list_range4');
-- DROP TABLEt_list_range4;

--hash-list
CREATE TABLE t_hash_list3(c1 int, c2 int, c3 int)
PARTITION BY HASH (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_HASH3
    (
        SUBPARTITION P_HASH3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3 VALUES (11,12,13,14,15)
    ),
    PARTITION P_HASH4 TABLESPACE hw_subpartition_tablespace_ts1,
    PARTITION P_HASH5
);
ALTER TABLE t_hash_list3 MODIFY PARTITION P_HASH1 ADD SUBPARTITION P_HASH1_4 VALUES (16,17,18,19,20) TABLESPACE hw_subpartition_tablespace_ts3;
ALTER TABLE t_hash_list3 MODIFY PARTITION P_HASH2 ADD SUBPARTITION P_HASH2_4 VALUES (16,17,18,19,20);
ALTER TABLE t_hash_list3 MODIFY PARTITION P_HASH3 ADD SUBPARTITION P_HASH3_4 VALUES (16,17,18,19,20);
SELECT pg_get_tabledef('t_hash_list3');
-- DROP TABLEt_hash_list3;

CREATE TABLE t_hash_list4(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY HASH (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_HASH1 TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_HASH1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH1_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH1_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH2 TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_HASH2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH2_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH2_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH3 TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_HASH3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH3_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH3_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH4
    (
        SUBPARTITION P_HASH4_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_HASH4_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_HASH4_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_HASH4_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_HASH5 TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_HASH6
);
ALTER TABLE t_hash_list4 MODIFY PARTITION P_HASH1 ADD SUBPARTITION P_HASH1_5 VALUES(21,22,23,24,25) TABLESPACE hw_subpartition_tablespace_ts3;
ALTER TABLE t_hash_list4 MODIFY PARTITION P_HASH2 ADD SUBPARTITION P_HASH2_5 VALUES(21,22,23,24,25) TABLESPACE hw_subpartition_tablespace_ts2;
ALTER TABLE t_hash_list4 MODIFY PARTITION P_HASH3 ADD SUBPARTITION P_HASH3_5 VALUES(21,22,23,24,25);
ALTER TABLE t_hash_list4 MODIFY PARTITION P_HASH4 ADD SUBPARTITION P_HASH4_5 VALUES(21,22,23,24,25);
SELECT pg_get_tabledef('t_hash_list4');
-- DROP TABLEt_hash_list4;

--
----test create index with tablespace----
--
CREATE TABLE t_range_list(c1 int, c2 int, c3 int) TABLESPACE hw_subpartition_tablespace_ts1
PARTITION BY RANGE (c1) SUBPARTITION BY LIST (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) TABLESPACE hw_subpartition_tablespace_ts1
    (
        SUBPARTITION P_RANGE1_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE1_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE1_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE1_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) TABLESPACE hw_subpartition_tablespace_ts2
    (
        SUBPARTITION P_RANGE2_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE2_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE2_3 VALUES (DEFAULT)
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15) TABLESPACE hw_subpartition_tablespace_ts3
    (
        SUBPARTITION P_RANGE3_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE3_2 VALUES ( 6, 7, 8, 9,10) TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION P_RANGE3_3 VALUES (11,12,13,14,15) TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION P_RANGE3_4 VALUES (16,17,18,19,20)
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20)
    (
        SUBPARTITION P_RANGE4_1 VALUES ( 1, 2, 3, 4, 5) TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION P_RANGE4_2 VALUES (DEFAULT) TABLESPACE hw_subpartition_tablespace_ts2
    ),
    PARTITION P_RANGE5 VALUES LESS THAN (25) TABLESPACE hw_subpartition_tablespace_ts3,
    PARTITION P_RANGE6 VALUES LESS THAN (30)
);

CREATE INDEX t_range_list_idx ON t_range_list(c1,c2) LOCAL
(
    PARTITION idx_p1(
        SUBPARTITION idx_p1_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION idx_p1_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION idx_p1_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION idx_p1_4
    ),
    PARTITION idx_p2 TABLESPACE hw_subpartition_tablespace_ts2(
        SUBPARTITION idx_p2_1,
        SUBPARTITION idx_p2_2,
        SUBPARTITION idx_p2_3
    ),
    PARTITION idx_p3 TABLESPACE hw_subpartition_tablespace_ts2(
        SUBPARTITION idx_p3_1 TABLESPACE hw_subpartition_tablespace_ts1,
        SUBPARTITION idx_p3_2 TABLESPACE hw_subpartition_tablespace_ts2,
        SUBPARTITION idx_p3_3 TABLESPACE hw_subpartition_tablespace_ts3,
        SUBPARTITION idx_p3_4
    ),
    PARTITION idx_p4(
        SUBPARTITION idx_p4_1,
        SUBPARTITION idx_p4_2 TABLESPACE hw_subpartition_tablespace_ts2
    ),
    PARTITION idx_p5 TABLESPACE hw_subpartition_tablespace_ts3(
        SUBPARTITION idx_p5_1
    ),
    PARTITION idx_p6(
        SUBPARTITION idx_p6_1 TABLESPACE hw_subpartition_tablespace_ts2
    )
) TABLESPACE hw_subpartition_tablespace_ts1;

ALTER TABLE  t_range_list TRUNCATE SUBPARTITION P_RANGE1_1;

SELECT p.relname, t.spcname FROM pg_partition p, pg_class c, pg_namespace n, pg_tablespace t
WHERE p.parentid = c.oid
    AND c.relname='t_range_list_idx'
    AND c.relnamespace=n.oid
    AND n.nspname=CURRENT_SCHEMA
    AND p.reltablespace = t.oid
ORDER BY p.relname;

SELECT pg_get_indexdef('hw_subpartition_tablespace.t_range_list_idx'::regclass);

CREATE TABLE range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id)
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01'),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01')
);
INSERT INTO range_sales SELECT generate_series(1,1000),
                               generate_series(1,1000),
                               date_pli('2008-01-01', generate_series(1,1000)),
                               generate_series(1,1000)%10,
                               generate_series(1,1000)%10,
                               generate_series(1,1000)%1000,
                               generate_series(1,1000);
CREATE INDEX range_sales_idx ON range_sales(product_id) LOCAL;
--success, add 1 partition
ALTER TABLE range_sales ADD PARTITION time_2012 VALUES LESS THAN ('2013-01-01') TABLESPACE hw_subpartition_tablespace_ts3;
--success, add 1 partition
ALTER TABLE range_sales ADD PARTITION time_end VALUES LESS THAN (MAXVALUE) TABLESPACE hw_subpartition_tablespace_ts3;

create table test_index_lt (a int, b int, c int)
partition by list(a)
(
 PARTITION p1 VALUES (3, 4, 5),
 PARTITION p2 VALUES (1, 2)
);

ALTER TABLE test_index_lt ADD PARTITION p3  VALUES (6) TABLESPACE hw_subpartition_tablespace_ts2;
ALTER TABLE test_index_lt MOVE PARTITION FOR (5) TABLESPACE hw_subpartition_tablespace_ts3;

ALTER TABLE test_index_lt MOVE PARTITION p3 TABLESPACE hw_subpartition_tablespace_ts3;

create index test_index_lt_idx on test_index_lt(a) local;

ALTER TABLE test_index_lt MODIFY PARTITION p1 UNUSABLE LOCAL INDEXES ;

ALTER TABLE test_index_lt MODIFY PARTITION p1 rebuild UNUSABLE LOCAL INDEXES ;

-- DROP TABLEt_range_list;

--finish
drop tablespace hw_subpartition_tablespace_ts1;
drop tablespace hw_subpartition_tablespace_ts2;
drop tablespace hw_subpartition_tablespace_ts3;
\! rm -fr '@testtablespace@/hw_subpartition_tablespace_ts1'
\! rm -fr '@testtablespace@/hw_subpartition_tablespace_ts2'
\! rm -fr '@testtablespace@/hw_subpartition_tablespace_ts3'

DROP SCHEMA hw_subpartition_tablespace CASCADE;
RESET CURRENT_SCHEMA;
