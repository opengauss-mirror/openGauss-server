-- 不相关表
DROP TABLE IF EXISTS tmp_table;
CREATE TABLE tmp_table (id int, null_val date);
INSERT INTO tmp_table VALUES(generate_series(1,10), null);

-- ********************************
-- * 目标表：行存表；源表：行存表 *
-- ********************************
DROP TABLE IF EXISTS target_table, source_table;
CREATE TABLE target_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=ROW);
CREATE TABLE source_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=ROW);

INSERT INTO source_table VALUES (generate_series(11,20),'A'||(generate_series(11,20))||'Z', date'2000-03-01'+generate_series(11,20), generate_series(11,20));
INSERT INTO source_table VALUES (21, null, null, null);

-- 相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + 4 FROM target_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21 /* 返回null，select列 */),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - 10 AND c3 >= '2000-01-01') /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 非相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(id) + 4 FROM tmp_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT null_val FROM tmp_table WHERE id = 1 /* 返回null，select列 */),
	(SELECT id FROM tmp_table WHERE id > 7 AND id < 9) /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.id + t2.id FROM tmp_table t1 JOIN tmp_table t2 ON t1.id = t2.id AND t1.id < 2 ) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 子查询嵌套
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + (SELECT id FROM tmp_table WHERE id > 3 AND id <= 4) FROM target_table /* SELECT列嵌套 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - (SELECT MAX(id) FROM tmp_table) AND c3 >= '2000-01-01') /* WHERE条件嵌套 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- ********************************
-- * 目标表：列存表；源表：列存表 *
-- ********************************
DROP TABLE IF EXISTS target_table, source_table;
CREATE TABLE target_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=COLUMN);
CREATE TABLE source_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=COLUMN);

INSERT INTO source_table VALUES (generate_series(11,20),'A'||(generate_series(11,20))||'Z', date'2000-03-01'+generate_series(11,20), generate_series(11,20));
INSERT INTO source_table VALUES (21, null, null, null);

-- 相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + 4 FROM target_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21 /* 返回null，select列 */),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - 10 AND c3 >= '2000-01-01') /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 非相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(id) + 4 FROM tmp_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT null_val FROM tmp_table WHERE id = 1 /* 返回null，select列 */),
	(SELECT id FROM tmp_table WHERE id > 7 AND id < 9) /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.id + t2.id FROM tmp_table t1 JOIN tmp_table t2 ON t1.id = t2.id AND t1.id < 2 ) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 子查询嵌套
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + (SELECT id FROM tmp_table WHERE id > 3 AND id <= 4) FROM target_table /* SELECT列嵌套 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - (SELECT MAX(id) FROM tmp_table) AND c3 >= '2000-01-01') /* WHERE条件嵌套 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- ********************************
-- * 目标表：行存表；源表：列存表 *
-- ********************************
DROP TABLE IF EXISTS target_table, source_table;
CREATE TABLE target_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=ROW);
CREATE TABLE source_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=COLUMN);

INSERT INTO source_table VALUES (generate_series(11,20),'A'||(generate_series(11,20))||'Z', date'2000-03-01'+generate_series(11,20), generate_series(11,20));
INSERT INTO source_table VALUES (21, null, null, null);

-- 相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + 4 FROM target_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21 /* 返回null，select列 */),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - 10 AND c3 >= '2000-01-01') /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 非相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(id) + 4 FROM tmp_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT null_val FROM tmp_table WHERE id = 1 /* 返回null，select列 */),
	(SELECT id FROM tmp_table WHERE id > 7 AND id < 9) /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.id + t2.id FROM tmp_table t1 JOIN tmp_table t2 ON t1.id = t2.id AND t1.id < 2 ) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 子查询嵌套
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + (SELECT id FROM tmp_table WHERE id > 3 AND id <= 4) FROM target_table /* SELECT列嵌套 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - (SELECT MAX(id) FROM tmp_table) AND c3 >= '2000-01-01') /* WHERE条件嵌套 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- ********************************
-- * 目标表：列存表；源表：行存表 *
-- ********************************
DROP TABLE IF EXISTS target_table, source_table;
CREATE TABLE target_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=COLUMN);
CREATE TABLE source_table (c1 int, c2 varchar(200), c3 date, c4 numeric(18,9))
WITH (ORIENTATION=ROW);

INSERT INTO source_table VALUES (generate_series(11,20),'A'||(generate_series(11,20))||'Z', date'2000-03-01'+generate_series(11,20), generate_series(11,20));
INSERT INTO source_table VALUES (21, null, null, null);

-- 相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + 4 FROM target_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21 /* 返回null，select列 */),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - 10 AND c3 >= '2000-01-01') /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 非相关子查询
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(id) + 4 FROM tmp_table /* 返回单行单列 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT null_val FROM tmp_table WHERE id = 1 /* 返回null，select列 */),
	(SELECT id FROM tmp_table WHERE id > 7 AND id < 9) /* 带WHERE条件 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.id + t2.id FROM tmp_table t1 JOIN tmp_table t2 ON t1.id = t2.id AND t1.id < 2 ) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

-- 子查询嵌套
TRUNCATE target_table;
INSERT INTO target_table VALUES (generate_series(1,10),'A'||(generate_series(1,10))||'Z', date'2000-03-01'+generate_series(1,10), generate_series(1,10));

MERGE INTO target_table t
  USING source_table s ON t.c1 + (SELECT MIN(c1) + (SELECT id FROM tmp_table WHERE id > 3 AND id <= 4) FROM target_table /* SELECT列嵌套 */) = s.c1
WHEN MATCHED THEN
  UPDATE SET (c2, c3, c4) = (s.c2,
    (SELECT c3 FROM source_table WHERE c1 = 21),
	(SELECT c4 FROM target_table WHERE c1 = s.c1 - (SELECT MAX(id) FROM tmp_table) AND c3 >= '2000-01-01') /* WHERE条件嵌套 */)
WHEN NOT MATCHED THEN
  INSERT VALUES (s.c1, s.c2, s.c3,
    (SELECT t2.c4 FROM tmp_table t1 JOIN target_table t2 ON t1.id = t2.c1 AND t2.c1 + 8 = s.c1) /* 多表级联 */);

SELECT c1, c2, to_char(c3, 'YYYY/MM/DD'), c4 FROM target_table ORDER BY c1;

DROP TABLE IF EXISTS target_table, source_table, tmp_table;
