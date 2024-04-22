-- base test
DROP TABLE IF EXISTS table_name;
CREATE TABLE table_name(node1, node2) AS
SELECT 'A1', 'B1' UNION ALL
SELECT 'A1', 'B2' UNION ALL
SELECT 'A2', 'B1' UNION ALL
SELECT 'A2', 'B3' UNION ALL
SELECT 'B1', 'K5' UNION ALL
SELECT 'B1', 'I2' UNION ALL
SELECT 'A3', 'G7' UNION ALL
SELECT 'A3', 'H9' UNION ALL
SELECT 'B2', 'J1' UNION ALL
SELECT 'B2', 'K5' UNION ALL
SELECT 'H9', 'L7' ;

SELECT
CASE LEVEL WHEN 2 THEN PRIOR node1 ELSE node1 END AS node0,
CASE LEVEL WHEN 2 THEN node1 ELSE node2 END AS node1,    
CASE LEVEL WHEN 2 THEN node2 END AS node2
FROM   table_name
WHERE  LEVEL = 2
OR     (LEVEL = 1 AND CONNECT_BY_ISLEAF = 1)
START WITH node1 LIKE 'A%'
CONNECT BY PRIOR node2 = node1;

SELECT
CASE LEVEL WHEN 2 THEN PRIOR PRIOR node1 ELSE node1 END AS node0,
CASE LEVEL WHEN 2 THEN node1 ELSE node2 END AS node1,    
CASE LEVEL WHEN 2 THEN node2 END AS node2
FROM   table_name
WHERE  LEVEL = 2
OR     (LEVEL = 1 AND CONNECT_BY_ISLEAF = 1)
START WITH node1 LIKE 'A%'
CONNECT BY PRIOR node2 = node1;

SELECT
CASE LEVEL WHEN 2 THEN PRIOR(PRIOR node1) ELSE node1 END AS node0,
CASE LEVEL WHEN 2 THEN node1 ELSE node2 END AS node1,    
CASE LEVEL WHEN 2 THEN node2 END AS node2
FROM   table_name
WHERE  LEVEL = 2
OR     (LEVEL = 1 AND CONNECT_BY_ISLEAF = 1)
START WITH node1 LIKE 'A%'
CONNECT BY PRIOR node2 = node1;

SELECT
PRIOR 1
FROM   table_name;

SELECT
PRIOR 1
FROM   table_name
WHERE  LEVEL = 2
OR     (LEVEL = 1 AND CONNECT_BY_ISLEAF = 1)
START WITH node1 LIKE 'A%'
CONNECT BY PRIOR node2 = node1;

SELECT
PRIOR 'test'
FROM   table_name;

SELECT
PRIOR 'test'
FROM   table_name
WHERE  LEVEL = 2
OR     (LEVEL = 1 AND CONNECT_BY_ISLEAF = 1)
START WITH node1 LIKE 'A%'
CONNECT BY PRIOR node2 = node1;

SELECT
PRIOR repeat('test')
FROM   table_name;

SELECT
PRIOR repeat('test')
FROM   table_name
WHERE  LEVEL = 2
OR     (LEVEL = 1 AND CONNECT_BY_ISLEAF = 1)
START WITH node1 LIKE 'A%'
CONNECT BY PRIOR node2 = node1;

SELECT
PRIOR NULL
FROM   table_name;

SELECT
PRIOR NULL
FROM   table_name
WHERE  LEVEL = 2
OR     (LEVEL = 1 AND CONNECT_BY_ISLEAF = 1)
START WITH node1 LIKE 'A%'
CONNECT BY PRIOR node2 = node1;

-- test about data type
DROP TABLE IF EXISTS test_type_table;
CREATE TABLE test_type_table
(
    pid int,
    id int,
    name text,
    "int1" tinyint,
    "int2" smallint,
    "int4" integer,
    "int8" bigint,
    "float4" float4,
    "float8" float8,
    "numeric" decimal(20, 6),
    "bit5" bit(5),
    "boolean" boolean,
    "date" date,
    "time" time,
    "timetz" timetz,
    "timestamp" timestamp,
    "timestamptz" timestamptz,
    "char" char(20),
    "varchar" varchar(100),
    "blob" blob,
    "text" text
);

INSERT INTO test_type_table VALUES
(0, 1, 'top_father', 1, 1, 1, 1, 1.1, 1.1, 1.1, b'00001', true, '2024-01-01', '00:00:01', '00:00:01', '2024-01-01 00:00:01', '2024-01-01 00:00:01', 'top_father', 'top_father', '0A', 'top_father'), 
(1, 2, 'second_father1', 2, 2, 2, 2, 1.2, 1.2, 1.2, b'00010', false, '2024-01-02', '00:00:02', '00:00:02', '2024-01-02 00:00:02', '2024-01-02 00:00:02', 'second_father1', 'second_father1', '0B', 'second_father1'),
(1, 3, 'second_father2', 3, 3, 3, 3, 1.3, 1.3, 1.3, b'00100', true, '2024-01-03', '00:00:03', '00:00:03', '2024-01-03 00:00:03', '2024-01-03 00:00:03', 'second_father2', 'second_father2', '0C', 'second_father2'),
(2, 4, 'third_father1', 4, 4, 4, 4, 1.4, 1.4, 1.4, b'01000', false, '2024-01-04', '00:00:04', '00:00:04', '2024-01-04 00:00:04', '2024-01-04 00:00:04', 'third_father1', 'third_father1', '0D', 'third_father1'),
(3, 5, 'third_father2', 5, 5, 5, 5, 1.5, 1.5, 1.5, b'10000', true, '2024-01-05', '00:00:05', '00:00:05', '2024-01-05 00:00:05', '2024-01-05 00:00:05', 'third_father2', 'third_father2', '0E', 'third_father2');

SELECT
PRIOR name AS father_name,
PRIOR "int1" AS father_int1,
name AS current_name,
"int1" AS current_int1
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "int2" AS father_int2,
name AS current_name,
"int2" AS current_int2
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "int4" AS father_int4,
name AS current_name,
"int4" AS current_int4
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "int8" AS father_int8,
name AS current_name,
"int8" AS current_int8
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "float4" AS father_float4,
name AS current_name,
"float4" AS current_float4
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "float8" AS father_float8,
name AS current_name,
"float8" AS current_float8
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "numeric" AS father_numeric,
name AS current_name,
"numeric" AS current_numeric
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "bit5" AS father_bit5,
name AS current_name,
"bit5" AS current_bit5
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "boolean" AS father_boolean,
name AS current_name,
"boolean" AS current_boolean
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "date" AS father_date,
name AS current_name,
"date" AS current_date
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "time" AS father_time,
name AS current_name,
"time" AS current_time
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "timetz" AS father_timetz,
name AS current_name,
"timetz" AS current_timetz
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "timestamp" AS father_timestamp,
name AS current_name,
"timestamp" AS current_timestamp
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "timestamptz" AS father_timestamptz,
name AS current_name,
"timestamptz" AS current_timestamptz
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "char" AS father_char,
name AS current_name,
"char" AS current_char
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "varchar" AS father_varchar,
name AS current_name,
"varchar" AS current_varchar
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "blob" AS father_blob,
name AS current_name,
"blob" AS current_blob
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "text" AS father_text,
name AS current_name,
"text" AS current_text
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid;

-- test about WHERE clause
SELECT
PRIOR name AS father_name,
PRIOR "int1" AS father_int1,
name AS current_name,
"int1" AS current_int1
FROM test_type_table WHERE PRIOR "int1" > 1
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "int2" AS father_int2,
name AS current_name,
"int2" AS current_int2
FROM test_type_table WHERE PRIOR "int2" > 1
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "int4" AS father_int4,
name AS current_name,
"int4" AS current_int4
FROM test_type_table WHERE PRIOR "int4" > 1
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "int8" AS father_int8,
name AS current_name,
"int8" AS current_int8
FROM test_type_table WHERE PRIOR "int8" > 1
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "float4" AS father_float4,
name AS current_name,
"float4" AS current_float4
FROM test_type_table WHERE PRIOR "float4" > 1.1
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "float8" AS father_float8,
name AS current_name,
"float8" AS current_float8
FROM test_type_table WHERE PRIOR "float8" > 1.1
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "numeric" AS father_numeric,
name AS current_name,
"numeric" AS current_numeric
FROM test_type_table WHERE PRIOR "numeric" > 1.1
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "bit5" AS father_bit5,
name AS current_name,
"bit5" AS current_bit5
FROM test_type_table WHERE "bit5" > b'00001'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "boolean" AS father_boolean,
name AS current_name,
"boolean" AS current_boolean
FROM test_type_table WHERE PRIOR "boolean"
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "date" AS father_date,
name AS current_name,
"date" AS current_date
FROM test_type_table WHERE PRIOR "date" > '2024-01-01'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "time" AS father_time,
name AS current_name,
"time" AS current_time
FROM test_type_table WHERE PRIOR "time" > '00:00:01'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "timetz" AS father_timetz,
name AS current_name,
"timetz" AS current_timetz
FROM test_type_table WHERE PRIOR "timetz" > '00:00:01'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "timestamp" AS father_timestamp,
name AS current_name,
"timestamp" AS current_timestamp
FROM test_type_table WHERE PRIOR "timestamp" > '2024-01-01 00:00:01'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "timestamptz" AS father_timestamptz,
name AS current_name,
"timestamptz" AS current_timestamptz
FROM test_type_table WHERE PRIOR "timestamptz" > '2024-01-01 00:00:01'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "char" AS father_char,
name AS current_name,
"char" AS current_char
FROM test_type_table WHERE PRIOR "char" LIKE '%second%'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "varchar" AS father_varchar,
name AS current_name,
"varchar" AS current_varchar
FROM test_type_table WHERE PRIOR "varchar" LIKE '%second%'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "blob" AS father_blob,
name AS current_name,
"blob" AS current_blob
FROM test_type_table WHERE PRIOR "blob" > '0A'
START WITH id = 1
CONNECT BY PRIOR id = pid;

SELECT
PRIOR name AS father_name,
PRIOR "text" AS father_text,
name AS current_name,
"text" AS current_text
FROM test_type_table WHERE PRIOR "text" LIKE '%second%'
START WITH id = 1
CONNECT BY PRIOR id = pid;

-- test about GROUP BY clause
SELECT
PRIOR "int1",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int1";

SELECT
PRIOR "int2",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int2";

SELECT
PRIOR "int4",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int4";

SELECT
PRIOR "int8",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int8";

SELECT
PRIOR "float4",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "float4";

SELECT
PRIOR "float8",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "float8";

SELECT
PRIOR "numeric",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "numeric";

SELECT
PRIOR "bit5",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "bit5";

SELECT
PRIOR "boolean",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "boolean";

SELECT
PRIOR "date",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "date";

SELECT
PRIOR "time",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "time";

SELECT
PRIOR "timetz",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "timetz";

SELECT
PRIOR "timestamp",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "timestamp";

SELECT
PRIOR "timestamptz",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "timestamptz";

SELECT
PRIOR "char",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "char";

SELECT
PRIOR "varchar",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "varchar";

SELECT
PRIOR "blob",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "blob";

SELECT
PRIOR "text",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "text";

-- test about HAVING clause
SELECT
PRIOR "int1",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int1" HAVING PRIOR "int1" > 1;

SELECT
PRIOR "int2",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int2" HAVING PRIOR "int2" > 1;

SELECT
PRIOR "int4",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int4" HAVING PRIOR "int4" > 1;

SELECT
PRIOR "int8",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "int8" HAVING PRIOR "int8" > 1;

SELECT
PRIOR "float4",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "float4" HAVING PRIOR "float4" > 1.1;

SELECT
PRIOR "float8",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "float8" HAVING PRIOR "float8" > 1.1;

SELECT
PRIOR "numeric",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "numeric" HAVING PRIOR "numeric" > 1.1;

SELECT
PRIOR "bit5",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "bit5" HAVING PRIOR "bit5" > b'00001';

SELECT
PRIOR "boolean",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "boolean" HAVING PRIOR "boolean";

SELECT
PRIOR "date",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "date" HAVING PRIOR "date" > '2024-01-01';

SELECT
PRIOR "time",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "time" HAVING PRIOR "time" > '00:00:01';

SELECT
PRIOR "timetz",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "timetz" HAVING PRIOR "timetz" > '00:00:01';

SELECT
PRIOR "timestamp",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "timestamp" HAVING PRIOR "timestamp" > '2024-01-01 00:00:01';

SELECT
PRIOR "timestamptz",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "timestamptz" HAVING PRIOR "timestamptz" > '2024-01-01 00:00:01';

SELECT
PRIOR "char",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "char" HAVING PRIOR "char" LIKE '%second%';

SELECT
PRIOR "varchar",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "varchar" HAVING PRIOR "varchar" LIKE '%second%';

SELECT
PRIOR "blob",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "blob" HAVING PRIOR "blob" > '0A';

SELECT
PRIOR "text",
COUNT(ID)
FROM test_type_table
START WITH id = 1
CONNECT BY PRIOR id = pid GROUP BY PRIOR "text" HAVING PRIOR "text" LIKE '%second%';
