DROP TABLE IF EXISTS student;
CREATE TABLE student(name varchar, score int);
INSERT INTO student VALUES('AA',2);
INSERT INTO student VALUES('AA',3);
INSERT INTO student VALUES('DD',5);
INSERT INTO student VALUES('DD',null);
 
SELECT /*+ 
    SET(enable_bitmapscan ON) 
    SET(enable_hashjoin ON) 
    SET(enable_seqscan ON) 
*/
    ARRAY[2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2]::NUMERIC[] AS arr,
    tmp.arr1 AS arr1,
    '2025-03-17 05:20:00'::TIMESTAMP AS time,
    'e33'::RAW AS str
FROM (
    SELECT /*+ REDUCE_ORDER_BY */
        ARRAY[2, 2, 2, 2, 2, 2, 2, 2, 2, 2]::INT2[] AS arr1,
        ARRAY[2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2]::NUMERIC[] AS arr2,
        student.name AS class_name,
        '62.73402336139341 years -72.18656137006248 days'::RELTIME AS reltime
    FROM student AS student
    WHERE 
        student.score >= -11
        AND (
            student.score >= 17
            OR student.name NOT ILIKE '%BBBB%'
            OR student.score = 20
        )
    ORDER BY 1, 2, 3, 4
    LIMIT 67
) AS tmp
WHERE 
    tmp.arr2 @> (
        tmp.arr2::NUMERIC[] 
        || array_delete(
            array_union_distinct(
                ARRAY[2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2]::NUMERIC[],
                CASE 
                    WHEN tmp.reltime != '-73.9136959480673 years'::RELTIME THEN tmp.arr2
                    WHEN tmp.arr1 < ARRAY[2, 2, 2, 2, 2, 2, 2, 2, 2, 2]::INT2[] THEN 
                        ARRAY[2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2, 2.2]::NUMERIC[]
                END
            )
        )::NUMERIC[]
    )
    AND (
        (
            ARRAY[2, 2, 2, 2, 2, 2, 2, 2, 2, 2]::INT2[],
            '-44.118209545483246 years 0.96175669228262 months'::RELTIME
        ) != (
            array_union(
                array_except_distinct(
                    array_trim(tmp.arr1, -28),
                    array_except_distinct(
                        ARRAY[2, 2, 2, 2, 2, 2, 2, 2, 2, 2]::INT2[],
                        tmp.arr1
                    )
                ),
                array_deleteidx(
                    (
                        ARRAY[2, 2, 2, 2, 2, 2, 2, 2, 2, 2]::INT2[]::INT2[]
                        || ARRAY[2, 2, 2, 2, 2, 2, 2, 2, 2, 2]::INT2[]::INT2[]
                    ),
                    1
                )
            ),
            tmp.reltime
        )
        OR tmp.arr1 @> ARRAY[2, 2, 2, 2, 2, 2, 2, 2, 2, 2]::INT2[]
    )
ORDER BY 1, 2, 3, 4;

DROP TABLE IF EXISTS student;