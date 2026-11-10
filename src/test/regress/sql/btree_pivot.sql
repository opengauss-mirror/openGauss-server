\pset format unaligned
-- Truncated btree high keys do not contain every attribute of the leaf tuple.
-- Keep the first key constant and vary the second key, with wide trailing keys
-- and eight copies per key to cover both ordinary tuples and posting tuples.
CREATE SCHEMA btree_pivot;
SET search_path = btree_pivot;
SET query_dop = 1;
SET enable_force_smp = off;

CREATE TABLE btree_pivot_data (
    c1 smallint,
    c2 varchar(256),
    c3 varchar(256),
    c4 varchar(256),
    c5 varchar(256)
);
INSERT INTO btree_pivot_data
SELECT 1, 'B' || lpad(n::text, 4, '0') || repeat('张三李四', 12),
       'C' || repeat('张三李四', 12), 'D' || repeat('张三李四', 12),
       'E' || repeat('张三李四', 12)
FROM generate_series(1, 1024) AS n, generate_series(1, 8) AS copies;

-- Real NULLs must retain their usual semantics, unlike an absent pivot suffix.
INSERT INTO btree_pivot_data
SELECT CASE WHEN nullColumn = 1 THEN NULL ELSE 1 END,
       CASE WHEN nullColumn = 2 THEN NULL ELSE 'B' || lpad(n::text, 4, '0') ||
            repeat('张三李四', 12) END,
       CASE WHEN nullColumn = 3 THEN NULL ELSE 'C' || repeat('张三李四', 12) END,
       CASE WHEN nullColumn = 4 THEN NULL ELSE 'D' || repeat('张三李四', 12) END,
       CASE WHEN nullColumn = 5 THEN NULL ELSE 'E' || repeat('张三李四', 12) END
FROM generate_series(1, 32) AS n, generate_series(1, 5) AS nullColumn;
VACUUM ANALYZE btree_pivot_data;

-- Compare both complete counts and bounded ordered results against a seqscan.
-- Check the plans as well, so an unintended seqscan cannot hide a regression.
CREATE FUNCTION btree_pivot_check(forwardOrder text, backwardOrder text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    caseRecord record;
    countQuery text;
    rowQuery text;
    planQuery text;
    planLine text;
    planText text;
    expectedPlan text;
    referenceCount bigint;
    indexCount bigint;
    referenceRows text;
    indexRows text;
    directionMode integer;
    optimizationMode integer;
    scanMode integer;
    queryKind integer;
    checks integer := 0;
BEGIN
    FOR caseRecord IN
        SELECT * FROM (VALUES
            ('fourth_not_null', 'c4 IS NOT NULL'),
            ('fourth_null', 'c4 IS NULL'),
            ('fifth_not_null', 'c5 IS NOT NULL'),
            ('fifth_null', 'c5 IS NULL'),
            ('first_null', 'c1 IS NULL'),
            ('second_null', 'c2 IS NULL'),
            ('fourth_equal', 'c4 = ' || quote_literal('D' || repeat('张三李四', 12))),
            ('fourth_less', 'c4 < ' || quote_literal('Z' || repeat('张三李四', 12))),
            ('fourth_ge', 'c4 >= ' || quote_literal('D' || repeat('张三李四', 12))),
            ('prefix_and_null', 'c1 = 1 AND c4 IS NULL'),
            -- c1 is equal on most pivots, then row members c4 and c5 are absent.
            ('row_gap_lt', '(c1,c4,c5) < (1,' ||
                quote_literal('D' || repeat('张三李四', 12)) || ',' || quote_literal('Z') || ')'),
            ('row_gap_le', '(c1,c4,c5) <= (1,' ||
                quote_literal('D' || repeat('张三李四', 12)) || ',' ||
                quote_literal('E' || repeat('张三李四', 12)) || ')'),
            ('row_gap_gt', '(c1,c4,c5) > (1,' ||
                quote_literal('D' || repeat('张三李四', 12)) || ',' || quote_literal('A') || ')'),
            ('row_gap_ge', '(c1,c4,c5) >= (1,' ||
                quote_literal('D' || repeat('张三李四', 12)) || ',' ||
                quote_literal('E' || repeat('张三李四', 12)) || ')'),
            ('row_full_le', '(c1,c2,c3,c4,c5) <= (1,' ||
                quote_literal('B0512' || repeat('张三李四', 12)) || ',' ||
                quote_literal('C' || repeat('张三李四', 12)) || ',' ||
                quote_literal('D' || repeat('张三李四', 12)) || ',' ||
                quote_literal('E' || repeat('张三李四', 12)) || ')'),
            ('row_full_ge', '(c1,c2,c3,c4,c5) >= (1,' ||
                quote_literal('B0512' || repeat('张三李四', 12)) || ',' ||
                quote_literal('C' || repeat('张三李四', 12)) || ',' ||
                quote_literal('D' || repeat('张三李四', 12)) || ',' ||
                quote_literal('E' || repeat('张三李四', 12)) || ')')
        ) AS cases(caseName, predicateText)
    LOOP
        countQuery := 'SELECT count(*) FROM btree_pivot_data WHERE ' || caseRecord.predicateText;
        PERFORM set_config('enable_seqscan', 'on', true);
        PERFORM set_config('enable_bitmapscan', 'off', true);
        PERFORM set_config('enable_indexscan', 'off', true);
        PERFORM set_config('enable_indexonlyscan', 'off', true);
        EXECUTE countQuery INTO referenceCount;

        FOR directionMode IN 0..1 LOOP
            rowQuery := 'SELECT md5(COALESCE(string_agg(rowText, '''' ORDER BY rowText), '''')) FROM (' ||
                'SELECT ROW(c1,c2,c3,c4,c5)::text AS rowText FROM btree_pivot_data WHERE ' ||
                caseRecord.predicateText || ' ORDER BY ' ||
                CASE WHEN directionMode = 0 THEN forwardOrder ELSE backwardOrder END ||
                ' LIMIT 31) AS limitedRows';
            PERFORM set_config('enable_seqscan', 'on', true);
            PERFORM set_config('enable_indexscan', 'off', true);
            PERFORM set_config('enable_indexonlyscan', 'off', true);
            EXECUTE rowQuery INTO referenceRows;

            FOR optimizationMode IN 0..1 LOOP
                PERFORM set_config('enable_indexscan_optimization',
                    CASE WHEN optimizationMode = 0 THEN 'off' ELSE 'on' END, true);
                FOR scanMode IN 0..1 LOOP
                    PERFORM set_config('enable_seqscan', 'off', true);
                    PERFORM set_config('enable_indexscan', 'on', true);
                    PERFORM set_config('enable_indexonlyscan',
                        CASE WHEN scanMode = 0 THEN 'off' ELSE 'on' END, true);
                    expectedPlan := CASE WHEN scanMode = 0 THEN 'Index Scan' ELSE 'Index Only Scan' END;
                    FOR queryKind IN 0..1 LOOP
                        planQuery := CASE WHEN queryKind = 0 THEN countQuery ELSE rowQuery END;
                        planText := '';
                        FOR planLine IN EXECUTE 'EXPLAIN (COSTS OFF) ' || planQuery LOOP
                            planText := planText || planLine || E'\n';
                        END LOOP;
                        IF position(expectedPlan IN planText) = 0 THEN
                            RAISE EXCEPTION 'wrong plan for % (scan %, optimization %, direction %): %',
                                caseRecord.caseName, scanMode, optimizationMode, directionMode, planText;
                        END IF;
                        IF (caseRecord.caseName = 'fourth_not_null' OR
                            (forwardOrder = 'c1,c2,c3,c4,c5' AND caseRecord.caseName LIKE 'row_gap_%')) AND
                            position('Index Cond' IN planText) = 0 THEN
                            RAISE EXCEPTION 'missing index condition for %: %', caseRecord.caseName, planText;
                        END IF;
                        IF queryKind = 1 AND position('Sort' IN planText) <> 0 THEN
                            RAISE EXCEPTION 'unexpected sort for %: %', caseRecord.caseName, planText;
                        END IF;
                    END LOOP;
                    EXECUTE countQuery INTO indexCount;
                    EXECUTE rowQuery INTO indexRows;
                    IF indexCount IS DISTINCT FROM referenceCount OR indexRows IS DISTINCT FROM referenceRows THEN
                        RAISE EXCEPTION 'wrong result for % (scan %, optimization %, direction %)',
                            caseRecord.caseName, scanMode, optimizationMode, directionMode;
                    END IF;
                    checks := checks + 1;
                END LOOP;
            END LOOP;
        END LOOP;
    END LOOP;
    RETURN checks;
END;
$$;

CREATE INDEX btree_pivot_idx ON btree_pivot_data (c1, c2, c3, c4, c5) WITH (deduplication = off);
SELECT 'ascending, deduplication off' AS configuration,
       btree_pivot_check('c1,c2,c3,c4,c5', 'c1 DESC,c2 DESC,c3 DESC,c4 DESC,c5 DESC') = 128 AS passed;

ALTER INDEX btree_pivot_idx SET (deduplication = on);
REINDEX INDEX btree_pivot_idx;
SELECT 'ascending, deduplication on' AS configuration,
       btree_pivot_check('c1,c2,c3,c4,c5', 'c1 DESC,c2 DESC,c3 DESC,c4 DESC,c5 DESC') = 128 AS passed;
DROP INDEX btree_pivot_idx;

CREATE INDEX btree_pivot_idx ON btree_pivot_data (c1 ASC, c2 DESC, c3 ASC, c4 DESC, c5 ASC)
WITH (deduplication = off);
SELECT 'mixed, deduplication off' AS configuration,
       btree_pivot_check('c1 ASC,c2 DESC,c3 ASC,c4 DESC,c5 ASC',
                        'c1 DESC,c2 ASC,c3 DESC,c4 ASC,c5 DESC') = 128 AS passed;
ALTER INDEX btree_pivot_idx SET (deduplication = on);
REINDEX INDEX btree_pivot_idx;
SELECT 'mixed, deduplication on' AS configuration,
       btree_pivot_check('c1 ASC,c2 DESC,c3 ASC,c4 DESC,c5 ASC',
                        'c1 DESC,c2 ASC,c3 DESC,c4 ASC,c5 DESC') = 128 AS passed;

DROP FUNCTION btree_pivot_check(text, text);
DROP TABLE btree_pivot_data;
RESET enable_force_smp;
RESET query_dop;
RESET search_path;
DROP SCHEMA btree_pivot;
\pset format aligned
