/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * 09_SQLSplitter.sql
 *    DB4AI.Snapshot test case.
 *
 *
 * -------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION _db4ai_test.split(
    IN i_statement TEXT        -- input for splitting
)
RETURNS TEXT[] LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $$
DECLARE
    pattern TEXT;
    res_arr TEXT[];

    nested INT := 0;          -- level of nesting
    quoted BOOLEAN := FALSE;  -- inside quoted identifier
    cur_ch VARCHAR;           -- current character in tokenizer
    idx INTEGER := 0;         -- loop counter, cannot use FOR .. iterator
    start_pos INTEGER := 1;
    stmt TEXT := i_statement;
BEGIN

-- BEGIN splitter code for testing

    pattern := '';

    LOOP
        idx := idx + 1;
        cur_ch := substr(stmt, idx, 1);
        EXIT WHEN cur_ch IS NULL OR cur_ch = '';

        CASE cur_ch
        WHEN '"' THEN
            IF quoted AND substr(stmt, idx + 1, 1) = '"' THEN
                idx := idx + 1;
            ELSE
                quoted := NOT quoted;
            END IF;
            IF quoted THEN
                CONTINUE;
            END IF;
        WHEN '(' THEN
            nested := nested + 1;
            CONTINUE;
        WHEN ')' THEN
            nested := nested - 1;
            IF nested < 0 THEN
                RAISE EXCEPTION 'syntax error at or near '')'' in ''%'' at position ''%''', stmt, idx;
            END IF;
            CONTINUE;
        WHEN ' ' THEN
            IF quoted OR nested > 0 THEN
                CONTINUE;
            ELSIF pattern IS NULL OR length(pattern) = 0 THEN
                start_pos := idx;
                CONTINUE;
            END IF;
        ELSE
            pattern := pattern || upper(cur_ch);
            CONTINUE;
        END CASE;

-- END splitter code for testing

        IF pattern IN ('FROM', 'WHERE') THEN
            RETURN ARRAY [ left(i_statement, start_pos - 1), substr(i_statement, start_pos + 1) ];
        END IF;
        pattern := '';
        start_pos := idx;
    END LOOP;

--   RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION _db4ai_test.test(
)
RETURNS VOID LANGUAGE plpgsql SECURITY INVOKER SET client_min_messages TO NOTICE
AS $test$
BEGIN

    PERFORM _db4ai_test.assert_equal($$_db4ai_test.split('SET a=b, "cc ( ()) "=(SELECT "dddd ()" FROM ( WHERE) ) FROM yyy WHERE zzz')$$, $$'{"SET a=b, \"cc ( ()) \"=(SELECT \"dddd ()\" FROM ( WHERE) )","FROM yyy WHERE zzz"}'$$);

END;
$test$;
