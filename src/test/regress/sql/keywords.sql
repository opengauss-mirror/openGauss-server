-- Test keywords refers to `kwlist.h`

CREATE SCHEMA keywords;
SET CURRENT_SCHEMA TO keywords;

CREATE OR REPLACE PROCEDURE keywords.test_stmt(sql_stmt VARCHAR) AS
BEGIN
    EXECUTE IMMEDIATE sql_stmt;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Test failed. sql: ''%''', sql_stmt;
END;
/

CREATE PROCEDURE keywords.test_table(keyword_name VARCHAR) AS
BEGIN
    PERFORM keywords.test_stmt('CREATE TABLE ' || keyword_name || ' (' || keyword_name || ' INT)');
    PERFORM keywords.test_stmt('DROP TABLE ' || keyword_name);
END;
/

CREATE PROCEDURE keywords.test_index(keyword_name VARCHAR) AS
BEGIN
    PERFORM keywords.test_stmt('CREATE TABLE keywords.tbl (v INT)');
    PERFORM keywords.test_stmt('CREATE INDEX ' || keyword_name || ' ON keywords.tbl(v)');
    PERFORM keywords.test_stmt('DROP INDEX ' || keyword_name);
    PERFORM keywords.test_stmt('DROP TABLE keywords.tbl');
END;
/

CREATE PROCEDURE keywords.test_type(keyword_name VARCHAR) AS
BEGIN
    PERFORM keywords.test_stmt('CREATE TYPE ' || keyword_name || ' AS (v INT)');
    PERFORM keywords.test_stmt('DROP TYPE keywords.' || keyword_name);
END;
/

CREATE PROCEDURE keywords.test_func(keyword_name VARCHAR) AS
BEGIN
    PERFORM keywords.test_stmt('CREATE FUNCTION ' || keyword_name || '() RETURNS INT LANGUAGE SQL AS ''SELECT 1''');
    PERFORM keywords.test_stmt('DROP FUNCTION ' || keyword_name);
END;
/

-- To hide hints
\set VERBOSITY terse

-- Traverse all keywords
DECLARE
    keyword_name VARCHAR;
    catcode_name CHAR;
BEGIN
    FOR keyword_name, catcode_name IN (SELECT word, catcode FROM pg_get_keywords()) LOOP
        IF catcode_name = 'U' OR catcode_name = 'C' THEN
            PERFORM keywords.test_table(keyword_name);
            PERFORM keywords.test_index(keyword_name);
            PERFORM keywords.test_type(keyword_name);
        ELSIF catcode_name = 'T' THEN
            PERFORM keywords.test_func(keyword_name);
        ELSE
            CONTINUE;
        END IF;
    END LOOP;
END;
/

\unset VERBOSITY

-- Tests for keywords that contains in a multi-word token
CREATE INDEX ON ON;

SET CURRENT_SCHEMA TO DEFAULT;
drop function keywords.test_table(character varying);
drop function keywords.test_index(character varying);
drop function keywords.test_type(character varying);
drop function keywords.test_func(character varying);
drop function keywords.test_stmt(character varying);
drop function keywords."current_schema"();
DROP SCHEMA keywords CASCADE;
