DROP FUNCTION IF EXISTS pg_catalog.generate_procoverage_report(int8, int8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5734;
CREATE OR REPLACE FUNCTION pg_catalog.generate_procoverage_report(int8, int8) RETURNS text
LANGUAGE INTERNAL as 'generate_procoverage_report';

DROP FUNCTION IF EXISTS pg_catalog.array_integer_agg_add(int[], int[]) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.array_integer_agg_add(int[], int[]) RETURNS int[] AS $$
DECLARE
    result int[];
    len int;
    i int;
BEGIN
    IF $1 IS NULL THEN
        RETURN $2;
    END IF;

    IF $2 IS NULL THEN
        RETURN $1;
    END IF;

    len := GREATEST(array_length($1, 1), array_length($2, 1));
    result := $1;

    FOR i IN 1..len LOOP
        result[i] := COALESCE($1[i], 0) + COALESCE($2[i], 0);
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

DROP AGGREGATE IF EXISTS pg_catalog.array_integer_sum(int[]);
CREATE AGGREGATE pg_catalog.array_integer_sum(int[]) (
    SFUNC = array_integer_agg_add,
    STYPE = int[]
);

DROP FUNCTION IF EXISTS pg_catalog.coverage_arrays(booleans bool[], integers int[]) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.coverage_arrays(booleans bool[], integers int[])
RETURNS int[] AS $$
DECLARE
    result int[] := ARRAY[]::int[];
    i int;
BEGIN
    FOR i IN 1..array_length(booleans, 1) LOOP
        IF booleans[i] THEN
            result := array_append(result, integers[i]);
        ELSE
            result := array_append(result, -1);
        END IF;
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

DROP FUNCTION IF EXISTS pg_catalog.calculate_coverage(numbers int[]) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.calculate_coverage(numbers int[])
RETURNS float8 AS $$
DECLARE
    count_ge_1 int := 0;
    count_ge_0 int := 0;
    ratio float8;
BEGIN
	FOR i IN 1..array_length(numbers, 1) LOOP
		IF numbers[i] >= 1 THEN
			count_ge_1 := count_ge_1 + 1;
		END IF;
		IF numbers[i] >= 0 THEN
			count_ge_0 := count_ge_0 + 1;
		END IF;
	END LOOP;

	IF count_ge_0 > 0 THEN
		ratio := count_ge_1::float8 / count_ge_0::float8;
	END IF;

    RETURN ratio;
END;
$$ LANGUAGE plpgsql IMMUTABLE;