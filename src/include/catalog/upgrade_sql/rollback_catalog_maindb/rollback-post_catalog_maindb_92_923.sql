DROP AGGREGATE IF EXISTS pg_catalog.array_integer_sum(int[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_integer_agg_add(int[], int[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.calculate_coverage(numbers int[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.coverage_arrays(booleans bool[], integers int[]) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.generate_procoverage_report(int8, int8) CASCADE;