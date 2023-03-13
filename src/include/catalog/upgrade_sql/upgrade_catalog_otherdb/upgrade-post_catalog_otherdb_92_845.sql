DROP FUNCTION IF EXISTS pg_catalog.numeric_sum;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5435;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_sum(internal)
RETURNS numeric
LANGUAGE internal
AS 'numeric_sum';

DROP FUNCTION IF EXISTS pg_catalog.int8_avg_accum_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5439;
CREATE OR REPLACE FUNCTION pg_catalog.int8_avg_accum_numeric(internal, int8)
RETURNS internal
LANGUAGE internal
AS 'int8_avg_accum_numeric';

DROP FUNCTION IF EXISTS pg_catalog.numeric_accum_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5440;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_accum_numeric(internal, numeric)
RETURNS internal
LANGUAGE internal
AS 'numeric_accum_numeric';

DROP FUNCTION IF EXISTS pg_catalog.numeric_avg_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5441;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_avg_numeric(internal)
RETURNS numeric
LANGUAGE internal
AS 'numeric_avg_numeric';

DROP FUNCTION IF EXISTS pg_catalog.numeric_avg_accum_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5442;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_avg_accum_numeric(internal, numeric)
RETURNS _numeric
LANGUAGE internal
AS 'numeric_avg_accum_numeric';

DROP FUNCTION IF EXISTS pg_catalog.numeric_stddev_pop_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5443;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_stddev_pop_numeric(internal)
RETURNS numeric
LANGUAGE internal
AS 'numeric_stddev_pop_numeric';

DROP FUNCTION IF EXISTS pg_catalog.numeric_stddev_samp_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5444;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_stddev_samp_numeric(internal)
RETURNS numeric
LANGUAGE internal
AS 'numeric_stddev_samp_numeric';

DROP FUNCTION IF EXISTS pg_catalog.numeric_var_pop_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5445;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_var_pop_numeric(internal)
RETURNS numeric
LANGUAGE internal
AS 'numeric_var_pop_numeric';

DROP FUNCTION IF EXISTS pg_catalog.numeric_var_samp_numeric;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5446;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_var_samp_numeric(numeric)
RETURNS varchar
LANGUAGE internal
AS 'numeric_var_samp_numeric';