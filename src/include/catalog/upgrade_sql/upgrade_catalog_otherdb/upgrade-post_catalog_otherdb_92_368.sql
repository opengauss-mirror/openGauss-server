/* add sys fuction hll_duplicatecheck */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4350;
CREATE OR REPLACE FUNCTION pg_catalog.hll_duplicatecheck(HLL) RETURNS INT4 LANGUAGE INTERNAL as 'hll_duplicatecheck';

/* add sys fuction hll_log2explicit */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4378;
CREATE OR REPLACE FUNCTION pg_catalog.hll_log2explicit(HLL) RETURNS INT4 LANGUAGE INTERNAL as 'hll_log2explicit';

/* add sys fuction hll_log2sparse */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4379;
CREATE OR REPLACE FUNCTION pg_catalog.hll_log2sparse(HLL) RETURNS INT4 LANGUAGE INTERNAL as 'hll_log2sparse';
