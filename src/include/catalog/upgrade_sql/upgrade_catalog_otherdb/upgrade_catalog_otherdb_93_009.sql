SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 971;
CREATE OR REPLACE FUNCTION pg_catalog.bin_to_num(VARIADIC bins numeric[]) 
RETURNS numeric LANGUAGE INTERNAL IMMUTABLE STRICT as 'bin_to_num';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 974;
CREATE OR REPLACE FUNCTION pg_catalog.bin_to_num() 
RETURNS numeric LANGUAGE INTERNAL IMMUTABLE as 'bin_to_num_noparam';

comment on function pg_catalog.bin_to_num(VARIADIC bins numeric[]) is 'convert bits to number';
comment on function pg_catalog.bin_to_num() is 'convert bits to number';
