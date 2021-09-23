create schema hll_misc;
set current_schema = hll_misc;

--------------CONTENTS--------------------
--     hyperloglog test cases
------------------------------------------
-- 1. check if inplace upgrade sanity
------------------------------------------

------------------------------------------
-- 1. check inplace upgrade sanity
------------------------------------------

select oid, proname, prorettype, proargtypes, prosrc from pg_proc where proname like 'hll%' order by oid;

select oid, * from pg_type where typname in 
	('hll', '_hll',
	'hll_hashval', '_hll_hashval',
	'hll_trans_type', '_hll_trans_type') order by oid;

with type_oids as
(
	select oid  from pg_type where typname in 
	('hll', '_hll',
	'hll_hashval', '_hll_hashval',
	'hll_trans_type', '_hll_trans_type')
)
select oid, * from pg_operator where oprleft in (select * from type_oids)
								or oprright in  (select * from type_oids)
								order by oid;

select * from pg_aggregate where aggtranstype = (select oid from pg_type where typname = 'hll_trans_type') order by 1;

with type_oids as
(
	select oid  from pg_type where typname in 
	('hll', '_hll',
	'hll_hashval', '_hll_hashval',
	'hll_trans_type', '_hll_trans_type')
)
select * from pg_cast where castsource in ( select * from type_oids )
						or casttarget in ( select * from type_oids ) order by 1;

--final cleaning
drop schema hll_misc cascade;
reset current_schema;
