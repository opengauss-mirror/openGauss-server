With BlacklistCheck As
(
select 
    'BlacklistCheck - unsupported synx - inherit table'as CheckItem, 
    count(1) as Result ,
    '0' as Expected,
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetInheritTable.sql' as Failed_Process_Script
from pg_inherits

union all

select 
    'BlacklistCheck - unsupported synx - create table with oids' as CheckItem, 
    count(1)  as Result, 
    '0'  as Expected,
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetTableWithOids.sql' as Failed_Process_Script
from pg_class where oid > 16384 and relhasoids = true

union all

select 
    'BlacklistCheck - unsupported synx - foreign key constraint' as CheckItem, 
    count(1)  as Result, 
    '0'  as Expected, 
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetTableUseUnsupportConstraint.sql' as Failed_Process_Script
from pg_constraint where contype = 'f'

union all

select 
    'BlacklistCheck - unsupported synx - exclusion constraint'as CheckItem, 
    count(1)  as Result, 
    '0'  as Expected, 
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetTableUseUnsupportConstraint.sql' as Failed_Process_Script
from pg_constraint where contype = 'x'

union all

select 
	'BlacklistCheck - unsupported synx - trigger constraint' as CheckItem, 
    count(1)  as Result, 
    '0'  as Expected, 
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetTableUseUnsupportConstraint.sql' as Failed_Process_Script
from pg_constraint where contype = 't'

union all

select 
    'BlacklistCheck - unsupported synx - unsupported indexs 'as CheckItem, 
    count(1)  as Result, 
    '0'   as Expected, 
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetTableUseUnsupportIndex.sql' as Failed_Process_Script
from pg_indexes where indexdef not like '%btree%' and indexdef not like '%psort%'

union all

select 
    'BlacklistCheck - unsupported synx - unsupported locator type 'as CheckItem, 
    count(1)  as Result, 
    '0'   as Expected, 
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetTableUseUnsupportLocatortype.sql' as Failed_Process_Script
from pgxc_class where pclocatortype not in ('R', 'H') and pcrelid not in (select oid from pg_class where relkind='f') and pcrelid not in  (select oid from pg_class where reloptions::text like '%internal_mask=33029%')

union all

select 
    'BlacklistCheck - unsupported synx - sequence' as CheckItem, 
    count(1)  as Result, 
    '0'   as Expected, 
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetSequenceName.sql' as Failed_Process_Script
from pg_class where relkind = 'S'

union all

SELECT 
    'BlacklistCheck - unsupported synx - to group' as CheckItem, 
    (case when count(distinct pgroup) < 2  then 1 else  count(distinct pgroup) end) as Result, 
    '1'  as Expected, 
    ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
    'GetTableUseToGroup.sql' as Failed_Process_Script
FROM pgxc_class

union all

SELECT
	'BlacklistCheck - unsupported synx - to node' as CheckItem, 
	count(1)  as Result, 
	'0'  as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTableUseTonode.sql' as Failed_Process_Script
FROM pgxc_class where pgroup is null

union all

SELECT 
	'BlacklistCheck - unsupported synx - create extension' as CheckItem, 
	count(1)  as Result, 
	'0'  as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetThirdPartExtension.sql' as Failed_Process_Script
FROM pg_extension where oid > 16384

union all

SELECT 
	'BlacklistCheck - unsupported synx - create rule' as CheckItem, 
	count(1)  as Result, 
	'0'  as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTableUseRule.sql' as Failed_Process_Script
from pg_rewrite where oid > 16384 and rulename != '_RETURN'

union all

SELECT 
	'BlacklistCheck - unsupported synx - create language 'as CheckItem, 
	count(1)  as Result, 
	'0'  as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetUserDefinedLanguage.sql' as Failed_Process_Script
from pg_language where oid > 16384 

union all

select 
	'BlacklistCheck - unsupported datatype - line 'as CheckItem, 
	count(1)  as Result, 
	'0'   as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTable_ProcUseUnsupportDataType.sql' as Failed_Process_Script
from pg_attribute where atttypid in (628, 629)

union all

select 
	'BlacklistCheck - unsupported datatype - xml 'as CheckItem, 
	count(1)  as Result, 
	'0'   as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTable_ProcUseUnsupportDataType.sql' as Failed_Process_Script
from pg_attribute where atttypid in (142)

union all

SELECT 
	'BlacklistCheck - unsupported datatype - reg* 'as CheckItem, 
	count(1)  as Result, 
	'0'   as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTable_ProcUseUnsupportDataType.sql' as Failed_Process_Script
from pg_catalog.pg_attribute  where attisdropped = false  and atttypid in (select oid from pg_type where typname like '%reg%' and typrelid > 0) and attrelid >16384

union all

select 
	'BlacklistCheck - unsupported datatype - pg_node_tree 'as CheckItem, 
	count(1)  as Result, 
	'0'   as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTable_ProcUseUnsupportDataType.sql' as Failed_Process_Script
from pg_attribute where atttypid in (194) and attrelid > 16387

union all

SELECT 
	'BlacklistCheck - unsupported datatype - user defined type 'as CheckItem, 
	count(1)  as Result , 
	'0' as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTable_ProcUseUnsupportDataType.sql' as Failed_Process_Script
FROM pg_catalog.pg_attribute where atttypid > 16384

union all

SELECT
	'BlacklistCheck - unsupported table - HDFS foreign table'as CheckItem, 
	count(1)  as Result , 
	'0' as Expected, 
	ARRAY['V100R005C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetTable_unsupportHDFSForeignTable.sql' as Failed_Process_Script
FROM pg_class a, pg_namespace b, (select ftrelid from pg_foreign_table t, pg_foreign_server s where t.ftserver = s.oid and s. srvoptions is not null) c
where a.oid = c.ftrelid  and a. relnamespace= b.oid

union all

SELECT
	'BlacklistCheck - unsupported synx - user defined aggregate 'as CheckItem, 
	count(1)  as Result , 
	'0' as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetUserDefinedAggregate.sql' as Failed_Process_Script
FROM pg_catalog.pg_proc where proisagg and oid > 16384

union all

SELECT
	'BlacklistCheck - unsupported synx - user defined conversion 'as CheckItem, 
	count(1)  as Result , 
	'0' as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetUserDefinedConversion.sql' as Failed_Process_Script
FROM pg_catalog.pg_conversion where oid > 16384

union all

SELECT
	'BlacklistCheck - unsupported synx - user defined nodegroup'as CheckItem, 
	count(1)  as Result , 
	'1' as Expected, 
	ARRAY['V100R005C10','V100R006C00','V100R006C10'] as Version,
	ARRAY['upgrade','dilatation','replace']  as Action,
	'GetUserDefinedNodeGroup.sql' as Failed_Process_Script
FROM pg_catalog.pgxc_group
),

t as
(
	select
		CheckItem,
		Result,
		Expected,
		CASE WHEN Result=Expected THEN 'SUCCESS' ELSE 'FAILED' END as Status,
		Failed_Process_Script
	from BlacklistCheck
	order by CheckItem
)
select
	CheckItem,
	Result,
    Expected,
	Status,
	Failed_Process_Script
from t 
;
