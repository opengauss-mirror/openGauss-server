#include "compat.h"

#include "commands/cluster.h"
#include "utils.h"

#include "access/skey.h"
#include "commands/extension.h"
#include "access/heapam.h"

#include "knl/knl_session.h"
#include "knl/knl_guc/knl_session_attr_sql.h"
#include "knl/knl_guc/knl_instance_attr_common.h"
#include "knl/knl_thread.h"

#include "utils/syscache.h"
#include "commands/copy.h"

#include "catalog/pg_type_fn.h"
#include "catalog/pg_ts_config.h"

#include "utils/dynamic_loader.h"
#include "utils/globalplancore.h"

#include "compat/tableam.h"
#include "utils/guc_tables.h"
#include "utils/array.h"
#include "commands/explain.h"
#include "compression/compression.h"
#include "compression/array.h"
#include "compression/dictionary.h"
#include "compression/gorilla.h"
#include "compression/deltadelta.h"

#include "catalog/pg_cast.h"
#include "catalog/pg_database.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_language.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_rewrite.h"

#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_foreign_server.h"

#include "storage/fsm_internals.h"
#include "storage/freespace.h"

#include "optimizer/cost.h"





static SlruCtlData MultiXactOffsetCtlData;





#define CastRelationId	2605
#define ConversionRelationId  2607
#define Anum_pg_conversion_conname		1
#define Anum_pg_conversion_connamespace 2
#define Anum_pg_conversion_conowner		3
#define Anum_pg_conversion_conforencoding		4
#define Anum_pg_conversion_contoencoding		5
#define Anum_pg_conversion_conproc		6
#define Anum_pg_conversion_condefault	7
#define DatabaseRelationId	1262
#define LanguageRelationId	2612
#define Anum_pg_language_lanname		1
#define Anum_pg_language_lanowner		2
#define Anum_pg_language_lanispl		3
#define Anum_pg_language_lanpltrusted	4
#define Anum_pg_language_lanplcallfoid	5
#define Anum_pg_language_laninline		6
#define Anum_pg_language_lanvalidator	7
#define Anum_pg_language_lanacl			8
#define Anum_pg_database_datname		1
#define Anum_pg_database_datdba			2
#define Anum_pg_database_encoding		3
#define Anum_pg_database_datcollate		4
#define Anum_pg_database_datctype		5
#define Anum_pg_database_datistemplate	6
#define Anum_pg_database_datallowconn	7
#define Anum_pg_database_datconnlimit	8
#define Anum_pg_database_datlastsysoid	9
#define Anum_pg_database_datfrozenxid	10
#define Anum_pg_database_dattablespace	11
#define Anum_pg_database_compatibility	12
#define Anum_pg_database_datacl			13
#define Anum_pg_database_datfrozenxid64	14
#define Anum_pg_database_datminmxid		15
#define ExtensionRelationId 3079
#define Anum_pg_extension_extname			1
#define Anum_pg_extension_extowner			2
#define Anum_pg_extension_extnamespace		3
#define Anum_pg_extension_extrelocatable	4
#define Anum_pg_extension_extversion		5
#define Anum_pg_extension_extconfig			6
#define Anum_pg_extension_extcondition		7
#define ForeignDataWrapperRelationId	2328
#define Anum_pg_foreign_data_wrapper_fdwname         1
#define Anum_pg_foreign_data_wrapper_fdwowner        2
#define Anum_pg_foreign_data_wrapper_fdwhandler      3
#define Anum_pg_foreign_data_wrapper_fdwvalidator    4
#define Anum_pg_foreign_data_wrapper_fdwacl          5
#define Anum_pg_foreign_data_wrapper_fdwoptions      6
#define TemplateDbOid			1
#define AuthIdRelationId	1260
#define AttrDefaultRelationId  2604
#define AccessMethodOperatorRelationId	2602

#define PROPARALLEL_UNSAFE		'u' 
#define PROPARALLEL_SAFE		's' 

#ifdef MULTIXACT_DEBUG
#define debug_elog2(a,b) elog(a,b)
#define debug_elog3(a,b,c) elog(a,b,c)
#define debug_elog4(a,b,c,d) elog(a,b,c,d)
#define debug_elog5(a,b,c,d,e) elog(a,b,c,d,e)
#define debug_elog6(a,b,c,d,e,f) elog(a,b,c,d,e,f)
#else
#define debug_elog2(a,b)
#define debug_elog3(a,b,c)
#define debug_elog4(a,b,c,d)
#define debug_elog5(a,b,c,d,e)
#define debug_elog6(a,b,c,d,e,f)
#endif


#define MultiXactOffsetCtl	(&MultiXactOffsetCtlData)


#define USEMEM(state,amt)	((state)->availMem -= (amt))
#define LACKMEM(state)		((state)->availMem < 0)
#define FREEMEM(state,amt)	((state)->availMem += (amt))




#define DATATYPE_PAIR(left, right, type1, type2)                                                   \
	((left == type1 && right == type2) || (left == type2 && right == type1))

static const unit_conversion memory_unit_conversion_table[] =
{
	{"TB", GUC_UNIT_KB, 1024 * 1024 * 1024},
	{"GB", GUC_UNIT_KB, 1024 * 1024},
	{"MB", GUC_UNIT_KB, 1024},
	{"kB", GUC_UNIT_KB, 1},

	{"TB", GUC_UNIT_BLOCKS, (1024 * 1024 * 1024) / (BLCKSZ / 1024)},
	{"GB", GUC_UNIT_BLOCKS, (1024 * 1024) / (BLCKSZ / 1024)},
	{"MB", GUC_UNIT_BLOCKS, 1024 / (BLCKSZ / 1024)},
	{"kB", GUC_UNIT_BLOCKS, -(BLCKSZ / 1024)},

	{"TB", GUC_UNIT_XBLOCKS, (1024 * 1024 * 1024) / (XLOG_BLCKSZ / 1024)},
	{"GB", GUC_UNIT_XBLOCKS, (1024 * 1024) / (XLOG_BLCKSZ / 1024)},
	{"MB", GUC_UNIT_XBLOCKS, 1024 / (XLOG_BLCKSZ / 1024)},
	{"kB", GUC_UNIT_XBLOCKS, -(XLOG_BLCKSZ / 1024)},

	{"TB", GUC_UNIT_XSEGS, (1024 * 1024 * 1024) / (XLOG_SEG_SIZE / 1024)},
	{"GB", GUC_UNIT_XSEGS, (1024 * 1024) / (XLOG_SEG_SIZE / 1024)},
	{"MB", GUC_UNIT_XSEGS, -(XLOG_SEG_SIZE / (1024 * 1024))},
	{"kB", GUC_UNIT_XSEGS, -(XLOG_SEG_SIZE / 1024)},

	{""}						/* end of table marker */
}; 


static const unit_conversion time_unit_conversion_table[] =
{
	{"d", GUC_UNIT_MS, 1000 * 60 * 60 * 24},
	{"h", GUC_UNIT_MS, 1000 * 60 * 60},
	{"min", GUC_UNIT_MS, 1000 * 60},
	{"s", GUC_UNIT_MS, 1000},
	{"ms", GUC_UNIT_MS, 1},

	{"d", GUC_UNIT_S, 60 * 60 * 24},
	{"h", GUC_UNIT_S, 60 * 60},
	{"min", GUC_UNIT_S, 60},
	{"s", GUC_UNIT_S, 1},
	{"ms", GUC_UNIT_S, -1000},

	{"d", GUC_UNIT_MIN, 60 * 24},
	{"h", GUC_UNIT_MIN, 60},
	{"min", GUC_UNIT_MIN, 1},
	{"s", GUC_UNIT_MIN, -60},
	{"ms", GUC_UNIT_MIN, -1000 * 60},

	{""}						/* end of table marker */
}; 


#define ARRAY_ALGORITHM_DEFINITION                                                                 \
	{                                                                                              \
		.iterator_init_forward = tsl_array_decompression_iterator_from_datum_forward,              \
		.iterator_init_reverse = tsl_array_decompression_iterator_from_datum_reverse,              \
		.compressed_data_send = array_compressed_send,                                             \
		.compressed_data_recv = array_compressed_recv,                                             \
		.compressor_for_type = array_compressor_for_type,                                          \
		.compressed_data_storage = TOAST_STORAGE_EXTENDED,                                         \
	}
	
#define DICTIONARY_ALGORITHM_DEFINITION                                                            \
	{                                                                                              \
		.iterator_init_forward = tsl_dictionary_decompression_iterator_from_datum_forward,         \
		.iterator_init_reverse = tsl_dictionary_decompression_iterator_from_datum_reverse,         \
		.compressed_data_send = dictionary_compressed_send,                                        \
		.compressed_data_recv = dictionary_compressed_recv,                                        \
		.compressor_for_type = dictionary_compressor_for_type,                                     \
		.compressed_data_storage = TOAST_STORAGE_EXTENDED,                                         \
	}
	
#define GORILLA_ALGORITHM_DEFINITION                                                               \
	{                                                                                              \
		.iterator_init_forward = gorilla_decompression_iterator_from_datum_forward,                \
		.iterator_init_reverse = gorilla_decompression_iterator_from_datum_reverse,                \
		.compressed_data_send = gorilla_compressed_send,                                           \
		.compressed_data_recv = gorilla_compressed_recv,                                           \
		.compressor_for_type = gorilla_compressor_for_type,                                        \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}
	
#define DELTA_DELTA_ALGORITHM_DEFINITION                                                           \
	{                                                                                              \
		.iterator_init_forward = delta_delta_decompression_iterator_from_datum_forward,            \
		.iterator_init_reverse = delta_delta_decompression_iterator_from_datum_reverse,            \
		.compressed_data_send = deltadelta_compressed_send,                                        \
		.compressed_data_recv = deltadelta_compressed_recv,                                        \
		.compressor_for_type = delta_delta_compressor_for_type,                                    \
		.compressed_data_storage = TOAST_STORAGE_EXTERNAL,                                         \
	}

static dlist_head dsm_segment_list = DLIST_STATIC_INIT(dsm_segment_list);




#define RelationAllowsEarlyPruning(rel) \
( \
	 RelationNeedsWAL(rel) \
  && !IsCatalogRelation(rel) \
  && !RelationIsAccessibleInLogicalDecoding(rel) \
  && !RelationHasUnloggedIndex(rel) \
)
#define OLD_SNAPSHOT_TIME_MAP_ENTRIES (old_snapshot_threshold + OLD_SNAPSHOT_PADDING_ENTRIES)

#define MULTIXACT_OFFSETS_PER_PAGE (BLCKSZ / sizeof(MultiXactOffset))


#define MXOffsetToMemberOffset(xid) \
	(MXOffsetToFlagsOffset(xid) + MULTIXACT_FLAGBYTES_PER_GROUP + \
	 ((xid) % MULTIXACT_MEMBERS_PER_MEMBERGROUP) * sizeof(TransactionId))

#define MXOffsetToFlagsOffset(xid) \
	((((xid) / (TransactionId) MULTIXACT_MEMBERS_PER_MEMBERGROUP) % \
	  (TransactionId) MULTIXACT_MEMBERGROUPS_PER_PAGE) * \
	 (TransactionId) MULTIXACT_MEMBERGROUP_SIZE)

#define MXOffsetToFlagsBitShift(xid) \
	(((xid) % (TransactionId) MULTIXACT_MEMBERS_PER_MEMBERGROUP) * \
	 MXACT_MEMBER_BITS_PER_XACT)

#define MXACT_MEMBER_XACT_BITMASK	((1 << MXACT_MEMBER_BITS_PER_XACT) - 1)

#define EarlyPruningEnabled(rel) (old_snapshot_threshold >= 0 && RelationAllowsEarlyPruning(rel))


#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)


#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}

#ifdef XIDCACHE_DEBUG

/* counters for XidCache measurement */
static long xc_by_recent_xmin = 0;
static long xc_by_known_xact = 0;
static long xc_by_my_xact = 0;
static long xc_by_latest_xid = 0;
static long xc_by_main_xid = 0;
static long xc_by_child_xid = 0;
static long xc_by_known_assigned = 0;
static long xc_no_overflow = 0;
static long xc_slow_answer = 0;

#define xc_by_recent_xmin_inc()		(xc_by_recent_xmin++)
#define xc_by_known_xact_inc()		(xc_by_known_xact++)
#define xc_by_my_xact_inc()			(xc_by_my_xact++)
#define xc_by_latest_xid_inc()		(xc_by_latest_xid++)
#define xc_by_main_xid_inc()		(xc_by_main_xid++)
#define xc_by_child_xid_inc()		(xc_by_child_xid++)
#define xc_by_known_assigned_inc()	(xc_by_known_assigned++)
#define xc_no_overflow_inc()		(xc_no_overflow++)
#define xc_slow_answer_inc()		(xc_slow_answer++)

static void DisplayXidCache(void);
#else							/* !XIDCACHE_DEBUG */

#define xc_by_recent_xmin_inc()		((void) 0)
#define xc_by_known_xact_inc()		((void) 0)
#define xc_by_my_xact_inc()			((void) 0)
#define xc_by_latest_xid_inc()		((void) 0)
#define xc_by_main_xid_inc()		((void) 0)
#define xc_by_child_xid_inc()		((void) 0)
#define xc_by_known_assigned_inc()	((void) 0)
#define xc_no_overflow_inc()		((void) 0)
#define xc_slow_answer_inc()		((void) 0)
#endif   /* XIDCACHE_DEBUG */

#define TOTAL_MAX_CACHED_SUBXIDS \
	((PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS)

#define PredicateLockHashPartitionLockByIndex(i) \
	(&t_thrd.shemem_ptr_cxt.mainLWLockArray[PREDICATELOCK_MANAGER_LWLOCK_OFFSET + (i)].lock)

#define AMTYPE_INDEX					'i'		/* index access method */

#define HEAP_MOVED (HEAP_MOVED_OFF | HEAP_MOVED_IN)

#define EarlyPruningEnabled(rel) (old_snapshot_threshold >= 0 && RelationAllowsEarlyPruning(rel))

#define REINDEXOPT_VERBOSE 1 << 0		/* print progress info */



#define PGSTAT_NUM_PROGRESS_PARAM	10
#define SxactIsDoomed(sxact) (((sxact)->flags & SXACT_FLAG_DOOMED) != 0)
#define SxactIsReadOnly(sxact) (((sxact)->flags & SXACT_FLAG_READ_ONLY) != 0)
#define SxactHasSummaryConflictIn(sxact) (((sxact)->flags & SXACT_FLAG_SUMMARY_CONFLICT_IN) != 0)
#define SxactHasSummaryConflictOut(sxact) (((sxact)->flags & SXACT_FLAG_SUMMARY_CONFLICT_OUT) != 0)
#define Anum_pg_class_relrowsecurity		24
#define Anum_pg_class_relforcerowsecurity	25
#define Anum_pg_class_relispopulated		26
#define SxactIsPrepared(sxact) (((sxact)->flags & SXACT_FLAG_PREPARED) != 0)
#define SxactIsReadOnly(sxact) (((sxact)->flags & SXACT_FLAG_READ_ONLY) != 0)
#define SxactIsCommitted(sxact) (((sxact)->flags & SXACT_FLAG_COMMITTED) != 0)
#define SxactHasConflictOut(sxact) (((sxact)->flags & SXACT_FLAG_CONFLICT_OUT) != 0)

#define MultiXactIdGetDatum(X) ((Datum) SET_4_BYTES((X)))
#define ERRCODE_SNAPSHOT_TOO_OLD MAKE_SQLSTATE('7','2','0','0','0')
#define HEAP_MOVED_OFF			0x4000
#define HEAP_MOVED_IN			0x8000	
#define HeapTupleHeaderGetXvac(tup) \
( \
	((tup)->t_infomask & HEAP_MOVED) ? \
		(tup)->t_choice.t_heap.t_field3.t_xvac \
	: \
		InvalidTransactionId \
)



static const uint8 number_of_ones_for_visible[256] = {
	0, 1, 0, 1, 1, 2, 1, 2, 0, 1, 0, 1, 1, 2, 1, 2,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	0, 1, 0, 1, 1, 2, 1, 2, 0, 1, 0, 1, 1, 2, 1, 2,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	2, 3, 2, 3, 3, 4, 3, 4, 2, 3, 2, 3, 3, 4, 3, 4,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	2, 3, 2, 3, 3, 4, 3, 4, 2, 3, 2, 3, 3, 4, 3, 4,
	0, 1, 0, 1, 1, 2, 1, 2, 0, 1, 0, 1, 1, 2, 1, 2,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	0, 1, 0, 1, 1, 2, 1, 2, 0, 1, 0, 1, 1, 2, 1, 2,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	2, 3, 2, 3, 3, 4, 3, 4, 2, 3, 2, 3, 3, 4, 3, 4,
	1, 2, 1, 2, 2, 3, 2, 3, 1, 2, 1, 2, 2, 3, 2, 3,
	2, 3, 2, 3, 3, 4, 3, 4, 2, 3, 2, 3, 3, 4, 3, 4
};
static const uint8 number_of_ones_for_frozen[256] = {
	0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 1, 1, 2, 2,
	0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 1, 1, 2, 2,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 1, 1, 2, 2,
	0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 1, 1, 2, 2,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	2, 2, 3, 3, 2, 2, 3, 3, 3, 3, 4, 4, 3, 3, 4, 4,
	2, 2, 3, 3, 2, 2, 3, 3, 3, 3, 4, 4, 3, 3, 4, 4,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	1, 1, 2, 2, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 3, 3,
	2, 2, 3, 3, 2, 2, 3, 3, 3, 3, 4, 4, 3, 3, 4, 4,
	2, 2, 3, 3, 2, 2, 3, 3, 3, 3, 4, 4, 3, 3, 4, 4
};

static bool
has_parallel_hazard_checker(Oid func_id, void *context)
{
#ifdef OG30
return true;
#endif
	char		proparallel = 'u';

	if (((has_parallel_hazard_arg *) context)->allow_restricted)
		return (proparallel == PROPARALLEL_UNSAFE);
	else
		return (proparallel != PROPARALLEL_SAFE);
} 

static bool
has_parallel_hazard_walker(Node *node, has_parallel_hazard_arg *context)
{
	if (node == NULL)
		return false;

	/* Check for hazardous functions in node itself */
	if (check_functions_in_node(node, has_parallel_hazard_checker,
								context))
		return true;

	/*
	 * It should be OK to treat MinMaxExpr as parallel-safe, since btree
	 * opclass support functions are generally parallel-safe.  XmlExpr is a
	 * bit more dubious but we can probably get away with it.  We err on the
	 * side of caution by treating CoerceToDomain as parallel-restricted.
	 * (Note: in principle that's wrong because a domain constraint could
	 * contain a parallel-unsafe function; but useful constraints probably
	 * never would have such, and assuming they do would cripple use of
	 * parallel query in the presence of domain types.)
	 */
	if (IsA(node, CoerceToDomain))
	{
		if (!context->allow_restricted)
			return true;
	}

	/*
	 * As a notational convenience for callers, look through RestrictInfo.
	 */
	else if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = (RestrictInfo *) node;

		return has_parallel_hazard_walker((Node *) rinfo->clause, context);
	}

	/*
	 * Since we don't have the ability to push subplans down to workers at
	 * present, we treat subplan references as parallel-restricted.  We need
	 * not worry about examining their contents; if they are unsafe, we would
	 * have found that out while examining the whole tree before reduction of
	 * sublinks to subplans.  (Really we should not see SubLink during a
	 * not-allow_restricted scan, but if we do, return true.)
	 */
	else if (IsA(node, SubLink) ||
			 IsA(node, SubPlan) ||
			 IsA(node, AlternativeSubPlan))
	{
		if (!context->allow_restricted)
			return true;
	}

	/*
	 * We can't pass Params to workers at the moment either, so they are also
	 * parallel-restricted.
	 */
	else if (IsA(node, Param))
	{
		if (!context->allow_restricted)
			return true;
	}

	/*
	 * When we're first invoked on a completely unplanned tree, we must
	 * recurse into subqueries so to as to locate parallel-unsafe constructs
	 * anywhere in the tree.
	 */
	else if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		/* SELECT FOR UPDATE/SHARE must be treated as unsafe */
		if (query->rowMarks != NULL)
			return true;

		/* Recurse into subselects */
		return query_tree_walker(query,
								(bool (*)()) has_parallel_hazard_walker,
								 context, 0);
	}

	/* Recurse to check arguments */
	return expression_tree_walker(node,
								 (bool (*)()) has_parallel_hazard_walker,
								  context);
}