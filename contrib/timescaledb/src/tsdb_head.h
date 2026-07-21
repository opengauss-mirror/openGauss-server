#ifndef TSDBHEAD_H
#define TSDBHEAD_H


#include "event_trigger.h"
#include "postmaster/bgworker.h"
#include "catalog/objectaddress.h"
#include "catalog/objectaccess.h"

#include "catalog/pg_constraint.h"
#include "catalog/pg_class.h"
#include "parser/parse_coerce.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "utils/acl.h"
#include "access/htup.h"
#include "lib/ilist.h"
#include "commands/copy.h"
#include "commands/explain.h"
#include "access/slru.h"

#include "utils/evtcache.h"
#include "parser/parse_coerce.h"
#include "access/reloptions.h"
#include "tsdb_shm.h"
#include "event_trigger.h"
#include <libpq/pqsignal.h>

static const struct
{
	LOCKMODE	hwlock;
	int			lockstatus;
	int			updstatus;
} tupleLockExtraInfo[MaxLockTupleMode + 1] =
{
	{							/* LockTupleKeyShare */
		AccessShareLock,
		MultiXactStatusForKeyShare,
		-1						/* KeyShare does not allow updating tuples */
	},
	{							/* LockTupleShare */
		RowShareLock,
		MultiXactStatusForShare,
		-1						/* Share does not allow updating tuples */
	},
	{							/* LockTupleNoKeyExclusive */
		ExclusiveLock,
		MultiXactStatusForNoKeyUpdate,
		MultiXactStatusNoKeyUpdate
	},
	{							/* LockTupleExclusive */
		AccessExclusiveLock,
		MultiXactStatusForUpdate,
		MultiXactStatusUpdate
	}
}; 

#define LockTupleTuplock(rel, tup, mode) \
	LockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock,0) 

#define DEFAULT_PARALLEL_SETUP_COST  1000.0
#define DEFAULT_PARALLEL_TUPLE_COST 0.1
#define DEFAULT_CPU_TUPLE_COST 0.01
#define REINDEX_REL_FORCE_INDEXES_UNLOGGED	0x08

extern bool row_security; 




extern PGDLLIMPORT int NamedLWLockTrancheRequests;


extern PGDLLIMPORT double parallel_setup_cost;
extern PGDLLIMPORT double parallel_tuple_cost;


extern PGDLLIMPORT int old_snapshot_threshold;






#define SizeofTriggerEvent(evt) \
	(((evt)->ate_flags & AFTER_TRIGGER_TUP_BITS) == AFTER_TRIGGER_2CTID ? \
	 sizeof(AfterTriggerEventData) : \
		((evt)->ate_flags & AFTER_TRIGGER_TUP_BITS) == AFTER_TRIGGER_1CTID ? \
		sizeof(AfterTriggerEventDataOneCtid) : \
			sizeof(AfterTriggerEventDataZeroCtids))
#define CHUNK_DATA_START(cptr) ((char *) (cptr) + MAXALIGN(sizeof(AfterTriggerEventChunk)))
#define AFTER_TRIGGER_OFFSET			0x0FFFFFFF
#define INVALID_CONTROL_SLOT		((uint32) -1) 


typedef IndexBuildResult *(*ambuild_function) (Relation heapRelation,
													  Relation indexRelation,
												struct IndexInfo *indexInfo);

typedef bool (*check_function_callback) (Oid func_id, void *context); 

#define VALGRIND_MEMPOOL_ALLOC(context, addr, size)			do {} while (0)
#define AssertNotInCriticalSection(context) \
	Assert(t_thrd.int_cxt.CritSectionCount == 0 || (context)->allowInCritSection)
#define COPYTUP(state,stup,tup) ((*(state)->copytup) (state, stup, tup))

#define PREDICATELOCK_MANAGER_LWLOCK_OFFSET \
	(LOCK_MANAGER_LWLOCK_OFFSET + NUM_LOCK_PARTITIONS)
#define LOCKMODE_from_mxstatus(status) \
			(tupleLockExtraInfo[TUPLOCK_from_mxstatus((status))].hwlock)

#define CONSTRAINT_FOREIGN			'f'
#define HEAP_RELOPT_NAMESPACES { "toast", NULL }
#define X_CLOSE_IMMEDIATE 2



#define X_OPENING 0
#define X_CLOSING 1
#define X_NOWHITESPACE 4
#define InheritsRelationId	2611
#define NUM_PREDICATELOCK_PARTITIONS  (1 << LOG2_NUM_PREDICATELOCK_PARTITIONS)
#define REINDEX_REL_FORCE_INDEXES_UNLOGGED	0x08
#define RI_KEYS_ALL_NULL				0
#define RI_KEYS_SOME_NULL				1
#define RI_KEYS_NONE_NULL				2
#define CLUSTER_SORT 3

#define MAXNUMMESSAGES 4096 
#define EXTNODENAME_MAX_LEN		64
#define RESARRAY_MAX_ARRAY 64 
#define XLH_LOCK_ALL_FROZEN_CLEARED	0x01 
#define VISIBILITYMAP_ALL_FROZEN	0x02
#define ObjectIdAttributeNumber					(-2)

#define PQ_RECV_BUFFER_SIZE 8192 
#define pq_flush() (PqCommMethods->flush())


#define		PROC_VACUUM_FOR_WRAPAROUND	0x08

#define DSM_IMPL_NONE		0
#define DSM_CREATE_NULL_IF_MAXSEGMENTS			0x0001
#define PG_DYNSHMEM_CONTROL_MAGIC		0x9a503d32 
#define SECURITY_NOFORCE_RLS			0x0004
#define MAX_RESOWNER_LOCKS 15 

#define DO_AGGSPLIT_SKIPFINAL(as)	(((as) & AGGSPLITOP_SKIPFINAL) != 0) 
#define DO_AGGSPLIT_DESERIALIZE(as) (((as) & AGGSPLITOP_DESERIALIZE) != 0)
#define DO_AGGSPLIT_COMBINE(as)		(((as) & AGGSPLITOP_COMBINE) != 0)  
#define STD_FUZZ_FACTOR 1.01 
#define PG_SIGNAL_COUNT 32 
#define AFTER_TRIGGER_1CTID		0x40000000 
#define AFTER_TRIGGER_2CTID		0xC0000000 
#define AFTER_TRIGGER_FDW_FETCH			0x80000000 
#define AFTER_TRIGGER_FDW_REUSE			0x00000000 

#define RelationIsPopulated(relation) ((relation)->rd_rel->relispopulated) 

#define F_ARRAY_APPEND 378 
#define EVTTRIGGEROID	3838 

#define FKCONSTR_MATCH_SIMPLE 's' 
#define PG_DIAG_CONSTRAINT_NAME 'n'

#define HEAP_XMAX_EXCL_LOCK		0x0040	/* xmax is exclusive locker */
#define HEAP_XMAX_KEYSHR_LOCK (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_SHARED_LOCK)

#define DEFACLOBJ_RELATION		'r'		/* table, view */
#define DEFACLOBJ_SEQUENCE		'S'		/* sequence */
#define DEFACLOBJ_FUNCTION		'f'		/* function */
#define DEFACLOBJ_TYPE			'T'		/* type */

#define CONSTRAINT_PRIMARY			'p'
#define CONSTRAINT_UNIQUE			'u'
#define CONSTRAINT_EXCLUSION		'x'



#ifdef FDDEBUG
#define DO_DB(A) \
	do { \
		int			_do_db_save_errno = errno; \
		A; \
		errno = _do_db_save_errno; \
	} while (0)
#else
#define DO_DB(A) \
	((void) 0)
#endif

#define OperatorRelationId	2617
#define XLogRecPtrIsInvalid(r)	((r) == InvalidXLogRecPtr)

#define HeapTupleHeaderGetNatts_tsdb(tup) \
	((tup)->t_infomask2 & HEAP_NATTS_MASK) 

#define HeapKeyTest(tuple, \
					tupdesc, \
					nkeys, \
					keys, \
					result) \
do \
{ \
	/* Use underscores to protect the variables passed in as parameters */ \
	int			__cur_nkeys = (nkeys); \
	ScanKey		__cur_keys = (keys); \
 \
	(result) = true; /* may change */ \
	for (; __cur_nkeys--; __cur_keys++) \
	{ \
		Datum	__atp; \
		bool	__isnull; \
		Datum	__test; \
 \
		if (__cur_keys->sk_flags & SK_ISNULL) \
		{ \
			(result) = false; \
			break; \
		} \
 \
		__atp = heap_getattr_tsdb((tuple), \
							 __cur_keys->sk_attno, \
							 (tupdesc), \
							 &__isnull); \
 \
		if (__isnull) \
		{ \
			(result) = false; \
			break; \
		} \
 \
		__test = FunctionCall2Coll(&__cur_keys->sk_func, \
								   __cur_keys->sk_collation, \
								   __atp, __cur_keys->sk_argument); \
 \
		if (!DatumGetBool(__test)) \
		{ \
			(result) = false; \
			break; \
		} \
	} \
} while (0)

#define Natts_pg_inherits				3
#define Anum_pg_inherits_inhrelid		1
#define Anum_pg_inherits_inhparent		2
#define Anum_pg_inherits_inhseqno		3


#define Anum_pg_constraint_conname			1
#define Anum_pg_constraint_connamespace		2
#define Anum_pg_constraint_contype			3
#define Anum_pg_constraint_condeferrable	4
#define Anum_pg_constraint_condeferred		5
#define Anum_pg_constraint_convalidated		6
#define Anum_pg_constraint_conrelid			7
#define Anum_pg_constraint_contypid			8
#define Anum_pg_constraint_conindid			9
#define Anum_pg_constraint_confrelid		10
#define Anum_pg_constraint_confupdtype		11
#define Anum_pg_constraint_confdeltype		12
#define Anum_pg_constraint_confmatchtype	13
#define Anum_pg_constraint_conislocal		14
#define Anum_pg_constraint_coninhcount		15
#define Anum_pg_constraint_connoinherit		16

#define RESARRAY_IS_ARRAY(resarr) ((resarr)->capacity <= RESARRAY_MAX_ARRAY)
#define TRACE_POSTGRESQL_SORT_START(INT1, INT2, INT3, INT4, INT5) 

#define RESARRAY_INIT_SIZE 16


#define OperatorFamilyRelationId  2753
#ifdef HEAPDEBUGALL
#define HEAPDEBUG_1 \
	elog(DEBUG2, "heap_getnext([%s,nkeys=%d],dir=%d) called", \
		 RelationGetRelationName(scan->rs_rd), scan->rs_nkeys, (int) direction)
#define HEAPDEBUG_2 \
	elog(DEBUG2, "heap_getnext returning EOS")
#define HEAPDEBUG_3 \
	elog(DEBUG2, "heap_getnext returning tuple")
#else
#define HEAPDEBUG_1
#define HEAPDEBUG_2
#define HEAPDEBUG_3
#endif   /* !defined(HEAPDEBUGALL) */


#ifndef WIN32
#define SAME_INODE(A,B) ((A).st_ino == (B).inode && (A).st_dev == (B).device)
#else
#define SAME_INODE(A,B) false
#endif

typedef uint32 TriggerFlags; 

typedef void (*PG_init_t) (void);


#define RESARRAY_MAX_ITEMS(capacity) \
	((capacity) <= RESARRAY_MAX_ARRAY ? (capacity) : (capacity)/4 * 3) 


#define RI_MAX_NUMKEYS INDEX_MAX_KEYS
#define pg_attribute_printf(f,a) 


#define Anum_pg_db_role_setting_setdatabase		1
#define Anum_pg_db_role_setting_setrole			2
#define Anum_pg_db_role_setting_setconfig		3


#define MAX_UNIT_LEN		3
#define PolicyRelationId	3256 
#define MQH_INITIAL_BUFSIZE				8192
#define GUC_UNIT_XSEGS			0x4000 
#define VFD_CLOSED (-1)

#define HEAPBLK_TO_OFFSET(x) (((x) % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK)
#define VISIBILITYMAP_VALID_BITS	0x03		/* OR of all valid
#define FilePosIsUnknown(pos) ((pos) < 0)




#define NUM_FIXED_LWLOCKS \
	(PREDICATELOCK_MANAGER_LWLOCK_OFFSET + NUM_PREDICATELOCK_PARTITIONS)

#define LACKMEM(state)		((state)->availMem < 0 && !(state)->batchUsed)

#define SizeOfHeapLockUpdated	(offsetof(xl_heap_lock_updated, flags) + sizeof(uint8))
#define XLOG_HEAP2_LOCK_UPDATED 0x60
#define ALLOCSET_SEPARATE_THRESHOLD  8192


#define LogicalTapeReadExact(tapeset, tapenum, ptr, len) \
	do { \
		if (LogicalTapeRead(tapeset, tapenum, ptr, len) != (size_t) (len)) \
			elog(ERROR, "unexpected end of data"); \
	} while(0)


#define HEAP_XMIN_COMMITTED		0x0100	/* t_xmin committed */
#define HEAP_XMIN_INVALID		0x0200	/* t_xmin invalid/aborted */

#define HeapTupleHeaderGetRawXmin(tup) \
( \
	(tup)->t_choice.t_heap.t_xmin \
) 

#define HeapTupleHeaderGetXmin_tsdb(tup) \
( \
	HeapTupleHeaderXminFrozen(tup) ? \
		FrozenTransactionId : HeapTupleHeaderGetRawXmin(tup) \
) 

#define HeapTupleHeaderGetRawXmax_tsdb(tup) \
( \
	(tup)->t_choice.t_heap.t_xmax \
) 
#define SET_LOCKTAG_SPECULATIVE_INSERTION(locktag,xid,token) \
	((locktag).locktag_field1 = (xid), \
	 (locktag).locktag_field2 = (token),		\
	 (locktag).locktag_field3 = 0, \
	 (locktag).locktag_field4 = 0, \
	 (locktag).locktag_type = 6, \
	 (locktag).locktag_lockmethodid = DEFAULT_LOCKMETHOD)

#define SET_LOCKTAG_TUPLE(locktag, dboid, reloid, bucketid, blocknum, offnum) \
    ((locktag).locktag_field1 = (dboid),                            \
        (locktag).locktag_field2 = (reloid),                        \
        (locktag).locktag_field3 = (blocknum),                      \
        (locktag).locktag_field4 = (offnum),                        \
        (locktag).locktag_field5 = (bucketid),                      \
        (locktag).locktag_type = LOCKTAG_TUPLE,                     \
        (locktag).locktag_lockmethodid = DEFAULT_LOCKMETHOD)




#define HEAP_XMAX_IS_MULTI		0x1000	/* t_xmax is a MultiXactId */
#define HEAP_XMAX_INVALID		0x0800	/* t_xmax invalid/aborted */


#define HEAP_XMAX_IS_LOCKED_ONLY_tsdb(infomask) \
	(((infomask) & HEAP_XMAX_LOCK_ONLY) || \
	 (((infomask) & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK)) 



#define HEAP_LOCKED_UPGRADED(infomask) \
( \
	 ((infomask) & HEAP_XMAX_IS_MULTI) != 0 && \
	 ((infomask) & HEAP_XMAX_LOCK_ONLY) != 0 && \
	 (((infomask) & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)) == 0) \
) 


#define HeapTupleHeaderGetUpdateXid_tsdb(tup,tup_head) \
( \
	(!((tup_head)->t_infomask & HEAP_XMAX_INVALID) && \
	 ((tup_head)->t_infomask & HEAP_XMAX_IS_MULTI) && \
	 !((tup_head)->t_infomask & HEAP_XMAX_LOCK_ONLY)) ? \
		HeapTupleGetUpdateXid(tup) \
	: \
		HeapTupleHeaderGetRawXmax_tsdb(tup_head) \
)

#define UnlockTupleTuplock(rel, tup, mode) \
	UnlockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock) 


#ifndef offsetof
#define offsetof(type, field) ((long)&((type*)0)->field)
#endif /* offsetof */

#define GUC_UNIT				(GUC_UNIT_MEMORY | GUC_UNIT_TIME)



typedef struct Compressor Compressor;
typedef ArrayType Acl;

#define PVC_INCLUDE_WINDOWFUNCS 0x0004	/* include WindowFuncs in output list */
#define PVC_RECURSE_WINDOWFUNCS 0x0008	/* recurse into WindowFunc arguments */

typedef uint32 dsm_handle; 
typedef void (*parallel_worker_main_type) (struct dsm_segment *seg,struct shm_toc *toc); 

#define pg_node_attr(...) 



#define ALLOCSET_SMALL_SIZES \
	ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE 
#define ObjectAddressSubSet(addr, class_id, object_id, object_sub_id) \
	do { \
		(addr).classId = (class_id); \
		(addr).objectId = (object_id); \
		(addr).objectSubId = (object_sub_id); \
	} while (0) 
#define ObjectAddressSet(addr, class_id, object_id) \
	ObjectAddressSubSet(addr, class_id, object_id, 0) 
#ifdef __GNUC__
#define pg_attribute_unused() __attribute__((unused))
#else
#define pg_attribute_unused()
#endif 

#define create_pathtarget(root, tlist) \
	set_pathtarget_cost_width(root, make_pathtarget_from_tlist(tlist)) 

#define DO_AGGSPLIT_SERIALIZE(as)	(((as) & AGGSPLITOP_SERIALIZE) != 0) 



 /* Timestamp limits */
 #define MIN_TIMESTAMP   INT64CONST(-211813488000000000)
 /* == (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY */
 #define END_TIMESTAMP   INT64CONST(9223371331200000000)
 /* == (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY */ 
#define DATETIME_MIN_JULIAN (0)   

typedef void (*pqsigfunc) (int signo); 

extern Datum make_interval(PG_FUNCTION_ARGS);
extern pqsigfunc pqsignal(int signo, pqsigfunc func);

#define PG_SETMASK(mask)	sigprocmask(SIG_SETMASK, mask, NULL)

#define IS_VALID_TIMESTAMP(t)  (MIN_TIMESTAMP <= (t) && (t) < END_TIMESTAMP) 

#define BGWORKER_SHMEM_ACCESS						0x0001
#define BGWORKER_BACKEND_DATABASE_CONNECTION		0x0002
#define BGW_DEFAULT_RESTART_INTERVAL	60
#define BGW_NEVER_RESTART				-1
#define BGW_MAXLEN						64
#define BGW_EXTRALEN					128  



 


#define InvalidPid			(-1)
#define BGW_ACK_QUEUE_SIZE (MAXALIGN(shm_mq_minimum_size + sizeof(int)))
 


#define ERRCODE_ASSERT_FAILURE MAKE_SQLSTATE('P','0','0','0','4')
#define ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED MAKE_SQLSTATE('3','9','P','0','3') 



extern void RunObjectPostAlterHook(Oid classId, Oid objectId, int subId,
					   Oid auxiliaryId, bool is_internal); 
#define InvokeObjectPostAlterHook(classId,objectId,subId)			\
	InvokeObjectPostAlterHookArg((classId),(objectId),(subId),		\
								 InvalidOid,false)

#define InvokeObjectPostAlterHookArg(classId,objectId,subId,		\
									 auxiliaryId,is_internal)		\
	do {															\
		if (object_access_hook)										\
			RunObjectPostAlterHook((classId),(objectId),(subId),	\
								   (auxiliaryId),(is_internal));	\
	} while(0)  


#define CONSTRAINT_TRIGGER			't'
#define ConstraintRelationId  2606
#define TRIGGER_TYPE_BEFORE		(1 << 1) 
#define TRIGGER_TYPE_INSERT				(1 << 2) 

#define Anum_pg_trigger_tgrelid			1
#define Anum_pg_trigger_tgname			2
#define Anum_pg_trigger_tgfoid			3
#define Anum_pg_trigger_tgtype			4
#define Anum_pg_trigger_tgenabled		5
#define Anum_pg_trigger_tgisinternal	6
#define Anum_pg_trigger_tgconstrrelid	7
#define Anum_pg_trigger_tgconstrindid	8
#define Anum_pg_trigger_tgconstraint	9
#define Anum_pg_trigger_tgdeferrable	10
#define Anum_pg_trigger_tginitdeferred	11
#define Anum_pg_trigger_tgnargs			12
#define Anum_pg_trigger_tgattr			13
#define Anum_pg_trigger_tgargs			14
#define Anum_pg_trigger_tgqual			15

/* Bits within tgtype */
#define TRIGGER_TYPE_ROW				(1 << 0)
#define TRIGGER_TYPE_BEFORE				(1 << 1)
#define TRIGGER_TYPE_INSERT				(1 << 2)
#define TRIGGER_TYPE_DELETE				(1 << 3)
#define TRIGGER_TYPE_UPDATE				(1 << 4)
#define TRIGGER_TYPE_TRUNCATE			(1 << 5)
#define TRIGGER_TYPE_INSTEAD			(1 << 6)

#define TRIGGER_TYPE_LEVEL_MASK			(TRIGGER_TYPE_ROW)
#define TRIGGER_TYPE_STATEMENT			0

/* Note bits within TRIGGER_TYPE_TIMING_MASK aren't adjacent */
#define TRIGGER_TYPE_TIMING_MASK \
	(TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD)
#define TRIGGER_TYPE_AFTER				0

#define TRIGGER_TYPE_EVENT_MASK \
	(TRIGGER_TYPE_INSERT | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_TRUNCATE)

/* Macros for manipulating tgtype */
#define TRIGGER_CLEAR_TYPE(type)		((type) = 0)

#define TRIGGER_SETT_ROW(type)			((type) |= TRIGGER_TYPE_ROW)
#define TRIGGER_SETT_STATEMENT(type)	((type) |= TRIGGER_TYPE_STATEMENT)
#define TRIGGER_SETT_BEFORE(type)		((type) |= TRIGGER_TYPE_BEFORE)
#define TRIGGER_SETT_AFTER(type)		((type) |= TRIGGER_TYPE_AFTER)
#define TRIGGER_SETT_INSTEAD(type)		((type) |= TRIGGER_TYPE_INSTEAD)
#define TRIGGER_SETT_INSERT(type)		((type) |= TRIGGER_TYPE_INSERT)
#define TRIGGER_SETT_DELETE(type)		((type) |= TRIGGER_TYPE_DELETE)
#define TRIGGER_SETT_UPDATE(type)		((type) |= TRIGGER_TYPE_UPDATE)
#define TRIGGER_SETT_TRUNCATE(type)		((type) |= TRIGGER_TYPE_TRUNCATE)

#define TriggerRelationId  2620 

#define list_make5_oid(x1,x2,x3,x4,x5)	lcons_oid(x1, list_make4_oid(x2, x3, x4, x5)) 
 
#define Int4LessOperator	97

extern void RunObjectPostCreateHook(Oid classId, Oid objectId, int subId,
						bool is_internal); 
#define InvokeObjectPostCreateHook(classId,objectId,subId)			\
	InvokeObjectPostCreateHookArg((classId),(objectId),(subId),false)
#define InvokeObjectPostCreateHookArg(classId,objectId,subId,is_internal) \
	do {															\
		if (object_access_hook)										\
			RunObjectPostCreateHook((classId),(objectId),(subId),	\
									(is_internal));					\
	} while(0) 

#define heap_getattr_tsdb(tup, attnum, tupleDesc, isnull) \
	( \
		((attnum) > 0) ? \
		( \
			((attnum) > (int) HeapTupleHeaderGetNatts_tsdb((tup)->t_data)) ? \
			( \
				(*(isnull) = true), \
				(Datum)NULL \
			) \
			: \
				fastgetattr((tup), (attnum), (tupleDesc), (isnull)) \
		) \
		: \
			heap_getsysattr((tup), (attnum), (tupleDesc), (isnull)) \
	) 

#define RI_INIT_CONSTRAINTHASHSIZE		64

#define RI_INIT_QUERYHASHSIZE			(RI_INIT_CONSTRAINTHASHSIZE * 4)
#define COMPARETUP(state,a,b)	((*(state)->comparetup) (a, b, state))
#define RUN_FIRST		0
#define HEAPCOMPARE(tup1,tup2) \
	(checkIndex && ((tup1)->tupindex != (tup2)->tupindex || \
					(tup1)->tupindex == HEAP_RUN_NEXT) ? \
	 ((tup1)->tupindex) - ((tup2)->tupindex) : \
	 COMPARETUP(state, tup1, tup2))
#define HEAP_RUN_NEXT	INT_MAX
#define FilePosIsUnknown(pos) ((pos) < 0)
#define TOAST_TUPLES_PER_PAGE	4
#define MaximumBytesPerTuple(tuplesPerPage) \
	MAXALIGN_DOWN((BLCKSZ - \
				   MAXALIGN(SizeOfPageHeaderData + (tuplesPerPage) * sizeof(ItemIdData))) \
				  / (tuplesPerPage))
#define SizeOfHeapLockUpdated	(offsetof(xl_heap_lock_updated, flags) + sizeof(uint8))
#define XLOG_HEAP2_LOCK_UPDATED 0x60
#define ALLOCSET_SEPARATE_THRESHOLD  8192
#define LogicalTapeReadExact(tapeset, tapenum, ptr, len) \
	do { \
		if (LogicalTapeRead(tapeset, tapenum, ptr, len) != (size_t) (len)) \
			elog(ERROR, "unexpected end of data"); \
	} while(0)
#define TAPE_BUFFER_OVERHEAD		(BLCKSZ * 3)
#define WRITETUP(state,tape,stup)	((*(state)->writetup) (state, tape, stup))
#define RUN_SECOND		1
#define cmp_ssup(a, b, ssup) \
	ApplySortComparator((a)->datum1, (a)->isnull1, \
						(b)->datum1, (b)->isnull1, ssup)


#define AFTER_TRIGGER_TUP_BITS			0xC0000000

#define CHECK_STACK_DEPTH() \
	do { \
		if (t_thrd.log_cxt.errordata_stack_depth < 0) \
		{ \
			t_thrd.log_cxt.errordata_stack_depth = -1; \
			ereport(ERROR, (errmsg_internal("errstart was not called"))); \
		} \
	} while (0)
#define RelationAllowsEarlyPruning(rel) \
( \
	 RelationNeedsWAL(rel) \
  && !IsCatalogRelation(rel) \
  && !RelationIsAccessibleInLogicalDecoding(rel) \
  && !RelationHasUnloggedIndex(rel) \
)
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE)
#define HEAPBLK_TO_OFFSET(x) (((x) % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK)
#define AccessMethodProcedureRelationId  2603
#define RewriteRelationId  2618
#define DefaultAclRelationId	826
#define TransformRelationId 3576
#define PG_DIAG_SCHEMA_NAME		's'
#define PG_DIAG_TABLE_NAME		't'
#define PG_DIAG_COLUMN_NAME		'c'
#define PG_DIAG_DATATYPE_NAME	'd'
#define SpinLockAcquire(lock) S_LOCK(lock)

#define OLD_SNAPSHOT_TIME_MAP_ENTRIES (old_snapshot_threshold + OLD_SNAPSHOT_PADDING_ENTRIES)
#define MULTIXACT_MEMBER_SAFE_THRESHOLD		(MaxMultiXactOffset / 2)
#define MULTIXACT_MEMBER_DANGER_THRESHOLD	\
	(MaxMultiXactOffset - MaxMultiXactOffset / 4)
#define PROGRESS_VACUUM_PHASE					0
#define PROGRESS_VACUUM_PHASE_FINAL_CLEANUP		6

struct BackgroundWorkerHandle
{
	int			slot;
	uint64		generation;
}; 

typedef struct BackgroundWorkerHandle BackgroundWorkerHandle; 







typedef void (*create_upper_paths_hook_type) (PlannerInfo *root,
													 UpperRelationKind stage,
													   RelOptInfo *input_rel,
													 RelOptInfo *output_rel); 


extern PGDLLIMPORT create_upper_paths_hook_type create_upper_paths_hook;
typedef UpsertAction OnConflictAction;
#ifndef ONCONFLICT_NONE
#define ONCONFLICT_NONE UPSERT_NONE
#endif

typedef struct ParallelWorkerInfo
{
	BackgroundWorkerHandle *bgwhandle;
	shm_mq_handle *error_mqh;
	int32		pid;
} ParallelWorkerInfo; 

typedef struct ParallelContext
{
	dlist_node	node;
	SubTransactionId subid;
	int			nworkers;
	int			nworkers_launched;
	parallel_worker_main_type entrypoint;
	char	   *library_name;
	char	   *function_name;
	ErrorContextCallback *error_context_stack;
	shm_toc_estimator estimator;
	dsm_segment *seg;
	void	   *private_memory;
	shm_toc    *toc;
	ParallelWorkerInfo *worker;
}ParallelContext; 


typedef struct OnConflictExpr
{
	NodeTag		type;
	OnConflictAction action;	/* DO NOTHING or UPDATE? */

	/* Arbiter */
	List	   *arbiterElems;	/* unique index arbiter list (of
								 * InferenceElem's) */
	Node	   *arbiterWhere;	/* unique index arbiter WHERE clause */
	Oid			constraint;		/* pg_constraint OID for arbiter */

	/* ON CONFLICT UPDATE */
	List	   *onConflictSet;	/* List of ON CONFLICT SET TargetEntrys */
	Node	   *onConflictWhere;	/* qualifiers to restrict UPDATE to */
	int			exclRelIndex;	/* RT index of 'excluded' relation */
	List	   *exclRelTlist;	/* tlist of the EXCLUDED pseudo relation */
} OnConflictExpr; 

typedef struct ModifyTablePath
{
	Path		path;
	CmdType		operation;		/* INSERT, UPDATE, or DELETE */
	bool		canSetTag;		/* do we set the command tag/es_processed? */
	Index		nominalRelation;	/* Parent RT index for use of EXPLAIN */
	List	   *resultRelations;	/* integer list of RT indexes */
	List	   *subpaths;		/* Path(s) producing source data */
	List	   *subroots;		/* per-target-table PlannerInfos */
	List	   *withCheckOptionLists;	/* per-target-table WCO lists */
	List	   *returningLists; /* per-target-table RETURNING tlists */
	List	   *rowMarks;		/* PlanRowMarks (non-locking only) */
	OnConflictExpr *onconflict; /* ON CONFLICT clause, or NULL */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
} ModifyTablePath;


enum CheckEnableRlsResult
{
	RLS_NONE,
	RLS_NONE_ENV,
	RLS_ENABLED
}; 

typedef struct ConstraintAwareAppendState
{
	ExtensiblePlanState csstate;
	Plan *subplan;
	Size num_append_subplans;
} ConstraintAwareAppendState; 

typedef struct ConstraintAwareAppendPath
{
	ExtensiblePath cpath;
} ConstraintAwareAppendPath; 



typedef enum VolatileFunctionStatus
{
	VOLATILITY_UNKNOWN = 0,
	VOLATILITY_VOLATILE,
	VOLATILITY_NOVOLATILE
} VolatileFunctionStatus; 



#define AGGSPLITOP_COMBINE		0x01
#define AGGSPLITOP_SKIPFINAL	0x02 
#define AGGSPLITOP_SERIALIZE	0x04
#define AGGSPLITOP_DESERIALIZE	0x08 

typedef struct AggPath
{
	Path		path;
	Path	   *subpath;		/* path representing input source */
	AggStrategy aggstrategy;	/* basic strategy, see nodes.h */
	AggSplit	aggsplit;		/* agg-splitting mode, see nodes.h */
	double		numGroups;		/* estimated number of groups in input */
	List	   *groupClause;	/* a list of SortGroupClause's */
	List	   *qual;			/* quals (HAVING quals), if any */
} AggPath; 

typedef struct GatherPath
{
	Path		path;
	Path	   *subpath;		/* path for each worker */
	bool		single_copy;	/* path must not be executed >1x */
	int			num_workers;	/* number of workers sought to help */
} GatherPath; 


typedef struct MinMaxAggPath
{
	Path		path;
	List	   *mmaggregates;	/* list of MinMaxAggInfo */
	List	   *quals;			/* HAVING quals, if any */
} MinMaxAggPath; 

typedef enum RoleSpecType
{
	ROLESPEC_CSTRING,			/* role name is stored as a C string */
	ROLESPEC_CURRENT_USER,		/* role spec is CURRENT_USER */
	ROLESPEC_SESSION_USER,		/* role spec is SESSION_USER */
	ROLESPEC_PUBLIC				/* role name is "public" */
} RoleSpecType; 

typedef struct RoleSpec
{
	NodeTag		type;
	RoleSpecType roletype;		/* Type of this rolespec */
	char	   *rolename;		/* filled only for ROLESPEC_CSTRING */
	int			location;		/* token location, or -1 if unknown */
} RoleSpec;




/* ----------------
 *		compiler constants for pg_event_trigger
 * ----------------
 */
#define Natts_pg_event_trigger					6
#define Anum_pg_event_trigger_evtname			1
#define Anum_pg_event_trigger_evtevent			2
#define Anum_pg_event_trigger_evtowner			3
#define Anum_pg_event_trigger_evtfoid			4
#define Anum_pg_event_trigger_evtenabled		5
#define Anum_pg_event_trigger_evttags			6

#define Anum_pg_index_indislive			12








#define AT_REWRITE_ALTER_PERSISTENCE	0x01
#define AT_REWRITE_DEFAULT_VAL			0x02
#define AT_REWRITE_COLUMN_REWRITE		0x04
#define AT_REWRITE_ALTER_OID			0x08

/*
 * EventTriggerData is the node type that is passed as fmgr "context" info
 * when a function is called by the event trigger manager.
 */
#define CALLED_AS_EVENT_TRIGGER(fcinfo) \
	((fcinfo)->context != NULL && IsA((fcinfo)->context, EventTriggerData))


 



typedef enum ReindexObjectType
{
	REINDEX_OBJECT_INDEX,		/* index */
	REINDEX_OBJECT_TABLE,		/* table or materialized view */
	REINDEX_OBJECT_SCHEMA,		/* schema */
	REINDEX_OBJECT_SYSTEM,		/* system catalogs */
	REINDEX_OBJECT_DATABASE		/* database */
} ReindexObjectType; 





typedef enum
{
	BgWorkerStart_PostmasterStart,
	BgWorkerStart_ConsistentState,
	BgWorkerStart_RecoveryFinished
} BgWorkerStartTime;





typedef struct RewriteStateData
{
	Relation	rs_old_rel;		/* source heap */
	Relation	rs_new_rel;		/* destination heap */
	Page		rs_buffer;		/* page currently being built */
	BlockNumber rs_blockno;		/* block where page will go */
	bool		rs_buffer_valid;	/* T if any tuples in buffer */
	bool		rs_use_wal;		/* must we WAL-log inserts? */
	bool		rs_logical_rewrite;		/* do we need to do logical rewriting */
	TransactionId rs_oldest_xmin;		/* oldest xmin used by caller to
										 * determine tuple visibility */
	TransactionId rs_freeze_xid;/* Xid that will be used as freeze cutoff
								 * point */
	TransactionId rs_logical_xmin;		/* Xid that will be used as cutoff
										 * point for logical rewrites */
	MultiXactId rs_cutoff_multi;/* MultiXactId that will be used as cutoff
								 * point for multixacts */
	MemoryContext rs_cxt;		/* for hash tables and entries and tuples in
								 * them */
	XLogRecPtr	rs_begin_lsn;	/* XLogInsertLsn when starting the rewrite */
	HTAB	   *rs_unresolved_tups;		/* unmatched A tuples */
	HTAB	   *rs_old_new_tid_map;		/* unmatched B tuples */
	HTAB	   *rs_logical_mappings;	/* logical remapping files */
	uint32		rs_num_rewrite_mappings;		/* # in memory mappings */
}	RewriteStateData; 


typedef enum CompressionAlgorithms
{
	/* Not a real algorithm, if this does get used, it's a bug in the code */
	_INVALID_COMPRESSION_ALGORITHM = 0,

	COMPRESSION_ALGORITHM_ARRAY,
	COMPRESSION_ALGORITHM_DICTIONARY,
	COMPRESSION_ALGORITHM_GORILLA,
	COMPRESSION_ALGORITHM_DELTADELTA,

	/* When adding an algorithm also add a static assert statement below */
	/* end of real values */
	_END_COMPRESSION_ALGORITHMS,
	_MAX_NUM_COMPRESSION_ALGORITHMS = 128,
} CompressionAlgorithms;

typedef struct ArrayBuildStateArr
{
	MemoryContext mcontext;		/* where all the temp stuff is kept */
	char	   *data;			/* accumulated data */
	bits8	   *nullbitmap;		/* bitmap of is-null flags, or NULL if none */
	int			abytes;			/* allocated length of "data" */
	int			nbytes;			/* number of bytes used so far */
	int			aitems;			/* allocated length of bitmap (in elements) */
	int			nitems;			/* total number of elements in result */
	int			ndims;			/* current dimensions of result */
	int			dims[MAXDIM];
	int			lbs[MAXDIM];
	Oid			array_type;		/* data type of the arrays */
	Oid			element_type;	/* data type of the array elements */
	bool		private_cxt;	/* use private memory context */
} ArrayBuildStateArr; 


typedef struct WindowAggPath
{
	Path		path;
	Path	   *subpath;		/* path representing input source */
	WindowClause *winclause;	/* WindowClause we'll be using */
	List	   *winpathkeys;	/* PathKeys for PARTITION keys + ORDER keys */
} WindowAggPath;

typedef struct SortPath
{
	Path		path;
	Path	   *subpath;		/* path representing input source */
} SortPath; 


typedef struct
{
	/*
	 * This identifier is used when system catalog takes two IDs to identify a
	 * particular tuple of the catalog. It is only used when the caller want
	 * to identify an entry of pg_inherits, pg_db_role_setting or
	 * pg_user_mapping. Elsewhere, InvalidOid should be set.
	 */
	Oid			auxiliary_id;

	/*
	 * If this flag is set, the user hasn't requested that the object be
	 * altered, but we're doing it anyway for some internal reason.
	 * Permissions-checking hooks may want to skip checks if, say, we're alter
	 * the constraints of a temporary heap during CLUSTER.
	 */
	bool		is_internal;
} ObjectAccessPostAlter; 

typedef enum WCOKind
{
	WCO_VIEW_CHECK,				/* WCO on an auto-updatable view */
	WCO_RLS_INSERT_CHECK,		/* RLS INSERT WITH CHECK policy */
	WCO_RLS_UPDATE_CHECK,		/* RLS UPDATE WITH CHECK policy */
	WCO_RLS_CONFLICT_CHECK		/* RLS ON CONFLICT DO UPDATE USING policy */
} WCOKind; 

typedef struct VacuumParams
{
	int			freeze_min_age; /* min freeze age, -1 to use default */
	int			freeze_table_age;		/* age at which to scan whole table */
	int			multixact_freeze_min_age;		/* min multixact freeze age,
												 * -1 to use default */
	int			multixact_freeze_table_age;		/* multixact age at which to
												 * scan whole table */
	bool		is_wraparound;	/* force a for-wraparound vacuum */
	int			log_min_duration;		/* minimum execution threshold in ms
										 * at which  verbose logs are
										 * activated, -1 to use default */
} VacuumParams; 

typedef struct BackgroundWorkerSlot
{
	bool		in_use;
	bool		terminate;
	pid_t		pid;			/* InvalidPid = not started yet; 0 = dead */
	uint64		generation;		/* incremented when slot is recycled */
	BackgroundWorker worker;
} BackgroundWorkerSlot; 

typedef struct BackgroundWorkerArray
{
	int			total_slots;
	BackgroundWorkerSlot slot[FLEXIBLE_ARRAY_MEMBER];
} BackgroundWorkerArray; 

typedef struct
{
	PlannerInfo *root;
	AggSplit	aggsplit;
	AggClauseCosts *costs;
} get_agg_clause_costs_context;






typedef enum IndexAMProperty
{
	AMPROP_UNKNOWN = 0,			/* anything not known to core code */
	AMPROP_ASC,					/* column properties */
	AMPROP_DESC,
	AMPROP_NULLS_FIRST,
	AMPROP_NULLS_LAST,
	AMPROP_ORDERABLE,
	AMPROP_DISTANCE_ORDERABLE,
	AMPROP_RETURNABLE,
	AMPROP_SEARCH_ARRAY,
	AMPROP_SEARCH_NULLS,
	AMPROP_CLUSTERABLE,			/* index properties */
	AMPROP_INDEX_SCAN,
	AMPROP_BITMAP_SCAN,
	AMPROP_BACKWARD_SCAN,
	AMPROP_CAN_ORDER,			/* AM properties */
	AMPROP_CAN_UNIQUE,
	AMPROP_CAN_MULTI_COL,
	AMPROP_CAN_EXCLUDE
} IndexAMProperty; 

/* build empty index */
typedef void (*ambuildempty_function) (Relation indexRelation);

/* insert this tuple */
typedef bool (*aminsert_function) (Relation indexRelation,
											   Datum *values,
											   bool *isnull,
											   ItemPointer heap_tid,
											   Relation heapRelation,
											   IndexUniqueCheck checkUnique);

/* bulk delete */
typedef IndexBulkDeleteResult *(*ambulkdelete_function) (IndexVacuumInfo *info,
												IndexBulkDeleteResult *stats,
											IndexBulkDeleteCallback callback,
													   void *callback_state);

/* post-VACUUM cleanup */
typedef IndexBulkDeleteResult *(*amvacuumcleanup_function) (IndexVacuumInfo *info,
											   IndexBulkDeleteResult *stats);

/* can indexscan return IndexTuples? */
typedef bool (*amcanreturn_function) (Relation indexRelation, int attno);

/* estimate cost of an indexscan */
typedef void (*amcostestimate_function) (struct PlannerInfo *root,
													 struct IndexPath *path,
													 double loop_count,
													 Cost *indexStartupCost,
													 Cost *indexTotalCost,
											   Selectivity *indexSelectivity,
												   double *indexCorrelation);

/* parse index reloptions */
typedef bytea *(*amoptions_function) (Datum reloptions,
												  bool validate);

/* report AM, index, or index column property */
typedef bool (*amproperty_function) (Oid index_oid, int attno,
								  IndexAMProperty prop, const char *propname,
												 bool *res, bool *isnull);

/* validate definition of an opclass for this AM */
typedef bool (*amvalidate_function) (Oid opclassoid);

/* prepare for index scan */
typedef IndexScanDesc (*ambeginscan_function) (Relation indexRelation,
														   int nkeys,
														   int norderbys);

/* (re)start index scan */
typedef void (*amrescan_function) (IndexScanDesc scan,
											   ScanKey keys,
											   int nkeys,
											   ScanKey orderbys,
											   int norderbys);

/* next valid tuple */
typedef bool (*amgettuple_function) (IndexScanDesc scan,
												 ScanDirection direction);

/* fetch all valid tuples */
typedef int64 (*amgetbitmap_function) (IndexScanDesc scan,
												   TIDBitmap *tbm);

/* end index scan */
typedef void (*amendscan_function) (IndexScanDesc scan);

/* mark current scan position */
typedef void (*ammarkpos_function) (IndexScanDesc scan);

/* restore marked scan position */
typedef void (*amrestrpos_function) (IndexScanDesc scan);


typedef struct IndexAmRoutine
{
	NodeTag		type;

	/*
	 * Total number of strategies (operators) by which we can traverse/search
	 * this AM.  Zero if AM does not have a fixed set of strategy assignments.
	 */
	uint16		amstrategies;
	/* total number of support functions that this AM uses */
	uint16		amsupport;
	/* does AM support ORDER BY indexed column's value? */
	bool		amcanorder;
	/* does AM support ORDER BY result of an operator on indexed column? */
	bool		amcanorderbyop;
	/* does AM support backward scanning? */
	bool		amcanbackward;
	/* does AM support UNIQUE indexes? */
	bool		amcanunique;
	/* does AM support multi-column indexes? */
	bool		amcanmulticol;
	/* does AM require scans to have a constraint on the first index column? */
	bool		amoptionalkey;
	/* does AM handle ScalarArrayOpExpr quals? */
	bool		amsearcharray;
	/* does AM handle IS NULL/IS NOT NULL quals? */
	bool		amsearchnulls;
	/* can index storage data type differ from column data type? */
	bool		amstorage;
	/* can an index of this type be clustered on? */
	bool		amclusterable;
	/* does AM handle predicate locks? */
	bool		ampredlocks;
	/* type of data stored in index, or InvalidOid if variable */
	Oid			amkeytype;

	/* interface functions */
	ambuild_function ambuild;
	ambuildempty_function ambuildempty;
	aminsert_function aminsert;
	ambulkdelete_function ambulkdelete;
	amvacuumcleanup_function amvacuumcleanup;
	amcanreturn_function amcanreturn;	/* can be NULL */
	amcostestimate_function amcostestimate;
	amoptions_function amoptions;
	amproperty_function amproperty;		/* can be NULL */
	amvalidate_function amvalidate;
	ambeginscan_function ambeginscan;
	amrescan_function amrescan;
	amgettuple_function amgettuple;		/* can be NULL */
	amgetbitmap_function amgetbitmap;	/* can be NULL */
	amendscan_function amendscan;
	ammarkpos_function ammarkpos;		/* can be NULL */
	amrestrpos_function amrestrpos;		/* can be NULL */
} IndexAmRoutine; 


typedef enum
{
	CEOUC_WAIT,
	CEOUC_NOWAIT,
	CEOUC_LIVELOCK_PREVENTING_WAIT
} CEOUC_WAIT_MODE; 






typedef enum XLTW_Oper
{
	XLTW_None,
	XLTW_Update,
	XLTW_Delete,
	XLTW_Lock,
	XLTW_LockUpdated,
	XLTW_InsertIndex,
	XLTW_InsertIndexUnique,
	XLTW_FetchUpdated,
	XLTW_RecheckExclusionConstr
} XLTW_Oper; 
typedef struct RI_ConstraintInfo
{
	Oid			constraint_id;	/* OID of pg_constraint entry (hash key) */
	bool		valid;			/* successfully initialized? */
	uint32		oidHashValue;	/* hash value of pg_constraint OID */
	NameData	conname;		/* name of the FK constraint */
	Oid			pk_relid;		/* referenced relation */
	Oid			fk_relid;		/* referencing relation */
	char		confupdtype;	/* foreign key's ON UPDATE action */
	char		confdeltype;	/* foreign key's ON DELETE action */
	char		confmatchtype;	/* foreign key's match type */
	int			nkeys;			/* number of key columns */
	int16		pk_attnums[RI_MAX_NUMKEYS];		/* attnums of referenced cols */
	int16		fk_attnums[RI_MAX_NUMKEYS];		/* attnums of referencing cols */
	Oid			pf_eq_oprs[RI_MAX_NUMKEYS];		/* equality operators (PK =
												 * FK) */
	Oid			pp_eq_oprs[RI_MAX_NUMKEYS];		/* equality operators (PK =
												 * PK) */
	Oid			ff_eq_oprs[RI_MAX_NUMKEYS];		/* equality operators (FK =
												 * FK) */
	dlist_node	valid_link;		/* Link in list of valid entries */
} RI_ConstraintInfo;

typedef struct NamedLWLockTrancheRequest
{
	char		tranche_name[NAMEDATALEN];
	int			num_lwlocks;
} NamedLWLockTrancheRequest;

typedef enum
{
	AllocateDescFile,
	AllocateDescPipe,
	AllocateDescDir,
	AllocateDescRawFD
} AllocateDescKind; 
typedef struct AllocateDesc {
    AllocateDescKind kind;
    union {
        FILE* file;
        DIR* dir;
        int sock;
        int fd;
    } desc;
    SubTransactionId create_subid;
} AllocateDesc; 


typedef enum TimeoutId
{
	/* Predefined timeout reasons */
	STARTUP_PACKET_TIMEOUT,
	DEADLOCK_TIMEOUT,
	LOCK_TIMEOUT,
	STATEMENT_TIMEOUT,
	STANDBY_DEADLOCK_TIMEOUT,
	STANDBY_TIMEOUT,
	STANDBY_LOCK_TIMEOUT,
	IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
	/* First user-definable timeout reason */
	USER_TIMEOUT,
	/* Maximum number of timeout reasons */
	MAX_TIMEOUTS = 16
} TimeoutId; 



typedef struct
{
	bool		is_internal;
} ObjectAccessPostCreate; 

CATALOG(pg_policy,3256)
{
	NameData	polname;		/* Policy name. */
	Oid			polrelid;		/* Oid of the relation with policy. */
	char		polcmd;			/* One of ACL_*_CHR, or '*' for all */

#ifdef CATALOG_VARLEN
	Oid			polroles[1];	/* Roles associated with policy, not-NULL */
	pg_node_tree polqual;		/* Policy quals. */
	pg_node_tree polwithcheck;	/* WITH CHECK quals. */
#endif
} FormData_pg_policy; 

typedef FormData_pg_policy *Form_pg_policy;

 



static const int MultiXactStatusLock[MaxMultiXactStatus + 1] =
{
	LockTupleKeyShare,			/* ForKeyShare */
	LockTupleNoKeyExclusive,	/* ForNoKeyUpdate */
	LockTupleExclusive,			/* ForUpdate */
	LockTupleNoKeyExclusive,	/* NoKeyUpdate */
	LockTupleExclusive			/* Update */
}; 

#define TUPLOCK_from_mxstatus(status) \
			(MultiXactStatusLock[(status)])

typedef struct
{
	EventTriggerEvent event;
	List	   *triggerlist;
} EventTriggerCacheEntry;

typedef enum
{
	ETCS_NEEDS_REBUILD,
	ETCS_REBUILD_STARTED,
	ETCS_VALID,
} EventTriggerCacheStateType; 


typedef struct ViewOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	bool		security_barrier;
	int			check_option_offset;
} ViewOptions;

typedef struct ResourceArray
{
	Datum	   *itemsarr;		/* buffer for storing values */
	Datum		invalidval;		/* value that is considered invalid */
	uint32		capacity;		/* allocated length of itemsarr[] */
	uint32		nitems;			/* how many items are stored in items array */
	uint32		maxitems;		/* current limit on nitems before enlarging */
	uint32		lastidx;		/* index of last item returned by GetAny */
} ResourceArray; 

typedef struct ResourceOwnerData
{
	ResourceOwner parent;		/* NULL if no parent (toplevel owner) */
	ResourceOwner firstchild;	/* head of linked list of children */
	ResourceOwner nextchild;	/* next child of same parent */
	const char *name;			/* name (just for debugging) */

	/* We have built-in support for remembering: */
	ResourceArray bufferarr;	/* owned buffers */
	ResourceArray catrefarr;	/* catcache references */
	ResourceArray catlistrefarr;	/* catcache-list pins */
	ResourceArray relrefarr;	/* relcache references */
	ResourceArray planrefarr;	/* plancache references */
	ResourceArray tupdescarr;	/* tupdesc references */
	ResourceArray snapshotarr;	/* snapshot references */
	ResourceArray filearr;		/* open temporary files */
	ResourceArray dsmarr;		/* dynamic shmem segments */

	/* We can remember up to MAX_RESOWNER_LOCKS references to local locks. */
	int			nlocks;			/* number of owned locks */
	LOCALLOCK  *locks[MAX_RESOWNER_LOCKS];		/* list of owned locks */
}	ResourceOwnerData; 

CATALOG(pg_transform,3576)
{
	Oid			trftype;
	Oid			trflang;
	regproc		trffromsql;
	regproc		trftosql;
} FormData_pg_transform;
typedef FormData_pg_transform *Form_pg_transform;

typedef FormData_pg_constraint *Form_pg_constraint;

typedef struct AfterTriggerEventChunk
{
	struct AfterTriggerEventChunk *next;		/* list link */
	char	   *freeptr;		/* start of free space in chunk */
	char	   *endfree;		/* end of free space in chunk */
	char	   *endptr;			/* end of chunk */
	/* event data follows here */
} AfterTriggerEventChunk; 

typedef struct AfterTriggerEventList
{
	AfterTriggerEventChunk *head;
	AfterTriggerEventChunk *tail;
	char	   *tailfree;		/* freeptr of tail chunk */
} AfterTriggerEventList; 

typedef struct SetConstraintTriggerData
{
	Oid			sct_tgoid;
	bool		sct_tgisdeferred;
} SetConstraintTriggerData; 

typedef struct SetConstraintStateData
{
	bool		all_isset;
	bool		all_isdeferred;
	int			numstates;		/* number of trigstates[] entries in use */
	int			numalloc;		/* allocated size of trigstates[] */
	SetConstraintTriggerData trigstates[FLEXIBLE_ARRAY_MEMBER];
} SetConstraintStateData; 
typedef SetConstraintStateData *SetConstraintState; 

typedef struct AfterTriggersData
{
	CommandId	firing_counter; /* next firing ID to assign */
	SetConstraintState state;	/* the active S C state */
	AfterTriggerEventList events;		/* deferred-event list */
	int			query_depth;	/* current query list index */
	AfterTriggerEventList *query_stack; /* events pending from each query */
	Tuplestorestate **fdw_tuplestores;	/* foreign tuples from each query */
	int			maxquerydepth;	/* allocated len of above array */
	MemoryContext event_cxt;	/* memory context for events, if any */

	/* these fields are just for resetting at subtrans abort: */

	SetConstraintState *state_stack;	/* stacked S C states */
	AfterTriggerEventList *events_stack;		/* stacked list pointers */
	int		   *depth_stack;	/* stacked query_depths */
	CommandId  *firing_stack;	/* stacked firing_counters */
	int			maxtransdepth;	/* allocated len of above arrays */
} AfterTriggersData; 

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	CopyState	cstate;			/* CopyStateData for the command */
	uint64		processed;		/* # of tuples processed */
} DR_copy; 


typedef struct
{
	char		extnodename[EXTNODENAME_MAX_LEN];
	const void *extnodemethods;
} ExtensibleNodeEntry;

typedef struct
{
	void		(*comm_reset) (void);
	int			(*flush) (void);
	int			(*flush_if_writable) (void);
	bool		(*is_send_pending) (void);
	int			(*putmessage) (char msgtype, const char *s, size_t len);
	void		(*putmessage_noblock) (char msgtype, const char *s, size_t len);
	void		(*startcopyout) (void);
	void		(*endcopyout) (bool errorAbort);
} PQcommMethods; 

extern PGDLLIMPORT PQcommMethods *PqCommMethods; 

typedef struct
{
	List	   *args;			/* list of (T_String) Values or NIL */
	Oid			funcoids[3];	/* OIDs of trigger functions */
	/* The three function OIDs are stored in the order update, delete, child */
} OldTriggerInfo; 

typedef struct
{
	char		unit[MAX_UNIT_LEN + 1]; /* unit, as a string, like "kB" or
										 * "min" */
	int			base_unit;		/* GUC_UNIT_XXX */
	int			multiplier;		/* If positive, multiply the value with this
								 * for unit -> base_unit conversion.  If
								 * negative, divide (with the absolute value) */
} unit_conversion; 

typedef struct
{
	List	   *varlist;
	int			flags;
} pull_var_clause_context;

typedef struct ArrayIteratorData
{
	/* basic info about the array, set up during array_create_iterator() */
	ArrayType  *arr;			/* array we're iterating through */
	bits8	   *nullbitmap;		/* its null bitmap, if any */
	int			nitems;			/* total number of elements in array */
	int16		typlen;			/* element type's length */
	bool		typbyval;		/* element type's byval property */
	char		typalign;		/* element type's align property */

	/* information about the requested slice size */
	int			slice_ndim;		/* slice dimension, or 0 if not slicing */
	int			slice_len;		/* number of elements per slice */
	int		   *slice_dims;		/* slice dims array */
	int		   *slice_lbound;	/* slice lbound array */
	Datum	   *slice_values;	/* workspace of length slice_len */
	bool	   *slice_nulls;	/* workspace of length slice_len */

	/* current position information, updated on each iteration */
	char	   *data_ptr;		/* our current position in the array */
	int			current_item;	/* the item # we're at in the array */
}			ArrayIteratorData;


typedef struct AfterTriggerEventData
{
	TriggerFlags ate_flags;		/* status bits and offset to shared data */
	ItemPointerData ate_ctid1;	/* inserted, deleted, or old updated tuple */
	ItemPointerData ate_ctid2;	/* new updated tuple */
} AfterTriggerEventData;
typedef struct AfterTriggerSharedData
{
	TriggerEvent ats_event;		/* event type indicator, see trigger.h */
	Oid			ats_tgoid;		/* the trigger's ID */
	Oid			ats_relid;		/* the relation it's on */
	CommandId	ats_firing_id;	/* ID for firing cycle */
	struct AfterTriggersTableData *ats_table;	/* transition table access */
	Bitmapset  *ats_modifiedcols;	/* modified columns */
} AfterTriggerSharedData; 

typedef struct AfterTriggerEventData *AfterTriggerEvent; 
typedef struct AfterTriggerSharedData *AfterTriggerShared; 


typedef void (*on_dsm_detach_callback) (dsm_segment *, Datum arg);
typedef struct
{
	TransactionId xmin;			/* tuple xmin */
	ItemPointerData tid;		/* tuple location in old heap */
} TidHashKey;
typedef struct
{
	TidHashKey	key;			/* expected xmin/old location of B tuple */
	ItemPointerData old_tid;	/* A's location in the old heap */
	HeapTuple	tuple;			/* A's tuple contents */
} UnresolvedTupData;


typedef struct
{
	TidHashKey	key;			/* actual xmin/old location of B tuple */
	ItemPointerData new_tid;	/* where we put it in the new heap */
} OldToNewMappingData;

typedef struct {
    Tuple tuple;  /* the tuple proper */
    Datum datum1; /* value of first key column */
    bool isnull1; /* is first key column NULL? */
    int tupindex; /* see notes above */
} SortTuple;

typedef struct RewriteMappingFile
{
	XLogRecPtr	lsn;
	char		fname[MAXPGPATH];
} RewriteMappingFile;
typedef struct xl_heap_lock_updated
{
	TransactionId xmax;
	OffsetNumber offnum;
	uint8		infobits_set;
	uint8		flags;
} xl_heap_lock_updated;

typedef enum
{
	TSS_INITIAL,				/* Loading tuples; still within memory limit */
	TSS_BOUNDED,				/* Loading tuples into bounded-size heap */
	TSS_BUILDRUNS,				/* Loading tuples; writing to tape */
	TSS_SORTEDINMEM,			/* Sort completed entirely in memory */
	TSS_SORTEDONTAPE,			/* Sort completed, final run is on tape */
	TSS_FINALMERGE				/* Performing final merge on-the-fly */
} TupSortStatus;

typedef int (*SortTupleComparator) (const SortTuple *a, const SortTuple *b,
												Tuplesortstate *state); 
		
struct Tuplesortstate
{
	TupSortStatus status;		/* enumerated value as shown above */
	int			nKeys;			/* number of columns in sort key */
	bool		randomAccess;	/* did caller request random access? */
	bool		bounded;		/* did caller specify a maximum number of
								 * tuples to return? */
	bool		boundUsed;		/* true if we made use of a bounded heap */
	int			bound;			/* if bounded, the maximum number of tuples */
	bool		tuples;			/* Can SortTuple.tuple ever be set? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	int			maxTapes;		/* number of tapes (Knuth's T) */
	int			tapeRange;		/* maxTapes-1 (Knuth's P) */
	MemoryContext sortcontext;	/* memory context holding most sort data */
	MemoryContext tuplecontext; /* sub-context of sortcontext for tuple data */
	LogicalTapeSet *tapeset;	/* logtape.c object for tapes in a temp file */

	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are sorting from the routines that don't need to know it.
	 * They are set up by the tuplesort_begin_xxx routines.
	 *
	 * Function to compare two tuples; result is per qsort() convention, ie:
	 * <0, 0, >0 according as a<b, a=b, a>b.  The API must match
	 * qsort_arg_comparator.
	 */
	SortTupleComparator comparetup;

	/*
	 * Function to copy a supplied input tuple into palloc'd space and set up
	 * its SortTuple representation (ie, set tuple/datum1/isnull1).  Also,
	 * state->availMem must be decreased by the amount of space used for the
	 * tuple copy (note the SortTuple struct itself is not counted).
	 */
	void		(*copytup) (Tuplesortstate *state, SortTuple *stup, void *tup);

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  After writing the tuple,
	 * pfree() the out-of-line data (not the SortTuple struct!), and increase
	 * state->availMem by the amount of memory space thereby released.
	 */
	void		(*writetup) (Tuplesortstate *state, int tapenum,
										 SortTuple *stup);

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  Create a palloc'd copy,
	 * initialize tuple/datum1/isnull1 in the target SortTuple struct, and
	 * decrease state->availMem by the amount of memory space consumed. (See
	 * batchUsed notes for details on how memory is handled when incremental
	 * accounting is abandoned.)
	 */
	void		(*readtup) (Tuplesortstate *state, SortTuple *stup,
										int tapenum, unsigned int len);

	/*
	 * Function to move a caller tuple.  This is usually implemented as a
	 * memmove() shim, but function may also perform additional fix-up of
	 * caller tuple where needed.  Batch memory support requires the movement
	 * of caller tuples from one location in memory to another.
	 */
	void		(*movetup) (void *dest, void *src, unsigned int len);

	/*
	 * This array holds the tuples now in sort memory.  If we are in state
	 * INITIAL, the tuples are in no particular order; if we are in state
	 * SORTEDINMEM, the tuples are in final sorted order; in states BUILDRUNS
	 * and FINALMERGE, the tuples are organized in "heap" order per Algorithm
	 * H.  (Note that memtupcount only counts the tuples that are part of the
	 * heap --- during merge passes, memtuples[] entries beyond tapeRange are
	 * never in the heap and are used to hold pre-read tuples.)  In state
	 * SORTEDONTAPE, the array is not used.
	 */
	SortTuple  *memtuples;		/* array of SortTuple structs */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * Memory for tuples is sometimes allocated in batch, rather than
	 * incrementally.  This implies that incremental memory accounting has
	 * been abandoned.  Currently, this only happens for the final on-the-fly
	 * merge step.  Large batch allocations can store tuples (e.g.
	 * IndexTuples) without palloc() fragmentation and other overhead.
	 */
	bool		batchUsed;

	/*
	 * While building initial runs, this indicates if the replacement
	 * selection strategy is in use.  When it isn't, then a simple hybrid
	 * sort-merge strategy is in use instead (runs are quicksorted).
	 */
	bool		replaceActive;

	/*
	 * While building initial runs, this is the current output run number
	 * (starting at RUN_FIRST).  Afterwards, it is the number of initial runs
	 * we made.
	 */
	int			currentRun;

	/*
	 * Unless otherwise noted, all pointer variables below are pointers to
	 * arrays of length maxTapes, holding per-tape data.
	 */

	/*
	 * These variables are only used during merge passes.  mergeactive[i] is
	 * true if we are reading an input run from (actual) tape number i and
	 * have not yet exhausted that run.  mergenext[i] is the memtuples index
	 * of the next pre-read tuple (next to be loaded into the heap) for tape
	 * i, or 0 if we are out of pre-read tuples.  mergelast[i] similarly
	 * points to the last pre-read tuple from each tape.  mergeavailslots[i]
	 * is the number of unused memtuples[] slots reserved for tape i, and
	 * mergeavailmem[i] is the amount of unused space allocated for tape i.
	 * mergefreelist and mergefirstfree keep track of unused locations in the
	 * memtuples[] array.  The memtuples[].tupindex fields link together
	 * pre-read tuples for each tape as well as recycled locations in
	 * mergefreelist. It is OK to use 0 as a null link in these lists, because
	 * memtuples[0] is part of the merge heap and is never a pre-read tuple.
	 */
	bool	   *mergeactive;	/* active input run source? */
	int		   *mergenext;		/* first preread tuple for each source */
	int		   *mergelast;		/* last preread tuple for each source */
	int		   *mergeavailslots;	/* slots left for prereading each tape */
	int64	   *mergeavailmem;	/* availMem for prereading each tape */
	int			mergefreelist;	/* head of freelist of recycled slots */
	int			mergefirstfree; /* first slot never used in this merge */

	/*
	 * Per-tape batch state, when final on-the-fly merge consumes memory from
	 * just a few large allocations.
	 *
	 * Aside from the general benefits of performing fewer individual retail
	 * palloc() calls, this also helps make merging more cache efficient,
	 * since each tape's tuples must naturally be accessed sequentially (in
	 * sorted order).
	 */
	int64		spacePerTape;	/* Space (memory) for tuples (not slots) */
	char	  **mergetuples;	/* Each tape's memory allocation */
	char	  **mergecurrent;	/* Current offset into each tape's memory */
	char	  **mergetail;		/* Last item's start point for each tape */
	char	  **mergeoverflow;	/* Retail palloc() "overflow" for each tape */

	/*
	 * Variables for Algorithm D.  Note that destTape is a "logical" tape
	 * number, ie, an index into the tp_xxx[] arrays.  Be careful to keep
	 * "logical" and "actual" tape numbers straight!
	 */
	int			Level;			/* Knuth's l */
	int			destTape;		/* current output tape (Knuth's j, less 1) */
	int		   *tp_fib;			/* Target Fibonacci run counts (A[]) */
	int		   *tp_runs;		/* # of real runs on each tape */
	int		   *tp_dummy;		/* # of dummy runs for each tape (D[]) */
	int		   *tp_tapenum;		/* Actual tape numbers (TAPE[]) */
	int			activeTapes;	/* # of active input tapes in merge pass */

	/*
	 * These variables are used after completion of sorting to keep track of
	 * the next tuple to return.  (In the tape case, the tape's current read
	 * position is also critical state.)
	 */
	int			result_tape;	/* actual tape number of finished output */
	int			current;		/* array index (only used if SORTEDINMEM) */
	bool		eof_reached;	/* reached EOF (needed for cursors) */

	/* markpos_xxx holds marked position for mark and restore */
	long		markpos_block;	/* tape block# (only used if SORTEDONTAPE) */
	int			markpos_offset; /* saved "current", or offset in tape block */
	bool		markpos_eof;	/* saved "eof_reached" */

	/*
	 * The sortKeys variable is used by every case other than the hash index
	 * case; it is set by tuplesort_begin_xxx.  tupDesc is only used by the
	 * MinimalTuple and CLUSTER routines, though.
	 */
	TupleDesc	tupDesc;
	SortSupport sortKeys;		/* array of length nKeys */

	/*
	 * This variable is shared by the single-key MinimalTuple case and the
	 * Datum case (which both use qsort_ssup()).  Otherwise it's NULL.
	 */
	SortSupport onlyKey;

	/*
	 * Additional state for managing "abbreviated key" sortsupport routines
	 * (which currently may be used by all cases except the hash index case).
	 * Tracks the intervals at which the optimization's effectiveness is
	 * tested.
	 */
	int64		abbrevNext;		/* Tuple # at which to next check
								 * applicability */

	/*
	 * These variables are specific to the CLUSTER case; they are set by
	 * tuplesort_begin_cluster.
	 */
	IndexInfo  *indexInfo;		/* info about index being used for reference */
	EState	   *estate;			/* for evaluating index expressions */

	/*
	 * These variables are specific to the IndexTuple case; they are set by
	 * tuplesort_begin_index_xxx and used only by the IndexTuple routines.
	 */
	Relation	heapRel;		/* table the index is being built on */
	Relation	indexRel;		/* index being built */

	/* These are specific to the index_btree subcase: */
	bool		enforceUnique;	/* complain if we find duplicate tuples */

	/* These are specific to the index_hash subcase: */
	uint32		hash_mask;		/* mask for sortable part of hash code */

	/*
	 * These variables are specific to the Datum case; they are set by
	 * tuplesort_begin_datum and used only by the DatumTuple routines.
	 */
	Oid			datumType;
	/* we need typelen in order to know how to copy the Datums. */
	int			datumTypeLen;
};




typedef struct
{
	int			eflags;			/* capability flags */
	bool		eof_reached;	/* read has reached EOF */
	int			current;		/* next array index to read */
	int			file;			/* temp file# */
	off_t		offset;			/* byte offset in file */
} TSReadPointer; 

typedef enum
{
	TSS_INMEM,					/* Tuples still fit in memory */
	TSS_WRITEFILE,				/* Writing to temp file */
	TSS_READFILE				/* Reading from temp file */
} TupStoreStatus; 

typedef struct Tuplestorestate
{
	TupStoreStatus status;		/* enumerated value as shown above */
	int			eflags;			/* capability flags (OR of pointers' flags) */
	bool		backward;		/* store extra length words in file? */
	bool		interXact;		/* keep open through transactions? */
	bool		truncated;		/* tuplestore_trim has removed tuples? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	BufFile    *myfile;			/* underlying file, or NULL if none */
	MemoryContext context;		/* memory context for holding tuples */
	ResourceOwner resowner;		/* resowner for holding temp files */

	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are handling from the routines that don't need to know it.
	 * They are set up by the tuplestore_begin_xxx routines.
	 *
	 * (Although tuplestore.c currently only supports heap tuples, I've copied
	 * this part of tuplesort.c so that extension to other kinds of objects
	 * will be easy if it's ever needed.)
	 *
	 * Function to copy a supplied input tuple into palloc'd space. (NB: we
	 * assume that a single pfree() is enough to release the tuple later, so
	 * the representation must be "flat" in one palloc chunk.) state->availMem
	 * must be decreased by the amount of space used.
	 */
	void	   *(*copytup) (Tuplestorestate *state, void *tup);

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  After writing the tuple,
	 * pfree() it, and increase state->availMem by the amount of memory space
	 * thereby released.
	 */
	void		(*writetup) (Tuplestorestate *state, void *tup);

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  Create and return a
	 * palloc'd copy, and decrease state->availMem by the amount of memory
	 * space consumed.
	 */
	void	   *(*readtup) (Tuplestorestate *state, unsigned int len);

	/*
	 * This array holds pointers to tuples in memory if we are in state INMEM.
	 * In states WRITEFILE and READFILE it's not used.
	 *
	 * When memtupdeleted > 0, the first memtupdeleted pointers are already
	 * released due to a tuplestore_trim() operation, but we haven't expended
	 * the effort to slide the remaining pointers down.  These unused pointers
	 * are set to NULL to catch any invalid accesses.  Note that memtupcount
	 * includes the deleted pointers.
	 */
	void	  **memtuples;		/* array of pointers to palloc'd tuples */
	int			memtupdeleted;	/* the first N slots are currently unused */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * These variables are used to keep track of the current positions.
	 *
	 * In state WRITEFILE, the current file seek position is the write point;
	 * in state READFILE, the write position is remembered in writepos_xxx.
	 * (The write position is the same as EOF, but since BufFileSeek doesn't
	 * currently implement SEEK_END, we have to remember it explicitly.)
	 */
	TSReadPointer *readptrs;	/* array of read pointers */
	int			activeptr;		/* index of the active read pointer */
	int			readptrcount;	/* number of pointers currently valid */
	int			readptrsize;	/* allocated length of readptrs array */

	int			writepos_file;	/* file# (valid if READFILE state) */
	off_t		writepos_offset;	/* offset (valid if READFILE state) */
} Tuplestorestate; 


typedef struct vfd
{
	int			fd;				/* current FD, or VFD_CLOSED if none */
	unsigned short fdstate;		/* bitflags for VFD's state */
	ResourceOwner resowner;		/* owner, for automatic cleanup */
	File		nextFree;		/* link to next free VFD, if in freelist */
	File		lruMoreRecently;	/* doubly linked recency-of-use list */
	File		lruLessRecently;
	off_t		seekPos;		/* current logical file position, or -1 */
	off_t		fileSize;		/* current size of file (0 if not temporary) */
	char	   *fileName;		/* name of file, or NULL for unused VFD */
	/* NB: fileName is malloc'd, and must be free'd when closing the VFD */
	int			fileFlags;		/* open(2) flags for (re)opening the file */
	int			fileMode;		/* mode to pass to open(2) */
} Vfd;

typedef struct RI_CompareKey
{
	Oid			eq_opr;			/* the equality operator to apply */
	Oid			typeidd;			/* the data type to apply it to */
} RI_CompareKey;

typedef struct RI_CompareHashEntry
{
	RI_CompareKey key;
	bool		valid;			/* successfully initialized? */
	FmgrInfo	eq_opr_finfo;	/* call info for equality fn */
	FmgrInfo	cast_func_finfo;	/* in case we must coerce input */
} RI_CompareHashEntry;

typedef struct RI_QueryKey
{
	Oid			constr_id;		/* OID of pg_constraint entry */
	int32		constr_queryno; /* query type ID, see RI_PLAN_XXX above */
} RI_QueryKey;
typedef struct RI_QueryHashEntry
{
	RI_QueryKey key;
	SPIPlanPtr	plan;
} RI_QueryHashEntry;

typedef void (*timeout_handler_proc) (void);

typedef struct
{
	bool		allow_restricted;
} has_parallel_hazard_arg;
typedef struct XactLockTableWaitInfo
{
	XLTW_Oper	oper;
	Relation	rel;
	ItemPointer ctid;
} XactLockTableWaitInfo;

typedef struct WaitEvent
{
	int			pos;			/* position in the event data structure */
	uint32		events;			/* triggered events */
	pgsocket	fd;				/* socket fd associated with event */
	void	   *user_data;		/* pointer provided in AddWaitEventToSet */
#ifdef WIN32
	bool		reset;			/* Is reset of the event required? */
#endif
} WaitEvent;

struct WaitEventSet
{
	int			nevents;		/* number of registered events */
	int			nevents_space;	/* maximum number of events in this set */

	/*
	 * Array, of nevents_space length, storing the definition of events this
	 * set is waiting for.
	 */
	WaitEvent  *events;

	/*
	 * If WL_LATCH_SET is specified in any wait event, latch is a pointer to
	 * said latch, and latch_pos the offset in the ->events array. This is
	 * useful because we check the state of the latch before performing doing
	 * syscalls related to waiting.
	 */
	Latch	   *latch;
	int			latch_pos;

#if defined(WAIT_USE_EPOLL)
	int			epoll_fd;
	/* epoll_wait returns events in a user provided arrays, allocate once */
	struct epoll_event *epoll_ret_events;
#elif defined(WAIT_USE_POLL)
	/* poll expects events to be waited on every poll() call, prepare once */
	struct pollfd *pollfds;
#elif defined(WAIT_USE_WIN32)

	/*
	 * Array of windows events. The first element always contains
	 * pgwin32_signal_event, so the remaining elements are offset by one (i.e.
	 * event->pos + 1).
	 */
	HANDLE	   *handles;
#endif
};

typedef struct WaitEventSet WaitEventSet;
typedef struct PendingRelDelete
{
	RelFileNode relnode;		/* relation that may need to be deleted */
	BackendId	backend;		/* InvalidBackendId if not a temp rel */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingRelDelete *next;		/* linked-list link */
} PendingRelDelete;





typedef enum
{
	COLLATE_NONE,				/* expression is of a noncollatable datatype */
	COLLATE_IMPLICIT,			/* collation was derived implicitly */
	COLLATE_CONFLICT,			/* we had a conflict of implicit collations */
	COLLATE_EXPLICIT			/* collation was derived explicitly */
} CollateStrength;

typedef struct _IndexList
{
	Oid			il_heap;
	Oid			il_ind;
	IndexInfo  *il_info;
	struct _IndexList *il_next;
} IndexList;

typedef struct
{
	const char *stmtType;		/* "CREATE SCHEMA" or "ALTER SCHEMA" */
	char	   *schemaname;		/* name of schema */
	RoleSpec   *authrole;		/* owner of schema */
	List	   *sequences;		/* CREATE SEQUENCE items */
	List	   *tables;			/* CREATE TABLE items */
	List	   *views;			/* CREATE VIEW items */
	List	   *indexes;		/* CREATE INDEX items */
	List	   *triggers;		/* CREATE TRIGGER items */
	List	   *grants;			/* GRANT items */
} CreateSchemaStmtContext;

typedef struct
{
	ParseState *pstate;			/* parse state (for error reporting) */
	Oid			collation;		/* OID of current collation, if any */
	CollateStrength strength;	/* strength of current collation choice */
	int			location;		/* location of expr that set collation */
	/* Remaining fields are only valid when strength == COLLATE_CONFLICT */
	Oid			collation2;		/* OID of conflicting collation */
	int			location2;		/* location of expr that set collation2 */
} assign_collations_context;
typedef struct ProcState
{
	/* procPid is zero in an inactive ProcState array entry. */
	pid_t		procPid;		/* PID of backend, for signaling */
	PGPROC	   *proc;			/* PGPROC of backend */
	/* nextMsgNum is meaningless if procPid == 0 or resetState is true. */
	int			nextMsgNum;		/* next message number to read */
	bool		resetState;		/* backend needs to reset its state */
	bool		signaled;		/* backend has been sent catchup signal */
	bool		hasMessages;	/* backend has unread messages */

	/*
	 * Backend only sends invalidations, never receives them. This only makes
	 * sense for Startup process during recovery because it doesn't maintain a
	 * relcache, yet it fires inval messages to allow query backends to see
	 * schema changes.
	 */
	bool		sendOnly;		/* backend only sends, never receives */

	/*
	 * Next LocalTransactionId to use for each idle backend slot.  We keep
	 * this here because it is indexed by BackendId and it is convenient to
	 * copy the value to and from local memory when MyBackendId is set. It's
	 * meaningless in an active ProcState entry.
	 */
	LocalTransactionId nextLXID;
} ProcState;

typedef struct SISeg
{
	/*
	 * General state information
	 */
	int			minMsgNum;		/* oldest message still needed */
	int			maxMsgNum;		/* next message number to be assigned */
	int			nextThreshold;	/* # of messages to call SICleanupQueue */
	int			lastBackend;	/* index of last active procState entry, +1 */
	int			maxBackends;	/* size of procState array */

	slock_t		msgnumLock;		/* spinlock protecting maxMsgNum */

	/*
	 * Circular buffer holding shared-inval messages
	 */
	SharedInvalidationMessage buffer[MAXNUMMESSAGES];

	/*
	 * Per-backend invalidation state info (has MaxBackends entries).
	 */
	ProcState	procState[FLEXIBLE_ARRAY_MEMBER];
} SISeg;

typedef struct OldSnapshotControlData
{
	/*
	 * Variables for old snapshot handling are shared among processes and are
	 * only allowed to move forward.
	 */
	slock_t		mutex_current;	/* protect current_timestamp */
	int64		current_timestamp;		/* latest snapshot timestamp */
	slock_t		mutex_latest_xmin;		/* protect latest_xmin and
										 * next_map_update */
	TransactionId latest_xmin;	/* latest snapshot xmin */
	int64		next_map_update;	/* latest snapshot valid up to */
	slock_t		mutex_threshold;	/* protect threshold fields */
	int64		threshold_timestamp;	/* earlier snapshot is old */
	TransactionId threshold_xid;	/* earlier xid may be gone */

	/*
	 * Keep one xid per minute for old snapshot error handling.
	 *
	 * Use a circular buffer with a head offset, a count of entries currently
	 * used, and a timestamp corresponding to the xid at the head offset.  A
	 * count_used value of zero means that there are no times stored; a
	 * count_used value of OLD_SNAPSHOT_TIME_MAP_ENTRIES means that the buffer
	 * is full and the head must be advanced to add new entries.  Use
	 * timestamps aligned to minute boundaries, since that seems less
	 * surprising than aligning based on the first usage timestamp.  The
	 * latest bucket is effectively stored within latest_xmin.  The circular
	 * buffer is updated when we get a new xmin value that doesn't fall into
	 * the same interval.
	 *
	 * It is OK if the xid for a given time slot is from earlier than
	 * calculated by adding the number of minutes corresponding to the
	 * (possibly wrapped) distance from the head offset to the time of the
	 * head entry, since that just results in the vacuuming of old tuples
	 * being slightly less aggressive.  It would not be OK for it to be off in
	 * the other direction, since it might result in vacuuming tuples that are
	 * still expected to be there.
	 *
	 * Use of an SLRU was considered but not chosen because it is more
	 * heavyweight than is needed for this, and would probably not be any less
	 * code to implement.
	 *
	 * Persistence is not needed.
	 */
	int			head_offset;	/* subscript of oldest tracked time */
	int64		head_timestamp; /* time corresponding to head xid */
	int			count_used;		/* how many slots are in use */
	TransactionId xid_by_minute[FLEXIBLE_ARRAY_MEMBER];
} OldSnapshotControlData;

typedef struct BackgroundWorker_TS {
    SHM_QUEUE           links; /* list link if process is in a list */
    uint64              bgw_id;
    ThreadId            bgw_notify_pid;    /* SIGUSR1 this backend on start/stop */
    BgwHandleStatus     bgw_status;        /* Status of this bgworker */
    uint64              bgw_status_dur;    /* duration in this status */
    BgWorkerErrorData   bgw_edata;         /* error information of a bgworker */
    pg_atomic_uint32    disable_count;     /* indicate whether the bgworker is disabled */
    slist_node          rw_lnode;          /* list link */
    char  bgw_name[64];
    char  bgw_extra[128];
    Datum bgw_main_arg;
    int	bgw_flags;
    int bgw_start_time;/* in seconds, or BGW_NEVER_RESTART */
    int	bgw_restart_time;
    char bgw_library_name[64];/* only if bgw_main is NULL */
    char bgw_function_name[64];/* in seconds, or BGW_NEVER_RESTART */
} BackgroundWorker_TS;

#define BackgroundWorker BackgroundWorker_TS 

extern PGDLLIMPORT BackgroundWorker *MyBgworkerEntry;
#endif
