/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "utils/formatting.h"
#include "utils/atomic.h"
#include "replication/slot.h"

THR_LOCAL ProtocolVersion FrontendProtocol = PG_PROTOCOL_LATEST;

THR_LOCAL volatile bool InterruptPending = false;

THR_LOCAL char my_exec_path[MAXPGPATH]; /* full path to my executable */

/*
 * Hook on object accesses.  This is intended as infrastructure for security
 * and logging plugins.
 */
THR_LOCAL object_access_hook_type object_access_hook = NULL;

uint8 ce_cache_refresh_type = 0;

/*
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
THR_LOCAL bool IsUnderPostmaster = false;

volatile ThreadId PostmasterPid = 0;
bool IsPostmasterEnvironment = false;
bool open_join_children = true;
bool will_shutdown = false;


/********************************************
 * 1.HARD-WIRED BINARY VERSION NUMBER
 *
 *    version num range detail for openGauss:
 *      version | start at | release at | reserve to
 *      --------+----------+------------+------------
 *       1.0.X  |  00000   |   92072    |   92086   
 *       1.1.X  |  92087   |   92298    |   92298   
 *       2.0.X  |  92298   |   92298    |   92303   
 *       2.1.X  |  92304   |   92421    |   92423   
 *       3.0.X  |  92424   |   92605    |   92655   
 *       3.1.X  |    -     |     -      |     -     
 *       5.0.X  |  92656   |   92848    |   92898   
 *       NEXT   |  92899   |     ?      |     ?     
 *
 ********************************************/
const uint32 GRAND_VERSION_NUM = 92927;

/********************************************
 * 2.VERSION NUM FOR EACH FEATURE
 *   Please write indescending order.
 ********************************************/
const uint32 PUBLICATION_DDL_VERSION_NUM = 92921;
const uint32 UPSERT_ALIAS_VERSION_NUM = 92920;
const uint32 SUPPORT_GS_DEPENDENCY_VERSION_NUM = 92916;
const uint32 SPQ_VERSION_NUM = 92915;
const uint32 PARTITION_ACCESS_EXCLUSIVE_LOCK_UPGRADE_VERSION = 92913;
const uint32 PAGE_DIST_VERSION_NUM = 92912;
const uint32 NODE_REFORM_INFO_VERSION_NUM = 92911;
const uint32 GB18030_2022_VERSION_NUM = 92908;
const uint32 PARAM_MARK_VERSION_NUM = 92907;
const uint32 TIMESCALE_DB_VERSION_NUM = 92904;
const uint32 MULTI_CHARSET_VERSION_NUM = 92903;
const uint32 NBTREE_INSERT_OPTIMIZATION_VERSION_NUM = 92902;
const uint32 NBTREE_DEDUPLICATION_VERSION_NUM = 92902;
const uint32 SRF_FUSION_VERSION_NUM = 92847;
const uint32 INDEX_HINT_VERSION_NUM = 92845;
const uint32 INNER_UNIQUE_VERSION_NUM = 92845;
const uint32 CREATE_TABLE_AS_VERSION_NUM = 92845;
const uint32 EVENT_VERSION_NUM = 92844;
const uint32 SLOW_SQL_VERSION_NUM = 92844;
const uint32 CHARACTER_SET_VERSION_NUM = 92844;
const uint32 SELECT_INTO_FILE_VERSION_NUM = 92844;
const uint32 PARTITION_ENHANCE_VERSION_NUM = 92844;
const uint32 CREATE_INDEX_IF_NOT_EXISTS_VERSION_NUM = 92843;
const uint32 B_DUMP_TRIGGER_VERSION_NUM = 92843;
const uint32 INSERT_RIGHT_REF_VERSION_NUM = 92842;
const uint32 UNION_NULL_VERSION_NUM = 92841;
const uint32 FDW_SUPPORT_JOIN_AGG_VERSION_NUM = 92839;
const uint32 SUPPORT_VIEW_AUTO_UPDATABLE = 92838;
const uint32 NEGETIVE_BOOL_VERSION_NUM = 92835;
const uint32 SELECT_INTO_VAR_VERSION_NUM = 92834;
const uint32 MAT_VIEW_RECURSIVE_VERSION_NUM = 92833;
const uint32 DOLPHIN_ENABLE_DROP_NUM = 92830;
const uint32 PG_AUTHID_PASSWORDEXT_VERSION_NUM = 92830;
const uint32 SKIP_LOCKED_VERSION_NUM = 92829;
const uint32 REPLACE_INTO_VERSION_NUM = 92828;
const uint32 STANDBY_STMTHIST_VERSION_NUM = 92827;
const uint32 PLAN_SELECT_VERSION_NUM = 92826;
const uint32 MULTI_PARTITIONS_VERSION_NUM = 92825;
const uint32 MULTI_MODIFY_VERSION_NUM = 92814;
const uint32 CSN_TIME_BARRIER_VERSION = 92801;
const uint32 SELECT_STMT_HAS_USERVAR = 92924;

const uint32 SQL_PATCH_VERSION_NUM = 92675;
const uint32 SWITCH_ROLE_VERSION_NUM = 92668;
const uint32 ON_UPDATE_TIMESTAMP_VERSION_NUM = 92664;
const uint32 LOGICAL_DECODE_FLATTEN_TOAST_VERSION_NUM = 92664;
const uint32 COMMENT_SUPPORT_VERSION_NUM = 92662;
const uint32 KEYWORD_IGNORE_COMPART_VERSION_NUM = 92659;
const uint32 ADVANCE_CATALOG_XMIN_VERSION_NUM = 92659;
const uint32 CREATE_FUNCTION_DEFINER_VERSION = 92658;
const uint32 PUBLICATION_INITIAL_DATA_VERSION_NAME = 92657;
const uint32 COMMENT_ROWTYPE_NEST_TABLEOF_VERSION_NUM = 92657;
const uint32 SUBSCRIPTION_BINARY_VERSION_NUM = 92656;
const uint32 INVALID_INVISIBLE_TUPLE_VERSION = 92605;
const uint32 SUPPORT_HASH_XLOG_VERSION_NUM = 92603;

const uint32 PITR_INIT_VERSION_NUM = 92599;
const uint32 DISASTER_READ_VERSION_NUM = 92592;
const uint32 ANALYZER_HOOK_VERSION_NUM = 92592;
const uint32 ENHANCED_TUPLE_LOCK_VERSION_NUM = 92583;
const uint32 PUBLICATION_VERSION_NUM = 92580;
const uint32 SUPPORT_DATA_REPAIR = 92579;
const uint32 WAIT_N_TUPLE_LOCK_VERSION_NUM = 92573;
const uint32 CREATE_INDEX_CONCURRENTLY_DIST_VERSION_NUM = 92569;
const uint32 SCAN_BATCH_MODE_VERSION_NUM = 92568;
const uint32 PARALLEL_DECODE_VERSION_NUM = 92556;
const uint32 HASUID_VERSION_NUM = 92550;
const uint32 V5R2C00_ADVANCE_CATALOG_XMIN_VERSION_NUM = 92525;
const uint32 PBESINGLEPARTITION_VERSION_NUM = 92523;
const uint32 PREDPUSH_SAME_LEVEL_VERSION_NUM = 92522;
const uint32 UPSERT_WHERE_VERSION_NUM = 92514;
const uint32 COMMENT_ROWTYPE_TABLEOF_VERSION_NUM = 92513;
const uint32 LARGE_SEQUENCE_VERSION_NUM = 92511;
const uint32 FUNC_PARAM_COL_VERSION_NUM = 92500;

const uint32 COMMENT_RECORD_PARAM_VERSION_NUM = 92484;
const uint32 PRIVS_DIRECTORY_VERSION_NUM = 92460;
const uint32 SUBPARTITION_VERSION_NUM = 92436;
const uint32 DEFAULT_MAT_CTE_NUM = 92429;
const uint32 SWCB_VERSION_NUM = 92427;
const uint32 MATERIALIZED_CTE_NUM = 92424;
const uint32 TWOPHASE_FILE_VERSION = 92414;
const uint32 BACKEND_VERSION_INCLUDE_NUM = 92412;
const uint32 TDE_VERSION_NUM = 92407;
const uint32 RELMAP_4K_VERSION_NUM = 92403;

const uint32 COMMENT_PCT_TYPE_VERSION_NUM = 92396;
const uint32 COPY_TRANSFORM_VERSION_NUM = 92394;
const uint32 DECODE_ABORT_VERSION_NUM = 92386;
const uint32 CLIENT_ENCRYPTION_PROC_VERSION_NUM = 92383;
const uint32 COMMENT_PROC_VERSION_NUM = 92372;
const uint32 SEGMENT_PAGE_VERSION_NUM = 92360;
const uint32 HINT_ENHANCEMENT_VERSION_NUM = 92359;

const uint32 GENERATED_COL_VERSION_NUM = 92355;
const uint32 INPLACE_UPDATE_VERSION_NUM = 92350;
const uint32 BACKEND_VERSION_PRE_END_NUM = 92350;
const uint32 BACKEND_VERSION_PRE_INCLUDE_NUM = 92305;

const uint32 FIX_SQL_ADD_RELATION_REF_COUNT = 92291;
const uint32 ML_OPT_MODEL_VERSION_NUM = 92284;
const uint32 BACKUP_SLOT_VERSION_NUM = 92282;
const uint32 RANGE_LIST_DISTRIBUTION_VERSION_NUM = 92272;
const uint32 EXTRA_SLOT_VERSION_NUM = 92260;
const uint32 PRIVS_VERSION_NUM = 92259;
const uint32 SUPPORT_GPI_VERSION_NUM = 92257;
const uint32 STP_SUPPORT_COMMIT_ROLLBACK = 92219;
const uint32 MATVIEW_VERSION_NUM = 92213;
const uint32 FUNCNAME_PUSHDOWN_VERSION_NUM = 92202;

const uint32 FIX_PBE_CUSTOME_PLAN_BUG_VERSION_NUM = 92148;
const uint32 EXECUTE_DIRECT_ON_MULTI_VERSION_NUM = 92140;
const uint32 GTMLITE_VERSION_NUM = 92110;

const uint32 PREDPUSH_VERSION_NUM = 92096;
const uint32 SUBLINKPULLUP_VERSION_NUM = 92094;
const uint32 PARTIALPUSH_VERSION_NUM = 92087;

/* This variable indicates wheather the instance is in progress of upgrade as a whole */
uint32 volatile WorkingGrandVersionNum = GRAND_VERSION_NUM;
bool InplaceUpgradePrecommit = false;

#ifdef PGXC
bool useLocalXid = false;
#endif

/* allow to store tables in segment storage while initdb */
bool EnableInitDBSegment = false;

/*
 *     EarlyBindingTLSVariables
 *         Bind static variables to another static TLS variable's address.
 *
 *	   This is needed because of the inability of the compiler: compiler
 *	   complains if you intialize a static TLS variable as another TLS
 *	   variable's address. So we do it for compiler in the earilest stage
 *	   of thread starting once.
 */
void EarlyBindingTLSVariables(void)
{
    static THR_LOCAL bool fDone = false;

    /* This shall be done only once per thread */
    if (!fDone) {
        /* Init number formatting cache */
        Init_NUM_cache();
        /* Init transaction state */
        InitCurrentTransactionState();
    }

    fDone = true;
}
