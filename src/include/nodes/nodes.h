/* -------------------------------------------------------------------------
 *
 * nodes.h
 *	  Definitions for tagged nodes.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/nodes/nodes.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODES_H
#define NODES_H
#ifdef FRONTEND_PARSER
#include "feparser_memutils.h"
#endif
/*
 * The first field of every node is NodeTag. Each node created (with makeNode)
 * will have one of the following tags as the value of its first field.
 *
 * Note that the numbers of the node tags are not contiguous. We left holes
 * here so that we can add more tags without changing the existing enum's.
 * (Since node tag numbers never exist outside backend memory, there's no
 * real harm in renumbering, it just costs a full rebuild ...)
 */
typedef enum NodeTag {
    T_Invalid = 0,

    /*
     * TAGS FOR EXECUTOR NODES (execnodes.h)
     */
    T_IndexInfo = 10,
    T_ExprContext,
    T_ProjectionInfo,
    T_JunkFilter,
    T_ResultRelInfo,
    T_EState,
    T_TupleTableSlot,

    /*
     * TAGS FOR PLAN NODES (plannodes.h)
     */
    T_Plan = 100,
    T_BaseResult,
    T_ModifyTable,
    T_Append,
    T_PartIterator,
    T_MergeAppend,
    T_RecursiveUnion,
    T_StartWithOp,
    T_BitmapAnd,
    T_BitmapOr,
    T_Scan,
    T_SeqScan,
    T_IndexScan,
    T_IndexOnlyScan,
    T_BitmapIndexScan,
    T_BitmapHeapScan,
    T_TidScan,
    T_SubqueryScan,
    T_FunctionScan,
    T_ValuesScan,
    T_CteScan,
    T_WorkTableScan,
    T_ForeignScan,
    T_ExtensiblePlan,
    T_Join,
    T_NestLoop,
    T_MergeJoin,
    T_HashJoin,
    T_Material,
    T_Sort,
    T_Group,
    T_Agg,
    T_WindowAgg,
    T_Unique,
    T_Hash,
    T_SetOp,
    T_LockRows,
    T_Limit,
    T_Stream,
#ifdef PGXC
    /*
     * TAGS FOR PGXC NODES
     * (planner.h, locator.h, nodemgr.h, groupmgr.h)
     */
    T_ExecNodes,
    T_SliceBoundary,
    T_ExecBoundary,
    T_SimpleSort,
    T_RemoteQuery,
    T_PGXCNodeHandle,
    T_AlterNodeStmt,
    T_CreateNodeStmt,
    T_DropNodeStmt,
    T_AlterCoordinatorStmt,
    T_CreateGroupStmt,
    T_AlterGroupStmt,
    T_DropGroupStmt,
    T_CreateResourcePoolStmt,
    T_AlterResourcePoolStmt,
    T_DropResourcePoolStmt,
    T_AlterGlobalConfigStmt,
    T_DropGlobalConfigStmt,
    T_CreateWorkloadGroupStmt,
    T_AlterWorkloadGroupStmt,
    T_DropWorkloadGroupStmt,
    T_CreateAppWorkloadGroupMappingStmt,
    T_AlterAppWorkloadGroupMappingStmt,
    T_DropAppWorkloadGroupMappingStmt,
#endif
    /* these aren't subclasses of Plan: */
    T_NestLoopParam,
    T_PartIteratorParam,
    T_PlanRowMark,
    T_PlanInvalItem,
    T_FuncInvalItem,
    /* TAGS FOR POLICY LABEL */
    T_PolicyFilterNode,
    T_CreatePolicyLabelStmt,
    T_AlterPolicyLabelStmt,
    T_DropPolicyLabelStmt,
    T_CreateAuditPolicyStmt,
    T_AlterAuditPolicyStmt,
    T_DropAuditPolicyStmt,
    T_MaskingPolicyCondition,
    T_CreateMaskingPolicyStmt,
    T_AlterMaskingPolicyStmt,
    T_DropMaskingPolicyStmt,
    T_CreateSecurityPolicyStmt,
	T_AlterSecurityPolicyStmt,
	T_DropSecurityPolicyStmt,
    T_AlterSchemaStmt,
    /*
     * TAGS FOR PLAN STATE NODES (execnodes.h)
     *
     * These should correspond one-to-one with Plan node types.
     */
    T_PlanState = 200,
    T_ResultState,
    T_VecToRowState,
    T_MergeActionState,
    T_ModifyTableState,
    T_DistInsertSelectState,
    T_AppendState,
    T_PartIteratorState,
    T_MergeAppendState,
    T_RecursiveUnionState,
    T_StartWithOpState,
    T_BitmapAndState,
    T_BitmapOrState,
    T_ScanState,
    T_SeqScanState,
    T_IndexScanState,
    T_IndexOnlyScanState,
    T_BitmapIndexScanState,
    T_BitmapHeapScanState,
    T_TidScanState,
    T_SubqueryScanState,
    T_FunctionScanState,
    T_ValuesScanState,
    T_CteScanState,
    T_WorkTableScanState,
    T_ForeignScanState,
    T_ExtensiblePlanState,
    T_JoinState,
    T_NestLoopState,
    T_MergeJoinState,
    T_HashJoinState,
    T_MaterialState,
    T_SortState,
    T_GroupState,
    T_AggState,
    T_WindowAggState,
    T_UniqueState,
    T_HashState,
    T_SetOpState,
    T_LockRowsState,
    T_LimitState,
#ifdef PGXC
    T_RemoteQueryState,
#endif
    T_TrainModelState,
    T_StreamState,

    /*
     * TAGS FOR PRIMITIVE NODES (primnodes.h)
     */
    T_Alias = 300,
    T_RangeVar,
    T_Expr,
    T_Var,
    T_Const,
    T_Param,
    T_Aggref,
    T_GroupingFunc,
    T_WindowFunc,
    T_InitList,
    T_ArrayRef,
    T_FuncExpr,
    T_NamedArgExpr,
    T_OpExpr,
    T_DistinctExpr,
    T_NullIfExpr,
    T_ScalarArrayOpExpr,
    T_BoolExpr,
    T_SubLink,
    T_SubPlan,
    T_AlternativeSubPlan,
    T_FieldSelect,
    T_FieldStore,
    T_RelabelType,
    T_CoerceViaIO,
    T_ArrayCoerceExpr,
    T_ConvertRowtypeExpr,
    T_CollateExpr,
    T_CaseExpr,
    T_CaseWhen,
    T_CaseTestExpr,
    T_ArrayExpr,
    T_RowExpr,
    T_RowCompareExpr,
    T_CoalesceExpr,
    T_MinMaxExpr,
    T_XmlExpr,
    T_NullTest,
    T_BooleanTest,
    T_CoerceToDomain,
    T_CoerceToDomainValue,
    T_SetToDefault,
    T_CurrentOfExpr,
    T_TargetEntry,
    T_RangeTblRef,
    T_JoinExpr,
    T_FromExpr,
	T_UpsertExpr,
    T_IntoClause,
    T_IndexVar,
#ifdef PGXC
    T_DistributeBy,
    T_PGXCSubCluster,
    T_DistState,
    T_ListSliceDefState,
#endif
    T_HashFilter,
    T_EstSPNode,
    T_Rownum,
    T_PseudoTargetEntry,

    /*
     * TAGS FOR EXPRESSION STATE NODES (execnodes.h)
     *
     * These correspond (not always one-for-one) to primitive nodes derived
     * from Expr.
     */
    T_ExprState = 400,
    T_GenericExprState,
    T_AggrefExprState,
    T_GroupingFuncExprState,
    T_WindowFuncExprState,
    T_ArrayRefExprState,
    T_FuncExprState,
    T_ScalarArrayOpExprState,
    T_BoolExprState,
    T_SubPlanState,
    T_AlternativeSubPlanState,
    T_FieldSelectState,
    T_FieldStoreState,
    T_CoerceViaIOState,
    T_ArrayCoerceExprState,
    T_ConvertRowtypeExprState,
    T_CaseExprState,
    T_CaseWhenState,
    T_ArrayExprState,
    T_RowExprState,
    T_RowCompareExprState,
    T_CoalesceExprState,
    T_MinMaxExprState,
    T_XmlExprState,
    T_NullTestState,
    T_HashFilterState,
    T_CoerceToDomainState,
    T_DomainConstraintState,
    T_WholeRowVarExprState, /* will be in a more natural position in 9.3 */
    T_RangePartitionDefState,
    T_IntervalPartitionDefState,
    T_PartitionState,
    T_RangePartitionindexDefState,
    T_SplitPartitionState,
    T_AddPartitionState,
    T_AddSubPartitionState,
    T_RangePartitionStartEndDefState,
    T_RownumState,
    T_ListPartitionDefState,
    T_HashPartitionDefState,

    /*
     * TAGS FOR PLANNER NODES (relation.h)
     */
    T_PlannerInfo = 500,
    T_PlannerGlobal,
    T_RelOptInfo,
    T_IndexOptInfo,
    T_ParamPathInfo,
    T_Path,
    T_IndexPath,
    T_BitmapHeapPath,
    T_BitmapAndPath,
    T_BitmapOrPath,
    T_NestPath,
    T_MergePath,
    T_HashPath,
    T_TidPath,
    T_ForeignPath,
    T_ExtensiblePath,
    T_AppendPath,
    T_MergeAppendPath,
    T_ResultPath,
    T_MaterialPath,
    T_UniquePath,
    T_PartIteratorPath,
    T_EquivalenceClass,
    T_EquivalenceMember,
    T_PathKey,
    T_RestrictInfo,
    T_PlaceHolderVar,
    T_SpecialJoinInfo,
    T_LateralJoinInfo,
    T_AppendRelInfo,
    T_PlaceHolderInfo,
    T_MinMaxAggInfo,
    T_PlannerParamItem,
#ifdef PGXC
    T_RemoteQueryPath,
#endif /* PGXC */
    T_StreamPath,
    T_MergeAction,
	T_UpsertState,
    T_SubqueryScanPath,

    /*
     * TAGS FOR MEMORY NODES (memnodes.h)
     */
    T_MemoryContext = 600,
    T_AllocSetContext,
    T_AsanSetContext,
    T_StackAllocSetContext,
    T_SharedAllocSetContext,
    T_MemalignAllocSetContext,
    T_MemalignSharedAllocSetContext,

    T_MemoryTracking,

    /*
     * TAGS FOR VALUE NODES (value.h)
     */
    T_Value = 650,
    T_Integer,
    T_Float,
    T_String,
    T_BitString,
    T_Null,

    /*
     * TAGS FOR LIST NODES (pg_list.h)
     */
    T_List,
    T_IntList,
    T_OidList,

    /*
     * TAGS FOR DOUBLE LIST NODES (pg_list.h)
     */
    T_DList,
    T_IntDList,
    T_OidDList,

    /*
     * TAGS FOR STATEMENT NODES (mostly in parsenodes.h)
     */
    T_Query = 700,
    T_PlannedStmt,
    T_InsertStmt,
    T_DeleteStmt,
    T_UpdateStmt,
    T_MergeStmt,
    T_SelectStmt,
    T_AlterTableStmt,
    T_AlterTableCmd,
    T_AlterDomainStmt,
    T_SetOperationStmt,
    T_GrantStmt,
    T_GrantRoleStmt,
    T_GrantDbStmt,
    T_AlterDefaultPrivilegesStmt,
    T_ClosePortalStmt,
    T_ClusterStmt,
    T_CopyStmt,
    T_CreateStmt,
    T_DefineStmt,
    T_DropStmt,
#ifdef ENABLE_MOT
    T_DropForeignStmt,
#endif
    T_TruncateStmt,
    T_PurgeStmt,
    T_TimeCapsuleStmt,
    T_CommentStmt,
    T_FetchStmt,
    T_IndexStmt,
    T_CreateFunctionStmt,
    T_AlterFunctionStmt,
    T_DoStmt,
    T_RenameStmt,
    T_RuleStmt,
    T_NotifyStmt,
    T_ListenStmt,
    T_UnlistenStmt,
    T_TransactionStmt,
    T_ViewStmt,
    T_LoadStmt,
    T_CreateDomainStmt,
    T_CreatedbStmt,
    T_DropdbStmt,
    T_VacuumStmt,
    T_ExplainStmt,
    T_CreateTableAsStmt,
    T_CreateSeqStmt,
    T_AlterSeqStmt,
    T_VariableSetStmt,
    T_VariableShowStmt,
    T_ShutdownStmt,
    T_DiscardStmt,
    T_CreateTrigStmt,
    T_CreatePLangStmt,
    T_CreateRoleStmt,
    T_AlterRoleStmt,
    T_DropRoleStmt,
    T_LockStmt,
    T_ConstraintsSetStmt,
    T_ReindexStmt,
    T_CheckPointStmt,
#ifdef PGXC
    T_BarrierStmt,
#endif
    T_CreateSchemaStmt,
    T_AlterDatabaseStmt,
    T_AlterDatabaseSetStmt,
    T_AlterRoleSetStmt,
    T_CreateConversionStmt,
    T_CreateCastStmt,
    T_CreateOpClassStmt,
    T_CreateOpFamilyStmt,
    T_AlterOpFamilyStmt,
    T_PrepareStmt,
    T_ExecuteStmt,
    T_DeallocateStmt,
    T_DeclareCursorStmt,
    T_CreateTableSpaceStmt,
    T_DropTableSpaceStmt,
    T_AlterObjectSchemaStmt,
    T_AlterOwnerStmt,
    T_DropOwnedStmt,
    T_ReassignOwnedStmt,
    T_CompositeTypeStmt,
    T_TableOfTypeStmt,
    T_CreateEnumStmt,
    T_CreateRangeStmt,
    T_AlterEnumStmt,
    T_AlterTSDictionaryStmt,
    T_AlterTSConfigurationStmt,
    T_CreateFdwStmt,
    T_AlterFdwStmt,
    T_CreateForeignServerStmt,
    T_AlterForeignServerStmt,
    T_CreateUserMappingStmt,
    T_AlterUserMappingStmt,
    T_DropUserMappingStmt,
    T_ExecDirectStmt,
    T_CleanConnStmt,
    T_AlterTableSpaceOptionsStmt,
    T_SecLabelStmt,
    T_CreateForeignTableStmt,
    T_CreateExtensionStmt,
    T_AlterExtensionStmt,
    T_AlterExtensionContentsStmt,
    T_CreateDataSourceStmt,
    T_AlterDataSourceStmt,
    T_ReplicaIdentityStmt,
    T_CreateDirectoryStmt,
    T_DropDirectoryStmt,
    T_CreateRlsPolicyStmt,
    T_AlterRlsPolicyStmt,
    T_RefreshMatViewStmt,
#ifndef ENABLE_MULTIPLE_NODES
    T_AlterSystemStmt,
#endif
    T_CreateWeakPasswordDictionaryStmt,
    T_DropWeakPasswordDictionaryStmt,
    T_CreatePackageStmt,
    T_CreatePackageBodyStmt,
    T_AddTableIntoCBIState,
	T_CreatePublicationStmt,
	T_AlterPublicationStmt,
	T_CreateSubscriptionStmt,
	T_AlterSubscriptionStmt,
	T_DropSubscriptionStmt,
    
    /*
     * TAGS FOR PARSE TREE NODES (parsenodes.h)
     */
    T_A_Expr = 900,
    T_ColumnRef,
    T_ParamRef,
    T_A_Const,
    T_FuncCall,
    T_A_Star,
    T_A_Indices,
    T_A_Indirection,
    T_A_ArrayExpr,
    T_ResTarget,
    T_TypeCast,
    T_CollateClause,
    T_SortBy,
    T_WindowDef,
    T_RangeSubselect,
    T_RangeFunction,
    T_RangeTableSample,
    T_RangeTimeCapsule,
    T_TypeName,
    T_ColumnDef,
    T_IndexElem,
    T_Constraint,
    T_DefElem,
    T_RangeTblEntry,
    T_TableSampleClause,
    T_TimeCapsuleClause,
    T_SortGroupClause,
    T_GroupingSet,
    T_WindowClause,
    T_PrivGrantee,
    T_FuncWithArgs,
    T_AccessPriv,
    T_DbPriv,
    T_CreateOpClassItem,
    T_TableLikeClause,
    T_FunctionParameter,
    T_LockingClause,
    T_RowMarkClause,
    T_XmlSerialize,
    T_WithClause,
    T_CommonTableExpr,
    T_StartWithOptions,
    T_PruningResult,
    T_SubPartitionPruningResult,
    T_Position,
    T_LoadWhenExpr,
    T_MergeWhenClause,
	T_UpsertClause,
	T_CopyColExpr,
    T_StartWithClause,
    T_StartWithTargetRelInfo,
    T_StartWithInfo,
    T_SqlLoadColPosInfo,
    T_SqlLoadScalarSpec,
    T_SqlLoadSequInfo,
    T_SqlLoadFillerInfo,
    T_SqlLoadConsInfo,
    T_SqlLoadColExpr,
    /*
     * TAGS FOR REPLICATION GRAMMAR PARSE NODES (replnodes.h)
     */
    T_IdentifySystemCmd,
    T_IdentifyVersionCmd,
    T_IdentifyModeCmd,
    T_IdentifyMaxLsnCmd,
    T_IdentifyConsistenceCmd,
    T_IdentifyChannelCmd,
    T_IdentifyAZCmd,
    T_BaseBackupCmd,
    T_CreateReplicationSlotCmd,
    T_DropReplicationSlotCmd,
    T_StartReplicationCmd,
    T_AdvanceReplicationCmd,
    T_StartDataReplicationCmd,
    T_FetchMotCheckpointCmd,

    /*
     * TAGS FOR RANDOM OTHER STUFF
     *
     * These are objects that aren't part of parse/plan/execute node tree
     * structures, but we give them NodeTags anyway for identification
     * purposes (usually because they are involved in APIs where we want to
     * pass multiple object types through the same pointer).
     */
    T_TriggerData = 970, /* in commands/trigger.h */
    T_ReturnSetInfo,     /* in nodes/execnodes.h */
    T_WindowObjectData,  /* private in nodeWindowAgg.c */
    T_TIDBitmap,         /* in nodes/tidbitmap.h */
    T_InlineCodeBlock,   /* in nodes/parsenodes.h */
    T_FdwRoutine,        /* in foreign/fdwapi.h */

    T_DistFdwDataNodeTask, /* in bulkload/dist_fdw.h */
    T_DistFdwFileSegment,  /* in bulkload/dist_fdw.h */
    T_SplitInfo,
    T_SplitMap,
    T_DfsPrivateItem,
    T_ErrorCacheEntry,
    T_ForeignPartState,
    T_RoachRoutine, /* in bulkload/roach_api.h */

    /*
     * Vectorized Plan Nodes
     */
    T_VecPlan = 1000,
    T_VecResult,
    T_VecModifyTable,
    T_VecAppend,
    T_VecPartIterator,
    T_VecMergeAppend,
    T_VecRecursiveUnion,
    T_VecScan,
    T_CStoreScan,
    T_DfsScan,
#ifdef ENABLE_MULTIPLE_NODES
    T_TsStoreScan,
#endif   /* ENABLE_MULTIPLE_NODES */
    T_VecIndexScan,
    T_VecIndexOnlyScan,
    T_VecBitmapIndexScan,
    T_VecBitmapHeapScan,
    T_VecSubqueryScan,
    T_VecForeignScan,
    T_VecNestLoop,
    T_VecMergeJoin,
    T_VecHashJoin,
    T_VecMaterial,
    T_VecSort,
    T_VecGroup,
    T_VecAgg,
    T_VecWindowAgg,
    T_VecUnique,
    T_VecHash,
    T_VecSetOp,
    T_VecLockRows,
    T_VecLimit,
    T_VecStream,
    T_RowToVec,
    T_VecToRow,
    T_DfsIndexScan,
    T_CStoreIndexScan,
    T_CStoreIndexCtidScan,
    T_CStoreIndexHeapScan,
    T_CStoreIndexAnd,
    T_CStoreIndexOr,
    T_VecRemoteQuery,
    T_CBTreeScanState,
    T_CBTreeOnlyScanState,
    T_CstoreBitmapIndexScanState,

    /*
     * Vectorized Execution Nodes
     */

    // this must put first for vector engine runtime state
    T_VecStartState = 2001,

    T_RowToVecState,
    T_VecAggState,
    T_VecHashJoinState,
    T_VecStreamState,
    T_VecSortState,
    T_VecForeignScanState,
    T_CStoreScanState,
    T_DfsScanState,
#ifdef ENABLE_MULTIPLE_NODES
    T_TsStoreScanState,
#endif   /* ENABLE_MULTIPLE_NODES */
    T_DfsIndexScanState,
    T_CStoreIndexScanState,
    T_CStoreIndexCtidScanState,
    T_CStoreIndexHeapScanState,
    T_CStoreIndexAndState,
    T_CStoreIndexOrState,
    T_VecRemoteQueryState,
    T_VecResultState,
    T_VecSubqueryScanState,
    T_VecModifyTableState,
    T_VecPartIteratorState,
    T_VecAppendState,
    T_VecLimitState,
    T_VecGroupState,
    T_VecUniqueState,
    T_VecSetOpState,
    T_VecNestLoopState,
    T_VecMaterialState,
    T_VecMergeJoinState,
    T_VecWindowAggState,

    // this must put last for vector engine runtime state
    T_VecEndState,

    /* @hdfs. support HDFS operation */
    T_HDFSTableAnalyze,
    T_ForeignTableDesc,
    T_AttrMetaData,
    T_RelationMetaData,
    T_ForeignOptions,

    /*
     * @hdfs
     * support infotmational constraint.
     */
    T_InformationalConstraint,
    T_GroupingId,
    T_GroupingIdExprState,
    T_BloomFilterSet,
    /* Hint type. Please only append new tag after the last hint type and never change the order. */
    T_HintState,
    T_OuterInnerRels,
    T_JoinMethodHint,
    T_LeadingHint,
    T_RowsHint,
    T_StreamHint,
    T_BlockNameHint,
    T_ScanMethodHint,
    T_MultiNodeHint,
    T_PredpushHint,
    T_PredpushSameLevelHint,
    T_SkewHint,
    T_RewriteHint,
    T_GatherHint,
    T_SetHint,
    T_PlanCacheHint,
    T_NoExpandHint,
    T_NoGPCHint,

    /*
     * pgfdw
     */
    T_PgFdwRemoteInfo,

    /* Create table like. */
    T_TableLikeCtx,

    /* Skew Hint Transform Info */
    T_SkewHintTransf,
    T_SkewRelInfo,
    T_SkewColumnInfo,
    T_SkewValueInfo,

    /* Skew Info */
    T_QualSkewInfo,

    /* Synonym */
    T_CreateSynonymStmt,
    T_DropSynonymStmt,

    T_BucketInfo,

    /* Encrypted Column */
    T_ClientLogicGlobalParam,
    T_CreateClientLogicGlobal,
    T_ClientLogicColumnParam,
    T_CreateClientLogicColumn,
    T_ClientLogicColumnRef,
    T_ExprWithComma,
    // DB4AI
    T_CreateModelStmt = 5000,
    T_PredictByFunction,
    T_TrainModel,
    T_ExplainModelStmt,
    // End DB4AI

    /* Plpgsql */
    T_PLDebug_variable,
    T_PLDebug_breakPoint,
    T_PLDebug_frame,

    T_TdigestData,
    T_CentroidPoint
} NodeTag;

/* if you add to NodeTag also need to add nodeTagToString */
extern char* nodeTagToString(NodeTag tag);

/*
 * The first field of a node of any type is guaranteed to be the NodeTag.
 * Hence the type of any node can be gotten by casting it to Node. Declaring
 * a variable to be of Node * (instead of void *) can also facilitate
 * debugging.
 */
typedef struct Node {
    NodeTag type;
} Node;

#define nodeTag(nodeptr) (((const Node*)(nodeptr))->type)

/*
 * newNode -
 *	  create a new node of the specified size and tag the node with the
 *	  specified tag.
 *
 * !WARNING!: Avoid using newNode directly. You should be using the
 *	  macro makeNode.  eg. to create a Query node, use makeNode(Query)
 *
 * Note: the size argument should always be a compile-time constant, so the
 * apparent risk of multiple evaluation doesn't matter in practice.
 */
#ifdef __GNUC__

/* With GCC, we can use a compound statement within an expression */
#ifndef FRONTEND_PARSER
#define newNode(size, tag)                                                \
    ({                                                                    \
        Node* _result;                                                    \
        AssertMacro((size) >= sizeof(Node)); /* need the tag, at least */ \
        _result = (Node*)palloc0fast(size);                               \
        _result->type = (tag);                                            \
        _result;                                                          \
    })
#else // !FRONTEND_PARSER
#define newNode(size, tag)                                                \
    ({                                                                    \
        Node *_result;                                                    \
        AssertMacro((size) >= sizeof(Node)); /* need the tag, at least */ \
        _result = (Node *)feparser_malloc0(size);                         \
        _result->type = (tag);                                            \
        _result;                                                          \
    })
#endif // !FRONTEND_PARSER
#else

#define newNode(size, tag)                                              \
    (AssertMacro((size) >= sizeof(Node)), /* need the tag, at least */  \
        t_thrd.utils_cxt.newNodeMacroHolder = (Node*)palloc0fast(size), \
        t_thrd.utils_cxt.newNodeMacroHolder->type = (tag),              \
        t_thrd.utils_cxt.newNodeMacroHolder)
#endif /* __GNUC__ */

#define makeNode(_type_) ((_type_*)newNode(sizeof(_type_), T_##_type_))
#define makeNodeWithSize(_type_, _size) ((_type_*)newNode(_size, T_##_type_))

#define NodeSetTag(nodeptr, t) (((Node*)(nodeptr))->type = (t))

#define IsA(nodeptr, _type_) (nodeTag(nodeptr) == T_##_type_)

/*
 * castNode(type, ptr) casts ptr to "type *", and if assertions are enabled,
 * verifies that the node has the appropriate type (using its nodeTag()).
 *
 * Use an inline function when assertions are enabled, to avoid multiple
 * evaluations of the ptr argument (which could e.g. be a function call).
 * iIf inline functions are not available - only a small number of platforms -
 * don't Assert, but use the non-checking version.
 */
#if defined(USE_ASSERT_CHECKING) && defined(PG_USE_INLINE)
static inline Node* castNodeImpl(NodeTag type, void* ptr)
{
    Assert(ptr == NULL || nodeTag(ptr) == type);
    return (Node*)ptr;
}
#define castNode(_type_, nodeptr) ((_type_*)castNodeImpl(T_##_type_, nodeptr))
#else
#define castNode(_type_, nodeptr) ((_type_*)(nodeptr))
#endif /* USE_ASSERT_CHECKING && PG_USE_INLINE */

/* ----------------------------------------------------------------
 *					  extern declarations follow
 * ----------------------------------------------------------------
 */

/*
 * nodes/{outfuncs.c,print.c}
 */
extern char* nodeToString(const void* obj);
extern void appendBitmapsetToString(void* str, void* bms);

/*
 * nodes/{readfuncs.c,read.c}
 */
extern void* stringToNode(char* str);

extern void* stringToNode_skip_extern_fields(char* str);

/*
 * nodes/copyfuncs.c
 */
extern void* copyObject(const void* obj);

/*
 * nodes/equalfuncs.c
 */
extern bool _equalSimpleVar(void* va, void* vb);
extern bool equal(const void* a, const void* b);

/*
 * Typedefs for identifying qualifier selectivities and plan costs as such.
 * These are just plain "double"s, but declaring a variable as Selectivity
 * or Cost makes the intent more obvious.
 *
 * These could have gone into plannodes.h or some such, but many files
 * depend on them...
 */
typedef double Selectivity; /* fraction of tuples a qualifier will pass */
typedef double Cost;        /* execution cost (in page-access units) */

/*
 * CmdType -
 *	  enums for type of operation represented by a Query or PlannedStmt
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum CmdType {
    CMD_UNKNOWN,
    CMD_SELECT,  /* select stmt */
    CMD_UPDATE,  /* update stmt */
    CMD_INSERT,  /* insert stmt */
    CMD_DELETE,  /* delete stmt */
    CMD_MERGE,   /* merge stmt */
    CMD_UTILITY, /* cmds like create, destroy, copy, vacuum,
                  * etc. */
    CMD_PREPARE,  /* prepare stmt */
    CMD_DEALLOCATE,  /* deallocate stmt*/
    CMD_EXECUTE,  /* execure stmt*/
    CMD_TRUNCATE,  /* truncate table*/
    CMD_REINDEX,  /* reindex table/index*/
    CMD_NOTHING,  /* dummy command for instead nothing rules
                  * with qual */
    CMD_DDL,
    CMD_DCL,
    CMD_DML,
    CMD_TCL
} CmdType;

/*
 * JoinType -
 *	  enums for types of relation joins
 *
 * JoinType determines the exact semantics of joining two relations using
 * a matching qualification.  For example, it tells what to do with a tuple
 * that has no match in the other relation.
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum JoinType {
    /*
     * The canonical kinds of joins according to the SQL JOIN syntax. Only
     * these codes can appear in parser output (e.g., JoinExpr nodes).
     */
    JOIN_INNER, /* matching tuple pairs only */
    JOIN_LEFT,  /* pairs + unmatched LHS tuples */
    JOIN_FULL,  /* pairs + unmatched LHS + unmatched RHS */
    JOIN_RIGHT, /* pairs + unmatched RHS tuples */

    /*
     * Semijoins and anti-semijoins (as defined in relational theory) do not
     * appear in the SQL JOIN syntax, but there are standard idioms for
     * representing them (e.g., using EXISTS).	The planner recognizes these
     * cases and converts them to joins.  So the planner and executor must
     * support these codes.  NOTE: in JOIN_SEMI output, it is unspecified
     * which matching RHS row is joined to.  In JOIN_ANTI output, the row is
     * guaranteed to be null-extended.
     */
    JOIN_SEMI, /* 1 copy of each LHS row that has match(es) */
    JOIN_ANTI, /* 1 copy of each LHS row that has no match */

    /*
     * These codes are used internally in the planner, but are not supported
     * by the executor (nor, indeed, by most of the planner).
     */
    JOIN_UNIQUE_OUTER, /* LHS path must be made unique */
    JOIN_UNIQUE_INNER, /* RHS path must be made unique */

    JOIN_RIGHT_SEMI, /* Right Semi Join */
    JOIN_RIGHT_ANTI, /* Right Anti join */

    JOIN_LEFT_ANTI_FULL, /* unmatched LHS tuples */
    JOIN_RIGHT_ANTI_FULL /* unmatched RHS tuples */

    /*
     * We might need additional join types someday.
     */
} JoinType;

/*
 * OUTER joins are those for which pushed-down quals must behave differently
 * from the join's own quals.  This is in fact everything except INNER and
 * SEMI joins.	However, this macro must also exclude the JOIN_UNIQUE symbols
 * since those are temporary proxies for what will eventually be an INNER
 * join.
 *
 * Note: semijoins are a hybrid case, but we choose to treat them as not
 * being outer joins.  This is okay principally because the SQL syntax makes
 * it impossible to have a pushed-down qual that refers to the inner relation
 * of a semijoin; so there is no strong need to distinguish join quals from
 * pushed-down quals.  This is convenient because for almost all purposes,
 * quals attached to a semijoin can be treated the same as innerjoin quals.
 *
 * We treat right semijoins/antijoins as semijoins/antijoins being treated.
 */
#define IS_OUTER_JOIN(jointype)                                                                                      \
    (((1 << (jointype)) & ((1 << JOIN_LEFT) | (1 << JOIN_FULL) | (1 << JOIN_RIGHT) | (1 << JOIN_ANTI) |              \
                              (1 << JOIN_RIGHT_ANTI) | (1 << JOIN_LEFT_ANTI_FULL) | (1 << JOIN_RIGHT_ANTI_FULL))) != \
        0)

typedef enum UpsertAction
{
    UPSERT_NONE,            /* No "DUPLICATE KEY UPDATE" clause */
    UPSERT_NOTHING,         /* DUPLICATE KEY UPDATE NOTHING */
    UPSERT_UPDATE           /* DUPLICATE KEY UPDATE ... */
}UpsertAction;

struct CentroidPoint {
    double mean;
    int64 count;
};

struct TdigestData {
    int32 vl_len_;
    double compression; 
    int cap;
    int merged_nodes;
    int unmerged_nodes;
    double merged_count;
    double unmerged_count;
   
    double valuetoc;
    CentroidPoint nodes[0];
};

#endif /* NODES_H */
