/* -------------------------------------------------------------------------
 *
 * nodes.cpp
 *	  support code for nodes (now that we have removed the home-brew
 *	  inheritance system, our support code for nodes is much simpler)
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/nodes.cpp
 *
 * HISTORY
 *	  Andrew Yu			Oct 20, 1994	file creation
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/nodes.h"

typedef struct TagStr {
    NodeTag tag;
    char* str;
} TagStr;

static const TagStr g_tagStrArr[] = {{T_Invalid, "Invalid"},
    {T_IndexInfo, "IndexInfo"},
    {T_ExprContext, "ExprContext"},
    {T_ProjectionInfo, "ProjectionInfo"},
    {T_JunkFilter, "JunkFilter"},
    {T_ResultRelInfo, "ResultRelInfo"},
    {T_EState, "EState"},
    {T_TupleTableSlot, "TupleTableSlot"},
    {T_Plan, "Plan"},
    {T_BaseResult, "BaseResult"},
    {T_ModifyTable, "ModifyTable"},
    {T_Append, "Append"},
    {T_PartIterator, "PartIterator"},
    {T_MergeAppend, "MergeAppend"},
    {T_RecursiveUnion, "RecursiveUnion"},
    {T_StartWithOp, "StartWithOp"},
    {T_BitmapAnd, "BitmapAnd"},
    {T_BitmapOr, "BitmapOr"},
    {T_Scan, "Scan"},
    {T_SeqScan, "SeqScan"},
#ifdef USE_SPQ
    {T_SpqSeqScan, "SpqSeqScan"},
    {T_AssertOp, "AssertOp"},
    {T_ShareInputScan, "ShareInputScan"},
    {T_Sequence, "Sequence"},
    {T_SpqIndexScan, "SpqIndexScan"},
    {T_SpqIndexOnlyScan, "SpqIndexOnlyScan"},
    {T_SpqBitmapHeapScan, "SpqBitmapHeapScan"},
#endif
    {T_IndexScan, "IndexScan"},
    {T_IndexOnlyScan, "IndexOnlyScan"},
    {T_BitmapIndexScan, "BitmapIndexScan"},
    {T_BitmapHeapScan, "BitmapHeapScan"},
    {T_AnnIndexScan, "AnnIndexScan"},
    {T_TidScan, "TidScan"},
    {T_TidRangeScan, "TidRangeScan"},
    {T_SubqueryScan, "SubqueryScan"},
    {T_FunctionScan, "FunctionScan"},
    {T_ValuesScan, "ValuesScan"},
    {T_CteScan, "CteScan"},
    {T_WorkTableScan, "WorkTableScan"},
    {T_ForeignScan, "ForeignScan"},
    {T_ExtensiblePlan, "ExtensiblePlan"},
    {T_Join, "Join"},
    {T_NestLoop, "NestLoop"},
    {T_MergeJoin, "MergeJoin"},
    {T_HashJoin, "HashJoin"},
    {T_AsofJoin, "AsofJoin"},
    {T_Material, "Material"},
    {T_Sort, "Sort"},
    {T_SortGroup, "SortGroup"},
    {T_Group, "Group"},
    {T_Agg, "Agg"},
    {T_WindowAgg, "WindowAgg"},
    {T_Unique, "Unique"},
    {T_Hash, "Hash"},
    {T_SetOp, "SetOp"},
    {T_LockRows, "LockRows"},
    {T_Limit, "Limit"},
    {T_Stream, "Stream"},
    {T_ExecNodes, "ExecNodes"},
    {T_SliceBoundary, "SliceBoundary"},
    {T_ExecBoundary, "ExecBoundary"},
    {T_SimpleSort, "SimpleSort"},
    {T_RemoteQuery, "RemoteQuery"},
    {T_PGXCNodeHandle, "PGXCNodeHandle"},
    {T_AlterNodeStmt, "AlterNodeStmt"},
    {T_CreateNodeStmt, "CreateNodeStmt"},
    {T_DropNodeStmt, "DropNodeStmt"},
    {T_CreateGroupStmt, "CreateGroupStmt"},
    {T_AlterGroupStmt, "AlterGroupStmt"},
    {T_DropGroupStmt, "DropGroupStmt"},
    {T_CreateResourcePoolStmt, "CreateResourcePoolStmt"},
    {T_AlterResourcePoolStmt, "AlterResourcePoolStmt"},
    {T_DropResourcePoolStmt, "DropResourcePoolStmt"},
    {T_AlterGlobalConfigStmt, "AlterGlobalConfigStmt"},
    {T_DropGlobalConfigStmt, "DropGlobalConfigStmt"},
    {T_CreateWorkloadGroupStmt, "CreateWorkloadGroupStmt"},
    {T_AlterWorkloadGroupStmt, "AlterWorkloadGroupStmt"},
    {T_DropWorkloadGroupStmt, "DropWorkloadGroupStmt"},
    {T_CreateAppWorkloadGroupMappingStmt, "CreateAppWorkloadGroupMappingStmt"},
    {T_AlterAppWorkloadGroupMappingStmt, "AlterAppWorkloadGroupMappingStmt"},
    {T_DropAppWorkloadGroupMappingStmt, "DropAppWorkloadGroupMappingStmt"},
    {T_NestLoopParam, "NestLoopParam"},
    {T_PartIteratorParam, "PartIteratorParam"},
    {T_PlanRowMark, "PlanRowMark"},
    {T_PlanInvalItem, "PlanInvalItem"},
    {T_PlanState, "PlanState"},
    {T_ResultState, "ResultState"},
    {T_MergeActionState, "MergeActionState"},
    {T_ModifyTableState, "ModifyTableState"},
    {T_DistInsertSelectState, "DistInsertSelectState"},
    {T_AppendState, "AppendState"},
    {T_PartIteratorState, "PartIteratorState"},
    {T_MergeAppendState, "MergeAppendState"},
    {T_RecursiveUnionState, "RecursiveUnionState"},
    {T_BitmapAndState, "BitmapAndState"},
    {T_BitmapOrState, "BitmapOrState"},
    {T_ScanState, "ScanState"},
    {T_SeqScanState, "SeqScanState"},
#ifdef USE_SPQ
    {T_SpqSeqScanState, "SpqSeqScanState"},
    {T_AssertOpState, "AssertOpState"},
    {T_ShareInputScanState, "ShareInputScanState"},
    {T_SequenceState, "SequenceState"},
#endif
    {T_IndexScanState, "IndexScanState"},
    {T_IndexOnlyScanState, "IndexOnlyScanState"},
    {T_BitmapIndexScanState, "BitmapIndexScanState"},
    {T_BitmapHeapScanState, "BitmapHeapScanState"},
    {T_AnnIndexScanState, "AnnIndexScanState"},
    {T_TidScanState, "TidScanState"},
    {T_TidRangeScanState, "TidRangeScanState"},
    {T_SubqueryScanState, "SubqueryScanState"},
    {T_FunctionScanState, "FunctionScanState"},
    {T_ValuesScanState, "ValuesScanState"},
    {T_CteScanState, "CteScanState"},
    {T_WorkTableScanState, "WorkTableScanState"},
    {T_ForeignScanState, "ForeignScanState"},
    {T_ExtensiblePlanState, "ExtensiblePlanState"},
    {T_JoinState, "JoinState"},
    {T_NestLoopState, "NestLoopState"},
    {T_MergeJoinState, "MergeJoinState"},
    {T_HashJoinState, "HashJoinState"},
    {T_MaterialState, "MaterialState"},
    {T_SortState, "SortState"},
    {T_SortGroupState, "SortGroupState"},
    {T_GroupState, "GroupState"},
    {T_AggState, "AggState"},
    {T_WindowAggState, "WindowAggState"},
    {T_UniqueState, "UniqueState"},
    {T_HashState, "HashState"},
    {T_SetOpState, "SetOpState"},
    {T_LockRowsState, "LockRowsState"},
    {T_LimitState, "LimitState"},
    {T_RemoteQueryState, "RemoteQueryState"},
    {T_StreamState, "StreamState"},
    {T_Alias, "Alias"},
    {T_RangeVar, "RangeVar"},
    {T_Expr, "Expr"},
    {T_Var, "Var"},
    {T_Const, "Const"},
    {T_Param, "Param"},
    {T_Aggref, "Aggref"},
    {T_GroupingFunc, "GroupingFunc"},
    {T_WindowFunc, "WindowFunc"},
    {T_InitList, "InitList"},
    {T_ArrayRef, "ArrayRef"},
    {T_FuncExpr, "FuncExpr"},
    {T_NamedArgExpr, "NamedArgExpr"},
    {T_OpExpr, "OpExpr"},
    {T_DistinctExpr, "DistinctExpr"},
    {T_NullIfExpr, "NullIfExpr"},
    {T_ScalarArrayOpExpr, "ScalarArrayOpExpr"},
    {T_BoolExpr, "BoolExpr"},
    {T_SubLink, "SubLink"},
    {T_SubPlan, "SubPlan"},
    {T_AlternativeSubPlan, "AlternativeSubPlan"},
    {T_FieldSelect, "FieldSelect"},
    {T_FieldStore, "FieldStore"},
    {T_RelabelType, "RelabelType"},
    {T_CoerceViaIO, "CoerceViaIO"},
    {T_ArrayCoerceExpr, "ArrayCoerceExpr"},
    {T_ConvertRowtypeExpr, "ConvertRowtypeExpr"},
    {T_CollateExpr, "CollateExpr"},
    {T_CaseExpr, "CaseExpr"},
    {T_CaseWhen, "CaseWhen"},
    {T_CaseTestExpr, "CaseTestExpr"},
    {T_ArrayExpr, "ArrayExpr"},
    {T_RowExpr, "RowExpr"},
    {T_RowCompareExpr, "RowCompareExpr"},
    {T_CoalesceExpr, "CoalesceExpr"},
    {T_MinMaxExpr, "MinMaxExpr"},
    {T_XmlExpr, "XmlExpr"},
    {T_NullTest, "NullTest"},
    {T_NanTest, "NanTest"},
    {T_InfiniteTest, "InfiniteTest"},
    {T_BooleanTest, "BooleanTest"},
    {T_CoerceToDomain, "CoerceToDomain"},
    {T_CoerceToDomainValue, "CoerceToDomainValue"},
    {T_SetToDefault, "SetToDefault"},
    {T_CurrentOfExpr, "CurrentOfExpr"},
    {T_TargetEntry, "TargetEntry"},
    {T_RangeTblRef, "RangeTblRef"},
    {T_JoinExpr, "JoinExpr"},
    {T_FromExpr, "FromExpr"},
    {T_IntoClause, "IntoClause"},
    {T_PrefixKey, "PrefixKey"},
    {T_DistributeBy, "DistributeBy"},
    {T_PGXCSubCluster, "PGXCSubCluster"},
    {T_DistState, "DistState"},
    {T_ListSliceDefState, "ListSliceDefState"},
    {T_HashFilter, "HashFilter"},
    {T_EstSPNode, "EstSPNode"},
    {T_ExprState, "ExprState"},
    {T_GenericExprState, "GenericExprState"},
    {T_AggrefExprState, "AggrefExprState"},
    {T_GroupingFuncExprState, "GroupingFuncExprState"},
    {T_WindowFuncExprState, "WindowFuncExprState"},
    {T_ArrayRefExprState, "ArrayRefExprState"},
    {T_FuncExprState, "FuncExprState"},
    {T_ScalarArrayOpExprState, "ScalarArrayOpExprState"},
    {T_BoolExprState, "BoolExprState"},
    {T_SubPlanState, "SubPlanState"},
    {T_AlternativeSubPlanState, "AlternativeSubPlanState"},
    {T_FieldSelectState, "FieldSelectState"},
    {T_FieldStoreState, "FieldStoreState"},
    {T_CoerceViaIOState, "CoerceViaIOState"},
    {T_ArrayCoerceExprState, "ArrayCoerceExprState"},
    {T_ConvertRowtypeExprState, "ConvertRowtypeExprState"},
    {T_CaseExprState, "CaseExprState"},
    {T_CaseWhenState, "CaseWhenState"},
    {T_ArrayExprState, "ArrayExprState"},
    {T_RowExprState, "RowExprState"},
    {T_RowCompareExprState, "RowCompareExprState"},
    {T_CoalesceExprState, "CoalesceExprState"},
    {T_MinMaxExprState, "MinMaxExprState"},
    {T_XmlExprState, "XmlExprState"},
    {T_NullTestState, "NullTestState"},
    {T_NanTestState, "NanTestState"},
    {T_InfiniteTestState, "InfiniteTestState"},
    {T_HashFilterState, "HashFilterState"},
    {T_CoerceToDomainState, "CoerceToDomainState"},
    {T_DomainConstraintState, "DomainConstraintState"},
    {T_WholeRowVarExprState, "WholeRowVarExprState"},
    {T_RangePartitionDefState, "RangePartitionDefState"},
    {T_ListPartitionDefState, "ListPartitionDefState"},
    {T_HashPartitionDefState, "HashPartitionDefState"},
    {T_IntervalPartitionDefState, "IntervalPartitionDefState"},
    {T_PartitionState, "PartitionState"},
    {T_RangePartitionindexDefState, "RangePartitionindexDefState"},
    {T_SplitPartitionState, "SplitPartitionState"},
    {T_AddPartitionState, "AddPartitionState"},
    {T_AddSubPartitionState, "AddSubPartitionState"},
    {T_RangePartitionStartEndDefState, "RangePartitionStartEndDefState"},
    {T_PlannerInfo, "PlannerInfo"},
    {T_PlannerGlobal, "PlannerGlobal"},
    {T_RelOptInfo, "RelOptInfo"},
    {T_IndexOptInfo, "IndexOptInfo"},
    {T_ParamPathInfo, "ParamPathInfo"},
    {T_Path, "Path"},
    {T_IndexPath, "IndexPath"},
    {T_BitmapHeapPath, "BitmapHeapPath"},
    {T_BitmapAndPath, "BitmapAndPath"},
    {T_BitmapOrPath, "BitmapOrPath"},
    {T_NestPath, "NestPath"},
    {T_MergePath, "MergePath"},
    {T_HashPath, "HashPath"},
    {T_AsofPath, "AsofPath"},
    {T_TidPath, "TidPath"},
    {T_TidRangePath, "TidRangePath"},
    {T_ForeignPath, "ForeignPath"},
    {T_ExtensiblePath, "ExtensiblePath"},
    {T_AppendPath, "AppendPath"},
    {T_MergeAppendPath, "MergeAppendPath"},
    {T_ResultPath, "ResultPath"},
    {T_MaterialPath, "MaterialPath"},
    {T_UniquePath, "UniquePath"},
    {T_PartIteratorPath, "PartIteratorPath"},
    {T_EquivalenceClass, "EquivalenceClass"},
    {T_EquivalenceMember, "EquivalenceMember"},
    {T_PathKey, "PathKey"},
    {T_PathTarget, "PathTarget"},
    {T_RestrictInfo, "RestrictInfo"},
    {T_PlaceHolderVar, "PlaceHolderVar"},
    {T_SpecialJoinInfo, "SpecialJoinInfo"},
    {T_AppendRelInfo, "AppendRelInfo"},
    {T_PlaceHolderInfo, "PlaceHolderInfo"},
    {T_MinMaxAggInfo, "MinMaxAggInfo"},
    {T_PlannerParamItem, "PlannerParamItem"},
    {T_RemoteQueryPath, "RemoteQueryPath"},
    {T_StreamPath, "StreamPath"},
    {T_MergeAction, "MergeAction"},
    {T_MemoryContext, "MemoryContext"},
    {T_AllocSetContext, "AllocSetContext"},
    {T_RackAllocSetContext, "RackAllocSetContext"},
    {T_OptAllocSetContext, "OptAllocSetContext"},
    {T_StackAllocSetContext, "StackAllocSetContext"},
    {T_SharedAllocSetContext, "SharedAllocSetContext"},
    {T_RackSharedAllocSetContext, "RackSharedAllocSetContext"},
    {T_MemalignAllocSetContext, "MemalignAllocSetContext"},
    {T_MemalignSharedAllocSetContext, "MemalignSharedAllocSetContext"},
    {T_MemoryTracking, "MemoryTracking"},
    {T_Value, "Value"},
    {T_Integer, "Integer"},
    {T_Float, "Float"},
    {T_String, "String"},
    {T_BitString, "BitString"},
    {T_TSQL_HexString, "HexString"},
    {T_Null, "Null"},
    {T_List, "List"},
    {T_IntList, "IntList"},
    {T_OidList, "OidList"},
    {T_DList, "DList"},
    {T_IntDList, "IntDList"},
    {T_OidDList, "OidDList"},
    {T_Query, "Query"},
    {T_PlannedStmt, "PlannedStmt"},
    {T_InsertStmt, "InsertStmt"},
    {T_DeleteStmt, "DeleteStmt"},
    {T_UpdateStmt, "UpdateStmt"},
    {T_MergeStmt, "MergeStmt"},
    {T_SelectStmt, "SelectStmt"},
    {T_SelectIntoVarList, "SelectIntoVarList"},
    {T_AlterTableStmt, "AlterTableStmt"},
    {T_AlterTriggerStmt, "AlterTriggerStmt"},
    {T_AlterTableCmd, "AlterTableCmd"},
    {T_AlterDomainStmt, "AlterDomainStmt"},
    {T_SetOperationStmt, "SetOperationStmt"},
    {T_GrantStmt, "GrantStmt"},
    {T_GrantRoleStmt, "GrantRoleStmt"},
    {T_GrantDbStmt, "GrantDbStmt"},
    {T_AlterDefaultPrivilegesStmt, "AlterDefaultPrivilegesStmt"},
    {T_ClosePortalStmt, "ClosePortalStmt"},
    {T_ClusterStmt, "ClusterStmt"},
    {T_CopyStmt, "CopyStmt"},
    {T_CreateStmt, "CreateStmt"},
    {T_DefineStmt, "DefineStmt"},
    {T_DropStmt, "DropStmt"},
    {T_TruncateStmt, "TruncateStmt"},
    {T_CommentStmt, "CommentStmt"},
    {T_FetchStmt, "FetchStmt"},
    {T_IndexStmt, "IndexStmt"},
    {T_CreateFunctionStmt, "CreateFunctionStmt"},
    {T_CreateEventStmt, "CreateEventStmt"},
    {T_AlterEventStmt, "AlterEventStmt"},
    {T_DropEventStmt, "DropEventStmt"},
    {T_ShowEventStmt, "ShowEventStmt"},
    {T_CreatePackageStmt, "CreatePackageStmt"},
    {T_CreatePackageBodyStmt, "CreatePackageBodyStmt"},
    {T_AddTableIntoCBIState, "AddTableIntoCBIState"},
    {T_AlterFunctionStmt, "AlterFunctionStmt"},
    {T_CompileStmt, "CompileStmt"},
    {T_DependenciesUndefined, "DependenciesUndefined"},
    {T_DependenciesVariable, "DependenciesVariable"},
    {T_DependenciesType, "DependenciesType"},
    {T_DependenciesProchead, "DependenciesProchead"},
    {T_DoStmt, "DoStmt"},
    {T_RenameStmt, "RenameStmt"},
    {T_RuleStmt, "RuleStmt"},
    {T_NotifyStmt, "NotifyStmt"},
    {T_ListenStmt, "ListenStmt"},
    {T_UnlistenStmt, "UnlistenStmt"},
    {T_TransactionStmt, "TransactionStmt"},
    {T_ViewStmt, "ViewStmt"},
    {T_LoadStmt, "LoadStmt"},
    {T_CreateDomainStmt, "CreateDomainStmt"},
    {T_CreatedbStmt, "CreatedbStmt"},
    {T_DropdbStmt, "DropdbStmt"},
    {T_VacuumStmt, "VacuumStmt"},
    {T_ExplainStmt, "ExplainStmt"},
    {T_CreateTableAsStmt, "CreateTableAsStmt"},
    {T_CreateSeqStmt, "CreateSeqStmt"},
    {T_AlterSeqStmt, "AlterSeqStmt"},
    {T_VariableSetStmt, "VariableSetStmt"},
    {T_VariableShowStmt, "VariableShowStmt"},
    {T_DiscardStmt, "DiscardStmt"},
    {T_CreateTrigStmt, "CreateTrigStmt"},
    {T_CreatePLangStmt, "CreatePLangStmt"},
    {T_CreateRoleStmt, "CreateRoleStmt"},
    {T_AlterRoleStmt, "AlterRoleStmt"},
    {T_DropRoleStmt, "DropRoleStmt"},
    {T_LockStmt, "LockStmt"},
    {T_TimeCapsuleStmt, "TimeCapsuleStmt"},
    {T_ConstraintsSetStmt, "ConstraintsSetStmt"},
    {T_ReindexStmt, "ReindexStmt"},
    {T_CheckPointStmt, "CheckPointStmt"},
    {T_BarrierStmt, "BarrierStmt"},
    {T_CreateSchemaStmt, "CreateSchemaStmt"},
    {T_AlterSchemaStmt, "AlterSchemaStmt"},
    {T_AlterDatabaseStmt, "AlterDatabaseStmt"},
    {T_AlterDatabaseSetStmt, "AlterDatabaseSetStmt"},
    {T_AlterRoleSetStmt, "AlterRoleSetStmt"},
    {T_CreateConversionStmt, "CreateConversionStmt"},
    {T_CreateCastStmt, "CreateCastStmt"},
    {T_CreateOpClassStmt, "CreateOpClassStmt"},
    {T_CreateOpFamilyStmt, "CreateOpFamilyStmt"},
    {T_AlterOpFamilyStmt, "AlterOpFamilyStmt"},
    {T_PrepareStmt, "PrepareStmt"},
    {T_ExecuteStmt, "ExecuteStmt"},
    {T_DeallocateStmt, "DeallocateStmt"},
    {T_DeclareCursorStmt, "DeclareCursorStmt"},
    {T_CursorExpression, "CursorExpression"},
    {T_CreateTableSpaceStmt, "CreateTableSpaceStmt"},
    {T_DropTableSpaceStmt, "DropTableSpaceStmt"},
    {T_AlterObjectSchemaStmt, "AlterObjectSchemaStmt"},
    {T_AlterOwnerStmt, "AlterOwnerStmt"},
    {T_DropOwnedStmt, "DropOwnedStmt"},
    {T_ReassignOwnedStmt, "ReassignOwnedStmt"},
    {T_CompositeTypeStmt, "CompositeTypeStmt"},
    {T_TableOfTypeStmt, "TableOfTypeStmt"},
    {T_CreateEnumStmt, "CreateEnumStmt"},
    {T_CreateRangeStmt, "CreateRangeStmt"},
    {T_AlterEnumStmt, "AlterEnumStmt"},
    {T_AlterTSDictionaryStmt, "AlterTSDictionaryStmt"},
    {T_AlterTSConfigurationStmt, "AlterTSConfigurationStmt"},
    {T_CreateFdwStmt, "CreateFdwStmt"},
    {T_AlterFdwStmt, "AlterFdwStmt"},
    {T_CreateForeignServerStmt, "CreateForeignServerStmt"},
    {T_AlterForeignServerStmt, "AlterForeignServerStmt"},
    {T_CreateUserMappingStmt, "CreateUserMappingStmt"},
    {T_AlterUserMappingStmt, "AlterUserMappingStmt"},
    {T_DropUserMappingStmt, "DropUserMappingStmt"},
    {T_ExecDirectStmt, "ExecDirectStmt"},
    {T_CleanConnStmt, "CleanConnStmt"},
    {T_AlterTableSpaceOptionsStmt, "AlterTableSpaceOptionsStmt"},
    {T_SecLabelStmt, "SecLabelStmt"},
    {T_CreateForeignTableStmt, "CreateForeignTableStmt"},
    {T_CreateExtensionStmt, "CreateExtensionStmt"},
    {T_AlterExtensionStmt, "AlterExtensionStmt"},
    {T_AlterExtensionContentsStmt, "AlterExtensionContentsStmt"},
    {T_CreateDataSourceStmt, "CreateDataSourceStmt"},
    {T_AlterDataSourceStmt, "AlterDataSourceStmt"},
    {T_ReplicaIdentityStmt, "ReplicaIdentityStmt"},
    {T_CreateDirectoryStmt, "CreateDirectoryStmt"},
    {T_DropDirectoryStmt, "DropDirectoryStmt"},
    {T_CreateRlsPolicyStmt, "CreateRlsPolicyStmt"},
    {T_AlterRlsPolicyStmt, "AlterRlsPolicyStmt"},
    {T_ShutdownStmt, "ShutdownStmt"},
    {T_CreateWeakPasswordDictionaryStmt, "CreateWeakPasswordDictionaryStmt"},
    {T_DropWeakPasswordDictionaryStmt, "DropWeakPasswordDictionaryStmt"},
    {T_CreatePolicyLabelStmt, "CreatePolicyLabelStmt"},
    {T_AlterPolicyLabelStmt, "AlterPolicyLabelStmt"},
    {T_DropPolicyLabelStmt, "DropPolicyLabelStmt"},
    {T_CreateAuditPolicyStmt, "CreateAuditPolicyStmt"},
    {T_AlterAuditPolicyStmt, "AlterAuditPolicyStmt"},
    {T_DropAuditPolicyStmt, "DropAuditPolicyStmt"},
    {T_CreateMaskingPolicyStmt, "CreateMaskingPolicyStmt"},
    {T_AlterMaskingPolicyStmt, "AlterMaskingPolicyStmt"},
    {T_DropMaskingPolicyStmt, "DropMaskingPolicyStmt"},
    {T_MaskingPolicyCondition, "MaskingPolicyCondition"},
    {T_PolicyFilterNode, "PolicyFilterNode"},
    {T_A_Expr, "A_Expr"},
    {T_ColumnRef, "ColumnRef"},
    {T_ParamRef, "ParamRef"},
    {T_A_Const, "A_Const"},
    {T_FuncCall, "FuncCall"},
    {T_A_Star, "A_Star"},
    {T_A_Indices, "A_Indices"},
    {T_A_Indirection, "A_Indirection"},
    {T_A_ArrayExpr, "A_ArrayExpr"},
    {T_ResTarget, "ResTarget"},
    {T_TypeCast, "TypeCast"},
    {T_CollateClause, "CollateClause"},
    {T_SortBy, "SortBy"},
    {T_WindowDef, "WindowDef"},
    {T_RangeSubselect, "RangeSubselect"},
    {T_RangeFunction, "RangeFunction"},
    {T_RangeTableSample, "RangeTableSample"},
    {T_TypeName, "TypeName"},
    {T_ColumnDef, "ColumnDef"},
    {T_IndexElem, "IndexElem"},
    {T_Constraint, "Constraint"},
    {T_DefElem, "DefElem"},
    {T_RangeTblEntry, "RangeTblEntry"},
    {T_TableSampleClause, "TableSampleClause"},
    {T_SortGroupClause, "SortGroupClause"},
    {T_GroupingSet, "GroupingSet"},
    {T_WindowClause, "WindowClause"},
    {T_PrivGrantee, "PrivGrantee"},
    {T_FuncWithArgs, "FuncWithArgs"},
    {T_AccessPriv, "AccessPriv"},
    {T_DbPriv, "DbPriv"},
    {T_CreateOpClassItem, "CreateOpClassItem"},
    {T_TableLikeClause, "TableLikeClause"},
    {T_FunctionParameter, "FunctionParameter"},
    {T_LockingClause, "LockingClause"},
    {T_RowMarkClause, "RowMarkClause"},
    {T_XmlSerialize, "XmlSerialize"},
    {T_WithClause, "WithClause"},
    {T_CommonTableExpr, "CommonTableExpr"},
    {T_PruningResult, "PruningResult"},
    {T_SubPartitionPruningResult, "SubPartitionPruningResult"},
    {T_Position, "Position"},
    {T_MergeWhenClause, "MergeWhenClause"},
    {T_IdentifySystemCmd, "IdentifySystemCmd"},
    {T_IdentifyVersionCmd, "IdentifyVersionCmd"},
    {T_IdentifyModeCmd, "IdentifyModeCmd"},
    {T_IdentifyMaxLsnCmd, "IdentifyMaxLsnCmd"},
    {T_IdentifyConsistenceCmd, "IdentifyConsistenceCmd"},
    {T_IdentifyChannelCmd, "IdentifyChannelCmd"},
#ifndef ENABLE_MULTIPLE_NODES
    {T_IdentifyAZCmd, "IdentifyAZCmd"},
#endif
    {T_BaseBackupCmd, "BaseBackupCmd"},
    {T_CreateReplicationSlotCmd, "CreateReplicationSlotCmd"},
    {T_DropReplicationSlotCmd, "DropReplicationSlotCmd"},
    {T_StartReplicationCmd, "StartReplicationCmd"},
    {T_AdvanceReplicationCmd, "AdvanceReplicationCmd"},
    {T_StartDataReplicationCmd, "StartDataReplicationCmd"},
    {T_FetchMotCheckpointCmd, "FetchMotCheckpointCmd"},
    {T_TriggerData, "TriggerData"},
    {T_ReturnSetInfo, "ReturnSetInfo"},
    {T_WindowObjectData, "WindowObjectData"},
    {T_TIDBitmap, "TIDBitmap"},
    {T_InlineCodeBlock, "InlineCodeBlock"},
    {T_FdwRoutine, "FdwRoutine"},
    {T_DistFdwDataNodeTask, "DistFdwDataNodeTask"},
    {T_DistFdwFileSegment, "DistFdwFileSegment"},
    {T_SplitInfo, "SplitInfo"},
    {T_SplitMap, "SplitMap"},
    {T_DfsPrivateItem, "DfsPrivateItem"},
    {T_ErrorCacheEntry, "ErrorCacheEntry"},
    {T_ForeignPartState, "ForeignPartState"},
    {T_RoachRoutine, "RoachRoutine"},
    {T_VecPlan, "VecPlan"},
    {T_VecResult, "VecResult"},
    {T_VecModifyTable, "VecModifyTable"},
    {T_VecAppend, "VecAppend"},
    {T_VecPartIterator, "VecPartIterator"},
    {T_VecMergeAppend, "VecMergeAppend"},
    {T_VecRecursiveUnion, "VecRecursiveUnion"},
    {T_VecScan, "VecScan"},
    {T_CStoreScan, "CStoreScan"},
#ifdef ENABLE_HTAP
    {T_IMCStoreScan, "IMCStoreScan"},
#ifdef USE_SPQ
    {T_SpqCStoreScan, "SpqCStoreScan"},
#endif
#endif
    {T_VecIndexScan, "VecIndexScan"},
    {T_VecIndexOnlyScan, "VecIndexOnlyScan"},
    {T_VecBitmapIndexScan, "VecBitmapIndexScan"},
    {T_VecBitmapHeapScan, "VecBitmapHeapScan"},
    {T_VecSubqueryScan, "VecSubqueryScan"},
    {T_VecForeignScan, "VecForeignScan"},
    {T_VecNestLoop, "VecNestLoop"},
    {T_VecMergeJoin, "VecMergeJoin"},
    {T_VecHashJoin, "VecHashJoin"},
    {T_VecAsofJoin, "VecAsofJoin"},
    {T_VecMaterial, "VecMaterial"},
    {T_VecSort, "VecSort"},
    {T_VecGroup, "VecGroup"},
    {T_VecAgg, "VecAgg"},
    {T_VecWindowAgg, "VecWindowAgg"},
    {T_VecUnique, "VecUnique"},
    {T_VecHash, "VecHash"},
    {T_VecSetOp, "VecSetOp"},
    {T_VecLockRows, "VecLockRows"},
    {T_VecLimit, "VecLimit"},
    {T_VecStream, "VecStream"},
    {T_RowToVec, "RowToVec"},
    {T_VecToRow, "VecToRow"},
    {T_CStoreIndexScan, "CStoreIndexScan"},
    {T_CStoreIndexCtidScan, "CStoreIndexCtidScan"},
    {T_CStoreIndexHeapScan, "CStoreIndexHeapScan"},
    {T_CStoreIndexAnd, "CStoreIndexAnd"},
    {T_CStoreIndexOr, "CStoreIndexOr"},
    {T_VecRemoteQuery, "VecRemoteQuery"},
    {T_CBTreeScanState, "CBTreeScanState"},
    {T_CBTreeOnlyScanState, "CBTreeOnlyScanState"},
    {T_CstoreBitmapIndexScanState, "CstoreBitmapIndexScanState"},
    {T_VecToRowState, "VecToRowState"},
    {T_VecStartState, "VecStartState"},
    {T_RowToVecState, "RowToVecState"},
    {T_VecAggState, "VecAggState"},
    {T_VecHashJoinState, "VecHashJoinState"},
    {T_VecAsofJoinState, "VecAsofJoinState"},
    {T_VecStreamState, "VecStreamState"},
    {T_VecSortState, "VecSortState"},
    {T_VecForeignScanState, "VecForeignScanState"},
    {T_CStoreScanState, "CStoreScanState"},
#ifdef ENABLE_HTAP
    {T_IMCStoreScanState, "IMCStoreScanState"},
#endif
    {T_CStoreIndexScanState, "CStoreIndexScanState"},
    {T_CStoreIndexCtidScanState, "CStoreIndexCtidScanState"},
    {T_CStoreIndexHeapScanState, "CStoreIndexHeapScanState"},
    {T_CStoreIndexAndState, "CStoreIndexAndState"},
    {T_CStoreIndexOrState, "CStoreIndexOrState"},
    {T_VecRemoteQueryState, "VecRemoteQueryState"},
    {T_VecResultState, "VecResultState"},
    {T_VecSubqueryScanState, "VecSubqueryScanState"},
    {T_VecModifyTableState, "VecModifyTableState"},
    {T_VecPartIteratorState, "VecPartIteratorState"},
    {T_VecAppendState, "VecAppendState"},
    {T_VecLimitState, "VecLimitState"},
    {T_VecGroupState, "VecGroupState"},
    {T_VecUniqueState, "VecUniqueState"},
    {T_VecSetOpState, "VecSetOpState"},
    {T_VecNestLoopState, "VecNestLoopState"},
    {T_VecMaterialState, "VecMaterialState"},
    {T_VecMergeJoinState, "VecMergeJoinState"},
    {T_VecWindowAggState, "VecWindowAggState"},
    {T_VecEndState, "VecEndState"},
    {T_HDFSTableAnalyze, "HDFSTableAnalyze"},
    {T_ForeignTableDesc, "ForeignTableDesc"},
    {T_AttrMetaData, "AttrMetaData"},
    {T_RelationMetaData, "RelationMetaData"},
    {T_ForeignOptions, "ForeignOptions"},
    {T_InformationalConstraint, "InformationalConstraint"},
    {T_GroupingId, "GroupingId"},
    {T_GroupingIdExprState, "GroupingIdExprState"},
    {T_BloomFilterSet, "BloomFilterSet"},
    {T_HintState, "HintState"},
    {T_OuterInnerRels, "OuterInnerRels"},
    {T_JoinMethodHint, "JoinMethodHint"},
    {T_LeadingHint, "LeadingHint"},
    {T_RowsHint, "RowsHint"},
    {T_RewriteHint, "RewriteHint"},
    {T_GatherHint, "GatherHint"},
    {T_StreamHint, "StreamHint"},
    {T_BlockNameHint, "BlockNameHint"},
    {T_ScanMethodHint, "ScanMethodHint"},
    {T_PgFdwRemoteInfo, "PgFdwRemoteInfo"},
    {T_TableLikeCtx, "TableLikeCtx"},
    {T_SkewHint, "SkewHint"},
    {T_SkewHintTransf, "SkewHintTransf"},
    {T_SkewRelInfo, "SkewRelInfo"},
    {T_SkewColumnInfo, "SkewColumnInfo"},
    {T_SkewValueInfo, "SkewValueInfo"},
    {T_QualSkewInfo, "QualSkewInfo"},
    // DB4AI
    {T_CreateModelStmt, "CreateModelStmt"},
    {T_PredictByFunction, "PredictByFunction"},
    {T_TrainModel, "TrainModel"},
    {T_TrainModelState, "TrainModelState"},
    {T_ExplainModelStmt, "ExplainModelStmt"},
    // End DB4AI
    {T_TdigestData, "TdigestData"},
    {T_CentroidPoint, "CentroidPoint"},
    {T_RotateClause, "RotateClause"},
    {T_UnrotateClause,  "UnrotateClause"},
    {T_RotateInCell, "RotateInCell"},
    {T_UnrotateInCell, "UnrotateInCell"},
    {T_KeepClause, "KeepClause"},
    {T_AdvanceCatalogXminCmd, "AdvanceCatalogXminCmd"},
    {T_UserSetElem, "UserSetElem"},
    {T_UserVar, "UserVar"},
    {T_SetVariableExpr, "SetVariableExpr"},
    {T_VariableMultiSetStmt, "VariableMultiSetStmt"},
    {T_IndexHintDefinition, "IndexHintDefinition"},
    {T_IndexHintRelationData, "IndexHintRelationData"},
    {T_FunctionSources, "FunctionSources"},
    {T_CondInfo, "CondInfo"},
    {T_GetDiagStmt, "GetDiagStmt"},
    {T_DolphinCallStmt, "DolphinCallStmt"},
    {T_CallContext, "CallContext"},
    {T_CreateMatViewLogStmt, "CreateMatViewLogStmt"},
    {T_DropMatViewLogStmt, "DropMatViewLogStmt"}
};

char* nodeTagToString(NodeTag tag)
{
    for (uint32 i = 0; i < sizeof(g_tagStrArr) / sizeof(g_tagStrArr[0]); i++) {
        if (g_tagStrArr[i].tag == tag) {
            return g_tagStrArr[i].str;
        }
    }

    return "UnknownTag";
}
