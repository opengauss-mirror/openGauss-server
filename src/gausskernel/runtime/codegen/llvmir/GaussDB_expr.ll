; ModuleID = '/data5/liyy/mppcode/GaussDB_expr.cc'
source_filename = "/data5/liyy/mppcode/GaussDB_expr.cc"
target datalayout = "e-m:e-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-unknown-linux-gnu"

%struct.FuncExprState = type { %struct.ExprState, %struct.List*, %struct.FmgrInfo, %struct.Tuplestorestate*, %struct.TupleTableSlot*, %struct.tupleDesc*, i8, i8, i8, i8, %struct.FunctionCallInfoData, %class.ScalarVector* }
%struct.ExprState = type <{ i32, [4 x i8], %struct.Expr*, i64 (%struct.ExprState*, %struct.ExprContext*, i8*, i32*)*, %class.ScalarVector* (%struct.ExprState*, %struct.ExprContext*, i8*, %class.ScalarVector*)*, i8* (i8*)*, %class.ScalarVector, i32, [4 x i8] }>
%struct.Expr = type { i32 }
%class.ScalarVector = type { i32, %struct.ScalarDesc, i8, i8*, %class.VarBuf*, i64*, { i64, i64 } }
%struct.ScalarDesc = type <{ i32, i32, i8, [3 x i8] }>
%class.VarBuf = type { %struct.varBuf*, %struct.varBuf*, %struct.MemoryContextData*, i32, i32 }
%struct.varBuf = type { i8*, i32, i32, %struct.varBuf* }
%struct.MemoryContextData = type { i32, %struct.MemoryContextMethods*, %struct.MemoryContextData*, %struct.MemoryContextData*, %struct.MemoryContextData*, %struct.MemoryContextData*, i8*, %union.pthread_rwlock_t, i8, i8, i32, i64 }
%struct.MemoryContextMethods = type { i8* (%struct.MemoryContextData*, i64, i64, i8*, i32)*, void (%struct.MemoryContextData*, i8*)*, i8* (%struct.MemoryContextData*, i8*, i64, i64, i8*, i32)*, void (%struct.MemoryContextData*)*, void (%struct.MemoryContextData*)*, void (%struct.MemoryContextData*)*, i64 (%struct.MemoryContextData*, i8*)*, i1 (%struct.MemoryContextData*)*, void (%struct.MemoryContextData*, i32)*, void (%struct.MemoryContextData*)* }
%union.pthread_rwlock_t = type { %struct.anon }
%struct.anon = type { i32, i32, i32, i32, i32, i32, i32, i32, i64, i64, i32 }
%struct.List = type { i32, i32, %struct.ListCell*, %struct.ListCell* }
%struct.ListCell = type { %union.anon.1, %struct.ListCell* }
%union.anon.1 = type { i8* }
%struct.FmgrInfo = type { i64 (%struct.FunctionCallInfoData*)*, i32, i16, i8, i8, i8, i8*, %struct.MemoryContextData*, %struct.Node*, i32, [64 x i8], i8*, i8, i32, i8, %class.ScalarVector* (%struct.FunctionCallInfoData*)*, %class.ScalarVector* (%struct.FunctionCallInfoData*)**, %struct.GenericFunRuntime* }
%struct.Node = type { i32 }
%struct.GenericFunRuntime = type { [33 x %class.ScalarVector**], [33 x i32], [33 x i64 (i64*)*], [33 x i64], [33 x i8], [1001 x i8], %struct.FunctionCallInfoData* }
%struct.Tuplestorestate = type opaque
%struct.TupleTableSlot = type { i32, i8, i8, i8, i8, %struct.HeapTupleData*, i8*, i32, i8, %struct.AttInMetadata*, i32, %struct.MemoryContextData*, %struct.tupleDesc*, %struct.MemoryContextData*, i32, i32, i64*, i8*, %struct.MinimalTupleData*, %struct.HeapTupleData, i64, i64 }
%struct.AttInMetadata = type { %struct.tupleDesc*, %struct.FmgrInfo*, i32*, i32* }
%struct.MinimalTupleData = type { i32, [6 x i8], i16, i16, i8, [1 x i8] }
%struct.HeapTupleData = type { i32, %struct.ItemPointerData, i32, i64, i64, i32, %struct.HeapTupleHeaderData* }
%struct.ItemPointerData = type { %struct.BlockIdData, i16 }
%struct.BlockIdData = type { i16, i16 }
%struct.HeapTupleHeaderData = type { %union.anon, %struct.ItemPointerData, i16, i16, i8, [1 x i8] }
%union.anon = type { %struct.HeapTupleFields }
%struct.HeapTupleFields = type { i32, i32, %union.anon.0 }
%union.anon.0 = type { i32 }
%struct.tupleDesc = type { i32, %struct.FormData_pg_attribute**, %struct.tupleConstr*, %struct.tupInitDefVal*, i32, i32, i8, i32 }
%struct.FormData_pg_attribute = type { i32, %struct.nameData, i32, i32, i16, i16, i32, i32, i32, i8, i8, i8, i8, i8, i8, i8, i8, i32, i32 }
%struct.nameData = type { [64 x i8] }
%struct.tupleConstr = type { %struct.attrDefault*, %struct.constrCheck*, i16*, i16, i16, i16, i8 }
%struct.attrDefault = type { i16, i8* }
%struct.constrCheck = type { i8*, i8*, i8, i8 }
%struct.tupInitDefVal = type { i64*, i8, i16 }
%struct.FunctionCallInfoData = type { %struct.FmgrInfo*, %struct.Node*, %struct.Node*, i32, i8, i16, i64*, i8*, i32*, [10 x i64], [10 x i8], [10 x i32], %class.ScalarVector*, %struct.UDFInfoType }
%struct.UDFInfoType = type <{ i64 (%struct.FunctionCallInfoData*, i32, i64)**, i64 (%struct.FunctionCallInfoData*, i32, i64)*, %struct.StringInfoData*, i8*, i32, i32, i64**, i8**, i64*, i8*, i8, [7 x i8] }>
%struct.StringInfoData = type { i8*, i32, i32, i32 }
%class.HashJoinTbl = type { %class.hashBasedOperator.base, i32, i32*, i32*, i32*, %struct.List*, i32, i8, %class.ScalarVector*, i8, i8, i8, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %struct.VecHashJoinState*, i32, i32, %struct.JoinStateLog, %class.hashOpSource*, [1000 x %struct.ReCheckCellLoc], [1000 x i8], i32, [1000 x i8], %class.hashFileSource*, %class.hashFileSource*, i8*, i32*, i8*, i8, i8*, %struct.hashCell**, i8, double, double, [2 x { i64, i64 }], [2 x { i64, i64 }], { i64, i64 }, [36 x { i64, i64 }], { i64, i64 }* }
%class.hashBasedOperator.base = type <{ i32 (...)**, %class.vechashtable*, %struct.MemoryContextData*, %struct.MemoryContextData*, [1000 x i64], [1000 x %struct.hashCell*], [1000 x i8], %struct.FmgrInfo*, %struct.FmgrInfo*, %struct.FmgrInfo*, %class.hashFileSource*, %class.hashFileSource*, i8, [3 x i8], i32, i64, i64, i64, i32, i32, i32*, i32*, %struct.ScalarDesc*, %struct.ScalarDesc*, i8, [3 x i8], i32, i32, [4 x i8], i64, i64, i64, i32, i8 }>
%class.vechashtable = type { i32, %struct.hashCell** }
%struct.hashCell = type { %union.anon.3, [1 x %struct.hashVal] }
%union.anon.3 = type { %struct.hashCell* }
%struct.hashVal = type { i64, i8 }
%class.VectorBatch = type { i32, i32, i8, i8*, %class.ScalarVector*, %struct.SysColContainer*, %struct.StringInfoData* }
%struct.SysColContainer = type <{ i32, [4 x i8], %class.ScalarVector*, [9 x i8], [7 x i8] }>
%struct.VecHashJoinState = type { %struct.HashJoinState, i32, i8*, %struct.FmgrInfo*, %class.ScalarVector* (%struct.ExprContext*)*, %class.ScalarVector* (%struct.ExprContext*)*, i8*, i8*, i8*, i8*, i32, %struct.BloomFilterRuntime, i8*, i8*, i8* }
%struct.HashJoinState = type { %struct.JoinState, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.HashJoinTableData*, i32, i32, i32, %struct.HashJoinTupleData*, %struct.HashJoinTupleData*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, i32, i8, i8, i8, i8 }
%struct.JoinState = type { %struct.PlanState, i32, %struct.List*, %struct.List* }
%struct.PlanState = type { i32, %struct.Plan*, %struct.EState*, %struct.Instrumentation*, %struct.List*, %struct.List*, %struct.PlanState*, %struct.PlanState*, %struct.List*, %struct.List*, %struct.Bitmapset*, %struct.TupleTableSlot*, %struct.ExprContext*, %struct.ProjectionInfo*, i8, i8, %struct.MemoryContextData*, i8, i8, i1 (%struct.ExprContext*, %class.VectorBatch*)*, %struct.List* }
%struct.Plan = type { i32, i32, i32, i32, double, double, double, double, i32, i32, %struct.List*, %struct.List*, %struct.Plan*, %struct.Plan*, i8, i32, %struct.List*, %struct.List*, %struct.ExecNodes*, %struct.Bitmapset*, %struct.Bitmapset*, i8, i8, i8, [2 x i32], i32, i8, i8, %struct.List*, %struct.List*, i32**, i32 }
%struct.ExecNodes = type { i32, %struct.List*, %struct.List*, %struct.Distribution, i8, %struct.List*, i32, i32, %struct.List*, i32, i8, %struct.List*, %struct.List* }
%struct.Distribution = type { i32, %struct.Bitmapset* }
%struct.EState = type { i32, i32, %struct.SnapshotData*, %struct.SnapshotData*, %struct.List*, %struct.PlannedStmt*, %struct.JunkFilter*, i32, %struct.ResultRelInfo*, i32, %struct.ResultRelInfo*, %struct.RelationData*, %struct.List*, %struct.HTAB*, %struct.PlanState*, %struct.PlanState*, %struct.PlanState*, %struct.PlanState*, %struct.List*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.ParamListInfoData*, %struct.ParamExecData*, %struct.MemoryContextData*, %struct.MemoryContextData*, %struct.List*, %struct.List*, i64, i64, i32, i32, i32, i8, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.ExprContext*, %struct.HeapTupleData**, i8*, i8*, %struct.List*, i8, i8, i8, %struct.List*, i32, %struct.BloomFilterControl, i8, i8, i8 }
%struct.SnapshotData = type { i1 (%struct.HeapTupleData*, %struct.SnapshotData*, i32)*, i64, i64, i64*, i64*, i32, i32, i32, i32, i32, i8, i64, i8, i8, i32, i32, i32, i8* }
%struct.PlannedStmt = type { i32, i32, i64, i8, i8, i8, i8, %struct.Plan*, %struct.List*, %struct.List*, %struct.Node*, %struct.List*, %struct.Bitmapset*, %struct.List*, %struct.List*, %struct.List*, i32, i32, i32, i32, i32, %struct.NodeDefinition*, i32, i32, [2 x i32], [2 x i32], i8, i32, i32, i32, i32, double, i32, i32, i32, i32, i32, [2 x i16*], i8*, %struct.List*, %struct.List*, i32, i32, i32, double, i8, i8, %struct.List*, %struct.List*, i32, %struct.NodeGroupQueryMem*, i8, i8 }
%struct.NodeDefinition = type { i32, i32, %struct.nameData, %struct.nameData, i32, i32, i32, %struct.nameData, i32, i32, i32, i8, i8, i8, i8, i32 }
%struct.NodeGroupQueryMem = type { i32, [64 x i8], [2 x i32] }
%struct.JunkFilter = type { i32, %struct.List*, %struct.tupleDesc*, i16*, %struct.TupleTableSlot*, i16, i16, i16, i16, %struct.List* }
%struct.ResultRelInfo = type { i32, i32, %struct.RelationData*, i32, %struct.RelationData**, %struct.IndexInfo**, %struct.TriggerDesc*, %struct.FmgrInfo*, %struct.List**, %struct.Instrumentation*, %struct.FdwRoutine*, i8*, %struct.List**, %struct.JunkFilter*, i16, %struct.ProjectionInfo*, %struct.MergeState*, i32 }
%struct.IndexInfo = type { i32, i32, [32 x i16], %struct.List*, %struct.List*, %struct.List*, %struct.List*, i32*, i32*, i16*, i8, i8, i8, i8, i16, %struct.UtilityDesc }
%struct.UtilityDesc = type { double, [2 x i32], i32, i32 }
%struct.TriggerDesc = type { %struct.Trigger*, i32, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8 }
%struct.Trigger = type { i32, i8*, i32, i16, i8, i8, i32, i32, i32, i8, i8, i16, i16, i16*, i8**, i8* }
%struct.FdwRoutine = type { i32, void (%struct.PlannerInfo*, %struct.RelOptInfo*, i32)*, void (%struct.PlannerInfo*, %struct.RelOptInfo*, i32)*, %struct.ForeignScan* (%struct.PlannerInfo*, %struct.RelOptInfo*, i32, %struct.ForeignPath*, %struct.List*, %struct.List*)*, void (%struct.ForeignScanState*, i32)*, %struct.TupleTableSlot* (%struct.ForeignScanState*)*, void (%struct.ForeignScanState*)*, void (%struct.ForeignScanState*)*, void (%struct.Query*, %struct.RangeTblEntry*, %struct.RelationData*)*, %struct.List* (%struct.PlannerInfo*, %struct.ModifyTable*, i32, i32)*, void (%struct.ModifyTableState*, %struct.ResultRelInfo*, %struct.List*, i32, i32)*, %struct.TupleTableSlot* (%struct.EState*, %struct.ResultRelInfo*, %struct.TupleTableSlot*, %struct.TupleTableSlot*)*, %struct.TupleTableSlot* (%struct.EState*, %struct.ResultRelInfo*, %struct.TupleTableSlot*, %struct.TupleTableSlot*)*, %struct.TupleTableSlot* (%struct.EState*, %struct.ResultRelInfo*, %struct.TupleTableSlot*, %struct.TupleTableSlot*)*, void (%struct.EState*, %struct.ResultRelInfo*)*, i32 (%struct.RelationData*)*, void (%struct.ForeignScanState*, %struct.ExplainState*)*, void (%struct.ModifyTableState*, %struct.ResultRelInfo*, %struct.List*, i32, %struct.ExplainState*)*, i1 (%struct.RelationData*, i32 (%struct.RelationData*, i32, %struct.HeapTupleData**, i32, double*, double*, i8*, i1)**, i32*, i8*, i1)*, i32 (%struct.RelationData*, i32, %struct.HeapTupleData**, i32, double*, double*, i8*, i1)*, %class.VectorBatch* (%struct.VecForeignScanState*)*, i32 ()*, void (%struct.Node*)*, void (%struct.Node*, i32, i32)*, void (%struct.ForeignScanState*, i8*, i32, i32)* }
%struct.PlannerInfo = type { i32, %struct.Query*, %struct.PlannerGlobal*, i32, %struct.PlannerInfo*, %struct.RelOptInfo**, i32, %struct.List*, %struct.Bitmapset*, %struct.RangeTblEntry**, %struct.Bitmapset*, %struct.List*, %struct.HTAB*, %struct.List**, i32, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.MemoryContextData*, double, double, double, i8, i8, i8, i8, i8, i32, %struct.List*, i32, %struct.Plan*, %struct.Bitmapset*, %struct.List*, i32, i8, i32, %struct.List*, i8*, %struct.List*, %struct.List*, i16*, i8, i32, %struct.ItstDisKey }
%struct.Query = type { i32, i32, i32, i64, i8, %struct.Node*, i32, i8, i8, i8, i8, i8, i8, i8, %struct.List*, %struct.List*, %struct.FromExpr*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.Node*, %struct.List*, %struct.List*, %struct.List*, %struct.Node*, %struct.Node*, %struct.List*, %struct.Node*, %struct.List*, %struct.HintState*, i8*, i8, i8, i8, i8, %struct.List*, %struct.ParamListInfoData*, i32, %struct.List*, %struct.List*, i8, i8, i8 }
%struct.FromExpr = type { i32, %struct.List*, %struct.Node* }
%struct.HintState = type { i32, i32, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List* }
%struct.PlannerGlobal = type { i32, %struct.ParamListInfoData*, %struct.List*, %struct.List*, %struct.List*, %struct.Bitmapset*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, i32, i32, i8, i32, i8, %struct.bloomfilter_context, i8, i32, i32, double, %struct.List*, %struct.PlannerContext* }
%struct.bloomfilter_context = type { i32, i8 }
%struct.PlannerContext = type { %struct.MemoryContextData*, %struct.MemoryContextData* }
%struct.RangeTblEntry = type { i32, i32, i8*, %struct.List*, i32, i32, i8, %struct.List*, i8, %struct.TableSampleClause*, i8, i8, %struct.Query*, i8, i32, %struct.List*, %struct.Node*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, i8*, i32, i8, %struct.List*, %struct.List*, %struct.List*, i8, %struct.Alias*, %struct.Alias*, %struct.Alias*, %struct.List*, i8, i8, i32, i32, %struct.Bitmapset*, %struct.Bitmapset*, %struct.Bitmapset*, %struct.Bitmapset*, i32, i8*, i8*, i8 }
%struct.TableSampleClause = type { i32, i32, %struct.List*, %struct.Expr* }
%struct.Alias = type { i32, i8*, %struct.List* }
%struct.ItstDisKey = type { %struct.List*, %struct.List* }
%struct.RelOptInfo = type { i32, i32, %struct.Bitmapset*, i8, i32, double, i32, i32, i16, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.Path*, %struct.List*, %struct.Path*, %struct.List*, i32, i32, i32, i16, i16, %struct.Bitmapset**, i32*, %struct.List*, double, double, double, double, %struct.PruningResult*, i32, %struct.PruningResult*, i32, %struct.PruningResult*, i32, %struct.Plan*, %struct.PlannerInfo*, %struct.FdwRoutine*, i8*, %struct.List*, %struct.QualCost, %struct.List*, i8, i32, i32, i8, %struct.List*, %struct.ItstDisKey, %struct.List*, %struct.List*, %struct.List*, %struct.RelOptInfo* }
%struct.Path = type { i32, i32, %struct.RelOptInfo*, %struct.ParamPathInfo*, double, double, double, double, double, %struct.List*, %struct.List*, i8, i32, %struct.Distribution, i32 }
%struct.ParamPathInfo = type { i32, %struct.Bitmapset*, double, %struct.List* }
%struct.PruningResult = type { i32, i32, %struct.PruningBoundary*, %struct.Bitmapset*, i32, %struct.Bitmapset*, %struct.List* }
%struct.PruningBoundary = type { i32, i32, i64*, i8*, i64*, i8* }
%struct.QualCost = type { double, double }
%struct.ForeignScan = type { %struct.Scan, i32, %struct.List*, %struct.List*, i8, i8, %struct.ErrorCacheEntry*, %struct.List*, %struct.RelationMetaData*, %struct.ForeignOptions*, i64, %struct.BloomFilterSet**, i32, i8, i8 }
%struct.Scan = type { %struct.Plan, i32, i8, i32, %struct.PruningResult*, i32, i8, i8, %struct.TableSampleClause*, %struct.OpMemInfo }
%struct.OpMemInfo = type { double, double, double, double }
%struct.ErrorCacheEntry = type { i32, i32, %struct.RangeTblEntry*, i8*, %class.ImportErrorLogger** }
%class.ImportErrorLogger = type { i32 (...)**, %struct.MemoryContextData*, %struct.tupleDesc* }
%struct.RelationMetaData = type { i32, i32, i32, i32, i32, i8*, i8, i8, i32, %struct.List* }
%struct.ForeignOptions = type { i32, i32, %struct.List* }
%struct.BloomFilterSet = type { i32, i64*, i64, i64, i64, i64, i64, i64, %struct.ValueBit*, i64, double, i8*, i64, double, i8*, i8, i8, i32, i32, i32, i32 }
%struct.ValueBit = type { [4 x i16] }
%struct.ForeignPath = type { %struct.Path, %struct.Path*, %struct.List* }
%struct.ForeignScanState = type { %struct.ScanState, %struct.FdwRoutine*, i8*, %struct.MemoryContextData*, %struct.ForeignOptions* }
%struct.ScanState = type { %struct.PlanState, %struct.RelationData*, %struct.HeapScanDescData*, %struct.TupleTableSlot*, i8, %struct.RelationData*, i8, i32, i32, %struct.List*, i32, %struct.List*, i8, i8, %struct.SeqScanAccessor*, i32, i32, i8, i8, %struct.SampleScanParams }
%struct.HeapScanDescData = type { %struct.RelationData*, %struct.SnapshotData*, i32, %struct.ScanKeyData*, i8, i8, i8, i8, i8, i32, i32, %struct.BufferAccessStrategyData*, i8, i8, i8, %struct.HeapTupleData, %struct.anon.2, i32, %struct.tupleDesc*, i32, %struct.ItemPointerData, i32, i32, i32, [291 x i16], %struct.SeqScanAccessor*, i32 }
%struct.ScanKeyData = type { i32, i16, i16, i32, i32, %struct.FmgrInfo, i64 }
%struct.BufferAccessStrategyData = type opaque
%struct.anon.2 = type { %struct.HeapTupleHeaderData, [8144 x i8] }
%struct.SeqScanAccessor = type { i32, i32, i32, i32 }
%struct.SampleScanParams = type { %struct.List*, %struct.ExprState*, i32, i8* }
%struct.ModifyTable = type { %struct.Plan, i32, i8, %struct.List*, i32, %struct.List*, %struct.List*, %struct.List*, %struct.List*, i32, i8, %struct.List*, %struct.List*, %struct.List*, %struct.List*, i8, %struct.ErrorCacheEntry*, i32, %struct.List*, %struct.List*, %struct.OpMemInfo }
%struct.ModifyTableState = type { %struct.PlanState, i32, i8, i8, %struct.PlanState**, %struct.PlanState**, %struct.PlanState**, %struct.PlanState**, %struct.PlanState**, i32, i32, %struct.ResultRelInfo*, %struct.List**, %struct.EPQState, i8, %struct.RelationData*, %struct.RelationData*, %struct.ErrorCacheEntry*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, i32, %struct.timeval }
%struct.EPQState = type { %struct.EState*, %struct.PlanState*, %struct.TupleTableSlot*, %struct.Plan*, %struct.List*, i32 }
%struct.timeval = type { i64, i64 }
%struct.ExplainState = type { %struct.StringInfoData*, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i32, %struct.PlannedStmt*, %struct.List*, i32, i32, %struct.List*, %class.PlanInformation*, %struct.DN_RunInfo, i32* }
%class.PlanInformation = type { %class.PlanTable*, %class.PlanTable*, %class.PlanTable*, %class.PlanTable*, %class.PlanTable*, %class.PlanTable*, %class.PlanTable*, %class.PlanTable*, i32, %class.PlanTable*, i8, i32 }
%class.PlanTable = type <{ %struct.StringInfoData*, i64, i32, [4 x i8], %struct.tupleDesc*, i8, i8, i8, i8, [4 x i8], %struct.StringInfoData, i8, [7 x i8], double, i32, i32, i32*, i32, i8, [3 x i8], i64**, i8**, %struct.MultiInfo***, [10 x i16], [4 x i8] }>
%struct.MultiInfo = type { i64*, i8* }
%struct.DN_RunInfo = type { i8, i32, i32* }
%struct.VecForeignScanState = type { %struct.ForeignScanState, %class.VectorBatch*, %class.VectorBatch*, %struct.MemoryContextData*, i8, i64*, i8* }
%struct.MergeState = type { %struct.List*, %struct.List* }
%struct.RelationData = type { %struct.RelFileNode, %struct.SMgrRelationData*, i32, i32, i8, i8, i8, i8, i64, i64, %struct.FormData_pg_class*, %struct.tupleDesc*, i32, %struct.LockInfoData, %struct.RuleLock*, %struct.MemoryContextData*, %struct.TriggerDesc*, %struct.List*, i32, %struct.Bitmapset*, %struct.Bitmapset*, i32, %struct.varlena*, %struct.FormData_pg_index*, %struct.HeapTupleData*, %struct.FormData_pg_am*, %struct.MemoryContextData*, %struct.RelationAmInfo*, i32*, i32*, i32*, %struct.FmgrInfo*, i16*, %struct.List*, %struct.List*, i32*, i32*, i16*, i8*, i32*, %struct.FdwRoutine*, i32, %struct.PartitionMap*, i32, %struct.PgStat_TableStatus*, %struct.RelationLocInfo* }
%struct.RelFileNode = type { i32, i32, i32 }
%struct.SMgrRelationData = type { %struct.RelFileNodeBackend, %struct.SMgrRelationData**, i32, i32, i32, i32, i32*, i32, i32, %struct._MdfdVec**, %struct.SMgrRelationData* }
%struct.RelFileNodeBackend = type { %struct.RelFileNode, i32 }
%struct._MdfdVec = type opaque
%struct.FormData_pg_class = type { %struct.nameData, i32, i32, i32, i32, i32, i32, i32, double, double, i32, i32, i32, i32, i32, i32, i32, i8, i8, i8, i8, i16, i16, i8, i8, i8, i8, i8, i8, i8, i8, i8, i32, i8, i64 }
%struct.LockInfoData = type { %struct.LockRelId }
%struct.LockRelId = type { i32, i32 }
%struct.RuleLock = type { i32, %struct.RewriteRule** }
%struct.RewriteRule = type { i32, i32, i16, %struct.Node*, %struct.List*, i8, i8 }
%struct.varlena = type { [4 x i8], [1 x i8] }
%struct.FormData_pg_index = type { i32, i32, i16, i8, i8, i8, i8, i8, i8, i8, i8, i8, %struct.int2vector, i8 }
%struct.int2vector = type { i32, i32, i32, i32, i32, i32, [1 x i16] }
%struct.FormData_pg_am = type { %struct.nameData, i16, i16, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i8, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32 }
%struct.RelationAmInfo = type { %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo, %struct.FmgrInfo }
%struct.PartitionMap = type { i32, i32, i8 }
%struct.PgStat_TableStatus = type { i32, i8, i32, %struct.PgStat_TableXactStatus*, %struct.PgStat_TableCounts }
%struct.PgStat_TableXactStatus = type { i64, i64, i64, i32, %struct.PgStat_TableXactStatus*, %struct.PgStat_TableStatus*, %struct.PgStat_TableXactStatus* }
%struct.PgStat_TableCounts = type { i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64 }
%struct.RelationLocInfo = type { i32, i8, %struct.List*, %struct.List*, %struct.ListCell*, %struct.nameData, i16* }
%struct.HTAB = type opaque
%struct.ParamListInfoData = type { void (%struct.ParamListInfoData*, i32)*, i8*, void (%struct.ParseState*, i8*)*, i8*, i32, [1 x %struct.ParamExternData] }
%struct.ParseState = type { %struct.ParseState*, i8*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.CommonTableExpr*, %struct.List*, i32, %struct.List*, %struct.Node*, i8, i8, i8, i8, i8, i8, i8, i8, %struct.RelationData*, %struct.RangeTblEntry*, %struct.Node* (%struct.ParseState*, %struct.ColumnRef*)*, %struct.Node* (%struct.ParseState*, %struct.ColumnRef*, %struct.Node*)*, %struct.Node* (%struct.ParseState*, %struct.ParamRef*)*, %struct.Node* (%struct.ParseState*, %struct.Param*, i32, i32, i32)*, i8*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, i8, i8, i8, i8, i8, i8, %struct.PlusJoinRTEInfo* }
%struct.CommonTableExpr = type { i32, i8*, %struct.List*, %struct.Node*, i32, i8, i32, %struct.List*, %struct.List*, %struct.List*, %struct.List* }
%struct.ColumnRef = type { i32, %struct.List*, i32 }
%struct.ParamRef = type { i32, i32, i32 }
%struct.Param = type { %struct.Expr, i32, i32, i32, i32, i32, i32 }
%struct.PlusJoinRTEInfo = type { i8, %struct.List* }
%struct.ParamExternData = type { i64, i8, i16, i32 }
%struct.ParamExecData = type { i8*, i32, i64, i8, i8*, i8, i8 }
%struct.BloomFilterControl = type { %"class.filter::BloomFilter"**, i32 }
%"class.filter::BloomFilter" = type { i32 (...)**, i8*, i8* }
%struct.Instrumentation = type { i8, i8, i8, i8, %struct.timeval, %struct.timeval, double, double, %struct.BufferUsage, double, double, double, double, double, double, %struct.BufferUsage, %struct.CPUUsage, %struct.CPUUsage, %struct.SortHashInfo, %struct.NetWorkPerfData, %struct.StreamSendData, %struct.RoughCheckInfo, %struct.MemoryInfo, i64, i64, i64, i64, i64, double, double, double, double, i32, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i8, i32, i32, i8, i32, i8, i8, i32, i64, i32, i32, [65 x i8], [65 x i8], [65 x i8], [1025 x i8], i32, i64 }
%struct.BufferUsage = type { i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, %struct.timeval, %struct.timeval }
%struct.CPUUsage = type { double }
%struct.SortHashInfo = type { i32, i32, i64, i32, i32, i32, i64, i64, i64, i64, i8, i32, i32, double, double, i64, i64, i64, i64, i64, i64, i64, i32, i32, i32 }
%struct.NetWorkPerfData = type { i64, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, double, double, double }
%struct.StreamSendData = type { %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, %struct.timeval, double, double, double, double, double, i8 }
%struct.RoughCheckInfo = type { i64, i64, i64 }
%struct.MemoryInfo = type { i64, i64, i64, i32, %struct.MemoryContextData*, %struct.List* }
%struct.Bitmapset = type { i32, [1 x i32] }
%struct.ProjectionInfo = type { i32, %struct.List*, %struct.ExprContext*, %struct.TupleTableSlot*, i32*, i8, i32, i32*, i32*, i32*, i32, i32, i32, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, %struct.List*, i8, %class.VectorBatch*, i1 (%struct.ExprContext*, %class.VectorBatch*)* }
%struct.HashJoinTableData = type opaque
%struct.HashJoinTupleData = type opaque
%struct.BloomFilterRuntime = type { %struct.List*, %struct.List*, %"class.filter::BloomFilter"** }
%struct.JoinStateLog = type { i32, %struct.hashCell*, i8 }
%class.hashOpSource = type { %class.hashSource, %struct.PlanState* }
%class.hashSource = type { i32 (...)** }
%struct.ReCheckCellLoc = type { %struct.hashCell*, i32, i32 }
%class.hashFileSource = type { %class.hashSource, i32, i64*, i64, i64*, i64*, i32, i32, i32*, %class.VectorBatch*, %struct.hashCell*, %struct.MemoryContextData*, i8**, %struct.TupleTableSlot*, %struct.MinimalTupleData*, i32, i32, i32, i64*, i8*, i32, i64 (i64*)**, [2 x { i64, i64 }], [2 x { i64, i64 }], { i64, i64 }, { i64, i64 }, { i64, i64 }, { i64, i64 }, { i64, i64 }, { i64, i64 }, { i64, i64 }, { i64, i64 }, { i64, i64 } }
%struct.NumericData = type { i32, %union.NumericChoice }
%union.NumericChoice = type { %struct.NumericLong }
%struct.NumericLong = type { i16, i16, [1 x i16] }
%struct.ExprContext = type { i32, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.MemoryContextData*, %struct.MemoryContextData*, %struct.ParamExecData*, %struct.ParamListInfoData*, i64*, i8*, i64, i8, %class.ScalarVector*, i64, i8, %struct.EState*, %struct.ExprContext_CB*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, i32, i8, %class.ScalarVector*, %class.ScalarVector* }
%struct.ExprContext_CB = type { %struct.ExprContext_CB*, void (i64)*, i64 }
%struct.varattrib_1b = type { i8, [1 x i8] }
%struct.anon.4 = type { i32, [1 x i8] }
%class.HashAggRunner = type <{ %class.BaseAggRunner, %struct.AggStateLog, %class.hashSource*, [1000 x i64], i32, i8, [3 x i8], i64, i64, double, double, %struct.MemoryContextData*, %struct.HashSegTbl*, i32, i32, i64, { i64, i64 }, i32, [4 x i8] }>
%class.BaseAggRunner = type { %class.hashBasedOperator.base, { i64, i64 }, %struct.VecAggState*, i32, i32, i8*, i32*, i32*, i32*, i8, i8, i32, %struct.finalAggInfo*, i32, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, i32, [1000 x %struct.hashCell*], %struct.SortDistinct*, %class.VarBuf*, i32, %struct.ExprContext* }
%struct.VecAggState = type { %struct.AggState, i8*, %struct.VecAggInfo*, i8*, i8*, i8*, i8*, i8* }
%struct.AggState = type { %struct.ScanState, %struct.List*, i32, %struct.AggStatePerPhaseData*, i32, i32, %struct.FmgrInfo*, %struct.AggStatePerAggData*, %struct.MemoryContextData**, %struct.ExprContext*, i8, i8, i32, i32, %struct.Bitmapset*, %struct.List*, i32, %struct.AggStatePerPhaseData*, %struct.Tuplesortstate*, %struct.Tuplesortstate*, %struct.TupleTableSlot*, %struct.AggStatePerGroupData*, %struct.HeapTupleData*, %struct.TupleHashTableData*, %struct.TupleTableSlot*, %struct.List*, i8, %struct.HASH_SEQ_STATUS, i8, i8*, %struct.FmgrInfo* }
%struct.AggStatePerAggData = type opaque
%struct.AggStatePerPhaseData = type opaque
%struct.Tuplesortstate = type opaque
%struct.AggStatePerGroupData = type opaque
%struct.TupleHashTableData = type { %struct.HTAB*, i32, i16*, %struct.FmgrInfo*, %struct.FmgrInfo*, %struct.MemoryContextData*, %struct.MemoryContextData*, i64, %struct.TupleTableSlot*, %struct.TupleTableSlot*, %struct.FmgrInfo*, %struct.FmgrInfo*, i64, i8, i8 }
%struct.HASH_SEQ_STATUS = type { %struct.HTAB*, i32, %struct.HASHELEMENT* }
%struct.HASHELEMENT = type { %struct.HASHELEMENT*, i32 }
%struct.VecAggInfo = type { %struct.FunctionCallInfoData, %struct.FunctionCallInfoData, %class.ScalarVector* (%struct.FunctionCallInfoData*)**, %class.ScalarVector* (%struct.FunctionCallInfoData*)**, i64 (%struct.FunctionCallInfoData*)** }
%struct.finalAggInfo = type { i32, %struct.VecAggInfo* }
%struct.SortDistinct = type { %class.Batchsortstate**, %class.VectorBatch**, i64*, %struct.hashCell*, i32* }
%class.Batchsortstate = type { %class.VecStore.base, %struct.MemoryContextData*, i32, %struct.RemoteQueryState*, %class.VectorBatch**, i32*, %struct.VecStreamState*, i32, %struct.ScanKeyData*, %struct.SortSupportData*, i64, i8, i8, i8, i32, %struct.MultiColumnsData, i8*, i64, i32, i32, i32, %struct.LogicalTapeSet*, i32, i8*, i32*, i32*, i32*, i64*, i32, i32, i32, i32, i32*, i32*, i32*, i32*, i32, i32, i8, i64, i32, i8, i64, i64, %struct.PGRUsage, i8*, i8*, { i64, i64 }, i32 (%struct.MultiColumns*, %struct.MultiColumns*, %class.Batchsortstate*)*, void (%class.Batchsortstate*, %struct.MultiColumns*, i8*)*, void (%class.Batchsortstate*, i32, %struct.MultiColumns*)*, void (%class.Batchsortstate*, %struct.MultiColumns*, i32, i32)*, i32 (%class.Batchsortstate*, i32, i1)*, void (%class.Batchsortstate*)*, void (%class.Batchsortstate*, %class.VectorBatch*, i32, i32)* }
%class.VecStore.base = type <{ %struct.MultiColumnsData, i64, i64, %class.ScalarVector*, i32, [4 x i8], %struct.tupleDesc*, i64, i64, i64, i8, i8, [6 x i8], i64, i32, i32, i32 }>
%struct.RemoteQueryState = type { %struct.ScanState, i32, %struct.pgxc_node_handle**, i32, i32, i32, i32, i32, %struct.tupleDesc*, i32, i32, i32, i32, i8*, i8*, i8*, i8*, i8*, i32, i8, i8, %struct.RemoteDataRowData, %struct.RowStoreManagerData*, i32*, i32, %struct._IO_FILE*, i64, i8*, i8*, i32, %struct.pgxc_node_handle**, i8*, i32, i32*, i32, i32, i8, %struct.Tuplestorestate*, i32, i64, i8*, i8*, i1 (%struct.RemoteQueryState*, %struct.TupleTableSlot*)*, i8, %struct.RemoteErrorData, i8, [2 x i64], [2 x i64], i32, i32, i8*, i8, %struct.NodeIdxInfo*, i32, i8 }
%struct.RemoteDataRowData = type { i8*, i32, i32 }
%struct.RowStoreManagerData = type { %struct.Bank**, %struct.MemoryContextData*, i32 }
%struct.Bank = type { %struct.List*, %struct.BufFile*, i64, i64, i32, i64, i32, i64, i32 }
%struct.BufFile = type opaque
%struct._IO_FILE = type { i32, i8*, i8*, i8*, i8*, i8*, i8*, i8*, i8*, i8*, i8*, i8*, %struct._IO_marker*, %struct._IO_FILE*, i32, i32, i64, i16, i8, [1 x i8], i8*, i64, i8*, i8*, i8*, i8*, i64, i32, [20 x i8] }
%struct._IO_marker = type { %struct._IO_marker*, %struct._IO_FILE*, i32 }
%struct.pgxc_node_handle = type { i32, i32, i32, i32, %struct.gsocket, i8, i32, %struct.RemoteQueryState*, %struct.StreamState*, i8*, i8*, i8*, i64, i64, i8*, i64, i64, i64, i64, i32, i8*, i32, %struct.PoolConnInfo, i8, %struct.pg_conn* }
%struct.gsocket = type { i16, i16, i16, i16 }
%struct.StreamState = type { %struct.ScanState, %struct.pgxc_node_handle**, i32, i32, i8, i8, i32, i8*, i8*, i8*, i8, %struct.StreamDataBuf, %class.StreamConsumer*, i1 (%struct.StreamState*)*, i1 (%struct.StreamState*)*, %struct.RemoteErrorData, i64, %struct.StreamNetCtl, %struct.StreamSharedContext*, %struct.TupleVector*, i32, i64*, i8* }
%struct.StreamDataBuf = type { i8*, i32, i32 }
%class.StreamConsumer = type { %class.StreamObj, i32, i8, %struct.StreamConnInfo*, %union.pthread_mutex_t, %union.pthread_cond_t, %struct.StreamSharedContext* }
%class.StreamObj = type { %class.StreamNodeGroup*, %class.StreamTransport**, %struct.StreamPair*, i32, %struct.StreamKey, %struct.MemoryContextData*, i32, i32, %struct.Stream*, i32, i64, i32, i8, i8, i32, %struct.ParallelDesc }
%class.StreamNodeGroup = type <{ %struct.List*, %struct.List*, i64, i32, i32, %struct.StreamNode*, %struct.List*, i32, i32, i32, [4 x i8], %union.pthread_mutex_t, %union.pthread_cond_t, i8, i8, [6 x i8] }>
%struct.StreamNode = type { %class.StreamObj*, %struct.List*, i32, i8* }
%class.StreamTransport = type { i32 (...)**, [64 x i8], i32, i32, i8, %struct.Port*, %struct.StreamBuffer* }
%struct.Port = type { i32, i8, i32, %struct.SockAddr, %struct.SockAddr, i8*, i8*, i32, i8*, %struct.libcommaddrinfo*, %struct.gsocket, i32, i8*, i8*, i8*, %struct.List*, %struct.HbaLine*, [4 x i8], i64, i32, i32, i32, i32, i32, i32, i32, %struct.pg_gssinfo*, %struct.stSSL*, %struct.stCertExtnData*, i8*, i64, [9 x i8], i8 }
%struct.SockAddr = type { %struct.sockaddr_storage, i32 }
%struct.sockaddr_storage = type { i16, i64, [112 x i8] }
%struct.libcommaddrinfo = type { i8*, [64 x i8], i32, i32, i32, i32, %struct.SctpStreamKey, i32, i8, i32, %struct.libcommaddrinfo*, %struct.gsocket }
%struct.SctpStreamKey = type { i64, i32, i32, i32 }
%struct.HbaLine = type { i32, i32, %struct.List*, %struct.List*, %struct.sockaddr_storage, %struct.sockaddr_storage, i32, i8*, i32, i8*, i8*, i8, i8*, i32, i8*, i8*, i8*, i8*, i8*, i8*, i8, i8*, i8*, i8, i8*, i8*, i8*, i32, i32 }
%struct.pg_gssinfo = type { %struct.gss_buffer_desc_struct, %struct.gss_cred_id_struct*, %struct.gss_ctx_id_struct*, %struct.gss_name_struct* }
%struct.gss_buffer_desc_struct = type { i64, i8* }
%struct.gss_cred_id_struct = type opaque
%struct.gss_ctx_id_struct = type opaque
%struct.gss_name_struct = type opaque
%struct.stSSL = type { i32, i32, %struct.stSSLMethod*, i32, i32, i32 (%struct.stSSL*)*, i8*, i32, i32, i32, i32, i32, i32, %struct.buf_mem_st*, %struct.buf_mem_st*, i8*, i32, i32, i8*, i32, %struct.stSSL2State*, %struct.stSSL3State*, i32, void (i32, i32, i32, i8*, i32, %struct.stSSL*, i8*)*, i8*, i32, %struct.stX509VerifyParam*, %struct.stSEC_List*, %struct.stSEC_List*, i8*, i32, %struct.comp_ctx_st*, i8*, i32, %struct.comp_ctx_st*, %struct.cert_st*, %struct.stSSLPSKINFO, i32 (i8*, i32, i8*, i32*)*, i32 (%struct.stSSL*, i8*, i8*, i32*, i8*, i32, i8*)*, i32 (%struct.stSSL*, i8*, i32, i8*, i32, i8*)*, i8*, i8*, i32, [32 x i8], %struct.stSSLSession*, i32 (%struct.stSSL*, i8*, i32*)*, i32, i32 (i32, %struct.stX509StoreCtx*)*, void (%struct.stSSL*, i32, i32)*, i32, i32, %struct.stSSLCtx*, i32, i32, %struct.stSEC_List*, i32, i32, i32, i32, i32, i32, %struct.stTLSSessionExtn*, i32, i32, i32, i32, i32, i32 (i8*, i8*, i32, i32)*, i32 (i8*, i8*, i32, i32)*, i32 (i8*, i8*, i32, i8*, i32*)*, i8*, i32 (i8*, i8*, i32, i8*, i32*)*, i8*, i32, %struct.stIpsiDTLS1State*, %struct.stRWAIO, %struct.stIpsiDTLS1CtxInfo*, %struct.stCBAppData, i32, %struct.stIpsiT12SignSupported, i32, i8*, i32, i32 (%struct.stSSL*, i8*)*, i8*, i32 }
%struct.stSSLMethod = type { i32, i32 (%struct.stSSL*)*, void (%struct.stSSL*)*, void (%struct.stSSL*)*, i32 (%struct.stSSL*)*, i32 (%struct.stSSL*)*, i32 (%struct.stSSL*, i8*, i32)*, i32 (%struct.stSSL*, i8*, i32)*, i32 (%struct.stSSL*, i8*, i32)*, i32 (%struct.stSSL*)*, i32 (%struct.stSSL*)*, i32 (%struct.stSSL*)*, i32 (%struct.stSSL*, i32, i32, i32, i32, i32*)*, i32 (%struct.stSSL*, i32, i8*, i32, i32)*, i32 (%struct.stSSL*, i32, i8*, i32)*, i32 (%struct.stSSL*)*, i32 (%struct.stSSL*, i32, i32, i8*)*, i32 (%struct.stSSLCtx*, i32, i32, i8*)*, %struct.stSSLCipher* (i8*)*, i32 (%struct.stSSLCipher*, i8*)*, i32 (%struct.stSSL*)*, i32 ()*, %struct.stSSLCipher* (i32)*, %struct.stSSLMethod* (i32)*, i32 ()*, %struct.stSSL3EncMethod*, i32 ()*, i32 (%struct.stSSL*, i32, void ()*)*, i32 (%struct.stSSLCtx*, i32, void ()*)* }
%struct.stSSLCipher = type { i32, i8*, i32, i32, i32, i32, i32, i32, i32, i32 }
%struct.stSSL3EncMethod = type opaque
%struct.buf_mem_st = type opaque
%struct.stSSL2State = type { i32, i32, i32, i32, i32, i32, i8*, i32, i32, i32, i32, i32, i8*, i8*, i8*, i32, i32, i32, i32, i32, i8*, i8*, i8*, i8*, i8*, i32, [32 x i8], i32, [16 x i8], i32, [48 x i8], i32, i32, %struct.anon.8 }
%struct.anon.8 = type { i32, i32, i32, i32, i32, i32, [32 x i8], i32, i32, i32, i32 }
%struct.stSSL3State = type { i32, i32, [8 x i8], [64 x i8], [8 x i8], [64 x i8], [32 x i8], [32 x i8], i32, i32, %struct.stSSL3Buffer, %struct.stSSL3Buffer, %struct.stSSL3_Record, %struct.stSSL3_Record, [2 x i8], i32, [4 x i8], i32, i32, i32, i32, i32, i8*, i8*, i8*, i32, i32, i32, i32, [2 x i8], i32, i32, i32, i32, i32, %struct.anon.9, %struct.stIpsiSecureReneg, i8*, i8*, i8*, i8*, [16 x i8], [16 x i8] }
%struct.stSSL3Buffer = type { i8*, i32, i32, i32 }
%struct.stSSL3_Record = type { i32, i32, i32, i8*, i8*, i8*, i32, [8 x i8], i32 }
%struct.anon.9 = type { [128 x i8], [128 x i8], i32, [128 x i8], i32, i32, i32, %struct.stSSLCipher*, %struct.stSEC_PKEY*, i32, i32, i32, i32, [7 x i8], %struct.stSEC_List*, i32, i32, i8*, i32, i32, %struct.stSSLComp*, i32 }
%struct.stSEC_PKEY = type { i32, i8*, i32 }
%struct.stSSLComp = type { i32, i8*, %struct.comp_method_st* }
%struct.comp_method_st = type { i32, i8*, i32 (i8**)*, void (i8**)*, i32 (i8**, i8*, i32, i8*, i32)*, i32 (i8**, i8*, i32, i8*, i32)*, i64 ()*, i64 ()* }
%struct.stIpsiSecureReneg = type { [64 x i8], i32, [64 x i8], i32, i32 }
%struct.stX509VerifyParam = type { i8*, %struct.tagDateTime, i32, i32, i32, i32 }
%struct.tagDateTime = type { i16, i8, i8, i8, i8, i16, i8, i8, i8, i8 }
%struct.comp_ctx_st = type { %struct.comp_method_st*, i64, i64, i64, i64, i8* }
%struct.cert_st = type opaque
%struct.stSSLPSKINFO = type { i32, [128 x i8], i32, [64 x i8], [128 x i8], i32 }
%struct.stSSLSession = type { i32, i32, [8 x i8], i32, [48 x i8], i32, [32 x i8], i32, [32 x i8], i32, %struct.stSess_Cert*, %struct.stCertExtnData*, i32, i32, i32, i32, i32, %struct.stSSLCipher*, i32, %struct.stSEC_List*, %struct.stSSLSession*, %struct.stSSLSession*, %struct.stTLSSessionExtn* }
%struct.stSess_Cert = type opaque
%struct.stX509StoreCtx = type { %struct.stX509Store*, i32, %struct.stCertExtnData*, %struct.stSEC_List*, %struct.stSEC_List*, %struct.stX509VerifyParam*, i8*, {}*, i32 (i32, %struct.stX509StoreCtx*)*, i32 (%struct.stCertExtnData**, %struct.stX509StoreCtx*, %struct.stCertExtnData*)*, i32 (%struct.stX509StoreCtx*, %struct.stCertExtnData*, %struct.stCertExtnData*)*, {}*, i32 (%struct.stX509StoreCtx*, %struct.stX509Crl**, %struct.stCertExtnData*)*, i32 (%struct.stX509StoreCtx*, %struct.stX509Crl*)*, i32 (%struct.stX509StoreCtx*, %struct.stX509Crl*, %struct.stCertExtnData*)*, {}*, {}*, i32, i32, i32, %struct.stSEC_List*, i32, i32, i32, %struct.stCertExtnData*, %struct.stCertExtnData*, %struct.stX509Crl*, i32, i32, %struct.stX509StoreCtx*, i32, i32, i8*, i32 }
%struct.stX509Store = type { i32, %struct.stSEC_List*, %struct.stX509VerifyParam*, i32 (%struct.stX509StoreCtx*)*, i32 (i32, %struct.stX509StoreCtx*)*, i32 (%struct.stCertExtnData**, %struct.stX509StoreCtx*, %struct.stCertExtnData*)*, i32 (%struct.stX509StoreCtx*, %struct.stCertExtnData*, %struct.stCertExtnData*)*, i32 (%struct.stX509StoreCtx*)*, i32 (%struct.stX509StoreCtx*, %struct.stX509Crl**, %struct.stCertExtnData*)*, i32 (%struct.stX509StoreCtx*, %struct.stX509Crl*)*, i32 (%struct.stX509StoreCtx*, %struct.stX509Crl*, %struct.stCertExtnData*)*, i32 (%struct.stX509StoreCtx*)*, i32 }
%struct.stX509Crl = type { %struct.stCRLInfo*, %struct.stAlgorithmIdentifier*, %struct.stSEC_ASNBITS, i32, i8* }
%struct.stCRLInfo = type { i32*, %struct.stAlgorithmIdentifier*, %struct.stName*, %struct.stTime*, %struct.stTime*, %struct.stSEC_List*, %struct.stSEC_List* }
%struct.stName = type { i32, %"union.stName::NameChoiceUnion" }
%"union.stName::NameChoiceUnion" = type { %struct.stSEC_List* }
%struct.stTime = type { i32, %"union.stTime::TimeChoiceUnion" }
%"union.stTime::TimeChoiceUnion" = type { %struct.stSEC_ASNOCTS* }
%struct.stSEC_ASNOCTS = type { i32, i8* }
%struct.stAlgorithmIdentifier = type { %struct.stSEC_ASNOCTS, i8* }
%struct.stSEC_ASNBITS = type { i32, i8*, i32 }
%struct.stSSLCtx = type { %struct.stSSLMethod*, %struct.stSEC_List*, %struct.stSEC_List*, %struct.stX509Store*, %struct.lhash_st*, i32, %struct.stSSLSession*, %struct.stSSLSession*, i32, i32, i32 (%struct.stSSL*, %struct.stSSLSession*)*, void (%struct.stSSLCtx*, %struct.stSSLSession*)*, %struct.stSSLSession* (%struct.stSSL*, i8*, i32, i32*)*, %struct.anon.7, i32, i32 (%struct.stX509StoreCtx*, i8*)*, i8*, i8*, i8*, i32 (%struct.stSSL*, %struct.stCertExtnData**, %struct.stSEC_PKEY**)*, i32, i32, i32, %struct.stSEC_List*, %struct.stSEC_List*, void (%struct.stSSL*, i32, i32)*, %struct.stSEC_List*, i32, i32, i32, %struct.cert_st*, i32, void (i32, i32, i32, i8*, i32, %struct.stSSL*, i8*)*, i8*, i32, i32, [32 x i8], i32 (i32, %struct.stX509StoreCtx*)*, i32 (%struct.stSSL*, i8*, i32*)*, %struct.stX509VerifyParam*, i32, %struct.stTLSExtension*, i32 (%struct.stSSL*, %struct.stCertExtnData**, %struct.stSEC_PKEY**)*, i32 (%struct.stSSL*, i32*, %struct.stSEC_List*, %struct.stSEC_PKEY**)*, i32 (%struct.stSSL*, %struct.stSEC_ASNOCTS*)*, i32 (%struct.stSSL*, %struct.stSEC_ASNOCTS*)*, i32 (i8*, %struct.stSEC_ASNOCTS*)*, i32 (%struct.stSSL*)*, i32 (i8*, i32, i8*, i32*)*, i32 (%struct.stSSL*, i8*, i8*, i32*, i8*, i32, i8*)*, i32 (%struct.stSSL*, i8*, i32, i8*, i32, i8*)*, i8*, i8*, %struct.stIpsiDTLS1CtxInfo*, %struct.stCBAppData, i8*, i32, i32, i32, i32, i32, i32, i8*, i32 (%struct.stSSL*, i8*)*, i8* }
%struct.lhash_st = type opaque
%struct.anon.7 = type { i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32 }
%struct.stTLSExtension = type opaque
%struct.stSEC_List = type { %struct.stSEC_ListNode*, %struct.stSEC_ListNode*, %struct.stSEC_ListNode*, i32, i32 }
%struct.stSEC_ListNode = type { %struct.stSEC_ListNode*, %struct.stSEC_ListNode*, i8* }
%struct.stTLSSessionExtn = type opaque
%struct.stIpsiDTLS1State = type opaque
%struct.stRWAIO = type { %struct.stIpsiAio*, %struct.stIpsiAio* }
%struct.stIpsiAio = type { %struct.stIpsiAioMethod*, i32, i32, i8*, %struct.stIpsiAioCB }
%struct.stIpsiAioMethod = type { i32, %struct.stIpsiAioMetFun }
%struct.stIpsiAioMetFun = type { %struct.stIpsiAioMethRdWrFun, %struct.stIpsiAioMethCrDestFun, %struct.stIpsiAioMethGetSetErrFun, i32 (%struct.stIpsiAio*, i32, i32, i8*)* }
%struct.stIpsiAioMethRdWrFun = type { i32 (%struct.stIpsiAio*, i8*, i32)*, i32 (%struct.stIpsiAio*, i8*, i32)* }
%struct.stIpsiAioMethCrDestFun = type { i32 (%struct.stIpsiAio*)*, i32 (%struct.stIpsiAio*)* }
%struct.stIpsiAioMethGetSetErrFun = type { i32 (%struct.stIpsiAio*)*, void (%struct.stIpsiAio*, i32)* }
%struct.stIpsiAioCB = type { i32 (%struct.stIpsiAio*, i8*, i32, i8*)*, i32 (%struct.stIpsiAio*, i8*, i32, i8*)*, i8*, i8* }
%struct.stIpsiDTLS1CtxInfo = type opaque
%struct.stCBAppData = type { i8*, i8* }
%struct.stIpsiT12SignSupported = type { i32, [18 x %struct.stIpsiT12SignHash] }
%struct.stIpsiT12SignHash = type { i8, i8 }
%struct.stCertExtnData = type { %struct.stCertificate*, i32, i32, i32, i32, i32, i32, i32, %struct.stSEC_ASNOCTS*, %struct.stAuthorityKeyIdentifier*, [64 x i8], %struct.stSEC_PKEY*, %struct.stSEC_List*, i32 }
%struct.stCertificate = type { %struct.stCertInfo*, %struct.stAlgorithmIdentifier*, %struct.stSEC_ASNBITS, i32 }
%struct.stCertInfo = type { i32*, %struct.stBIGINT, %struct.stAlgorithmIdentifier*, %struct.stName*, %struct.stValidity*, %struct.stName*, %struct.stSubjectPublicKeyInfo*, %struct.stSEC_ASNBITS, %struct.stSEC_ASNBITS, %struct.stSEC_List* }
%struct.stBIGINT = type { i32, [516 x i8] }
%struct.stValidity = type { %struct.stTime*, %struct.stTime* }
%struct.stSubjectPublicKeyInfo = type { %struct.stAlgorithmIdentifier*, %struct.stSEC_ASNBITS }
%struct.stAuthorityKeyIdentifier = type { %struct.stSEC_ASNOCTS, %struct.stSEC_List*, %struct.stBIGINT* }
%struct.StreamBuffer = type { [8192 x i8], i32, i32, i32, i8 }
%struct.StreamPair = type { %struct.StreamKey, %struct.List*, %struct.List*, i32, i32 }
%struct.StreamKey = type { i64, i32, i32 }
%struct.Stream = type { %struct.Scan, i32, i8*, %struct.ExecNodes*, %struct.List*, i8, %struct.SimpleSort*, i8, %struct.ParallelDesc, i8*, %struct.List* }
%struct.SimpleSort = type { i32, i32, i16*, i32*, i32*, i8*, i8 }
%struct.ParallelDesc = type { i32, i32, i32 }
%struct.StreamConnInfo = type { %union.StreamIdentity, [64 x i8], i32, i32, i32 }
%union.StreamIdentity = type { %struct.anon.10, [4 x i8] }
%struct.anon.10 = type { i32 }
%union.pthread_mutex_t = type { %"struct.(anonymous union)::__pthread_mutex_s" }
%"struct.(anonymous union)::__pthread_mutex_s" = type { i32, i32, i32, i32, i32, i32, %struct.__pthread_internal_list }
%struct.__pthread_internal_list = type { %struct.__pthread_internal_list*, %struct.__pthread_internal_list* }
%union.pthread_cond_t = type { %struct.anon.6 }
%struct.anon.6 = type { i32, i32, i64, i64, i64, i8*, i32, i32 }
%struct.StreamNetCtl = type { %union.StreamNetCtlLayer }
%union.StreamNetCtlLayer = type { %struct.anon.13 }
%struct.anon.13 = type { i32*, i32*, %struct.gsocket* }
%struct.StreamSharedContext = type { %struct.MemoryContextData*, %class.VectorBatch***, %struct.TupleVector***, %struct.StringInfoData***, i32**, i8**, i32*, %struct.SctpStreamKey, i8, %struct.hash_entry**, %struct.hash_entry*** }
%struct.hash_entry = type opaque
%struct.TupleVector = type { %struct.TupleTableSlot**, i32 }
%struct.PoolConnInfo = type { %struct.nameData, i32 }
%struct.pg_conn = type opaque
%struct.RemoteErrorData = type { i32, i8*, i8*, i32, i32 }
%struct.NodeIdxInfo = type { i32, i32 }
%struct.VecStreamState = type { %struct.StreamState, %class.VectorBatch*, void (%struct.VecStreamState*, %class.VectorBatch*)*, i8, i32, i32, i8*, i32* }
%struct.SortSupportData = type { %struct.MemoryContextData*, i32, i8, i8, i16, i8*, i32 (i64, i64, %struct.SortSupportData*)*, i8, i64 (i64, %struct.SortSupportData*)*, i1 (i32, %struct.SortSupportData*)*, i32 (i64, i64, %struct.SortSupportData*)* }
%struct.MultiColumnsData = type { %struct.MultiColumns*, i32, i32, i32 }
%struct.MultiColumns = type { i64*, i8*, i32, i32 }
%struct.LogicalTapeSet = type opaque
%struct.PGRUsage = type { %struct.timeval, %struct.rusage }
%struct.rusage = type { %struct.timeval, %struct.timeval, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64 }
%struct.AggStateLog = type { i8, %struct.hashCell*, i32, i32 }
%struct.HashSegTbl = type { i32, %struct.hashCell** }
%class.SortAggRunner = type { %class.BaseAggRunner, i8, i8, i8, i32, i32, %struct.GroupintAtomContainer*, %class.Batchsortstate*, %class.Batchsortstate*, %class.VectorBatch*, %class.hashSource*, { i64, i64 } }
%struct.GroupintAtomContainer = type { [2000 x %struct.hashCell*], %class.VarBuf*, %class.VarBuf*, i32 }
%struct.bictl = type { i64, %struct.MemoryContextData* }
%"class.filter::BloomFilterImpl" = type { %"class.filter::BloomFilter", %"class.filter::BitSet"*, i64, i64, i64, i64, i64, %struct.ValueBit*, i64, i64, i8, i8, i32, i32, i32, i32, %struct.MemoryContextData* }
%"class.filter::BitSet" = type { i32 (...)**, i64*, i64 }
%class.SonicEncodingDatumArray = type { %class.SonicDatumArray }
%class.SonicDatumArray = type { i32 (...)**, %struct.MemoryContextData*, %struct.DatumDesc, i8, i32, i32, i32, i32, %struct.atom**, %struct.atom*, i32, i32 }
%struct.DatumDesc = type { i32, i32, i32, i32 }
%struct.atom = type { i8*, i8* }
%class.SonicHashAgg = type <{ %class.SonicHash, { i64, i64 }, %struct.VecAggState*, i32, i32, i64, i64, i32, [4 x i8], i64, i32, [4 x i8], %struct.ExprContext*, %class.SonicHashSource*, %class.SonicHashPartition**, %class.SonicHashPartition**, i16, i16, i32, i16, [6 x i8], i16*, i16*, i16, i16, [4 x i8], i8*, i16*, i8, [7 x i8], %struct.finalAggInfo*, %struct.FmgrInfo*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, %class.VectorBatch*, i8, [3 x i8], i32, %class.SonicDatumArray*, %class.SonicDatumArray*, i8, [7 x i8], { i64, i64 }, double, double, { i64, i64 }*, { i64, i64 }*, [1000 x i16], i16, [1000 x i16], i16, [1000 x i32], [1000 x i32], [4 x i8] }>
%class.SonicHash = type { i32 (...)**, %class.SonicDatumArray**, %class.SonicDatumArray*, i8*, i8, i32, i64, i64, { i64, i64 }*, %struct.hashStateLog, i8, i8, %"struct.SonicHash::SonicHashInputOpAttr", %struct.FmgrInfo*, %struct.SonicHashMemoryControl, [16384 x i32], [1000 x i32], [1000 x i32], [1000 x i16], i16, [1000 x i8], [1000 x i64], [1000 x i8], [1000 x %struct.ArrayIdx] }
%struct.hashStateLog = type { i32, i8 }
%"struct.SonicHash::SonicHashInputOpAttr" = type { i8, i16*, i16*, i16, { i64, i64 }*, { i64, i64 }*, %struct.FmgrInfo*, %class.VectorBatch*, i16, %struct.tupleDesc* }
%struct.SonicHashMemoryControl = type { i8, i8, i32, i64, i32, i64, i64, i64, %struct.MemoryContextData*, %struct.MemoryContextData* }
%struct.ArrayIdx = type { i32, i16 }
%class.SonicHashSource = type { i32 (...)** }
%class.SonicHashPartition = type { %class.SonicHashSource, %struct.MemoryContextData*, i32, i16, i64, i64*, i64 }

$_ZZ18getScaleMultiplieriE6values = comdat any

@Int64MultiOutOfBound = external dso_local local_unnamed_addr constant [20 x i64], align 16
@ScaleMultipler = external dso_local local_unnamed_addr constant [20 x i64], align 16
@assert_enabled = external thread_local local_unnamed_addr global i8, align 1
@.str = private unnamed_addr constant [29 x i8] c"!(scale >= 0 && scale <= 38)\00", align 1
@.str.1 = private unnamed_addr constant [16 x i8] c"FailedAssertion\00", align 1
@.str.2 = private unnamed_addr constant [76 x i8] c"/data5/liyy/mppcode/GAUSS200_OLAP_TRUNK/Code/src/include/utils/biginteger.h\00", align 1
@_ZZ18getScaleMultiplieriE6values = linkonce_odr dso_local local_unnamed_addr constant [39 x i128] [i128 1, i128 10, i128 100, i128 1000, i128 10000, i128 100000, i128 1000000, i128 10000000, i128 100000000, i128 1000000000, i128 10000000000, i128 100000000000, i128 1000000000000, i128 10000000000000, i128 100000000000000, i128 1000000000000000, i128 10000000000000000, i128 100000000000000000, i128 1000000000000000000, i128 10000000000000000000, i128 100000000000000000000, i128 1000000000000000000000, i128 10000000000000000000000, i128 100000000000000000000000, i128 1000000000000000000000000, i128 10000000000000000000000000, i128 100000000000000000000000000, i128 1000000000000000000000000000, i128 10000000000000000000000000000, i128 100000000000000000000000000000, i128 1000000000000000000000000000000, i128 10000000000000000000000000000000, i128 100000000000000000000000000000000, i128 1000000000000000000000000000000000, i128 10000000000000000000000000000000000, i128 100000000000000000000000000000000000, i128 1000000000000000000000000000000000000, i128 10000000000000000000000000000000000000, i128 100000000000000000000000000000000000000], comdat, align 16

; Function Attrs: norecurse nounwind uwtable
define dso_local %class.ScalarVector* @llvmJittedFunc(%struct.FuncExprState* nocapture readnone, %class.HashJoinTbl* nocapture readnone, %struct.NumericData* nocapture readnone, %struct.ExprContext* nocapture readonly, i8* nocapture readnone, %class.ScalarVector* returned) local_unnamed_addr #0 {
  %7 = getelementptr inbounds %struct.ExprContext, %struct.ExprContext* %3, i64 0, i32 17
  %8 = load %class.VectorBatch*, %class.VectorBatch** %7, align 8, !tbaa !2
  %9 = getelementptr inbounds %class.VectorBatch, %class.VectorBatch* %8, i64 0, i32 4
  %10 = load %class.ScalarVector*, %class.ScalarVector** %9, align 8, !tbaa !11
  %11 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %10, i64 25, i32 0
  %12 = load i32, i32* %11, align 8, !tbaa !13
  %13 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %10, i64 25, i32 5
  %14 = load i64*, i64** %13, align 8, !tbaa !16
  %15 = bitcast i64* %14 to i8*
  %16 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %5, i64 0, i32 5
  %17 = load i64*, i64** %16, align 8, !tbaa !16
  %18 = bitcast i64* %17 to i8*
  %19 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %5, i64 0, i32 3
  %20 = load i8*, i8** %19, align 8, !tbaa !17
  %21 = icmp sgt i32 %12, 0
  br i1 %21, label %22, label %152

; <label>:22:                                     ; preds = %6
  %23 = zext i32 %12 to i64
  %24 = icmp eq i32 %12, 1
  br i1 %24, label %106, label %25

; <label>:25:                                     ; preds = %22
  %26 = getelementptr i64, i64* %17, i64 %23
  %27 = bitcast i64* %26 to i8*
  %28 = getelementptr i8, i8* %20, i64 %23
  %29 = getelementptr i64, i64* %14, i64 %23
  %30 = bitcast i64* %29 to i8*
  %31 = icmp ugt i8* %28, %18
  %32 = icmp ult i8* %20, %27
  %33 = and i1 %31, %32
  %34 = icmp ult i64* %17, %29
  %35 = icmp ult i64* %14, %26
  %36 = and i1 %34, %35
  %37 = or i1 %33, %36
  %38 = icmp ult i8* %20, %30
  %39 = icmp ugt i8* %28, %15
  %40 = and i1 %38, %39
  %41 = or i1 %37, %40
  br i1 %41, label %106, label %42

; <label>:42:                                     ; preds = %25
  %43 = and i64 %23, 4294967294
  %44 = add nsw i64 %43, -2
  %45 = lshr exact i64 %44, 1
  %46 = add nuw i64 %45, 1
  %47 = and i64 %46, 1
  %48 = icmp eq i64 %44, 0
  br i1 %48, label %86, label %49

; <label>:49:                                     ; preds = %42
  %50 = sub i64 %46, %47
  br label %51

; <label>:51:                                     ; preds = %51, %49
  %52 = phi i64 [ 0, %49 ], [ %83, %51 ]
  %53 = phi i64 [ %50, %49 ], [ %84, %51 ]
  %54 = getelementptr inbounds i64, i64* %14, i64 %52
  %55 = bitcast i64* %54 to <2 x i64>*
  %56 = load <2 x i64>, <2 x i64>* %55, align 8, !tbaa !18, !alias.scope !19
  %57 = shl <2 x i64> %56, <i64 1, i64 1>
  %58 = add <2 x i64> %57, <i64 5, i64 5>
  %59 = icmp slt <2 x i64> %58, <i64 10, i64 10>
  %60 = zext <2 x i1> %59 to <2 x i64>
  %61 = getelementptr inbounds i64, i64* %17, i64 %52
  %62 = bitcast i64* %61 to <2 x i64>*
  store <2 x i64> %60, <2 x i64>* %62, align 8, !tbaa !18, !alias.scope !22, !noalias !24
  %63 = getelementptr inbounds i8, i8* %20, i64 %52
  %64 = bitcast i8* %63 to <2 x i8>*
  %65 = load <2 x i8>, <2 x i8>* %64, align 1, !tbaa !26, !alias.scope !27, !noalias !19
  %66 = and <2 x i8> %65, <i8 -2, i8 -2>
  %67 = bitcast i8* %63 to <2 x i8>*
  store <2 x i8> %66, <2 x i8>* %67, align 1, !tbaa !26, !alias.scope !27, !noalias !19
  %68 = or i64 %52, 2
  %69 = getelementptr inbounds i64, i64* %14, i64 %68
  %70 = bitcast i64* %69 to <2 x i64>*
  %71 = load <2 x i64>, <2 x i64>* %70, align 8, !tbaa !18, !alias.scope !19
  %72 = shl <2 x i64> %71, <i64 1, i64 1>
  %73 = add <2 x i64> %72, <i64 5, i64 5>
  %74 = icmp slt <2 x i64> %73, <i64 10, i64 10>
  %75 = zext <2 x i1> %74 to <2 x i64>
  %76 = getelementptr inbounds i64, i64* %17, i64 %68
  %77 = bitcast i64* %76 to <2 x i64>*
  store <2 x i64> %75, <2 x i64>* %77, align 8, !tbaa !18, !alias.scope !22, !noalias !24
  %78 = getelementptr inbounds i8, i8* %20, i64 %68
  %79 = bitcast i8* %78 to <2 x i8>*
  %80 = load <2 x i8>, <2 x i8>* %79, align 1, !tbaa !26, !alias.scope !27, !noalias !19
  %81 = and <2 x i8> %80, <i8 -2, i8 -2>
  %82 = bitcast i8* %78 to <2 x i8>*
  store <2 x i8> %81, <2 x i8>* %82, align 1, !tbaa !26, !alias.scope !27, !noalias !19
  %83 = add i64 %52, 4
  %84 = add i64 %53, -2
  %85 = icmp eq i64 %84, 0
  br i1 %85, label %86, label %51, !llvm.loop !28

; <label>:86:                                     ; preds = %51, %42
  %87 = phi i64 [ 0, %42 ], [ %83, %51 ]
  %88 = icmp eq i64 %47, 0
  br i1 %88, label %104, label %89

; <label>:89:                                     ; preds = %86
  %90 = getelementptr inbounds i64, i64* %14, i64 %87
  %91 = bitcast i64* %90 to <2 x i64>*
  %92 = load <2 x i64>, <2 x i64>* %91, align 8, !tbaa !18, !alias.scope !19
  %93 = shl <2 x i64> %92, <i64 1, i64 1>
  %94 = add <2 x i64> %93, <i64 5, i64 5>
  %95 = icmp slt <2 x i64> %94, <i64 10, i64 10>
  %96 = zext <2 x i1> %95 to <2 x i64>
  %97 = getelementptr inbounds i64, i64* %17, i64 %87
  %98 = bitcast i64* %97 to <2 x i64>*
  store <2 x i64> %96, <2 x i64>* %98, align 8, !tbaa !18, !alias.scope !22, !noalias !24
  %99 = getelementptr inbounds i8, i8* %20, i64 %87
  %100 = bitcast i8* %99 to <2 x i8>*
  %101 = load <2 x i8>, <2 x i8>* %100, align 1, !tbaa !26, !alias.scope !27, !noalias !19
  %102 = and <2 x i8> %101, <i8 -2, i8 -2>
  %103 = bitcast i8* %99 to <2 x i8>*
  store <2 x i8> %102, <2 x i8>* %103, align 1, !tbaa !26, !alias.scope !27, !noalias !19
  br label %104

; <label>:104:                                    ; preds = %86, %89
  %105 = icmp eq i64 %43, %23
  br i1 %105, label %152, label %106

; <label>:106:                                    ; preds = %104, %25, %22
  %107 = phi i64 [ 0, %25 ], [ 0, %22 ], [ %43, %104 ]
  %108 = xor i64 %107, -1
  %109 = and i64 %23, 1
  %110 = icmp eq i64 %109, 0
  br i1 %110, label %123, label %111

; <label>:111:                                    ; preds = %106
  %112 = getelementptr inbounds i64, i64* %14, i64 %107
  %113 = load i64, i64* %112, align 8, !tbaa !18
  %114 = shl i64 %113, 1
  %115 = add i64 %114, 5
  %116 = icmp slt i64 %115, 10
  %117 = zext i1 %116 to i64
  %118 = getelementptr inbounds i64, i64* %17, i64 %107
  store i64 %117, i64* %118, align 8, !tbaa !18
  %119 = getelementptr inbounds i8, i8* %20, i64 %107
  %120 = load i8, i8* %119, align 1, !tbaa !26
  %121 = and i8 %120, -2
  store i8 %121, i8* %119, align 1, !tbaa !26
  %122 = or i64 %107, 1
  br label %123

; <label>:123:                                    ; preds = %106, %111
  %124 = phi i64 [ %107, %106 ], [ %122, %111 ]
  %125 = sub nsw i64 0, %23
  %126 = icmp eq i64 %108, %125
  br i1 %126, label %152, label %127

; <label>:127:                                    ; preds = %123, %127
  %128 = phi i64 [ %150, %127 ], [ %124, %123 ]
  %129 = getelementptr inbounds i64, i64* %14, i64 %128
  %130 = load i64, i64* %129, align 8, !tbaa !18
  %131 = shl i64 %130, 1
  %132 = add i64 %131, 5
  %133 = icmp slt i64 %132, 10
  %134 = zext i1 %133 to i64
  %135 = getelementptr inbounds i64, i64* %17, i64 %128
  store i64 %134, i64* %135, align 8, !tbaa !18
  %136 = getelementptr inbounds i8, i8* %20, i64 %128
  %137 = load i8, i8* %136, align 1, !tbaa !26
  %138 = and i8 %137, -2
  store i8 %138, i8* %136, align 1, !tbaa !26
  %139 = add nuw nsw i64 %128, 1
  %140 = getelementptr inbounds i64, i64* %14, i64 %139
  %141 = load i64, i64* %140, align 8, !tbaa !18
  %142 = shl i64 %141, 1
  %143 = add i64 %142, 5
  %144 = icmp slt i64 %143, 10
  %145 = zext i1 %144 to i64
  %146 = getelementptr inbounds i64, i64* %17, i64 %139
  store i64 %145, i64* %146, align 8, !tbaa !18
  %147 = getelementptr inbounds i8, i8* %20, i64 %139
  %148 = load i8, i8* %147, align 1, !tbaa !26
  %149 = and i8 %148, -2
  store i8 %149, i8* %147, align 1, !tbaa !26
  %150 = add nsw i64 %128, 2
  %151 = icmp eq i64 %150, %23
  br i1 %151, label %152, label %127, !llvm.loop !30

; <label>:152:                                    ; preds = %123, %127, %104, %6
  %153 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %5, i64 0, i32 0
  store i32 %12, i32* %153, align 8, !tbaa !13
  %154 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %5, i64 0, i32 1, i32 0
  store i32 20, i32* %154, align 4, !tbaa !31
  ret %class.ScalarVector* %5
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @LLVMIRmemcmp(i8* nocapture readonly, i8* nocapture readonly, i32) local_unnamed_addr #1 {
  br label %4

; <label>:4:                                      ; preds = %11, %3
  %5 = phi i8* [ %0, %3 ], [ %13, %11 ]
  %6 = phi i8* [ %1, %3 ], [ %12, %11 ]
  %7 = phi i32 [ %2, %3 ], [ %14, %11 ]
  %8 = load i8, i8* %5, align 1, !tbaa !26
  %9 = load i8, i8* %6, align 1, !tbaa !26
  %10 = icmp eq i8 %8, %9
  br i1 %10, label %11, label %16

; <label>:11:                                     ; preds = %4
  %12 = getelementptr inbounds i8, i8* %6, i64 1
  %13 = getelementptr inbounds i8, i8* %5, i64 1
  %14 = add nsw i32 %7, -1
  %15 = icmp eq i32 %14, 0
  br i1 %15, label %16, label %4

; <label>:16:                                     ; preds = %4, %11
  %17 = phi i64 [ 1, %11 ], [ 0, %4 ]
  ret i64 %17
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local { i32, i8* } @LLVMIRrtrim1(i64) local_unnamed_addr #1 {
  %2 = inttoptr i64 %0 to %struct.varattrib_1b*
  %3 = getelementptr inbounds %struct.varattrib_1b, %struct.varattrib_1b* %2, i64 0, i32 0
  %4 = load i8, i8* %3, align 1, !tbaa !32
  %5 = zext i8 %4 to i32
  %6 = and i32 %5, 1
  %7 = icmp eq i32 %6, 0
  br i1 %7, label %11, label %8

; <label>:8:                                      ; preds = %1
  %9 = getelementptr inbounds %struct.varattrib_1b, %struct.varattrib_1b* %2, i64 0, i32 1, i64 0
  %10 = lshr i32 %5, 1
  br label %17

; <label>:11:                                     ; preds = %1
  %12 = inttoptr i64 %0 to %struct.anon.4*
  %13 = getelementptr inbounds %struct.anon.4, %struct.anon.4* %12, i64 0, i32 1, i64 0
  %14 = getelementptr inbounds %struct.anon.4, %struct.anon.4* %12, i64 0, i32 0
  %15 = load i32, i32* %14, align 4, !tbaa !26
  %16 = lshr i32 %15, 2
  br label %17

; <label>:17:                                     ; preds = %11, %8
  %18 = phi i32 [ -4, %11 ], [ -1, %8 ]
  %19 = phi i32 [ %16, %11 ], [ %10, %8 ]
  %20 = phi i8* [ %13, %11 ], [ %9, %8 ]
  %21 = add nsw i32 %19, %18
  %22 = sext i32 %21 to i64
  br label %23

; <label>:23:                                     ; preds = %27, %17
  %24 = phi i64 [ %25, %27 ], [ %22, %17 ]
  %25 = add nsw i64 %24, -1
  %26 = icmp sgt i64 %24, 0
  br i1 %26, label %27, label %31

; <label>:27:                                     ; preds = %23
  %28 = getelementptr inbounds i8, i8* %20, i64 %25
  %29 = load i8, i8* %28, align 1, !tbaa !26
  %30 = icmp eq i8 %29, 32
  br i1 %30, label %23, label %31

; <label>:31:                                     ; preds = %27, %23
  %32 = trunc i64 %24 to i32
  %33 = insertvalue { i32, i8* } undef, i32 %32, 0
  %34 = insertvalue { i32, i8* } %33, i8* %20, 1
  ret { i32, i8* } %34
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local { i32, i8* } @LLVMIRbtrim1(i64) local_unnamed_addr #1 {
  %2 = inttoptr i64 %0 to %struct.varattrib_1b*
  %3 = getelementptr inbounds %struct.varattrib_1b, %struct.varattrib_1b* %2, i64 0, i32 0
  %4 = load i8, i8* %3, align 1, !tbaa !32
  %5 = zext i8 %4 to i32
  %6 = and i32 %5, 1
  %7 = icmp eq i32 %6, 0
  br i1 %7, label %11, label %8

; <label>:8:                                      ; preds = %1
  %9 = getelementptr inbounds %struct.varattrib_1b, %struct.varattrib_1b* %2, i64 0, i32 1, i64 0
  %10 = lshr i32 %5, 1
  br label %17

; <label>:11:                                     ; preds = %1
  %12 = inttoptr i64 %0 to %struct.anon.4*
  %13 = getelementptr inbounds %struct.anon.4, %struct.anon.4* %12, i64 0, i32 1, i64 0
  %14 = getelementptr inbounds %struct.anon.4, %struct.anon.4* %12, i64 0, i32 0
  %15 = load i32, i32* %14, align 4, !tbaa !26
  %16 = lshr i32 %15, 2
  br label %17

; <label>:17:                                     ; preds = %11, %8
  %18 = phi i32 [ -4, %11 ], [ -1, %8 ]
  %19 = phi i32 [ %16, %11 ], [ %10, %8 ]
  %20 = phi i8* [ %13, %11 ], [ %9, %8 ]
  %21 = add nsw i32 %19, %18
  %22 = sext i32 %21 to i64
  br label %23

; <label>:23:                                     ; preds = %27, %17
  %24 = phi i64 [ %25, %27 ], [ %22, %17 ]
  %25 = add nsw i64 %24, -1
  %26 = icmp sgt i64 %24, 0
  br i1 %26, label %27, label %31

; <label>:27:                                     ; preds = %23
  %28 = getelementptr inbounds i8, i8* %20, i64 %25
  %29 = load i8, i8* %28, align 1, !tbaa !26
  %30 = icmp eq i8 %29, 32
  br i1 %30, label %23, label %31

; <label>:31:                                     ; preds = %27, %23
  %32 = trunc i64 %24 to i32
  %33 = icmp sgt i32 %32, 0
  br i1 %33, label %34, label %49

; <label>:34:                                     ; preds = %31
  %35 = shl i64 %24, 32
  %36 = ashr exact i64 %35, 32
  br label %37

; <label>:37:                                     ; preds = %34, %43
  %38 = phi i64 [ 0, %34 ], [ %44, %43 ]
  %39 = phi i32 [ 0, %34 ], [ %45, %43 ]
  %40 = getelementptr inbounds i8, i8* %20, i64 %38
  %41 = load i8, i8* %40, align 1, !tbaa !26
  %42 = icmp eq i8 %41, 32
  br i1 %42, label %43, label %47

; <label>:43:                                     ; preds = %37
  %44 = add nuw nsw i64 %38, 1
  %45 = add nuw nsw i32 %39, 1
  %46 = icmp sgt i64 %36, %44
  br i1 %46, label %37, label %49

; <label>:47:                                     ; preds = %37
  %48 = trunc i64 %38 to i32
  br label %49

; <label>:49:                                     ; preds = %43, %47, %31
  %50 = phi i32 [ 0, %31 ], [ %48, %47 ], [ %45, %43 ]
  %51 = sub nsw i32 %32, %50
  %52 = zext i32 %50 to i64
  %53 = getelementptr inbounds i8, i8* %20, i64 %52
  %54 = insertvalue { i32, i8* } undef, i32 %51, 0
  %55 = insertvalue { i32, i8* } %54, i8* %53, 1
  ret { i32, i8* } %55
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @LLVMIRbpchareq(i32, i8* nocapture readonly, i32, i8* nocapture readonly) local_unnamed_addr #1 {
  %5 = sext i32 %0 to i64
  br label %6

; <label>:6:                                      ; preds = %10, %4
  %7 = phi i64 [ %8, %10 ], [ %5, %4 ]
  %8 = add nsw i64 %7, -1
  %9 = icmp sgt i64 %7, 0
  br i1 %9, label %10, label %14

; <label>:10:                                     ; preds = %6
  %11 = getelementptr inbounds i8, i8* %1, i64 %8
  %12 = load i8, i8* %11, align 1, !tbaa !26
  %13 = icmp eq i8 %12, 32
  br i1 %13, label %6, label %14

; <label>:14:                                     ; preds = %10, %6
  %15 = trunc i64 %7 to i32
  %16 = sext i32 %2 to i64
  br label %17

; <label>:17:                                     ; preds = %21, %14
  %18 = phi i64 [ %19, %21 ], [ %16, %14 ]
  %19 = add nsw i64 %18, -1
  %20 = icmp sgt i64 %18, 0
  br i1 %20, label %21, label %25

; <label>:21:                                     ; preds = %17
  %22 = getelementptr inbounds i8, i8* %3, i64 %19
  %23 = load i8, i8* %22, align 1, !tbaa !26
  %24 = icmp eq i8 %23, 32
  br i1 %24, label %17, label %25

; <label>:25:                                     ; preds = %21, %17
  %26 = trunc i64 %18 to i32
  %27 = icmp eq i32 %15, %26
  br i1 %27, label %28, label %42

; <label>:28:                                     ; preds = %25
  %29 = icmp eq i32 %15, 0
  br i1 %29, label %42, label %30

; <label>:30:                                     ; preds = %28, %37
  %31 = phi i8* [ %39, %37 ], [ %1, %28 ]
  %32 = phi i8* [ %38, %37 ], [ %3, %28 ]
  %33 = phi i32 [ %40, %37 ], [ %15, %28 ]
  %34 = load i8, i8* %31, align 1, !tbaa !26
  %35 = load i8, i8* %32, align 1, !tbaa !26
  %36 = icmp eq i8 %34, %35
  br i1 %36, label %37, label %42

; <label>:37:                                     ; preds = %30
  %38 = getelementptr inbounds i8, i8* %32, i64 1
  %39 = getelementptr inbounds i8, i8* %31, i64 1
  %40 = add nsw i32 %33, -1
  %41 = icmp eq i32 %40, 0
  br i1 %41, label %42, label %30

; <label>:42:                                     ; preds = %30, %37, %25, %28
  %43 = phi i64 [ 1, %28 ], [ 0, %25 ], [ 0, %30 ], [ 1, %37 ]
  ret i64 %43
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @LLVMIRbpcharne(i32, i8* nocapture readonly, i32, i8* nocapture readonly) local_unnamed_addr #1 {
  %5 = sext i32 %0 to i64
  br label %6

; <label>:6:                                      ; preds = %10, %4
  %7 = phi i64 [ %8, %10 ], [ %5, %4 ]
  %8 = add nsw i64 %7, -1
  %9 = icmp sgt i64 %7, 0
  br i1 %9, label %10, label %14

; <label>:10:                                     ; preds = %6
  %11 = getelementptr inbounds i8, i8* %1, i64 %8
  %12 = load i8, i8* %11, align 1, !tbaa !26
  %13 = icmp eq i8 %12, 32
  br i1 %13, label %6, label %14

; <label>:14:                                     ; preds = %10, %6
  %15 = trunc i64 %7 to i32
  %16 = sext i32 %2 to i64
  br label %17

; <label>:17:                                     ; preds = %21, %14
  %18 = phi i64 [ %19, %21 ], [ %16, %14 ]
  %19 = add nsw i64 %18, -1
  %20 = icmp sgt i64 %18, 0
  br i1 %20, label %21, label %25

; <label>:21:                                     ; preds = %17
  %22 = getelementptr inbounds i8, i8* %3, i64 %19
  %23 = load i8, i8* %22, align 1, !tbaa !26
  %24 = icmp eq i8 %23, 32
  br i1 %24, label %17, label %25

; <label>:25:                                     ; preds = %21, %17
  %26 = trunc i64 %18 to i32
  %27 = icmp eq i32 %15, %26
  br i1 %27, label %28, label %42

; <label>:28:                                     ; preds = %25
  %29 = icmp eq i32 %15, 0
  br i1 %29, label %42, label %30

; <label>:30:                                     ; preds = %28, %37
  %31 = phi i8* [ %39, %37 ], [ %1, %28 ]
  %32 = phi i8* [ %38, %37 ], [ %3, %28 ]
  %33 = phi i32 [ %40, %37 ], [ %15, %28 ]
  %34 = load i8, i8* %31, align 1, !tbaa !26
  %35 = load i8, i8* %32, align 1, !tbaa !26
  %36 = icmp eq i8 %34, %35
  br i1 %36, label %37, label %42

; <label>:37:                                     ; preds = %30
  %38 = getelementptr inbounds i8, i8* %32, i64 1
  %39 = getelementptr inbounds i8, i8* %31, i64 1
  %40 = add nsw i32 %33, -1
  %41 = icmp eq i32 %40, 0
  br i1 %41, label %42, label %30

; <label>:42:                                     ; preds = %30, %37, %25, %28
  %43 = phi i64 [ 0, %28 ], [ 1, %25 ], [ 1, %30 ], [ 0, %37 ]
  ret i64 %43
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @LLVMIRtexteq(i32, i8* nocapture readonly, i32, i8* nocapture readonly) local_unnamed_addr #1 {
  %5 = icmp eq i32 %0, %2
  br i1 %5, label %6, label %20

; <label>:6:                                      ; preds = %4
  %7 = icmp eq i32 %0, 0
  br i1 %7, label %20, label %8

; <label>:8:                                      ; preds = %6, %15
  %9 = phi i32 [ %18, %15 ], [ %0, %6 ]
  %10 = phi i8* [ %17, %15 ], [ %1, %6 ]
  %11 = phi i8* [ %16, %15 ], [ %3, %6 ]
  %12 = load i8, i8* %10, align 1, !tbaa !26
  %13 = load i8, i8* %11, align 1, !tbaa !26
  %14 = icmp eq i8 %12, %13
  br i1 %14, label %15, label %20

; <label>:15:                                     ; preds = %8
  %16 = getelementptr inbounds i8, i8* %11, i64 1
  %17 = getelementptr inbounds i8, i8* %10, i64 1
  %18 = add nsw i32 %9, -1
  %19 = icmp eq i32 %18, 0
  br i1 %19, label %20, label %8

; <label>:20:                                     ; preds = %8, %15, %4, %6
  %21 = phi i64 [ 1, %6 ], [ 0, %4 ], [ 0, %8 ], [ 1, %15 ]
  ret i64 %21
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @LLVMIRtextneq(i32, i8* nocapture readonly, i32, i8* nocapture readonly) local_unnamed_addr #1 {
  %5 = icmp eq i32 %0, %2
  br i1 %5, label %6, label %20

; <label>:6:                                      ; preds = %4
  %7 = icmp eq i32 %0, 0
  br i1 %7, label %20, label %8

; <label>:8:                                      ; preds = %6, %15
  %9 = phi i32 [ %18, %15 ], [ %0, %6 ]
  %10 = phi i8* [ %17, %15 ], [ %1, %6 ]
  %11 = phi i8* [ %16, %15 ], [ %3, %6 ]
  %12 = load i8, i8* %10, align 1, !tbaa !26
  %13 = load i8, i8* %11, align 1, !tbaa !26
  %14 = icmp eq i8 %12, %13
  br i1 %14, label %15, label %20

; <label>:15:                                     ; preds = %8
  %16 = getelementptr inbounds i8, i8* %11, i64 1
  %17 = getelementptr inbounds i8, i8* %10, i64 1
  %18 = add nsw i32 %9, -1
  %19 = icmp eq i32 %18, 0
  br i1 %19, label %20, label %8

; <label>:20:                                     ; preds = %8, %15, %4, %6
  %21 = phi i64 [ 0, %6 ], [ 1, %4 ], [ 1, %8 ], [ 0, %15 ]
  ret i64 %21
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local { i32, i8* } @LLVMIRsubstring_ASCII(i32, i8*, i32, i32) local_unnamed_addr #1 {
  %5 = add nsw i32 %3, %2
  %6 = icmp eq i32 %0, 0
  br i1 %6, label %89, label %7

; <label>:7:                                      ; preds = %4
  %8 = sext i32 %0 to i64
  %9 = getelementptr inbounds i8, i8* %1, i64 %8
  %10 = icmp sgt i32 %2, 1
  %11 = icmp sgt i32 %0, 0
  %12 = and i1 %10, %11
  br i1 %12, label %13, label %49

; <label>:13:                                     ; preds = %7, %43
  %14 = phi i8* [ %45, %43 ], [ %1, %7 ]
  %15 = phi i32 [ %31, %43 ], [ 1, %7 ]
  %16 = load i8, i8* %14, align 1, !tbaa !26
  %17 = sext i8 %16 to i32
  %18 = icmp sgt i8 %16, -1
  br i1 %18, label %29, label %19

; <label>:19:                                     ; preds = %13
  %20 = and i32 %17, 224
  %21 = icmp eq i32 %20, 192
  br i1 %21, label %29, label %22

; <label>:22:                                     ; preds = %19
  %23 = and i32 %17, 240
  %24 = icmp eq i32 %23, 224
  br i1 %24, label %29, label %25

; <label>:25:                                     ; preds = %22
  %26 = and i32 %17, 248
  %27 = icmp eq i32 %26, 240
  %28 = select i1 %27, i32 4, i32 1
  br label %29

; <label>:29:                                     ; preds = %25, %22, %19, %13
  %30 = phi i32 [ 1, %13 ], [ 2, %19 ], [ 3, %22 ], [ %28, %25 ]
  %31 = add nuw nsw i32 %30, %15
  %32 = icmp slt i32 %31, %5
  br i1 %32, label %36, label %33

; <label>:33:                                     ; preds = %29
  %34 = sub nsw i32 %5, %15
  %35 = sext i32 %34 to i64
  br label %43

; <label>:36:                                     ; preds = %29
  %37 = icmp slt i32 %31, %2
  br i1 %37, label %41, label %38

; <label>:38:                                     ; preds = %36
  %39 = sub nsw i32 %2, %15
  %40 = sext i32 %39 to i64
  br label %43

; <label>:41:                                     ; preds = %36
  %42 = zext i32 %30 to i64
  br label %43

; <label>:43:                                     ; preds = %33, %41, %38
  %44 = phi i64 [ %35, %33 ], [ %42, %41 ], [ %40, %38 ]
  %45 = getelementptr inbounds i8, i8* %14, i64 %44
  %46 = icmp slt i32 %31, %2
  %47 = icmp ult i8* %45, %9
  %48 = and i1 %46, %47
  br i1 %48, label %13, label %49

; <label>:49:                                     ; preds = %43, %7
  %50 = phi i8* [ %1, %7 ], [ %45, %43 ]
  %51 = phi i1 [ %11, %7 ], [ %47, %43 ]
  br i1 %51, label %52, label %89

; <label>:52:                                     ; preds = %49
  %53 = icmp sgt i32 %3, 0
  %54 = icmp ult i8* %50, %9
  %55 = and i1 %53, %54
  br i1 %55, label %56, label %83

; <label>:56:                                     ; preds = %52, %72
  %57 = phi i8* [ %80, %72 ], [ %50, %52 ]
  %58 = phi i32 [ %74, %72 ], [ %2, %52 ]
  %59 = load i8, i8* %57, align 1, !tbaa !26
  %60 = sext i8 %59 to i32
  %61 = icmp sgt i8 %59, -1
  br i1 %61, label %72, label %62

; <label>:62:                                     ; preds = %56
  %63 = and i32 %60, 224
  %64 = icmp eq i32 %63, 192
  br i1 %64, label %72, label %65

; <label>:65:                                     ; preds = %62
  %66 = and i32 %60, 240
  %67 = icmp eq i32 %66, 224
  br i1 %67, label %72, label %68

; <label>:68:                                     ; preds = %65
  %69 = and i32 %60, 248
  %70 = icmp eq i32 %69, 240
  %71 = select i1 %70, i32 4, i32 1
  br label %72

; <label>:72:                                     ; preds = %68, %65, %62, %56
  %73 = phi i32 [ 1, %56 ], [ 2, %62 ], [ 3, %65 ], [ %71, %68 ]
  %74 = add nsw i32 %73, %58
  %75 = icmp slt i32 %74, %5
  %76 = sub nsw i32 %5, %58
  %77 = sext i32 %76 to i64
  %78 = zext i32 %73 to i64
  %79 = select i1 %75, i64 %78, i64 %77
  %80 = getelementptr inbounds i8, i8* %57, i64 %79
  %81 = icmp ult i8* %80, %9
  %82 = and i1 %75, %81
  br i1 %82, label %56, label %83

; <label>:83:                                     ; preds = %72, %52
  %84 = phi i8* [ %50, %52 ], [ %80, %72 ]
  %85 = ptrtoint i8* %84 to i64
  %86 = ptrtoint i8* %50 to i64
  %87 = sub i64 %85, %86
  %88 = trunc i64 %87 to i32
  br label %89

; <label>:89:                                     ; preds = %49, %4, %83
  %90 = phi i32 [ %88, %83 ], [ 0, %4 ], [ 0, %49 ]
  %91 = phi i8* [ %50, %83 ], [ %1, %4 ], [ %1, %49 ]
  %92 = insertvalue { i32, i8* } undef, i32 %90, 0
  %93 = insertvalue { i32, i8* } %92, i8* %91, 1
  ret { i32, i8* } %93
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local { i32, i8* } @LLVMIRsubstring_UTF8(i32, i8*, i32, i32) local_unnamed_addr #1 {
  %5 = add nsw i32 %3, %2
  %6 = icmp eq i32 %0, 0
  br i1 %6, label %75, label %7

; <label>:7:                                      ; preds = %4
  %8 = sext i32 %0 to i64
  %9 = getelementptr inbounds i8, i8* %1, i64 %8
  %10 = icmp sgt i32 %2, 1
  %11 = icmp sgt i32 %0, 0
  %12 = and i1 %10, %11
  br i1 %12, label %13, label %36

; <label>:13:                                     ; preds = %7, %29
  %14 = phi i8* [ %31, %29 ], [ %1, %7 ]
  %15 = phi i32 [ %32, %29 ], [ 1, %7 ]
  %16 = load i8, i8* %14, align 1, !tbaa !26
  %17 = sext i8 %16 to i32
  %18 = icmp sgt i8 %16, -1
  br i1 %18, label %29, label %19

; <label>:19:                                     ; preds = %13
  %20 = and i32 %17, 224
  %21 = icmp eq i32 %20, 192
  br i1 %21, label %29, label %22

; <label>:22:                                     ; preds = %19
  %23 = and i32 %17, 240
  %24 = icmp eq i32 %23, 224
  br i1 %24, label %29, label %25

; <label>:25:                                     ; preds = %22
  %26 = and i32 %17, 248
  %27 = icmp eq i32 %26, 240
  %28 = select i1 %27, i64 4, i64 1
  br label %29

; <label>:29:                                     ; preds = %25, %22, %19, %13
  %30 = phi i64 [ 1, %13 ], [ 2, %19 ], [ 3, %22 ], [ %28, %25 ]
  %31 = getelementptr inbounds i8, i8* %14, i64 %30
  %32 = add nuw nsw i32 %15, 1
  %33 = icmp slt i32 %32, %2
  %34 = icmp ult i8* %31, %9
  %35 = and i1 %33, %34
  br i1 %35, label %13, label %36

; <label>:36:                                     ; preds = %29, %7
  %37 = phi i8* [ %1, %7 ], [ %31, %29 ]
  %38 = phi i1 [ %11, %7 ], [ %34, %29 ]
  br i1 %38, label %39, label %75

; <label>:39:                                     ; preds = %36
  %40 = icmp sgt i32 %3, 0
  %41 = icmp ult i8* %37, %9
  %42 = and i1 %40, %41
  br i1 %42, label %43, label %66

; <label>:43:                                     ; preds = %39, %59
  %44 = phi i8* [ %61, %59 ], [ %37, %39 ]
  %45 = phi i32 [ %62, %59 ], [ %2, %39 ]
  %46 = load i8, i8* %44, align 1, !tbaa !26
  %47 = sext i8 %46 to i32
  %48 = icmp sgt i8 %46, -1
  br i1 %48, label %59, label %49

; <label>:49:                                     ; preds = %43
  %50 = and i32 %47, 224
  %51 = icmp eq i32 %50, 192
  br i1 %51, label %59, label %52

; <label>:52:                                     ; preds = %49
  %53 = and i32 %47, 240
  %54 = icmp eq i32 %53, 224
  br i1 %54, label %59, label %55

; <label>:55:                                     ; preds = %52
  %56 = and i32 %47, 248
  %57 = icmp eq i32 %56, 240
  %58 = select i1 %57, i64 4, i64 1
  br label %59

; <label>:59:                                     ; preds = %55, %52, %49, %43
  %60 = phi i64 [ 1, %43 ], [ 2, %49 ], [ 3, %52 ], [ %58, %55 ]
  %61 = getelementptr inbounds i8, i8* %44, i64 %60
  %62 = add nsw i32 %45, 1
  %63 = icmp slt i32 %62, %5
  %64 = icmp ult i8* %61, %9
  %65 = and i1 %63, %64
  br i1 %65, label %43, label %66

; <label>:66:                                     ; preds = %59, %39
  %67 = phi i8* [ %37, %39 ], [ %61, %59 ]
  %68 = ptrtoint i8* %67 to i64
  %69 = ptrtoint i8* %37 to i64
  %70 = sub i64 %68, %69
  %71 = trunc i64 %70 to i32
  %72 = icmp ugt i8* %67, %9
  %73 = sext i1 %72 to i32
  %74 = add i32 %71, %73
  br label %75

; <label>:75:                                     ; preds = %36, %4, %66
  %76 = phi i32 [ %74, %66 ], [ 0, %4 ], [ 0, %36 ]
  %77 = phi i8* [ %37, %66 ], [ %1, %4 ], [ %1, %36 ]
  %78 = insertvalue { i32, i8* } undef, i32 %76, 0
  %79 = insertvalue { i32, i8* } %78, i8* %77, 1
  ret { i32, i8* } %79
}

; Function Attrs: uwtable
define dso_local i64 @LLVMIRtextgt(i32, i8*, i32, i8*, i32) local_unnamed_addr #2 {
  %6 = icmp slt i32 %0, %2
  %7 = select i1 %6, i32 %0, i32 %2
  %8 = icmp sgt i32 %7, 0
  br i1 %8, label %9, label %29

; <label>:9:                                      ; preds = %5
  %10 = sext i32 %7 to i64
  br label %13

; <label>:11:                                     ; preds = %23
  %12 = icmp slt i64 %25, %10
  br i1 %12, label %13, label %29

; <label>:13:                                     ; preds = %9, %11
  %14 = phi i64 [ 0, %9 ], [ %25, %11 ]
  %15 = getelementptr inbounds i8, i8* %1, i64 %14
  %16 = load i8, i8* %15, align 1, !tbaa !26
  %17 = add i8 %16, -48
  %18 = icmp ult i8 %17, 10
  br i1 %18, label %19, label %26

; <label>:19:                                     ; preds = %13
  %20 = getelementptr inbounds i8, i8* %3, i64 %14
  %21 = load i8, i8* %20, align 1, !tbaa !26
  %22 = icmp slt i8 %16, %21
  br i1 %22, label %31, label %23

; <label>:23:                                     ; preds = %19
  %24 = icmp sgt i8 %16, %21
  %25 = add nuw nsw i64 %14, 1
  br i1 %24, label %31, label %11

; <label>:26:                                     ; preds = %13
  %27 = tail call i32 @_Z10varstr_cmpPciS_ij(i8* nonnull %1, i32 %0, i8* %3, i32 %2, i32 %4)
  %28 = icmp sgt i32 %27, 0
  br label %31

; <label>:29:                                     ; preds = %11, %5
  %30 = icmp sgt i32 %0, %2
  br label %31

; <label>:31:                                     ; preds = %23, %19, %29, %26
  %32 = phi i1 [ %28, %26 ], [ %30, %29 ], [ true, %23 ], [ false, %19 ]
  %33 = zext i1 %32 to i64
  ret i64 %33
}

declare dso_local i32 @_Z10varstr_cmpPciS_ij(i8*, i32, i8*, i32, i32) local_unnamed_addr #3

; Function Attrs: uwtable
define dso_local i64 @LLVMIRtextlt(i32, i8*, i32, i8*, i32) local_unnamed_addr #2 {
  %6 = icmp slt i32 %0, %2
  %7 = select i1 %6, i32 %0, i32 %2
  %8 = icmp sgt i32 %7, 0
  br i1 %8, label %9, label %29

; <label>:9:                                      ; preds = %5
  %10 = sext i32 %7 to i64
  br label %13

; <label>:11:                                     ; preds = %23
  %12 = icmp slt i64 %25, %10
  br i1 %12, label %13, label %29

; <label>:13:                                     ; preds = %9, %11
  %14 = phi i64 [ 0, %9 ], [ %25, %11 ]
  %15 = getelementptr inbounds i8, i8* %1, i64 %14
  %16 = load i8, i8* %15, align 1, !tbaa !26
  %17 = add i8 %16, -48
  %18 = icmp ult i8 %17, 10
  br i1 %18, label %19, label %26

; <label>:19:                                     ; preds = %13
  %20 = getelementptr inbounds i8, i8* %3, i64 %14
  %21 = load i8, i8* %20, align 1, !tbaa !26
  %22 = icmp slt i8 %16, %21
  br i1 %22, label %29, label %23

; <label>:23:                                     ; preds = %19
  %24 = icmp sgt i8 %16, %21
  %25 = add nuw nsw i64 %14, 1
  br i1 %24, label %29, label %11

; <label>:26:                                     ; preds = %13
  %27 = tail call i32 @_Z10varstr_cmpPciS_ij(i8* nonnull %1, i32 %0, i8* %3, i32 %2, i32 %4)
  %28 = icmp slt i32 %27, 0
  br label %29

; <label>:29:                                     ; preds = %19, %23, %11, %5, %26
  %30 = phi i1 [ %28, %26 ], [ %6, %5 ], [ true, %19 ], [ false, %23 ], [ %6, %11 ]
  %31 = zext i1 %30 to i64
  ret i64 %31
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @int4_bool2(%struct.FunctionCallInfoData* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %struct.FunctionCallInfoData, %struct.FunctionCallInfoData* %0, i64 0, i32 6
  %3 = load i64*, i64** %2, align 8, !tbaa !34
  %4 = load i64, i64* %3, align 8, !tbaa !18
  %5 = trunc i64 %4 to i32
  %6 = icmp ne i32 %5, 0
  %7 = zext i1 %6 to i64
  ret i64 %7
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @LLVMIRtextlike(i32, i8* nocapture readonly, i32, i8* nocapture readonly) local_unnamed_addr #1 {
  %5 = add nsw i32 %2, -1
  %6 = sext i32 %5 to i64
  %7 = getelementptr inbounds i8, i8* %3, i64 %6
  %8 = load i8, i8* %7, align 1, !tbaa !26
  %9 = icmp eq i8 %8, 37
  br i1 %9, label %10, label %12

; <label>:10:                                     ; preds = %4
  %11 = icmp sgt i32 %5, %0
  br i1 %11, label %41, label %24

; <label>:12:                                     ; preds = %4
  %13 = load i8, i8* %3, align 1, !tbaa !26
  %14 = icmp eq i8 %13, 37
  br i1 %14, label %15, label %22

; <label>:15:                                     ; preds = %12
  %16 = icmp sgt i32 %5, %0
  br i1 %16, label %41, label %17

; <label>:17:                                     ; preds = %15
  %18 = getelementptr inbounds i8, i8* %3, i64 1
  %19 = sub nsw i32 %0, %5
  %20 = sext i32 %19 to i64
  %21 = getelementptr inbounds i8, i8* %1, i64 %20
  br label %24

; <label>:22:                                     ; preds = %12
  %23 = icmp eq i32 %0, %2
  br i1 %23, label %24, label %41

; <label>:24:                                     ; preds = %22, %17, %10
  %25 = phi i8* [ %1, %10 ], [ %21, %17 ], [ %1, %22 ]
  %26 = phi i32 [ %5, %10 ], [ %5, %17 ], [ %2, %22 ]
  %27 = phi i8* [ %3, %10 ], [ %18, %17 ], [ %3, %22 ]
  %28 = icmp eq i32 %26, 0
  br i1 %28, label %41, label %29

; <label>:29:                                     ; preds = %24, %36
  %30 = phi i8* [ %38, %36 ], [ %27, %24 ]
  %31 = phi i32 [ %39, %36 ], [ %26, %24 ]
  %32 = phi i8* [ %37, %36 ], [ %25, %24 ]
  %33 = load i8, i8* %32, align 1, !tbaa !26
  %34 = load i8, i8* %30, align 1, !tbaa !26
  %35 = icmp eq i8 %33, %34
  br i1 %35, label %36, label %41

; <label>:36:                                     ; preds = %29
  %37 = getelementptr inbounds i8, i8* %32, i64 1
  %38 = getelementptr inbounds i8, i8* %30, i64 1
  %39 = add nsw i32 %31, -1
  %40 = icmp eq i32 %39, 0
  br i1 %40, label %41, label %29

; <label>:41:                                     ; preds = %29, %36, %24, %22, %15, %10
  %42 = phi i64 [ 0, %10 ], [ 0, %15 ], [ 0, %22 ], [ 1, %24 ], [ 0, %29 ], [ 1, %36 ]
  ret i64 %42
}

; Function Attrs: nounwind readonly uwtable
define dso_local i64 @LLVMIRtextnotlike(i8* nocapture, i32, i8* nocapture, i32) local_unnamed_addr #4 {
  %5 = icmp sgt i32 %1, 0
  %6 = icmp sgt i32 %3, 0
  %7 = and i1 %5, %6
  br i1 %7, label %8, label %50

; <label>:8:                                      ; preds = %4, %42
  %9 = phi i8* [ %43, %42 ], [ %0, %4 ]
  %10 = phi i32 [ %46, %42 ], [ %3, %4 ]
  %11 = phi i8* [ %45, %42 ], [ %2, %4 ]
  %12 = phi i32 [ %44, %42 ], [ %1, %4 ]
  %13 = load i8, i8* %11, align 1, !tbaa !26
  %14 = icmp eq i8 %13, 37
  br i1 %14, label %15, label %39

; <label>:15:                                     ; preds = %8, %21
  %16 = phi i8* [ %18, %21 ], [ %11, %8 ]
  %17 = phi i32 [ %19, %21 ], [ %10, %8 ]
  %18 = getelementptr inbounds i8, i8* %16, i64 1
  %19 = add nsw i32 %17, -1
  %20 = icmp sgt i32 %17, 1
  br i1 %20, label %21, label %72

; <label>:21:                                     ; preds = %15
  %22 = load i8, i8* %18, align 1, !tbaa !26
  %23 = icmp eq i8 %22, 37
  br i1 %23, label %15, label %24

; <label>:24:                                     ; preds = %21
  %25 = icmp sgt i32 %12, 0
  br i1 %25, label %26, label %72

; <label>:26:                                     ; preds = %24, %35
  %27 = phi i8* [ %36, %35 ], [ %9, %24 ]
  %28 = phi i32 [ %37, %35 ], [ %12, %24 ]
  %29 = load i8, i8* %27, align 1, !tbaa !26
  %30 = icmp eq i8 %29, %22
  br i1 %30, label %31, label %35

; <label>:31:                                     ; preds = %26
  %32 = tail call i64 @LLVMIRtextnotlike(i8* nonnull %27, i32 %28, i8* nonnull %18, i32 %19)
  %33 = trunc i64 %32 to i32
  %34 = icmp eq i32 %33, 0
  br i1 %34, label %35, label %69

; <label>:35:                                     ; preds = %31, %26
  %36 = getelementptr inbounds i8, i8* %27, i64 1
  %37 = add nsw i32 %28, -1
  %38 = icmp sgt i32 %28, 1
  br i1 %38, label %26, label %72

; <label>:39:                                     ; preds = %8
  %40 = load i8, i8* %9, align 1, !tbaa !26
  %41 = icmp eq i8 %13, %40
  br i1 %41, label %42, label %72

; <label>:42:                                     ; preds = %39
  %43 = getelementptr inbounds i8, i8* %9, i64 1
  %44 = add nsw i32 %12, -1
  %45 = getelementptr inbounds i8, i8* %11, i64 1
  %46 = add nsw i32 %10, -1
  %47 = icmp sgt i32 %12, 1
  %48 = icmp sgt i32 %10, 1
  %49 = and i1 %47, %48
  br i1 %49, label %8, label %50

; <label>:50:                                     ; preds = %42, %4
  %51 = phi i8* [ %2, %4 ], [ %45, %42 ]
  %52 = phi i32 [ %3, %4 ], [ %46, %42 ]
  %53 = phi i1 [ %5, %4 ], [ %47, %42 ]
  br i1 %53, label %72, label %54

; <label>:54:                                     ; preds = %50
  %55 = icmp sgt i32 %52, 0
  br i1 %55, label %56, label %65

; <label>:56:                                     ; preds = %54, %61
  %57 = phi i32 [ %63, %61 ], [ %52, %54 ]
  %58 = phi i8* [ %62, %61 ], [ %51, %54 ]
  %59 = load i8, i8* %58, align 1, !tbaa !26
  %60 = icmp eq i8 %59, 37
  br i1 %60, label %61, label %65

; <label>:61:                                     ; preds = %56
  %62 = getelementptr inbounds i8, i8* %58, i64 1
  %63 = add nsw i32 %57, -1
  %64 = icmp sgt i32 %57, 1
  br i1 %64, label %56, label %65

; <label>:65:                                     ; preds = %56, %61, %54
  %66 = phi i32 [ %52, %54 ], [ %63, %61 ], [ %57, %56 ]
  %67 = icmp slt i32 %66, 1
  %68 = select i1 %67, i64 1, i64 -1
  br label %72

; <label>:69:                                     ; preds = %31
  %70 = shl i64 %32, 32
  %71 = ashr exact i64 %70, 32
  br label %72

; <label>:72:                                     ; preds = %39, %15, %35, %69, %24, %65, %50
  %73 = phi i64 [ 0, %50 ], [ %68, %65 ], [ -1, %24 ], [ %71, %69 ], [ -1, %35 ], [ 1, %15 ], [ 0, %39 ]
  ret i64 %73
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local { i16, i64 } @LLVMIRBINum2int8(i64) local_unnamed_addr #1 {
  %2 = inttoptr i64 %0 to %struct.NumericData*
  %3 = getelementptr inbounds %struct.NumericData, %struct.NumericData* %2, i64 0, i32 1, i32 0, i32 0
  %4 = load i16, i16* %3, align 4, !tbaa !26
  %5 = getelementptr inbounds %struct.NumericData, %struct.NumericData* %2, i64 0, i32 1, i32 0, i32 1
  %6 = bitcast i16* %5 to i64*
  %7 = load i64, i64* %6, align 2, !tbaa !18
  %8 = and i16 %4, 255
  %9 = insertvalue { i16, i64 } undef, i16 %8, 0
  %10 = insertvalue { i16, i64 } %9, i64 %7, 1
  ret { i16, i64 } %10
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local zeroext i1 @GetStatusofHashAggRunner(%class.HashAggRunner* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %class.HashAggRunner, %class.HashAggRunner* %0, i64 0, i32 0, i32 9
  %3 = load i8, i8* %2, align 8, !tbaa !38, !range !40
  %4 = icmp ne i8 %3, 0
  ret i1 %4
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i32 @GetmkeyofSortAggRunner(%class.SortAggRunner* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %class.SortAggRunner, %class.SortAggRunner* %0, i64 0, i32 0, i32 0, i32 19
  %3 = load i32, i32* %2, align 4, !tbaa !41
  ret i32 %3
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @BigI64pos(%struct.bictl* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %struct.bictl, %struct.bictl* %0, i64 0, i32 0
  %3 = load i64, i64* %2, align 8, !tbaa !43
  ret i64 %3
}

; Function Attrs: norecurse nounwind readnone uwtable
define dso_local i64 @Bi64LoadGlobalVar(i64, i32) local_unnamed_addr #5 {
  %3 = sext i32 %1 to i64
  %4 = getelementptr inbounds [20 x i64], [20 x i64]* @Int64MultiOutOfBound, i64 0, i64 %3
  %5 = load i64, i64* %4, align 8, !tbaa !18
  %6 = icmp ult i64 %5, %0
  br i1 %6, label %7, label %10

; <label>:7:                                      ; preds = %2
  %8 = uitofp i64 %0 to double
  %9 = fptoui double %8 to i64
  br label %18

; <label>:10:                                     ; preds = %2
  %11 = getelementptr inbounds [20 x i64], [20 x i64]* @ScaleMultipler, i64 0, i64 %3
  %12 = load i64, i64* %11, align 8, !tbaa !18
  %13 = icmp ult i64 %12, %0
  br i1 %13, label %14, label %18

; <label>:14:                                     ; preds = %10
  %15 = uitofp i64 %0 to double
  %16 = fmul double %15, 2.000000e+00
  %17 = fptoui double %16 to i64
  br label %18

; <label>:18:                                     ; preds = %10, %14, %7
  %19 = phi i64 [ %9, %7 ], [ %17, %14 ], [ undef, %10 ]
  ret i64 %19
}

; Function Attrs: uwtable
define dso_local i64 @Simplebi64add64CodeGen(i32, %struct.NumericData* nocapture readonly, %struct.NumericData* nocapture readonly) local_unnamed_addr #2 {
  %4 = getelementptr inbounds %struct.NumericData, %struct.NumericData* %1, i64 0, i32 1, i32 0, i32 0
  %5 = load i16, i16* %4, align 4, !tbaa !26
  %6 = getelementptr inbounds %struct.NumericData, %struct.NumericData* %2, i64 0, i32 1, i32 0, i32 0
  %7 = load i16, i16* %6, align 4, !tbaa !26
  %8 = getelementptr inbounds %struct.NumericData, %struct.NumericData* %1, i64 0, i32 1, i32 0, i32 1
  %9 = bitcast i16* %8 to i64*
  %10 = load i64, i64* %9, align 2, !tbaa !18
  %11 = getelementptr inbounds %struct.NumericData, %struct.NumericData* %2, i64 0, i32 1, i32 0, i32 1
  %12 = bitcast i16* %11 to i64*
  %13 = load i64, i64* %12, align 2, !tbaa !18
  %14 = and i16 %5, 255
  %15 = and i16 %7, 255
  %16 = icmp ugt i16 %14, %15
  %17 = select i1 %16, i16 %14, i16 %15
  %18 = zext i16 %17 to i32
  switch i32 %0, label %36 [
    i32 0, label %19
    i32 2, label %21
  ]

; <label>:19:                                     ; preds = %3
  %20 = add i64 %13, %10
  br label %51

; <label>:21:                                     ; preds = %3
  %22 = zext i16 %15 to i32
  %23 = sub nsw i32 %18, %22
  %24 = load i8, i8* @assert_enabled, align 1, !tbaa !45, !range !40
  %25 = icmp eq i8 %24, 0
  %26 = icmp ult i32 %23, 39
  %27 = or i1 %26, %25
  br i1 %27, label %29, label %28

; <label>:28:                                     ; preds = %21
  tail call void @_Z20ExceptionalConditionPKcS0_S0_i(i8* getelementptr inbounds ([29 x i8], [29 x i8]* @.str, i64 0, i64 0), i8* getelementptr inbounds ([16 x i8], [16 x i8]* @.str.1, i64 0, i64 0), i8* getelementptr inbounds ([76 x i8], [76 x i8]* @.str.2, i64 0, i64 0), i32 98) #8
  unreachable

; <label>:29:                                     ; preds = %21
  %30 = sext i32 %23 to i64
  %31 = getelementptr inbounds [39 x i128], [39 x i128]* @_ZZ18getScaleMultiplieriE6values, i64 0, i64 %30
  %32 = load i128, i128* %31, align 16, !tbaa !46
  %33 = trunc i128 %32 to i64
  %34 = mul i64 %13, %33
  %35 = add i64 %34, %10
  br label %51

; <label>:36:                                     ; preds = %3
  %37 = zext i16 %14 to i32
  %38 = sub nsw i32 %18, %37
  %39 = load i8, i8* @assert_enabled, align 1, !tbaa !45, !range !40
  %40 = icmp eq i8 %39, 0
  %41 = icmp ult i32 %38, 39
  %42 = or i1 %41, %40
  br i1 %42, label %44, label %43

; <label>:43:                                     ; preds = %36
  tail call void @_Z20ExceptionalConditionPKcS0_S0_i(i8* getelementptr inbounds ([29 x i8], [29 x i8]* @.str, i64 0, i64 0), i8* getelementptr inbounds ([16 x i8], [16 x i8]* @.str.1, i64 0, i64 0), i8* getelementptr inbounds ([76 x i8], [76 x i8]* @.str.2, i64 0, i64 0), i32 98) #8
  unreachable

; <label>:44:                                     ; preds = %36
  %45 = sext i32 %38 to i64
  %46 = getelementptr inbounds [39 x i128], [39 x i128]* @_ZZ18getScaleMultiplieriE6values, i64 0, i64 %45
  %47 = load i128, i128* %46, align 16, !tbaa !46
  %48 = trunc i128 %47 to i64
  %49 = mul i64 %10, %48
  %50 = add i64 %49, %13
  br label %51

; <label>:51:                                     ; preds = %44, %29, %19
  %52 = phi i64 [ %50, %44 ], [ %35, %29 ], [ %20, %19 ]
  ret i64 %52
}

; Function Attrs: nounwind uwtable
define dso_local zeroext i1 @match_key(%class.VectorBatch* nocapture readonly, i32, %struct.hashCell* nocapture readonly, i32) local_unnamed_addr #6 {
  %5 = icmp sgt i32 %3, 0
  br i1 %5, label %6, label %37

; <label>:6:                                      ; preds = %4
  %7 = getelementptr inbounds %class.VectorBatch, %class.VectorBatch* %0, i64 0, i32 4
  %8 = load %class.ScalarVector*, %class.ScalarVector** %7, align 8, !tbaa !11
  %9 = sext i32 %1 to i64
  %10 = sext i32 %3 to i64
  br label %11

; <label>:11:                                     ; preds = %6, %34
  %12 = phi i64 [ 0, %6 ], [ %35, %34 ]
  %13 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %8, i64 %12, i32 3
  %14 = load i8*, i8** %13, align 8, !tbaa !17
  %15 = getelementptr inbounds i8, i8* %14, i64 %9
  %16 = load i8, i8* %15, align 1, !tbaa !26
  %17 = getelementptr inbounds %struct.hashCell, %struct.hashCell* %2, i64 0, i32 1, i64 %12, i32 1
  %18 = load i8, i8* %17, align 8, !tbaa !48
  %19 = or i8 %18, %16
  %20 = and i8 %19, 1
  %21 = icmp eq i8 %20, 0
  br i1 %21, label %22, label %30, !prof !50

; <label>:22:                                     ; preds = %11
  %23 = getelementptr inbounds %class.ScalarVector, %class.ScalarVector* %8, i64 %12, i32 5
  %24 = load i64*, i64** %23, align 8, !tbaa !16
  %25 = getelementptr inbounds i64, i64* %24, i64 %9
  %26 = load i64, i64* %25, align 8, !tbaa !18
  %27 = getelementptr inbounds %struct.hashCell, %struct.hashCell* %2, i64 0, i32 1, i64 %12, i32 0
  %28 = load i64, i64* %27, align 8, !tbaa !51
  %29 = icmp eq i64 %26, %28
  br i1 %29, label %34, label %37

; <label>:30:                                     ; preds = %11
  %31 = and i8 %16, 1
  %32 = and i8 %31, %18
  %33 = icmp eq i8 %32, 0
  br i1 %33, label %37, label %34, !prof !52

; <label>:34:                                     ; preds = %22, %30
  %35 = add nuw nsw i64 %12, 1
  %36 = icmp slt i64 %35, %10
  br i1 %36, label %11, label %37

; <label>:37:                                     ; preds = %34, %22, %30, %4
  %38 = phi i1 [ true, %4 ], [ false, %30 ], [ false, %22 ], [ true, %34 ]
  ret i1 %38
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local i64 @getnumBitsBFImpl(%"class.filter::BloomFilterImpl"* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %"class.filter::BloomFilterImpl", %"class.filter::BloomFilterImpl"* %0, i64 0, i32 2
  %3 = load i64, i64* %2, align 8, !tbaa !53
  ret i64 %3
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local %struct.MemoryContextData* @getSonicContext(%class.SonicEncodingDatumArray* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %class.SonicEncodingDatumArray, %class.SonicEncodingDatumArray* %0, i64 0, i32 0, i32 1
  %3 = load %struct.MemoryContextData*, %struct.MemoryContextData** %2, align 8, !tbaa !56
  ret %struct.MemoryContextData* %3
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local %struct.MemoryContextData* @getHashContext(%class.SonicHashAgg* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %class.SonicHashAgg, %class.SonicHashAgg* %0, i64 0, i32 0, i32 14, i32 8
  %3 = load %struct.MemoryContextData*, %struct.MemoryContextData** %2, align 8, !tbaa !59
  ret %struct.MemoryContextData* %3
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local %class.SonicDatumArray** @getSonicDataArray(%class.SonicHashAgg* nocapture readonly) local_unnamed_addr #1 {
  %2 = getelementptr inbounds %class.SonicHashAgg, %class.SonicHashAgg* %0, i64 0, i32 0, i32 1
  %3 = load %class.SonicDatumArray**, %class.SonicDatumArray*** %2, align 8, !tbaa !64
  ret %class.SonicDatumArray** %3
}

; Function Attrs: norecurse nounwind readonly uwtable
define dso_local zeroext i8 @getDataNthNullFlag(i32, %class.SonicEncodingDatumArray* nocapture readonly) local_unnamed_addr #1 {
  %3 = getelementptr inbounds %class.SonicEncodingDatumArray, %class.SonicEncodingDatumArray* %1, i64 0, i32 0, i32 7
  %4 = load i32, i32* %3, align 8, !tbaa !65
  %5 = lshr i32 %0, %4
  %6 = getelementptr inbounds %class.SonicEncodingDatumArray, %class.SonicEncodingDatumArray* %1, i64 0, i32 0, i32 5
  %7 = load i32, i32* %6, align 8, !tbaa !66
  %8 = add i32 %7, -1
  %9 = and i32 %8, %0
  %10 = getelementptr inbounds %class.SonicEncodingDatumArray, %class.SonicEncodingDatumArray* %1, i64 0, i32 0, i32 8
  %11 = load %struct.atom**, %struct.atom*** %10, align 8, !tbaa !67
  %12 = sext i32 %5 to i64
  %13 = getelementptr inbounds %struct.atom*, %struct.atom** %11, i64 %12
  %14 = load %struct.atom*, %struct.atom** %13, align 8, !tbaa !68
  %15 = getelementptr inbounds %struct.atom, %struct.atom* %14, i64 0, i32 1
  %16 = load i8*, i8** %15, align 8, !tbaa !69
  %17 = sext i32 %9 to i64
  %18 = getelementptr inbounds i8, i8* %16, i64 %17
  %19 = load i8, i8* %18, align 1, !tbaa !26
  ret i8 %19
}

; Function Attrs: noreturn
declare dso_local void @_Z20ExceptionalConditionPKcS0_S0_i(i8*, i8*, i8*, i32) local_unnamed_addr #7

attributes #0 = { norecurse nounwind uwtable "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { norecurse nounwind readonly uwtable "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { uwtable "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #3 = { "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { nounwind readonly uwtable "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #5 = { norecurse nounwind readnone uwtable "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #6 = { nounwind uwtable "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #7 = { noreturn "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { noreturn }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"clang version 7.0.0 (tags/RELEASE_700/final)"}
!2 = !{!3, !7, i64 136}
!3 = !{!"_ZTS11ExprContext", !4, i64 0, !7, i64 8, !7, i64 16, !7, i64 24, !7, i64 32, !7, i64 40, !7, i64 48, !7, i64 56, !7, i64 64, !7, i64 72, !8, i64 80, !9, i64 88, !7, i64 96, !8, i64 104, !9, i64 112, !7, i64 120, !7, i64 128, !7, i64 136, !7, i64 144, !7, i64 152, !7, i64 160, !10, i64 168, !9, i64 172, !7, i64 176, !7, i64 184}
!4 = !{!"_ZTS7NodeTag", !5, i64 0}
!5 = !{!"omnipotent char", !6, i64 0}
!6 = !{!"Simple C++ TBAA"}
!7 = !{!"any pointer", !5, i64 0}
!8 = !{!"long", !5, i64 0}
!9 = !{!"bool", !5, i64 0}
!10 = !{!"int", !5, i64 0}
!11 = !{!12, !7, i64 24}
!12 = !{!"_ZTS11VectorBatch", !10, i64 0, !10, i64 4, !9, i64 8, !7, i64 16, !7, i64 24, !7, i64 32, !7, i64 40}
!13 = !{!14, !10, i64 0}
!14 = !{!"_ZTS12ScalarVector", !10, i64 0, !15, i64 4, !9, i64 16, !7, i64 24, !7, i64 32, !7, i64 40, !5, i64 48}
!15 = !{!"_ZTS10ScalarDesc", !10, i64 0, !10, i64 4, !9, i64 8}
!16 = !{!14, !7, i64 40}
!17 = !{!14, !7, i64 24}
!18 = !{!8, !8, i64 0}
!19 = !{!20}
!20 = distinct !{!20, !21}
!21 = distinct !{!21, !"LVerDomain"}
!22 = !{!23}
!23 = distinct !{!23, !21}
!24 = !{!25, !20}
!25 = distinct !{!25, !21}
!26 = !{!5, !5, i64 0}
!27 = !{!25}
!28 = distinct !{!28, !29}
!29 = !{!"llvm.loop.isvectorized", i32 1}
!30 = distinct !{!30, !29}
!31 = !{!14, !10, i64 4}
!32 = !{!33, !5, i64 0}
!33 = !{!"_ZTS12varattrib_1b", !5, i64 0, !5, i64 1}
!34 = !{!35, !7, i64 32}
!35 = !{!"_ZTS20FunctionCallInfoData", !7, i64 0, !7, i64 8, !7, i64 16, !10, i64 24, !9, i64 28, !36, i64 30, !7, i64 32, !7, i64 40, !7, i64 48, !5, i64 56, !5, i64 136, !5, i64 148, !7, i64 192, !37, i64 200}
!36 = !{!"short", !5, i64 0}
!37 = !{!"_ZTS11UDFInfoType", !7, i64 0, !7, i64 8, !7, i64 16, !7, i64 24, !10, i64 32, !10, i64 36, !7, i64 40, !7, i64 48, !7, i64 56, !7, i64 64, !9, i64 72}
!38 = !{!39, !9, i64 17256}
!39 = !{!"_ZTS13BaseAggRunner", !5, i64 17192, !7, i64 17208, !10, i64 17216, !10, i64 17220, !7, i64 17224, !7, i64 17232, !7, i64 17240, !7, i64 17248, !9, i64 17256, !9, i64 17257, !10, i64 17260, !7, i64 17264, !10, i64 17272, !7, i64 17280, !7, i64 17288, !7, i64 17296, !10, i64 17304, !5, i64 17312, !7, i64 25312, !7, i64 25320, !10, i64 25328, !7, i64 25336}
!40 = !{i8 0, i8 2}
!41 = !{!42, !10, i64 17108}
!42 = !{!"_ZTS17hashBasedOperator", !7, i64 8, !7, i64 16, !7, i64 24, !5, i64 32, !5, i64 8032, !5, i64 16032, !7, i64 17032, !7, i64 17040, !7, i64 17048, !7, i64 17056, !7, i64 17064, !9, i64 17072, !10, i64 17076, !8, i64 17080, !8, i64 17088, !8, i64 17096, !10, i64 17104, !10, i64 17108, !7, i64 17112, !7, i64 17120, !7, i64 17128, !7, i64 17136, !9, i64 17144, !10, i64 17148, !10, i64 17152, !8, i64 17160, !8, i64 17168, !8, i64 17176, !10, i64 17184, !9, i64 17188}
!43 = !{!44, !8, i64 0}
!44 = !{!"_ZTS5bictl", !8, i64 0, !7, i64 8}
!45 = !{!9, !9, i64 0}
!46 = !{!47, !47, i64 0}
!47 = !{!"__int128", !5, i64 0}
!48 = !{!49, !5, i64 8}
!49 = !{!"_ZTS7hashVal", !8, i64 0, !5, i64 8}
!50 = !{!"branch_weights", i32 2000, i32 1}
!51 = !{!49, !8, i64 0}
!52 = !{!"branch_weights", i32 4004000, i32 1}
!53 = !{!54, !8, i64 32}
!54 = !{!"_ZTSN6filter15BloomFilterImplIlEE", !7, i64 24, !8, i64 32, !8, i64 40, !8, i64 48, !8, i64 56, !8, i64 64, !7, i64 72, !8, i64 80, !8, i64 88, !9, i64 96, !9, i64 97, !55, i64 100, !10, i64 104, !10, i64 108, !10, i64 112, !7, i64 120}
!55 = !{!"_ZTS15BloomFilterType", !5, i64 0}
!56 = !{!57, !7, i64 8}
!57 = !{!"_ZTS15SonicDatumArray", !7, i64 8, !58, i64 16, !9, i64 32, !10, i64 36, !10, i64 40, !10, i64 44, !10, i64 48, !7, i64 56, !7, i64 64, !10, i64 72, !10, i64 76}
!58 = !{!"_ZTS9DatumDesc", !10, i64 0, !10, i64 4, !10, i64 8, !10, i64 12}
!59 = !{!60, !7, i64 216}
!60 = !{!"_ZTS9SonicHash", !7, i64 8, !7, i64 16, !7, i64 24, !5, i64 32, !10, i64 36, !8, i64 40, !8, i64 48, !7, i64 56, !61, i64 64, !5, i64 72, !5, i64 73, !62, i64 80, !7, i64 160, !63, i64 168, !5, i64 232, !5, i64 65768, !5, i64 69768, !5, i64 73768, !36, i64 75768, !5, i64 75770, !5, i64 76776, !5, i64 84776, !5, i64 85776}
!61 = !{!"_ZTS12hashStateLog", !10, i64 0, !9, i64 4}
!62 = !{!"_ZTSN9SonicHash20SonicHashInputOpAttrE", !9, i64 0, !7, i64 8, !7, i64 16, !36, i64 24, !7, i64 32, !7, i64 40, !7, i64 48, !7, i64 56, !36, i64 64, !7, i64 72}
!63 = !{!"_ZTS22SonicHashMemoryControl", !9, i64 0, !9, i64 1, !10, i64 4, !8, i64 8, !10, i64 16, !8, i64 24, !8, i64 32, !8, i64 40, !7, i64 48, !7, i64 56}
!64 = !{!60, !7, i64 8}
!65 = !{!57, !10, i64 48}
!66 = !{!57, !10, i64 40}
!67 = !{!57, !7, i64 56}
!68 = !{!7, !7, i64 0}
!69 = !{!70, !7, i64 8}
!70 = !{!"_ZTS4atom", !7, i64 0, !7, i64 8}
