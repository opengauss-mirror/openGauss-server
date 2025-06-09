/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
 #ifndef VECSPQCSTORESCAN_H
 #define VECSPQCSTORESCAN_H
  
 #ifdef USE_SPQ
  
 #include "postgres.h"
 #include "vecexecutor/vecnodes.h"
 #include "vecexecutor/vecnodeimcstorescan.h"
  
 #ifdef ENABLE_HTAP
 typedef IMCStoreScanState *(*init_spqcstorescan_hook_type)(SpqCStoreScan *node, Relation parentHeapRel, EState *estate,
                                                            int eflags, bool codegenInUplevel);
  
 typedef VectorBatch *(*exec_spqcstorescan_hook_type)(PlanState *node);
  
 extern THR_LOCAL init_spqcstorescan_hook_type init_cstorescan_hook;
 extern THR_LOCAL exec_spqcstorescan_hook_type exec_cstorescan_hook;
  
 #endif
 #endif
 #endif  // VECSPQCSTORESCAN_H