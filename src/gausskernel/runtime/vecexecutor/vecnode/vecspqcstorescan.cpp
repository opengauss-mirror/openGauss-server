/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
 #ifdef USE_SPQ
 #include "vecexecutor/vecspqcstorescan.h"
  
 #ifdef ENABLE_HTAP
 THR_LOCAL init_spqcstorescan_hook_type init_cstorescan_hook = nullptr;
 THR_LOCAL exec_spqcstorescan_hook_type exec_cstorescan_hook = nullptr;
 #endif
  
 #endif