/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef RACK_DEV_H
#define RACK_DEV_H

#include "fmgr.h"
#include "knl/knl_instance.h"

extern "C" Datum smb_total_buffer_info(PG_FUNCTION_ARGS);
extern "C" Datum smb_buf_manager_info(PG_FUNCTION_ARGS);
extern "C" Datum smb_dirty_page_queue_info(PG_FUNCTION_ARGS);
extern "C" Datum smb_status_info(PG_FUNCTION_ARGS);

#endif /* RACK_DEV_H */