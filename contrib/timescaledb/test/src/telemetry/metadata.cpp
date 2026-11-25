/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>

#include "export.h"
#include "telemetry/telemetry_metadata.h"

TS_FUNCTION_INFO_V1(ts_test_uuid);
TS_FUNCTION_INFO_V1(ts_test_exported_uuid);
TS_FUNCTION_INFO_V1(ts_test_install_timestamp);

Datum
ts_test_uuid(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_telemetry_metadata_get_uuid());
}

Datum
ts_test_exported_uuid(PG_FUNCTION_ARGS)

{
	PG_RETURN_DATUM(ts_telemetry_metadata_get_exported_uuid());
}

Datum
ts_test_install_timestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_telemetry_metadata_get_install_timestamp());
}
