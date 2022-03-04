/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * regioninfo.cpp
 *    support the obs foreign table, we get the region info from
 *    the region_map file, and clean the region info in database.
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/extension/foreign/regioninfo.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/stat.h>

#include "access/cstore_am.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"

#include "catalog/pg_foreign_server.h"
#include "fmgr.h"
#include "foreign/regioninfo.h"
#include "lib/stringinfo.h"
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "storage/dfs/dfs_connector.h"

#include "cjson/cJSON.h"

static Datum pgxc_clean_region_info();
static bool clean_region_info();

/**
 * @Description: get the region string from the region map file.
 * if the region is empty, we will get the value of defaultRegion.
 * @in region, the given region.
 * @return return the region string.
 */
char* readDataFromJsonFile(char* region)
{
    const char* regionMapfileName = "region_map";
    StringInfo regionURL = makeStringInfo();

    ereport(DEBUG1,
        (errcode(ERRCODE_FDW_ERROR),
            errmodule(MOD_OBS),
            errmsg("Get the region value from the %s file", regionMapfileName)));

    if (NULL == region) {
        region = "defaultRegion";
    }

    StringInfo regionMapfilePath = makeStringInfo();
    const char* gausshome = gs_getenv_r("GAUSSHOME");
    char real_gausshome[PATH_MAX + 1] = {'\0'};

    if (NULL != gausshome && 0 != strcmp(gausshome, "\0") && realpath(gausshome, real_gausshome) != NULL) {
        if (backend_env_valid(real_gausshome, "GAUSSHOME") == false) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Incorrect backend environment variable $GAUSSHOME"),
            errdetail("Please refer to the backend instance log for the detail")));
        }
        appendStringInfo(regionMapfilePath, "%s/etc/%s", real_gausshome, regionMapfileName);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_OBS),
                errmsg("Failed to get enviroment parameter $GAUSSHOME or it is NULL, "
                       "please set $GAUSSHOME as your installation directory!")));
    }

    FILE* fp = NULL;

    if ((fp = fopen(regionMapfilePath->data, "rb")) == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_OBS),
                errmsg("Failed to open file %s, errno = %d, reason = %s.", regionMapfileName, errno, strerror(errno))));
    }

    int32 nFileLength;
    if (fseek(fp, 0, SEEK_END)) {
        (void)fclose(fp);
        ereport(ERROR, (errcode_for_file_access(), errmsg("unable to fseek file \"%s\"", regionMapfileName)));
    }

    nFileLength = ftell(fp);

    if (nFileLength <= 0) {
        (void)fclose(fp);
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR), errmodule(MOD_OBS), errmsg("The %s file is empty.", regionMapfileName)));
    }

    rewind(fp);

    char* fileContext = (char*)palloc0((nFileLength + 1) * sizeof(char));
    int nReadSize;
    nReadSize = fread(fileContext, 1, nFileLength, fp);
    if (nReadSize != nFileLength) {
        (void)fclose(fp);
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR), errmodule(MOD_OBS), errmsg("Failed to read file %s.", regionMapfileName)));
    }

    (void)fclose(fp);

    cJSON* region_jsons = cJSON_Parse(fileContext);
    const cJSON* region_json = NULL;
    bool found = false;

    if (region_jsons == NULL) {
        const char* error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                    errmodule(MOD_OBS),
                    errmsg("Failed to parse %s file: %s.", regionMapfileName, error_ptr)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                    errmodule(MOD_OBS),
                    errmsg("Failed to parse %s file: unkonwn error.", regionMapfileName)));
        }
    }

    if (cJSON_IsArray(region_jsons)) {
        cJSON_ArrayForEach(region_json, region_jsons)
        {
            if (region_json != NULL && region_json->child != NULL && region_json->child->string != NULL && !found &&
                0 == pg_strcasecmp(region_json->child->string, region)) {
                found = true;
                appendStringInfo(regionURL, "%s", region_json->child->valuestring);
                ereport(DEBUG1,
                    (errcode(ERRCODE_FDW_ERROR), errmodule(MOD_OBS), errmsg("The region URL is %s.", regionURL->data)));
            }
        }
    } else {
        cJSON_Delete(region_jsons);
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_OBS),
                errmsg("Must exist array format in the %s json file.", regionMapfileName)));
    }

    cJSON_Delete(region_jsons);

    if (!found) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_OBS),
                errmsg("No such region name: %s in %s file.", region, regionMapfileName)));
    }

    pfree_ext(regionMapfilePath->data);
    pfree_ext(regionMapfilePath);

    return regionURL->data;
}

static bool clean_region_info()
{
#ifndef ENABLE_LITE_MODE
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;
    int cleanNum = 0;
    bool ret = true;

    rel = heap_open(ForeignServerRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, GetActiveSnapshot(), 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Oid srvOid = HeapTupleGetOid(tuple);
        PG_TRY();
        {
            if (dfs::InvalidOBSConnectorCache(srvOid)) {
                cleanNum++;
            }
        }
        PG_CATCH();
        {
            FlushErrorState();
            Form_pg_foreign_server server = (Form_pg_foreign_server)GETSTRUCT(tuple);
            tableam_scan_end(scan);
            heap_close(rel, AccessShareLock);
            ereport(LOG, (errmodule(MOD_DFS), errmsg("Failed to clean region info of %s.", NameStr(server->srvname))));

            ret = false;
        }
        PG_END_TRY();
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    ereport(LOG, (errmodule(MOD_DFS), errmsg("clean %d region info.", cleanNum)));

    return ret;
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    return true;
#endif
}

/**
 * @Description: clean the region map info for obs server.
 * @in none.
 * @return return true if clean it successfully, otherwise return false.
 */
Datum pg_clean_region_info(PG_FUNCTION_ARGS)
{
#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PG_RETURN_DATUM(pgxc_clean_region_info());
    }
#endif

    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to clean obs region info")));
    VarChar* result = NULL;
    if (clean_region_info()) {
        result = (VarChar*)cstring_to_text_with_len("success", strlen("success"));
    } else {
        result = (VarChar*)cstring_to_text_with_len("failure", strlen("failure"));
    }

    PG_RETURN_VARCHAR_P(result);
}

static Datum pgxc_clean_region_info()
{

    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    VarChar* result = NULL;

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_clean_region_info()");

    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncSum);

    FreeParallelFunctionState(state);
    if (clean_region_info()) {
        result = (VarChar*)cstring_to_text_with_len("success", strlen("success"));
    } else {
        result = (VarChar*)cstring_to_text_with_len("failure", strlen("failure"));
    }

    PG_RETURN_VARCHAR_P(result);
}
