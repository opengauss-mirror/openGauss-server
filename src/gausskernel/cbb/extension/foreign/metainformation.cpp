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
 * metainformation.cpp
 * support interface for pushdown metainformation.
 *
 * IDENTIFICATION
 *		  src/gausskernel/cbb/extension/foreign/metainformation.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "foreign/dummyserver.h"
#include "foreign/metainformation.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "foreign/regioninfo.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "access/heapam.h"

extern void getOBSKeyString(unsigned char** cipherKey);

void fillTxtCsvFormatForeignOption(Oid relId, ForeignOptions* options)
{
    options->stype = T_TXT_CSV_OBS_SERVER;
    options->fOptions = getFdwOptions(relId);
}

ForeignOptions* setForeignOptions(Oid relid)
{
    ForeignOptions* options = makeNode(ForeignOptions);
    ServerTypeOption srvType = getServerType(relid);
    errno_t rc = EOK;

    switch (srvType) {
        case T_OBS_SERVER: {
            DefElem* optionDef = NULL;

            options->stype = T_OBS_SERVER;
            options->fOptions = getFdwOptions(relid);

            /*
             * If we have the following code, we do not need to read region info
             * from the region_map on computing pool.
             */
            char* address = getFTOptionValue(options->fOptions, OPTION_NAME_ADDRESS);
            if (NULL == address) {
                char* region = getFTOptionValue(options->fOptions, OPTION_NAME_REGION);
                address = readDataFromJsonFile(region);

                optionDef = makeDefElem(OPTION_NAME_ADDRESS, (Node*)makeString(pstrdup(address)));
                options->fOptions = lappend(options->fOptions, optionDef);
            }

            unsigned char* obskey = NULL;
            getOBSKeyString(&obskey);
            if (NULL == obskey)
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_ACCELERATE),
                        errmsg("Failed to get obskey from cipher file")));
            if (0 == strlen((const char*)obskey))
                ereport(ERROR,
                    (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
                        errmodule(MOD_ACCELERATE),
                        errmsg("Failed to get obskey from cipher file")));

            optionDef = makeDefElem(OPTION_NAME_OBSKEY, (Node*)makeString(pstrdup((char*)obskey)));
            options->fOptions = lappend(options->fOptions, optionDef);

            rc = memset_s(obskey, strlen((const char*)obskey), 0, strlen((const char*)obskey));
            securec_check(rc, "\0", "\0");
            pfree(obskey);
            break;
        }
        case T_HDFS_SERVER: {
            options->stype = T_HDFS_SERVER;
            options->fOptions = getFdwOptions(relid);

            DummyServerOptions* option = getDummyServerOption();
            if (option->remoteservername) {
                DefElem* optionDef =
                    makeDefElem(OPTION_NAME_REMOTESERVERNAME, (Node*)makeString(option->remoteservername));
                options->fOptions = lappend(options->fOptions, optionDef);
            }
            break;
        }
        case T_TXT_CSV_OBS_SERVER: {
            fillTxtCsvFormatForeignOption(relid, options);
            break;
        }
        default: {
            Assert(0);
            break;
        }
    }

    return options;
}
