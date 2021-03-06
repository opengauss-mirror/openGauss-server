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
 * datasource.cpp
 *		  support for data source
 *
 * IDENTIFICATION
 *		  src/gausskernel/process/datasource/datasource.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/reloptions.h"
#include "catalog/pg_extension_data_source.h"
#include "datasource/datasource.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"

/*
 * get_data_source_oid
 * 	look up the OID by source name
 *
 * @IN sourcename: source name
 * @IN missing_ok: If missing_ok is false, throw an error if name not found.
 * 		If true, just return InvalidOid.
 * @RETURN: oid of the data source.
 */
Oid get_data_source_oid(const char* sourcename, bool missing_ok)
{
    Oid oid;

    oid = GetSysCacheOid1(DATASOURCENAME, CStringGetDatum(sourcename));

    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR,
            (errmodule(MOD_EC), errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("source \"%s\" does not exist", sourcename)));
    return oid;
}

/*
 * GetDataSource
 * 	look up the data source definition
 *
 * @IN sourceid:  data source oid
 * @RETURN:  a data source
 */
DataSource* GetDataSource(Oid sourceid)
{
    Form_pg_extension_data_source sourceform = NULL;
    DataSource* source = NULL;
    HeapTuple tp = NULL;
    Datum datum;
    bool isnull = false;

    tp = SearchSysCache1(DATASOURCEOID, ObjectIdGetDatum(sourceid));

    if (!HeapTupleIsValid(tp))
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for data source %u", sourceid)));

    sourceform = (Form_pg_extension_data_source)GETSTRUCT(tp);

    source = (DataSource*)palloc0(sizeof(DataSource));
    source->sourceid = sourceid;
    source->srcname = pstrdup(NameStr(sourceform->srcname));
    source->owner = sourceform->srcowner;

    /* Extract source type */
    datum = SysCacheGetAttr(DATASOURCEOID, tp, Anum_pg_extension_data_source_srctype, &isnull);
    source->srctype = isnull ? NULL : pstrdup(TextDatumGetCString(datum));

    /* Extract source version */
    datum = SysCacheGetAttr(DATASOURCEOID, tp, Anum_pg_extension_data_source_srcversion, &isnull);
    source->srcversion = isnull ? NULL : pstrdup(TextDatumGetCString(datum));

    /* Extract the srcoptions */
    datum = SysCacheGetAttr(DATASOURCEOID, tp, Anum_pg_extension_data_source_srcoptions, &isnull);
    if (isnull)
        source->options = NIL;
    else
        source->options = untransformRelOptions(datum);

    ReleaseSysCache(tp);

    return source;
}

/*
 * GetDataSourceByName
 * 	look up the data source definition by name.
 *
 * @IN sourcename:  source name
 * @IN missing_ok:  missing source name ok
 * @RETURN: data source
 */
DataSource* GetDataSourceByName(const char* sourcename, bool missing_ok)
{
    Oid sourceid;

    if (sourcename == NULL)
        return NULL;

    sourceid = get_data_source_oid(sourcename, missing_ok);

    if (!OidIsValid(sourceid))
        return NULL;

    return GetDataSource(sourceid);
}
