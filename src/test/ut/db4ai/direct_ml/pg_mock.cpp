/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd. 
openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------
 *
 * pg_mock.h
 *        Easy to test
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/pg_mock.h
 *
 * ---------------------------------------------------------------------------------------
**/
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "cjson/cJSON.h"
#include "db4ai/model_warehouse.h"
#include "db4ai/aifuncs.h"

extern Datum int8out(PG_FUNCTION_ARGS);

static Datum mock_array_out(PG_FUNCTION_ARGS)
{
    MemoryContext old_cxt;
    MemoryContext cxt = NULL;
    bool new_flinfo = (fcinfo->flinfo == NULL);
    if (new_flinfo) {
        cxt = AllocSetContextCreate(CurrentMemoryContext, "mock_array_out",
                                    ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_DEFAULT_MAXSIZE);
        old_cxt = MemoryContextSwitchTo(cxt);
        fcinfo->flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo));
        fcinfo->flinfo->fn_mcxt = cxt;
        MemoryContextSwitchTo(old_cxt);
    }
    
    Datum dt = array_out(fcinfo);
    
    if (new_flinfo)
        MemoryContextDelete(cxt);
    
    return dt;
}

enum OidTypeMap {
    OIDTYPEMAP_BOOLOID,
    OIDTYPEMAP_INT1OID,
    OIDTYPEMAP_INT2OID,
    OIDTYPEMAP_INT4OID,
    OIDTYPEMAP_INT8OID,
    OIDTYPEMAP_FLOAT4OID,
    OIDTYPEMAP_FLOAT8OID,
    OIDTYPEMAP_CSTRINGOID,
    OIDTYPEMAP_TEXTOID,
    OIDTYPEMAP_VARCHAROID,
    OIDTYPEMAP_FLOAT8ARRAYOID,
    OIDTYPEMAP_BOOLARRAYOID,
    OIDTYPEMAP_TEXTARRAYOID,
    OIDTYPEMAP_INT4ARRAYOID,
    OIDTYPEMAP_MAPPINGS_
};

// keep same order as enum
struct OidTypeInfo {
    bool typbyval;
    int typlen;
    char typalign;
    int arrtyp;
    int elemtyp;
    PGFunction outfunc;
} oid_types[OIDTYPEMAP_MAPPINGS_] = {
        {true,  1,  'c', BOOLARRAYOID,    0,         boolout},
        {true,  1,  'c', INT1ARRAYOID,    0,         int1out},
        {true,  2,  's', INT2ARRAYOID,    0,         int2out},
        {true,  4,  'i', INT4ARRAYOID,    0,         int4out},
        {true,  8,  'd', INT8ARRAYOID,    0,         int8out},
        {true,  4,  'i', FLOAT4ARRAYOID,  0,         float4out},
        {true,  8,  'd', FLOAT8ARRAYOID,  0,         float8out},
        {false, -2, 'c', CSTRINGARRAYOID, 0,         cstring_out},
        {false, -1, 'i', TEXTARRAYOID,    0,         textout},
        {false, -1, 'i', VARCHARARRAYOID, 0,         varcharout},
        // arrays
        {false, -1, 'd', FLOAT8ARRAYOID,  FLOAT8OID, mock_array_out},
        {false, -1, 'i', BOOLARRAYOID,    BOOLOID,   mock_array_out},
        {false, -1, 'i', TEXTARRAYOID,    TEXTOID,	mock_array_out},
        {false, -1, 'i', INT4ARRAYOID,    INT4OID,	mock_array_out},
};

static OidTypeInfo *get_type_map(Oid typid)
{
    int map = -1;
    switch (typid) {
        case BOOLOID:
            map = OIDTYPEMAP_BOOLOID;
            break;
        case INT1OID:
            map = OIDTYPEMAP_INT1OID;
            break;
        case INT2OID:
            map = OIDTYPEMAP_INT2OID;
            break;
        case INT4OID:
            map = OIDTYPEMAP_INT4OID;
            break;
        case INT8OID:
            map = OIDTYPEMAP_INT8OID;
            break;
        case FLOAT4OID:
            map = OIDTYPEMAP_FLOAT4OID;
            break;
        case FLOAT8OID:
            map = OIDTYPEMAP_FLOAT8OID;
            break;
        case CSTRINGOID:
            map = OIDTYPEMAP_CSTRINGOID;
            break;
        case TEXTOID:
            map = OIDTYPEMAP_TEXTOID;
            break;
        case VARCHAROID:
            map = OIDTYPEMAP_VARCHAROID;
            break;
        case FLOAT8ARRAYOID:
            map = OIDTYPEMAP_FLOAT8ARRAYOID;
            break;
        case BOOLARRAYOID:
            map = OIDTYPEMAP_BOOLARRAYOID;
            break;
        case TEXTARRAYOID:
            map = OIDTYPEMAP_TEXTARRAYOID;
            break;
        case INT4ARRAYOID:
            map = OIDTYPEMAP_INT4ARRAYOID;
            break;
        default:
            Assert(false);
            elog(FATAL, "get_type_map %d NOT IMPLEMENTED", typid);
    }
    
    return &oid_types[map];
}

static PGFunction mock_get_output_func(Oid functionId)
{
    return get_type_map(functionId)->outfunc;
}

// overwrite
void fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt)
{
    // fmgr_info_cxt_security(functionId, finfo, mcxt, false)
    finfo->fn_oid = functionId;
    finfo->fn_extra = NULL;
    finfo->fn_mcxt = mcxt;
    finfo->fn_expr = NULL; /* caller may set this later */
    finfo->fn_fenced = false;
    finfo->fnLibPath = NULL;

    finfo->fn_addr = mock_get_output_func(functionId);
}

// overwrite
void get_type_io_data(Oid typid, IOFuncSelector which_func, int16 *typlen, bool *typbyval, char *typalign,
                      char *typdelim, Oid *typioparam, Oid *func)
{
    Assert(which_func == IOFunc_output);
    get_typlenbyvalalign(typid, typlen, typbyval, typalign);
    *typdelim = ',';
    *typioparam = 0; // TODO
    *func = typid;
}

// overwrite
char *OidOutputFunctionCall(Oid functionId, Datum val)
{
    PGFunction func = mock_get_output_func(functionId);
    return DatumGetCString(DirectFunctionCall1(func, val));
}

// overwrite
Oid get_element_type(Oid typid)
{
    return get_type_map(typid)->elemtyp;
}

// overwrite
Oid get_array_type(Oid typid)
{
    return get_type_map(typid)->arrtyp;
}

// overwrite
void getTypeOutputInfo(Oid type, Oid *typOutput, bool *typIsVarlena)
{
    *typOutput = type;
    *typIsVarlena = !get_type_map(type)->typbyval;
}

// overwrite
void get_typlenbyvalalign(Oid typid, int16 *typlen, bool *typbyval, char *typalign)
{
    OidTypeInfo *typinfo = get_type_map(typid);
    *typbyval = typinfo->typbyval;
    *typlen = typinfo->typlen;
    *typalign = typinfo->typalign;
}

Oid get_base_element_type(Oid typid)
{
    return get_element_type(getBaseType(typid));
}

Oid getBaseType(Oid typid)
{
    // default interface
    // int32 typmod = -1;
    // return getBaseTypeAndTypmod(typid, &typmod);
    
    OidTypeInfo *typinfo = get_type_map(typid);
    // just to check if it is supported
    (void)typinfo;
    return typid;
}

Datum OidFunctionCall2Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2)
{
    if (functionId != F_ARRAY_OUT || collation != InvalidOid)
        elog(FATAL, "OidFunctionCall2Coll %d:%d NOT IMPLEMENTED", functionId, collation);
    
    PGFunction func = mock_get_output_func(DatumGetInt32(arg2));
    return DirectFunctionCall1(func, arg1);
}

////////////////////////////////////////////////////////////////

void model_store(const Model *model) {
	char filename[256];
	strcpy(filename, model->model_name);
	strcat(filename, ".json");

	FILE *fp = fopen(filename, "w");
	if (fp == NULL)
		elog(FATAL, "cannot save model '%s'", filename);
	
	cJSON *doc = cJSON_CreateObject();
	cJSON_AddStringToObject(doc, "name", model->model_name);
	cJSON_AddNumberToObject(doc, "owner", -1);
	cJSON_AddStringToObject(doc, "algorithm", algorithm_ml_to_string(model->algorithm));
    cJSON_AddStringToObject(doc, "create_time", DatumGetCString(DirectFunctionCall1(timestamptz_out, GetCurrentTimestamp())));
    cJSON_AddNumberToObject(doc, "processed_tuples", model->processed_tuples);
    cJSON_AddNumberToObject(doc, "discarded_tuples", model->discarded_tuples);
    cJSON_AddNumberToObject(doc, "preprocess_time", model->pre_time_secs);
    cJSON_AddNumberToObject(doc, "exec_time", model->exec_time_secs);
    cJSON_AddNumberToObject(doc, "return_type", model->return_type);

    if (model->hyperparameters != nullptr) {
		cJSON *hyperps = cJSON_AddObjectToObject(doc, "hyperparameters");
		foreach_cell(it, model->hyperparameters)
		{
			Hyperparameter *cell = (Hyperparameter *)lfirst(it);
			cJSON *hyperp = cJSON_AddObjectToObject(hyperps, cell->name);
			switch (cell->type) {
				case BOOLOID:
					cJSON_AddBoolToObject(hyperp, "value", DatumGetBool(cell->value));
					break;
				case INT4OID:
					cJSON_AddNumberToObject(hyperp, "value", DatumGetInt32(cell->value));
					break;
				case FLOAT8OID:
					cJSON_AddNumberToObject(hyperp, "value", DatumGetFloat8(cell->value));
					break;
				case VARCHAROID:
					cJSON_AddStringToObject(hyperp, "value", text_to_cstring(DatumGetVarCharP(cell->value)));
					break;
				case CSTRINGOID:
					cJSON_AddStringToObject(hyperp, "value", DatumGetCString(cell->value));
					break;
				default:
					printf("unsupported cell type %d\n", cell->type);
					Assert(false);
			}
		    cJSON_AddNumberToObject(hyperp, "type", cell->type);
		}
    }

    if (model->scores != nullptr) {
		cJSON *scores = cJSON_AddObjectToObject(doc, "scores");
		foreach_cell(it, model->scores)
		{
			TrainingScore *cell = (TrainingScore *)lfirst(it);
			cJSON_AddNumberToObject(scores, cell->name, cell->value);
		}
    }

    if (model->data.version != DB4AI_MODEL_UNDEFINED) {
        if (model->data.version >= DB4AI_MODEL_INVALID)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Invalid model version %d", model->data.version)));

		cJSON_AddNumberToObject(doc, "version", model->data.version);

		char *buff = (char*)palloc(model->data.size * 2 + 1);
		char *dst = buff;
		uint8_t* src = (uint8_t*)model->data.raw_data;
		for (size_t i=0 ; i<model->data.size ; i++) {
			sprintf(dst, "%02X", *src++);
			dst += 2;
		}
		*dst = 0;
		cJSON_AddStringToObject(doc, "data", buff);
		pfree(buff);
    }

	fputs(cJSON_Print(doc), fp);
	fclose(fp);
}

const Model *model_load(const char *model_name) {
	char filename[256];
	strcpy(filename, model_name);
	strcat(filename, ".json");

	FILE *fp = fopen(filename, "rb");
	if (fp == NULL)
		elog(FATAL, "file '%s' does not exists", filename);

	fseek(fp, 0L, SEEK_END);
	size_t len = ftell(fp);
	fseek(fp, 0L, SEEK_SET);

	char *buffer = (char*)palloc(len + 1);
	if (fread(buffer, 1, len, fp) != len)
		elog(FATAL, "cannot read model from '%s'", filename);

	buffer[len] = 0;
	fclose(fp);

	cJSON *doc = cJSON_Parse(buffer);
	if (doc == NULL)
		elog(FATAL, "invalid model in '%s'", filename);

	pfree(buffer);
	Model *model = (Model*) palloc0(sizeof(Model));

	cJSON *item = cJSON_GetObjectItem(doc, "name");
	Assert(cJSON_IsString(item));
	model->model_name = pstrdup(cJSON_GetStringValue(item));

	item = cJSON_GetObjectItem(doc, "owner");
	Assert(cJSON_IsNumber(item) && cJSON_GetNumberValue(item) == -1);

	item = cJSON_GetObjectItem(doc, "algorithm");
	Assert(cJSON_IsString(item));
	model->algorithm = get_algorithm_ml(cJSON_GetStringValue(item));

	item = cJSON_GetObjectItem(doc, "processed_tuples");
	Assert(cJSON_IsNumber(item));
	model->processed_tuples = (int64_t)cJSON_GetNumberValue(item);

	item = cJSON_GetObjectItem(doc, "discarded_tuples");
	Assert(cJSON_IsNumber(item));
	model->discarded_tuples = (int64_t)cJSON_GetNumberValue(item);

	item = cJSON_GetObjectItem(doc, "preprocess_time");
	Assert(cJSON_IsNumber(item));
	model->pre_time_secs = cJSON_GetNumberValue(item);

	item = cJSON_GetObjectItem(doc, "exec_time");
	Assert(cJSON_IsNumber(item));
	model->exec_time_secs = cJSON_GetNumberValue(item);

	item = cJSON_GetObjectItem(doc, "return_type");
	Assert(cJSON_IsNumber(item));
	model->return_type = (Oid)cJSON_GetNumberValue(item);

	model->hyperparameters = NULL;
	cJSON *hyperps = cJSON_GetObjectItem(doc, "hyperparameters");
	if (hyperps != NULL) {
		Assert(cJSON_IsObject(hyperps));
		cJSON *hyperp;
		cJSON_ArrayForEach(hyperp, hyperps) {
			Assert(cJSON_IsObject(hyperp));

			Hyperparameter *cell = (Hyperparameter *)palloc(sizeof(Hyperparameter));
			cell->name = pstrdup(hyperp->string);

			item = cJSON_GetObjectItem(hyperp, "type");
			Assert(cJSON_IsNumber(item));
			cell->type = (Oid)cJSON_GetNumberValue(item);

			item = cJSON_GetObjectItem(hyperp, "value");
			switch (cell->type) {
				case BOOLOID:
					Assert(cJSON_IsBool(item));
					cell->value = BoolGetDatum(cJSON_GetNumberValue(item) != 0);
					break;
				case INT4OID:
					Assert(cJSON_IsNumber(item));
					cell->value = Int32GetDatum((int) cJSON_GetNumberValue(item));
					break;
				case FLOAT8OID:
					Assert(cJSON_IsNumber(item));
					cell->value = Float8GetDatum(cJSON_GetNumberValue(item));
					break;
				case VARCHAROID:
					Assert(cJSON_IsString(item));
					cell->value = CStringGetTextDatum(pstrdup(cJSON_GetStringValue(item)));
					break;
				case CSTRINGOID:
					Assert(cJSON_IsString(item));
					cell->value = CStringGetDatum(pstrdup(cJSON_GetStringValue(item)));
					break;
				default:
					Assert(false);
			}
			model->hyperparameters = lappend(model->hyperparameters, cell);
		}
	}

	model->scores = NULL;
	cJSON *scores = cJSON_GetObjectItem(doc, "scores");
	if (scores != NULL) {
		Assert(cJSON_IsObject(scores));
		cJSON_ArrayForEach(item, scores) {
			Assert(cJSON_IsNumber(item));

			TrainingScore *cell = (TrainingScore *)palloc(sizeof(TrainingScore));
			cell->name = pstrdup(item->string);
			cell->value = cJSON_GetNumberValue(item);
			model->scores = lappend(model->scores, cell);
		}
	}

	item = cJSON_GetObjectItem(doc, "version");
	Assert(cJSON_IsNumber(item));
	model->data.version = (SerializedModelVersion)cJSON_GetNumberValue(item);
	if (model->data.version == DB4AI_MODEL_UNDEFINED) {
		model->data.raw_data = NULL;
		model->data.size = 0;
	} else {
		item = cJSON_GetObjectItem(doc, "data");
		Assert(cJSON_IsString(item));

		char *data = cJSON_GetStringValue(item);
		model->data.size = strlen(data) / 2;
		model->data.raw_data = palloc(model->data.size);

		unsigned int v;
		uint8_t* dst = (uint8_t*)model->data.raw_data;
		for (size_t c=0 ; c<model->data.size ; c++) {
			sscanf(data, "%2x", &v);
			*dst++ = v;
			data += 2;
		}
	}

	return model;
}

////////////////////////////////////////////////////////////////

void pg_mock_init(int log_level, int working_mem_mb)
{
    knl_instance_init();
    
    MemoryContextInit();
    PmTopMemoryContext = t_thrd.top_mem_cxt;
    
    knl_thread_init(MASTER_THREAD);
    
    SelfMemoryContext = THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT);
    MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    
    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;
    u_sess = t_thrd.fake_session;
    
    u_sess->attr.attr_memory.work_mem = working_mem_mb * 1024; // in KBytes
    
	session_timezone = pg_tzset("GMT");

    log_min_messages = log_level;
}

uint64_t get_clock_usecs()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

