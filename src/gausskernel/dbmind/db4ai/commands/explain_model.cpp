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
*---------------------------------------------------------------------------------------
*
* explain_model.cpp
*        Obtain the results of the model by parsing the model.
*
* IDENTIFICATION
*        src/gausskernel/dbmind/db4ai/commands/explain_model.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include "db4ai/explain_model.h"

#include "c.h"
#include "commands/explain.h"
#include "db4ai/model_warehouse.h"
#include "utils/builtins.h"
Datum db4ai_explain_model(PG_FUNCTION_ARGS)
{
    // This function is not supported in distributed deployment mode currently
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("No support for distributed scenarios yet.")));
#endif
    if (t_thrd.proc->workingVersionNum < 92582) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Before GRAND VERSION NUM 92582, we do not support gs_explain_model.")));
    }

    text* in_string = PG_GETARG_TEXT_PP(0);
    char* out_string = str_tolower(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string), PG_GET_COLLATION());
    text* result = ExecExplainModel(out_string);
    PG_RETURN_TEXT_P(result);
}

extern void do_model_explain(ExplainState *es, const Model *model);
text* ExecExplainModel(char* model_name)
{
    ExplainState explain_state;
    errno_t rc = memset_s(&explain_state, sizeof(ExplainState), 0, sizeof(ExplainState));
    securec_check(rc, "\0", "\0");

    explain_state.str = makeStringInfo();
    explain_state.format = EXPLAIN_FORMAT_TEXT;
    explain_state.opt_model_name = model_name;

    const Model* model = get_model(model_name, false);
    if (!model)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("empty model obtained for EXPLAIN MODEL")));

    do_model_explain(&explain_state, model);
    text* result = cstring_to_text(explain_state.str->data);
    pfree_ext(explain_state.str->data);
    return result;
}