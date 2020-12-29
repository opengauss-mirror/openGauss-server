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
 * pg_builtin_proc.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/pg_builtin_proc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/fmgrtab.h"
#include "catalog/pg_language.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "access/transam.h"
#include "utils/fmgroids.h"
#include "../utils/pg_builtin_proc.h"

static_assert(sizeof(true) == sizeof(char), "illegal bool size");
static_assert(sizeof(false) == sizeof(char), "illegal bool size");

#ifdef ENABLE_MULTIPLE_NODES
    FuncGroup g_func_groups[] = {
        #include "builtin_funcs.ini"
        #include "../../../distribute/kernel/catalog/distribute_builtin_funcs.ini"
    };
#else
    FuncGroup g_func_groups[] = {
        #include "builtin_funcs.ini"
    };
#endif

const int g_nfuncgroups = sizeof(g_func_groups) / sizeof(FuncGroup);

/* Search the built-in function by name, and return the group id of the matched functions */
const FuncGroup* SearchBuiltinFuncByName(const char* funcname)
{
    int mid, cmp;
    int low = 0;
    int high = g_nfuncgroups - 1;

    while (low <= high) {
        mid = (low + high) / 2;
        cmp = pg_strcasecmp(funcname, g_func_groups[mid].funcName);
        if (cmp == 0) {
            return &g_func_groups[mid];
        } else if (cmp < 0) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }
    // if not found the function
    return NULL;
}

static int cmp_func_by_oid(const void* a, const void* b)
{
    const Builtin_func* fa = *(const Builtin_func**)a;
    const Builtin_func* fb = *(const Builtin_func**)b;

    return (int)fa->foid - (int)fb->foid;
}

int FuncGroupCmp(const void* a, const void* b)
{
    return pg_strcasecmp(((FuncGroup*)a)->funcName, ((FuncGroup*)b)->funcName);
}

void SortBuiltinFuncGroups(FuncGroup* funcGroups)
{
    qsort(funcGroups, g_nfuncgroups, sizeof(FuncGroup), FuncGroupCmp);
}

const Builtin_func* g_sorted_funcs[nBuiltinFuncs];

void initBuiltinFuncs()
{
    SortBuiltinFuncGroups(g_func_groups);

    int nfunc = 0;
    for (int i = 0; i < g_nfuncgroups; i++) {
        const FuncGroup* fg = &g_func_groups[i];

        Assert(nfunc + fg->fnums <= nBuiltinFuncs);

        for (int j = 0; j < fg->fnums; j++) {
            g_sorted_funcs[nfunc++] = &fg->funcs[j];
        }
    }

    if (nfunc != nBuiltinFuncs) {
        ereport(PANIC,
            (errmsg("initialize the built-in function failed: %s",
                "the number of functions in is mismatch with the declaration")));
    }

    qsort(g_sorted_funcs, nBuiltinFuncs, sizeof(g_sorted_funcs[0]), cmp_func_by_oid);
}

const Builtin_func* SearchBuiltinFuncByOid(Oid id)
{
    /* Roughly check  */
    if (!IsSystemObjOid(id)) {
        return NULL;
    }

    int low = 0;
    int high = nBuiltinFuncs - 1;

    while (low <= high) {
        int mid = (low + high) / 2;
        if (id == g_sorted_funcs[mid]->foid) {
            return g_sorted_funcs[mid];
        } else if (id < g_sorted_funcs[mid]->foid) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }
    // if not found the function
    return NULL;
}
