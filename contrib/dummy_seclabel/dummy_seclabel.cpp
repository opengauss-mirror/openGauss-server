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
 * dummy_seclabel.c
 *
 * Dummy security label provider.
 *
 * This module does not provide anything worthwhile from a security
 * perspective, but allows regression testing independent of platform-specific
 * features like SELinux.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/seclabel.h"
#include "miscadmin.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

PG_MODULE_MAGIC;

/* Entrypoint of the module */
extern "C" void _PG_init(void);

static void dummy_object_relabel(const ObjectAddress* object, const char* seclabel)
{
    if (seclabel == NULL || strcmp(seclabel, "unclassified") == 0 || strcmp(seclabel, "classified") == 0)
        return;

    if (strcmp(seclabel, "secret") == 0 || strcmp(seclabel, "top secret") == 0) {
        if (!superuser())
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("only system admin can set '%s' label", seclabel)));
        return;
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("'%s' is not a valid security label", seclabel)));
}

void _PG_init(void)
{
    register_label_provider("dummy", dummy_object_relabel);
}
