/* -------------------------------------------------------------------------
 *
 * identitycmds.cpp
 *      Identity's commands for shark
 *
 * Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      contrib/shark/src/identitycmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "shark.h"
#include "knl/knl_session.h"
#include "commands/sequence.h"

static void pltsql_nextval_identity(Oid relid, int128 val);
static void update_scope_identity_stack(SeqTableIdentityData *elm);

/* Hook to tablecmds.cpp in the engine */
static void* g_prevInvokeNextvalHook = NULL;

void AssignIdentitycmdsHook()
{
    if (u_sess->hook_cxt.invokeNextvalHook) {
        g_prevInvokeNextvalHook = u_sess->hook_cxt.invokeNextvalHook;
    }
    u_sess->hook_cxt.invokeNextvalHook = (void*)&pltsql_nextval_identity;
}

int PltsqlNewScopeIdentityNestLevel()
{
    SharkContext* cxt = GetSessionContext();
    return ++(cxt->pltsqlScopeIdentityNestLevel);
}

void PltsqlRevertLastScopeIdentity(int nestLevel)
{
    ScopeIdentityStack* old_top = NULL;

    SharkContext* cxt = GetSessionContext();
    if (cxt->lastUsedScopeSeqIdentity == NULL ||
        cxt->lastUsedScopeSeqIdentity->nestLevel != cxt->pltsqlScopeIdentityNestLevel) {
        cxt->pltsqlScopeIdentityNestLevel = nestLevel - 1;
        return;
    }

    cxt->pltsqlScopeIdentityNestLevel = nestLevel - 1;
    old_top = cxt->lastUsedScopeSeqIdentity;
    cxt->lastUsedScopeSeqIdentity = old_top->prev;
    pfree(old_top);
}

static void pltsql_nextval_identity(Oid seqid, int128 val)
{
    if (!DB_IS_CMPT(D_FORMAT)) {
        return;
    }

    if (g_prevInvokeNextvalHook) {
        ((InvokeNextvalHookType)(g_prevInvokeNextvalHook))(seqid, val);
    }

    SeqTableIdentityData elm;
    elm.relid = seqid;
    elm.lastIdentityValid = true;
    elm.lastIdentity = val;

    /* update the scope identity */
    update_scope_identity_stack(&elm);
}

static void update_scope_identity_stack(SeqTableIdentityData *elm)
{
    SharkContext* cxt = GetSessionContext();
    ScopeIdentityStack* scope_identity = NULL;

    /*
     * If current elm is in the same scope (same nestLevel) as the current
     * top element in the stack, then update the top element to point to elm.
     * Otherwise, push elm to the stack and make it the new scope identity
     * value in the new scope.
     */
    if (cxt->lastUsedScopeSeqIdentity &&
        cxt->lastUsedScopeSeqIdentity->nestLevel == cxt->pltsqlScopeIdentityNestLevel) {
        /* Make a deep copy of elm. We do not know where elm came from */
        errno_t rc = memcpy_s((void*)&cxt->lastUsedScopeSeqIdentity->last_used_seq_identity_in_scope,
                              sizeof(SeqTableIdentityData), (void*)elm, sizeof(SeqTableIdentityData));
        securec_check(rc, "\0", "\0");
        return;
    }

    /* The previous nestLevel should be less than the one we are adding */
    Assert(!cxt->lastUsedScopeSeqIdentity ||
            cxt->lastUsedScopeSeqIdentity->nestLevel < cxt->pltsqlScopeIdentityNestLevel);

    scope_identity = (ScopeIdentityStack*)MemoryContextAllocZero(u_sess->top_portal_cxt, sizeof(ScopeIdentityStack));

    scope_identity->prev = cxt->lastUsedScopeSeqIdentity;
    scope_identity->nestLevel = cxt->pltsqlScopeIdentityNestLevel;

    /* Make a deep copy of elm. We do not know where elm came from */
    errno_t rc = memcpy_s((void*)&scope_identity->last_used_seq_identity_in_scope, sizeof(SeqTableIdentityData),
                          (void*)elm, sizeof(SeqTableIdentityData));
    securec_check(rc, "\0", "\0");

    cxt->lastUsedScopeSeqIdentity = scope_identity;
}

int128 last_scope_identity_value()
{
    SharkContext* cxt = GetSessionContext();
    SeqTableIdentityData* curr_seq_identity = NULL;

    /*
     * scope_identity is not defined or defined but it is not on the same
     * level as the current scope
     */
    if (cxt->lastUsedScopeSeqIdentity == NULL ||
        cxt->lastUsedScopeSeqIdentity->nestLevel != cxt->pltsqlScopeIdentityNestLevel) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("last scope identity not yet defined in this session")));
    }

    /* Check the current identity in the scope */
    curr_seq_identity = &cxt->lastUsedScopeSeqIdentity->last_used_seq_identity_in_scope;
    if (!OidIsValid(curr_seq_identity->relid) ||
        !SearchSysCacheExists1(RELOID, ObjectIdGetDatum(curr_seq_identity->relid))) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("last scope identity not yet defined in this session")));
    }

    if (!curr_seq_identity->lastIdentityValid)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("last identity not valid")));

    return curr_seq_identity->lastIdentity;
}
