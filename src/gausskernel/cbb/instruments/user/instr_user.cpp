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
 * instr_user.cpp
 *   functions for user stat, such as login/logout counter
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/user/instr_user.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/atomic.h"
#include "access/hash.h"
#include "utils/memutils.h"
#include "storage/lock/lwlock.h"
#include "utils/acl.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "workload/gscgroup.h"
#include "workload/statctl.h"
#include "instruments/instr_user.h"
#include "commands/user.h"

const int INSTR_USER_MAX_HASH_SIZE = 1000;

typedef struct {
    Oid user_oid;
    pg_atomic_uint64 login_counter;
    pg_atomic_uint64 logout_counter;
} InstrUser;

static uint32 instr_user_hash_code(const void* key, Size size)
{
    return hash_uint32(*((const uint32*)key));
}

static int instr_user_match(const void* key1, const void* key2, Size key_size)
{
    if (key1 != NULL && key2 != NULL && *((const Oid*)key1) == *((const Oid*)key2)) {
        return 0;
    }

    return 1;
}

static bool is_instr_user_enabled()
{
    return u_sess->attr.attr_resource.enable_resource_track;
}

/*
 * InitInstrUser - init user stat hash
 */
void InitInstrUser()
{
    // init memory context
    g_instance.stat_cxt.InstrUserContext = AllocSetContextCreate(g_instance.instance_context,
        "InstrUserContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    // init unique sql hash table
    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check_c(rc, "\0", "\0");

    ctl.hcxt = g_instance.stat_cxt.InstrUserContext;
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(InstrUser);
    ctl.hash = instr_user_hash_code;
    ctl.match = instr_user_match;
    ctl.num_partitions = NUM_INSTR_USER_PARTITIONS;

    g_instance.stat_cxt.InstrUserHTAB = hash_create("instr user hash table",
        INSTR_USER_MAX_HASH_SIZE,
        &ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_PARTITION | HASH_NOEXCEPT);
}

static LWLock* lock_instr_user_hash_partition(uint32 hashCode, LWLockMode lockMode)
{
    LWLock* partitionLock = GetMainLWLockByIndex(FirstInstrUserLock + (hashCode % NUM_INSTR_USER_PARTITIONS));
    (void)LWLockAcquire(partitionLock, lockMode);

    return partitionLock;
}

static void unlock_instr_user_hash_partition(uint32 hashCode)
{
    LWLock* partitionLock = GetMainLWLockByIndex(FirstInstrUserLock + (hashCode % NUM_INSTR_USER_PARTITIONS));
    LWLockRelease(partitionLock);
}

void lock_all_instr_user_partition(LWLockMode lockMode)
{
    for (int i = 0; i < NUM_INSTR_USER_PARTITIONS; i++)
        (void)LWLockAcquire(GetMainLWLockByIndex(FirstInstrUserLock + i), lockMode);
}

void unlock_all_instr_user_partition()
{
    for (int i = 0; i < NUM_INSTR_USER_PARTITIONS; i++)
        LWLockRelease(GetMainLWLockByIndex(FirstInstrUserLock + i));
}

/*
 * new inserted entry need to reset stat counter
 */
static void reset_instr_user(InstrUser* user)
{
    if (user == NULL) {
        return;
    }

    pg_atomic_write_u64(&(user->login_counter), 0);
    pg_atomic_write_u64(&(user->logout_counter), 0);
}

/*
 * InstrUpdateUserLogCounter - update user login/logout counter
 *
 * is_login    : true if is login, else is logout
 */
void InstrUpdateUserLogCounter(bool is_login)
{
    if (!is_instr_user_enabled() || u_sess->proc_cxt.MyProcPort == NULL ||
        u_sess->proc_cxt.MyProcPort->user_name == NULL) {
        return;
    }

    if (is_login) {
        u_sess->user_login_cxt.CurrentInstrLoginUserOid = get_role_oid(u_sess->proc_cxt.MyProcPort->user_name, true);
    }

    if (u_sess->user_login_cxt.CurrentInstrLoginUserOid == InvalidOid) {
        return;
    }

    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[user] user: %u, login :%d", u_sess->user_login_cxt.CurrentInstrLoginUserOid, is_login)));

    /* update user stat */
    InstrUser* entry = NULL;
    uint32 hash_code = instr_user_hash_code(&u_sess->user_login_cxt.CurrentInstrLoginUserOid, sizeof(Oid));

    (void)lock_instr_user_hash_partition(hash_code, LW_SHARED);
    entry = (InstrUser*)hash_search(
        g_instance.stat_cxt.InstrUserHTAB, &u_sess->user_login_cxt.CurrentInstrLoginUserOid, HASH_FIND, NULL);
    if (entry == NULL) {
        Assert(is_login);
        unlock_instr_user_hash_partition(hash_code);

        /* insert entry */
        bool is_found = false;
        (void)lock_instr_user_hash_partition(hash_code, LW_EXCLUSIVE);
        entry = (InstrUser*)hash_search(
            g_instance.stat_cxt.InstrUserHTAB, &u_sess->user_login_cxt.CurrentInstrLoginUserOid, HASH_ENTER, &is_found);
        if (entry == NULL) {
            unlock_instr_user_hash_partition(hash_code);
            ereport(WARNING, (errmodule(MOD_INSTR), errmsg("out of memory when allocating entry")));
            return;
        }
        if (!is_found) {
            reset_instr_user(entry);
        }
    }

    if (is_login) {
        pg_atomic_fetch_add_u64(&entry->login_counter, 1);
    } else {
        pg_atomic_fetch_add_u64(&entry->logout_counter, 1);
    }

    unlock_instr_user_hash_partition(hash_code);

    if (!is_login) {
        u_sess->user_login_cxt.CurrentInstrLoginUserOid = InvalidOid;
    }
}

/*
 * get user by oid from hash table
 *
 * return true if found valid entry in hash table
 */
bool get_instr_user_by_oid(Oid oid, InstrUser* user)
{
    if (!OidIsValid(oid) || user == NULL) {
        return false;
    }

    bool found = false;
    InstrUser* entry = NULL;
    uint32 hash_code = instr_user_hash_code(&oid, sizeof(Oid));

    (void)lock_instr_user_hash_partition(hash_code, LW_SHARED);
    entry = (InstrUser*)hash_search(g_instance.stat_cxt.InstrUserHTAB, &oid, HASH_FIND, NULL);

    /* so far we don't have purge operation for instr user hash table */
    Assert(entry != NULL && entry->user_oid == oid);
    if (entry != NULL && entry->user_oid == oid) {
        user->login_counter = entry->login_counter;
        user->logout_counter = entry->logout_counter;

        found = true;
    }
    unlock_instr_user_hash_partition(hash_code);

    return found;
}

/*
 * handle query to get user stat
 * 1, alloc space to store user OIDs
 * 2, fetch each entry by using the user oid
 */
void* get_instr_user_oids(long* num)
{
    if (!is_instr_user_enabled()) {
        ereport(WARNING,
            (errmodule(MOD_INSTR),
                errmsg("[user] GUC parameter 'enable_resource_track' user stat view will be empty!")));
        *num = 0;
        return NULL;
    }
    if (g_instance.stat_cxt.InstrUserHTAB == NULL) {
        ereport(WARNING, (errmodule(MOD_INSTR), errmsg("[user] user hash table is NULL")));
        *num = 0;
        return NULL;
    }

    lock_all_instr_user_partition(LW_SHARED);
    *num = hash_get_num_entries(g_instance.stat_cxt.InstrUserHTAB);
    if (*num == 0) {
        unlock_all_instr_user_partition();
        *num = 0;
        return NULL;
    }

    Oid* instr_user_oids = NULL;
    instr_user_oids = (Oid*)palloc0_noexcept(*num * sizeof(Oid));
    if (instr_user_oids == NULL) {
        unlock_all_instr_user_partition();
        ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[user] cannot alloc memory for user stat")));
        *num = 0;
        return NULL;
    }

    int i = 0;
    HASH_SEQ_STATUS hash_seq;
    InstrUser* entry = NULL;
    hash_seq_init(&hash_seq, g_instance.stat_cxt.InstrUserHTAB);

    while ((entry = (InstrUser*)hash_seq_search(&hash_seq)) != NULL) {
        *(instr_user_oids + i) = entry->user_oid;
        i++;
    }

    Assert(*num == i);

    unlock_all_instr_user_partition();
    return instr_user_oids;
}

static void create_instr_user_tuple(TupleDesc tupdesc, int* i)
{
    TupleDescInitEntry(tupdesc, (AttrNumber)++(*i), "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++(*i), "user_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++(*i), "user_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++(*i), "login_counter", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++(*i), "logout_counter", INT8OID, -1, 0);
}

static void set_instr_user_tuple_val(Datum* values, int arr_size, Oid user_oid)
{
    int i = 0;
    InstrUser user = {0};
    char user_name[NAMEDATALEN] = {0};

    // node name
    values[i++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);

    // user name
    if (GetRoleName(user_oid, user_name, sizeof(user_name)) != NULL) {
        values[i++] = PointerGetDatum(cstring_to_text_with_len(user_name, sizeof(user_name)));
    } else {
        values[i++] = CStringGetTextDatum("*REMOVED_USER*");
    }

    // user oid
    values[i++] = UInt32GetDatum(user_oid);

    if (get_instr_user_by_oid(user_oid, &user)) {
        // login counter
        values[i++] = UInt64GetDatum(user.login_counter);

        // logout counter
        values[i++] = UInt64GetDatum(user.logout_counter);
    }

    if (arr_size != i) {
        ereport(WARNING, (errmodule(MOD_INSTR), errmsg("[user] instr user column count mismatch")));
    }
}

/* get user login/logout stat info */
Datum get_instr_user_login(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    int i = 0;
    long num = 0;
    const int INSTRUMENTS_USER_ATTRNUM = 5;

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("only system/monitor admin can get user statistics info"))));
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(INSTRUMENTS_USER_ATTRNUM, false);
        create_instr_user_tuple(tupdesc, &i);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = get_instr_user_oids(&num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->max_calls == 0) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[INSTRUMENTS_USER_ATTRNUM];
        bool nulls[INSTRUMENTS_USER_ATTRNUM] = {false};
        HeapTuple tuple = NULL;

        Oid* user_oid = (Oid*)funcctx->user_fctx + funcctx->call_cntr;
        int rc = 0;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        set_instr_user_tuple_val(values, INSTRUMENTS_USER_ATTRNUM, *user_oid);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(funcctx);
    }
}
