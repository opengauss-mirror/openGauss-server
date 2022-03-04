/* -------------------------------------------------------------------------
 *
 * acl.c
 *	  Basic access control list data structures manipulation routines.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/acl.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>

#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "catalog/pg_directory.h"
#include "commands/dbcommands.h"
#include "commands/directory.h"
#include "commands/proclang.h"
#include "commands/tablespace.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "libpq/auth.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static const char* getid(const char* s, char* n);
static void putid(char* p, const char* s);
void check_acl(const Acl* acl);
static const char* aclparse(const char* s, AclItem* aip);
static bool aclitem_match(const AclItem* a1, const AclItem* a2);
static int aclitemComparator(const void* arg1, const void* arg2);
static void check_circularity(const Acl* old_acl, const AclItem* mod_aip, Oid ownerId);
static Acl* recursive_revoke(Acl* acl, Oid grantee, AclMode revoke_privs, Oid ownerId, DropBehavior behavior);
static int oidComparator(const void* arg1, const void* arg2);

static AclMode convert_priv_string(text* priv_type_text);
static AclMode convert_any_priv_string(text* priv_type_text, const priv_map* privileges);

static Oid convert_table_name(text* tablename);
static AclMode convert_table_priv_string(text* priv_type_text);
static AclMode convert_sequence_priv_string(text* priv_type_text);
static AttrNumber convert_column_name(Oid tableoid, text* column);
static AclMode convert_column_priv_string(text* priv_type_text);
static Oid convert_database_name(text* databasename);
static AclMode convert_database_priv_string(text* priv_type_text);
static Oid convert_foreign_data_wrapper_name(text* fdwname);
static AclMode convert_foreign_data_wrapper_priv_string(text* priv_type_text);
static Oid convert_function_name(text* functionname);
static AclMode convert_function_priv_string(text* priv_type_text);
static Oid convert_language_name(text* languagename);
static AclMode convert_language_priv_string(text* priv_type_text);
static Oid convert_schema_name(text* schemaname);
static AclMode convert_schema_priv_string(text* priv_type_text);
static Oid convert_server_name(text* servername);
static AclMode convert_server_priv_string(text* priv_type_text);
static Oid convert_tablespace_name(text* tablespacename);
static AclMode convert_tablespace_priv_string(text* priv_type_text);
static Oid convert_type_name(text* typname);
static AclMode convert_type_priv_string(text* priv_type_text);
static Oid convert_directory_name(text* dirname);
static AclMode convert_directory_priv_string(text* priv_dir_text);
static AclMode convert_role_priv_string(text* priv_type_text);
static AclResult pg_role_aclcheck(Oid role_oid, Oid roleid, AclMode mode);

static void RoleMembershipCacheCallback(Datum arg, int cacheid, uint32 hashvalue);
static Oid get_role_oid_or_public(const char* rolname);

static Oid convert_cmk_name(text *keyname);
static Oid convert_column_key_name(text *keyname);
static AclMode convert_cmk_priv_string(text *priv_type_text);
static AclMode convert_cek_priv_string(text *priv_type_text);

const struct AclAction {
    char action; 
    AclMode read;
} acl_action[] = {
    {ACL_INSERT_CHR, ACL_INSERT},
    {ACL_SELECT_CHR, ACL_SELECT},
    {ACL_UPDATE_CHR, ACL_UPDATE},
    {ACL_DELETE_CHR, ACL_DELETE},
    {ACL_TRUNCATE_CHR, ACL_TRUNCATE},
    {ACL_REFERENCES_CHR, ACL_REFERENCES},
    {ACL_TRIGGER_CHR, ACL_TRIGGER},
    {ACL_EXECUTE_CHR, ACL_EXECUTE},
    {ACL_USAGE_CHR, ACL_USAGE},
    {ACL_CREATE_CHR, ACL_CREATE},
    {ACL_CREATE_TEMP_CHR, ACL_CREATE_TEMP},
    {ACL_CONNECT_CHR, ACL_CONNECT},
    {ACL_COMPUTE_CHR, ACL_COMPUTE},
    {ACL_READ_CHR, ACL_READ},
    {ACL_WRITE_CHR, ACL_WRITE},
    {ACL_ALTER_CHR, ACL_ALTER},
    {ACL_DROP_CHR, ACL_DROP},
    {ACL_COMMENT_CHR, ACL_COMMENT},
    {ACL_INDEX_CHR, ACL_INDEX},
    {ACL_VACUUM_CHR, ACL_VACUUM},
};

const struct AclObjType {
    GrantObjectType objtype;
    AclMode world_default;
    AclMode owner_default;
} acl_obj_type[] = {
    {ACL_OBJECT_COLUMN, ACL_NO_RIGHTS, ACL_NO_RIGHTS},
    {ACL_OBJECT_SEQUENCE, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_SEQUENCE},
    {ACL_OBJECT_DATABASE, (ACL_CREATE_TEMP | ACL_CONNECT), ACL_ALL_RIGHTS_DATABASE},
    {ACL_OBJECT_FUNCTION, ACL_EXECUTE, ACL_ALL_RIGHTS_FUNCTION},
    {ACL_OBJECT_PACKAGE, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_PACKAGE},
    {ACL_OBJECT_LARGEOBJECT, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_LARGEOBJECT},
    {ACL_OBJECT_NAMESPACE, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_NAMESPACE},
    {ACL_OBJECT_NODEGROUP, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_NODEGROUP},
    {ACL_OBJECT_TABLESPACE, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_TABLESPACE},
    {ACL_OBJECT_FDW, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_FDW},
    {ACL_OBJECT_FOREIGN_SERVER, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_FOREIGN_SERVER},
    {ACL_OBJECT_DATA_SOURCE, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_DATA_SOURCE},
    {ACL_OBJECT_GLOBAL_SETTING, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_KEY},
    {ACL_OBJECT_COLUMN_SETTING, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_KEY},
    {ACL_OBJECT_DOMAIN, ACL_USAGE, ACL_ALL_RIGHTS_TYPE},
    {ACL_OBJECT_TYPE, ACL_USAGE, ACL_ALL_RIGHTS_TYPE},
    {ACL_OBJECT_DIRECTORY, ACL_NO_RIGHTS, ACL_ALL_RIGHTS_DIRECTORY},
};

/*
 * getid
 *		Consumes the first alphanumeric string (identifier) found in string
 *		's', ignoring any leading white space.	If it finds a double quote
 *		it returns the word inside the quotes.
 *
 * RETURNS:
 *		the string position in 's' that points to the next non-space character
 *		in 's', after any quotes.  Also:
 *		- loads the identifier into 'n'.  (If no identifier is found, 'n'
 *		  contains an empty string.)  'n' must be NAMEDATALEN bytes.
 */
static const char* getid(const char* s, char* n)
{
    int len = 0;
    bool in_quotes = false;

    while (isspace((unsigned char)*s)) {
        s++;
    }

    /* This code had better match what putid() does, below */
    for (; *s != '\0' && (isalnum((unsigned char)*s) || *s == '_' || *s == '"' || in_quotes); s++) {
        if (*s == '"') {
            /* safe to look at next char (could be '\0' though) */
            if (*(s + 1) != '"') {
                in_quotes = !in_quotes;
                continue;
            }
            /* it's an escaped double quote; skip the escaping char */
            s++;
        }

        /* Add the character to the string */
        if (len >= NAMEDATALEN - 1)
            ereport(ERROR,
                (errcode(ERRCODE_NAME_TOO_LONG),
                    errmsg("identifier too long"),
                    errdetail("Identifier must be less than %d characters.", NAMEDATALEN)));

        n[len++] = *s;
    }
    n[len] = '\0';
    while (isspace((unsigned char)*s)) {
        s++;
    }
    return s;
}

/*
 * Write a role name at *p, adding double quotes if needed.
 * There must be at least (2*NAMEDATALEN)+2 bytes available at *p.
 * This needs to be kept in sync with copyAclUserName in pg_dump/dumputils.c
 */
static void putid(char* p, const char* s)
{
    const char* src = NULL;
    bool safe = true;

    for (src = s; *src; src++) {
        /* This test had better match what getid() does, above */
        if (!isalnum((unsigned char)*src) && *src != '_') {
            safe = false;
            break;
        }
    }
    if (!safe)
        *p++ = '"';
    for (src = s; *src; src++) {
        /* A double quote character in a username is encoded as "" */
        if (*src == '"')
            *p++ = '"';
        *p++ = *src;
    }
    if (!safe)
        *p++ = '"';
    *p = '\0';
}

/*
 * aclparse
 *		Consumes and parses an ACL specification of the form:
 *				[group|user] [A-Za-z0-9]*=[rwaR]*
 *		from string 's', ignoring any leading white space or white space
 *		between the optional id type keyword (group|user) and the actual
 *		ACL specification.
 *
 *		The group|user decoration is unnecessary in the roles world,
 *		but we still accept it for backward compatibility.
 *
 *		This routine is called by the parser as well as aclitemin(), hence
 *		the added generality.
 *
 * RETURNS:
 *		the string position in 's' immediately following the ACL
 *		specification.	Also:
 *		- loads the structure pointed to by 'aip' with the appropriate
 *		  UID/GID, id type identifier and mode type values.
 */
static const char* aclparse(const char* s, AclItem* aip)
{
    AclMode privs, goption, read;
    char name[NAMEDATALEN] = {0};
    char name2[NAMEDATALEN] = {0};
    bool ddlChar = false;
    bool dmlChar = false;
    bool actionType = false;

    if (s == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
            errmsg("ACL string cannot be NULL.")));
    }

    if (aip == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
            errmsg("ACL item cannot be NULL.")));
    }

#ifdef ACLDEBUG
    elog(LOG, "aclparse: input = \"%s\"", s);
#endif
    s = getid(s, name);
    if (*s != '=') {
        /* we just read a keyword, not a name */
        if (strcmp(name, "group") != 0 && strcmp(name, "user") != 0)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("unrecognized key word: \"%s\"", name),
                    errhint("ACL key word must be \"group\" or \"user\".")));
        s = getid(s, name); /* move s to the name beyond the keyword */
        if (name[0] == '\0')
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("missing name"),
                    errhint("A name must follow the \"group\" or \"user\" key word.")));
    }

    if (*s != '=')
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("missing \"=\" sign")));

    privs = goption = ACL_NO_RIGHTS;

    for (++s, read = 0; isalpha((unsigned char)*s) || *s == '*'; s++) {
        if (*s == '*') {
            goption |= read;
        } else {
            for (size_t idx = 0; idx < (sizeof(acl_action) / sizeof(acl_action[0])); idx++) {
                if (acl_action[idx].action == *s) {
                    actionType = true;
                    read = acl_action[idx].read;
                    break;
                }
            }
            if (actionType) {
                actionType = false;
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid mode character: must be one of \"%s\" or \"%s\"",
                            ACL_ALL_RIGHTS_STR, ACL_ALL_DDL_RIGHTS_STR)));
            }
        }

        if (ACLMODE_FOR_DDL(read)) {
            ddlChar = true;
        } else {
            dmlChar = true;
        }
        if (dmlChar && ddlChar) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("not support: characters should belong to \"%s\" or \"%s\"",
                            ACL_ALL_RIGHTS_STR, ACL_ALL_DDL_RIGHTS_STR)));
        }

        privs |= read;
    }

    if (name[0] == '\0')
        aip->ai_grantee = ACL_ID_PUBLIC;
    else
        aip->ai_grantee = get_role_oid(name, false);

    /*
     * XXX Allow a degree of backward compatibility by defaulting the grantor
     * to the superuser.
     */
    if (*s == '/') {
        s = getid(s + 1, name2);
        if (name2[0] == '\0')
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("a name must follow the \"/\" sign")));
        aip->ai_grantor = get_role_oid(name2, false);
    } else {
        aip->ai_grantor = BOOTSTRAP_SUPERUSERID;
        ereport(WARNING,
            (errcode(ERRCODE_INVALID_GRANTOR), errmsg("defaulting grantor to user ID %u", BOOTSTRAP_SUPERUSERID)));
    }

    goption = REMOVE_DDL_FLAG(goption);
    ACLITEM_SET_PRIVS_GOPTIONS(*aip, privs, goption);

#ifdef ACLDEBUG
    elog(LOG, "aclparse: correctly read [%u %x %x]", aip->ai_grantee, privs, goption);
#endif

    return s;
}

/*
 * allocacl
 *		Allocates storage for a new Acl with 'n' entries.
 *
 * RETURNS:
 *		the new Acl
 */
Acl* allocacl(int n)
{
    Acl* new_acl = NULL;
    Size size;

    if (n < 0 || n > (MAX_INT32 - (int)ARR_OVERHEAD_NONULLS(1)) / (int)sizeof(aclitem)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid size: %d", n)));
    }
    size = ACL_N_SIZE(n);
    new_acl = (Acl*)palloc0(size);
    SET_VARSIZE(new_acl, size);
    new_acl->ndim = 1;
    new_acl->dataoffset = 0; /* we never put in any nulls */
    new_acl->elemtype = ACLITEMOID;
    ARR_LBOUND(new_acl)[0] = 1;
    ARR_DIMS(new_acl)[0] = n;
    return new_acl;
}

/*
 * Create a zero-entry ACL
 */
Acl* make_empty_acl(void)
{
    return allocacl(0);
}

/*
 * Copy an ACL
 */
Acl* aclcopy(const Acl* orig_acl)
{
    Acl* result_acl = NULL;

    result_acl = allocacl(ACL_NUM(orig_acl));

    errno_t errorno = EOK;
    if (ACL_NUM(orig_acl) * sizeof(AclItem) > 0) {
        errorno = memcpy_s(ACL_DAT(result_acl),
            ACL_NUM(orig_acl) * sizeof(AclItem),
            ACL_DAT(orig_acl),
            ACL_NUM(orig_acl) * sizeof(AclItem));
        securec_check(errorno, "\0", "\0");
    }

    return result_acl;
}

/*
 * Concatenate two ACLs
 *
 * This is a bit cheesy, since we may produce an ACL with redundant entries.
 * Be careful what the result is used for!
 */
Acl* aclconcat(const Acl* left_acl, const Acl* right_acl)
{
    Acl* result_acl = NULL;

    result_acl = allocacl(ACL_NUM(left_acl) + ACL_NUM(right_acl));

    errno_t errorno = EOK;
    if (ACL_NUM(left_acl) * sizeof(AclItem) > 0) {
        errorno = memcpy_s(ACL_DAT(result_acl),
            ACL_NUM(left_acl) * sizeof(AclItem),
            ACL_DAT(left_acl),
            ACL_NUM(left_acl) * sizeof(AclItem));
        securec_check(errorno, "\0", "\0");
    }
    if (ACL_NUM(right_acl) * sizeof(AclItem) > 0) {
        errorno = memcpy_s(ACL_DAT(result_acl) + ACL_NUM(left_acl),
            ACL_NUM(right_acl) * sizeof(AclItem),
            ACL_DAT(right_acl),
            ACL_NUM(right_acl) * sizeof(AclItem));
        securec_check(errorno, "\0", "\0");
    }

    return result_acl;
}

/*
 * Merge two ACLs
 *
 * This produces a properly merged ACL with no redundant entries.
 * Returns NULL on NULL input.
 */
Acl* aclmerge(const Acl* left_acl, const Acl* right_acl, Oid ownerId)
{
    Acl* result_acl = NULL;
    AclItem* aip = NULL;
    int i, num;

    /* Check for cases where one or both are empty/null */
    if (left_acl == NULL || ACL_NUM(left_acl) == 0) {
        if (right_acl == NULL || ACL_NUM(right_acl) == 0)
            return NULL;
        else
            return aclcopy(right_acl);
    } else {
        if (right_acl == NULL || ACL_NUM(right_acl) == 0)
            return aclcopy(left_acl);
    }

    /* Merge them the hard way, one item at a time */
    result_acl = aclcopy(left_acl);

    aip = ACL_DAT(right_acl);
    num = ACL_NUM(right_acl);

    for (i = 0; i < num; i++, aip++) {
        Acl* tmp_acl = NULL;

        tmp_acl = aclupdate(result_acl, aip, ACL_MODECHG_ADD, ownerId, DROP_RESTRICT);
        pfree_ext(result_acl);
        result_acl = tmp_acl;
    }

    return result_acl;
}

/*
 * Sort the items in an ACL (into an arbitrary but consistent order)
 */
void aclitemsort(Acl* acl)
{
    if (acl != NULL && ACL_NUM(acl) > 1)
        qsort(ACL_DAT(acl), ACL_NUM(acl), sizeof(AclItem), aclitemComparator);
}

/*
 * Check if two ACLs are exactly equal
 *
 * This will not detect equality if the two arrays contain the same items
 * in different orders.  To handle that case, sort both inputs first,
 * using aclitemsort().
 */
bool aclequal(const Acl* left_acl, const Acl* right_acl)
{
    /* Check for cases where one or both are empty/null */
    if (left_acl == NULL || ACL_NUM(left_acl) == 0) {
        if (right_acl == NULL || ACL_NUM(right_acl) == 0)
            return true;
        else
            return false;
    } else {
        if (right_acl == NULL || ACL_NUM(right_acl) == 0)
            return false;
    }

    if (ACL_NUM(left_acl) != ACL_NUM(right_acl))
        return false;

    if (memcmp(ACL_DAT(left_acl), ACL_DAT(right_acl), ACL_NUM(left_acl) * sizeof(AclItem)) == 0)
        return true;

    return false;
}

/*
 * Verify that an ACL array is acceptable (one-dimensional and has no nulls)
 */
void check_acl(const Acl* acl)
{
    if (ARR_ELEMTYPE(acl) != ACLITEMOID)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ACL array contains wrong data type")));
    if (ARR_NDIM(acl) != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ACL arrays must be one-dimensional")));
    if (ARR_HASNULL(acl))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("ACL arrays must not contain null values")));
}

/*
 * aclitemin
 *		Allocates storage for, and fills in, a new AclItem given a string
 *		's' that contains an ACL specification.  See aclparse for details.
 *
 * RETURNS:
 *		the new AclItem
 */
Datum aclitemin(PG_FUNCTION_ARGS)
{
    const char* s = PG_GETARG_CSTRING(0);
    AclItem* aip = NULL;

    aip = (AclItem*)palloc(sizeof(AclItem));
    s = aclparse(s, aip);
    while (isspace((unsigned char)*s)) {
        ++s;
    }
    if (*s)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("extra garbage at the end of the ACL specification")));

    PG_RETURN_ACLITEM_P(aip);
}

/*
 * aclitemout
 *		Allocates storage for, and fills in, a new null-delimited string
 *		containing a formatted ACL specification.  See aclparse for details.
 *
 * RETURNS:
 *		the new string
 */
Datum aclitemout(PG_FUNCTION_ARGS)
{
    AclItem* aip = PG_GETARG_ACLITEM_P(0);
    char* p = NULL;
    char* out = NULL;
    HeapTuple htup;
    unsigned i;
    errno_t errorno = EOK;

    if (ACLMODE_FOR_DDL(aip->ai_privs)) {
        out = (char*)palloc(strlen("=/") + 2 * N_ACL_DDL_RIGHTS + 2 * (2 * NAMEDATALEN + 2) + 1);
    } else {
        out = (char*)palloc(strlen("=/") + 2 * N_ACL_RIGHTS + 2 * (2 * NAMEDATALEN + 2) + 1);
    }

    p = out;
    *p = '\0';

    if (aip->ai_grantee != ACL_ID_PUBLIC) {
        htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(aip->ai_grantee));
        if (HeapTupleIsValid(htup)) {
            putid(p, NameStr(((Form_pg_authid)GETSTRUCT(htup))->rolname));
            ReleaseSysCache(htup);
        } else {
            /* Generate numeric OID if we don't find an entry */
            errorno =
                sprintf_s(p, strlen("=/") + (2 * N_ACL_RIGHTS + 2 * (2 * NAMEDATALEN + 2) + 1), "%u", aip->ai_grantee);
            securec_check_ss(errorno, "\0", "\0");
        }
    }
    while (*p)
        ++p;

    *p++ = '=';

    if (ACLMODE_FOR_DDL(aip->ai_privs)) {
        for (i = 0; i < N_ACL_DDL_RIGHTS; ++i) {
            if (ACLITEM_GET_PRIVS(*aip) & (1 << i))
                *p++ = ACL_ALL_DDL_RIGHTS_STR[i];
            if (ACLITEM_GET_GOPTIONS(*aip) & (1 << i))
                *p++ = '*';
        }
    } else {
        for (i = 0; i < N_ACL_RIGHTS; ++i) {
            if (ACLITEM_GET_PRIVS(*aip) & (1 << i))
                *p++ = ACL_ALL_RIGHTS_STR[i];
            if (ACLITEM_GET_GOPTIONS(*aip) & (1 << i))
                *p++ = '*';
        }
    }

    *p++ = '/';
    *p = '\0';

    htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(aip->ai_grantor));
    if (HeapTupleIsValid(htup)) {
        putid(p, NameStr(((Form_pg_authid)GETSTRUCT(htup))->rolname));
        ReleaseSysCache(htup);
    } else {
        /* Generate numeric OID if we don't find an entry */
        if (ACLMODE_FOR_DDL(aip->ai_privs)) {
            errorno = sprintf_s(p, strlen("=/") + (2 * N_ACL_DDL_RIGHTS + 2 * (2 * NAMEDATALEN + 2) + 1),
                "%u", aip->ai_grantor);
        } else {
            errorno = sprintf_s(p, strlen("=/") + (2 * N_ACL_RIGHTS + 2 * (2 * NAMEDATALEN + 2) + 1),
                "%u", aip->ai_grantor);
        }
        securec_check_ss(errorno, "\0", "\0");
    }

    PG_RETURN_CSTRING(out);
}

/*
 * aclitem_match
 *		Two AclItems are considered to match iff they have the same
 *		grantee and grantor and they represent the same type privileges.
 */
static bool aclitem_match(const AclItem* a1, const AclItem* a2)
{
    return a1->ai_grantee == a2->ai_grantee && a1->ai_grantor == a2->ai_grantor &&
        ACLMODE_FOR_DDL(a1->ai_privs) == ACLMODE_FOR_DDL(a2->ai_privs);
}

/*
 * aclitemComparator
 *		qsort comparison function for AclItems
 */
static int aclitemComparator(const void* arg1, const void* arg2)
{
    const AclItem* a1 = (const AclItem*)arg1;
    const AclItem* a2 = (const AclItem*)arg2;

    if (a1->ai_grantee > a2->ai_grantee)
        return 1;
    if (a1->ai_grantee < a2->ai_grantee)
        return -1;
    if (a1->ai_grantor > a2->ai_grantor)
        return 1;
    if (a1->ai_grantor < a2->ai_grantor)
        return -1;
    if (a1->ai_privs > a2->ai_privs)
        return 1;
    if (a1->ai_privs < a2->ai_privs)
        return -1;
    return 0;
}

/*
 * aclitem equality operator
 */
Datum aclitem_eq(PG_FUNCTION_ARGS)
{
    AclItem* a1 = PG_GETARG_ACLITEM_P(0);
    AclItem* a2 = PG_GETARG_ACLITEM_P(1);
    bool result = false;

    result = a1->ai_privs == a2->ai_privs && a1->ai_grantee == a2->ai_grantee && a1->ai_grantor == a2->ai_grantor;
    PG_RETURN_BOOL(result);
}

/*
 * aclitem hash function
 *
 * We make aclitems hashable not so much because anyone is likely to hash
 * them, as because we want array equality to work on aclitem arrays, and
 * with the typcache mechanism we must have a hash or btree opclass.
 */
Datum hash_aclitem(PG_FUNCTION_ARGS)
{
    AclItem* a = PG_GETARG_ACLITEM_P(0);

    /* not very bright, but avoids any issue of padding in struct */
    PG_RETURN_UINT32((uint32)(a->ai_privs + a->ai_grantee + a->ai_grantor));
}

/*
 * acldefault()  --- create an ACL describing default access permissions
 *
 * Change this routine if you want to alter the default access policy for
 * newly-created objects (or any object with a NULL acl entry).
 *
 * Note that these are the hard-wired "defaults" that are used in the
 * absence of any pg_default_acl entry.
 */
Acl* acldefault(GrantObjectType objtype, Oid ownerId, Oid objId)
{
    AclMode world_default;
    AclMode owner_default;
    int nacl;
    bool type_flag = false;
    Acl* acl = NULL;
    AclItem* aip = NULL;

    if (objtype == ACL_OBJECT_LANGUAGE) {
        /* Grant USAGE by default, for now */
        if (is_trust_language(objId))
            world_default = ACL_USAGE;
        else
            world_default = ACL_NO_RIGHTS;
        owner_default = ACL_ALL_RIGHTS_LANGUAGE;
    } else if (objtype == ACL_OBJECT_RELATION) {
        world_default = ACL_NO_RIGHTS;
        owner_default = u_sess->attr.attr_common.IsInplaceUpgrade ? ACL_NO_RIGHTS : ACL_ALL_RIGHTS_RELATION;
    } else {
        for (size_t idx = 0; idx < sizeof(acl_obj_type) / sizeof(acl_obj_type[0]); idx++) {
            if (acl_obj_type[idx].objtype == objtype) {
                world_default = acl_obj_type[idx].world_default;
                owner_default = acl_obj_type[idx].owner_default;
                type_flag = true;
                break;
            }
        }
        if (!type_flag) {
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype: %d", (int)objtype)));
            world_default = ACL_NO_RIGHTS; /* keep compiler quiet */
            owner_default = ACL_NO_RIGHTS;
        }
    }

    nacl = 0;
    if (world_default != ACL_NO_RIGHTS)
        nacl++;
    if (owner_default != ACL_NO_RIGHTS)
        nacl++;

    acl = allocacl(nacl);
    aip = ACL_DAT(acl);

    if (world_default != ACL_NO_RIGHTS) {
        aip->ai_grantee = ACL_ID_PUBLIC;
        aip->ai_grantor = ownerId;
        ACLITEM_SET_PRIVS_GOPTIONS(*aip, world_default, ACL_NO_RIGHTS);
        aip++;
    }

    /*
     * Note that the owner's entry shows all ordinary privileges but no grant
     * options.  This is because his grant options come "from the system" and
     * not from his own efforts.  (The SQL spec says that the owner's rights
     * come from a "_SYSTEM" authid.)  However, we do consider that the
     * owner's ordinary privileges are self-granted; this lets him revoke
     * them.  We implement the owner's grant options without any explicit
     * "_SYSTEM"-like ACL entry, by internally special-casing the owner
     * wherever we are testing grant options.
     */
    if (owner_default != ACL_NO_RIGHTS) {
        aip->ai_grantee = ownerId;
        aip->ai_grantor = ownerId;
        ACLITEM_SET_PRIVS_GOPTIONS(*aip, owner_default, ACL_NO_RIGHTS);
    }

    return acl;
}

/*
 * SQL-accessible version of acldefault().	Hackish mapping from "char" type to
 * ACL_OBJECT_* values, but it's only used in the information schema, not
 * documented for general use.
 */
Datum acldefault_sql(PG_FUNCTION_ARGS)
{
    char objtypec = PG_GETARG_CHAR(0);
    Oid owner = PG_GETARG_OID(1);
    GrantObjectType objtype = (GrantObjectType)0;

    switch (objtypec) {
        case 'c':
            objtype = ACL_OBJECT_COLUMN;
            break;
        case 'r':
            objtype = ACL_OBJECT_RELATION;
            break;
        case 's':
            objtype = ACL_OBJECT_SEQUENCE;
            break;
        case 'd':
            objtype = ACL_OBJECT_DATABASE;
            break;
        case 'f':
            objtype = ACL_OBJECT_FUNCTION;
            break;
        case 'p':
            objtype = ACL_OBJECT_PACKAGE;
            break;
        case 'l':
            objtype = ACL_OBJECT_LANGUAGE;
            break;
        case 'L':
            objtype = ACL_OBJECT_LARGEOBJECT;
            break;
        case 'n':
            objtype = ACL_OBJECT_NAMESPACE;
            break;
        case 't':
            objtype = ACL_OBJECT_TABLESPACE;
            break;
        case 'F':
            objtype = ACL_OBJECT_FDW;
            break;
        case 'S':
            objtype = ACL_OBJECT_FOREIGN_SERVER;
            break;
        case 'T':
            objtype = ACL_OBJECT_TYPE;
            break;
        case 'G':
            objtype = ACL_OBJECT_NODEGROUP;
            break;
        case 'K':
            objtype = ACL_OBJECT_GLOBAL_SETTING;
            break;
        case 'k':
            objtype = ACL_OBJECT_COLUMN_SETTING;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized objtype abbreviation: %c", objtypec)));
    }

    PG_RETURN_ACL_P(acldefault(objtype, owner));
}

/*
 * Update an ACL array to add or remove specified privileges.
 *
 *	old_acl: the input ACL array
 *	mod_aip: defines the privileges to be added, removed, or substituted
 *	modechg: ACL_MODECHG_ADD, ACL_MODECHG_DEL, or ACL_MODECHG_EQL
 *	ownerId: Oid of object owner
 *	behavior: RESTRICT or CASCADE behavior for recursive removal
 *
 * ownerid and behavior are only relevant when the update operation specifies
 * deletion of grant options.
 *
 * The result is a modified copy; the input object is not changed.
 *
 * NB: caller is responsible for having detoasted the input ACL, if needed.
 */
Acl* aclupdate(const Acl* old_acl, const AclItem* mod_aip, int modechg, Oid ownerId, DropBehavior behavior)
{
    Acl* new_acl = NULL;
    AclItem *old_aip = NULL, *new_aip = NULL;
    AclMode old_rights, old_goptions, new_rights, new_goptions;
    int dst, num;

    bool is_ddl_privileges = ACLMODE_FOR_DDL(mod_aip->ai_privs);

    /* Caller probably already checked old_acl, but be safe */
    check_acl(old_acl);

    /* If granting grant options, check for circularity */
    if (modechg != ACL_MODECHG_DEL && REMOVE_DDL_FLAG(ACLITEM_GET_GOPTIONS(*mod_aip)) != ACL_NO_RIGHTS)
        check_circularity(old_acl, mod_aip, ownerId);

    num = ACL_NUM(old_acl);
    old_aip = ACL_DAT(old_acl);

    /*
     * Search the ACL for an existing entry for this grantee and grantor. If
     * one exists, just modify the entry in-place (well, in the same position,
     * since we actually return a copy); otherwise, insert the new entry at
     * the end.
     */

    for (dst = 0; dst < num; ++dst) {
        if (aclitem_match(mod_aip, old_aip + dst)) {
            /* found a match, so modify existing item */
            new_acl = allocacl(num);
            new_aip = ACL_DAT(new_acl);
            errno_t errorno = EOK;
            errorno = memcpy_s(new_acl, ACL_SIZE(new_acl), old_acl, ACL_SIZE(old_acl));
            securec_check(errorno, "\0", "\0");
            break;
        }
    }

    if (dst == num) {
        /* need to append a new item */
        new_acl = allocacl(num + 1);
        new_aip = ACL_DAT(new_acl);
        errno_t errorno = EOK;
        if (num > 0) {
            errorno = memcpy_s(new_aip, num * sizeof(AclItem), old_aip, num * sizeof(AclItem));
            securec_check(errorno, "\0", "\0");
        }

        /* initialize the new entry with no permissions */
        new_aip[dst].ai_grantee = mod_aip->ai_grantee;
        new_aip[dst].ai_grantor = mod_aip->ai_grantor;
        ACLITEM_SET_PRIVS_GOPTIONS(new_aip[dst], ACL_NO_RIGHTS, ACL_NO_RIGHTS);
        num++; /* set num to the size of new_acl */
    }

    old_rights = ACLITEM_GET_RIGHTS(new_aip[dst]);
    old_goptions = ACLITEM_GET_GOPTIONS(new_aip[dst]);

    /* apply the specified permissions change */
    switch (modechg) {
        case ACL_MODECHG_ADD:
            ACLITEM_SET_RIGHTS(new_aip[dst], old_rights | ACLITEM_GET_RIGHTS(*mod_aip));
            break;
        case ACL_MODECHG_DEL:
            ACLITEM_SET_RIGHTS(new_aip[dst], old_rights & ~ACLITEM_GET_RIGHTS(*mod_aip));
            /* add ddl privileges flag to Aclitem */
            if (is_ddl_privileges) {
                new_aip[dst].ai_privs = ADD_DDL_FLAG(new_aip[dst].ai_privs);
            }
            break;
        case ACL_MODECHG_EQL:
            ACLITEM_SET_RIGHTS(new_aip[dst], ACLITEM_GET_RIGHTS(*mod_aip));
            break;
        default:
            break;
    }

    new_rights = ACLITEM_GET_RIGHTS(new_aip[dst]);
    new_goptions = ACLITEM_GET_GOPTIONS(new_aip[dst]);

    /*
     * If the adjusted entry has no permissions, delete it from the list.
     */
    if (REMOVE_DDL_FLAG(new_rights) == ACL_NO_RIGHTS) {
        errno_t errorno = EOK;
        errorno = memmove_s(
            new_aip + dst, (num - dst - 1) * sizeof(AclItem) + 1, new_aip + dst + 1, (num - dst - 1) * sizeof(AclItem));
        securec_check(errorno, "\0", "\0");
        /* Adjust array size to be 'num - 1' items */
        ARR_DIMS(new_acl)[0] = num - 1;
        SET_VARSIZE(new_acl, ACL_N_SIZE(num - 1));
    }

    /*
     * Remove abandoned privileges (cascading revoke).	Currently we can only
     * handle this when the grantee is not PUBLIC.
     */
    if ((old_goptions & ~new_goptions) != 0) {
        Assert(mod_aip->ai_grantee != ACL_ID_PUBLIC);
        if (is_ddl_privileges) {
            new_acl = recursive_revoke(new_acl, mod_aip->ai_grantee, ADD_DDL_FLAG(old_goptions & ~new_goptions),
                ownerId, behavior);
        } else {
            new_acl = recursive_revoke(new_acl, mod_aip->ai_grantee, (old_goptions & ~new_goptions), ownerId, behavior);
        }
    }

    return new_acl;
}

/*
 * Update an ACL array to reflect a change of owner to the parent object
 *
 *	old_acl: the input ACL array (must not be NULL)
 *	oldOwnerId: Oid of the old object owner
 *	newOwnerId: Oid of the new object owner
 *
 * The result is a modified copy; the input object is not changed.
 *
 * NB: caller is responsible for having detoasted the input ACL, if needed.
 */
Acl* aclnewowner(const Acl* old_acl, Oid oldOwnerId, Oid newOwnerId)
{
    Acl* new_acl = NULL;
    AclItem* new_aip = NULL;
    AclItem* old_aip = NULL;
    AclItem* dst_aip = NULL;
    AclItem* src_aip = NULL;
    AclItem* targ_aip = NULL;
    bool newpresent = false;
    int dst, src, targ, num;
    errno_t ss_rc = 0;

    check_acl(old_acl);

    /*
     * Make a copy of the given ACL, substituting new owner ID for old
     * wherever it appears as either grantor or grantee.  Also note if the new
     * owner ID is already present.
     */
    num = ACL_NUM(old_acl);
    new_acl = allocacl(num);
    /*
     * When oldOwner has none privilege, old_acl is empty and num == 0,
     * then new_acl is empty, return it directly.
     */
    if (num == 0)
        return new_acl;
    old_aip = ACL_DAT(old_acl);
    new_aip = ACL_DAT(new_acl);
    ss_rc = memcpy_s(new_aip, num * sizeof(AclItem), old_aip, num * sizeof(AclItem));
    securec_check(ss_rc, "\0", "\0");

    for (dst = 0, dst_aip = new_aip; dst < num; dst++, dst_aip++) {
        if (dst_aip->ai_grantor == oldOwnerId)
            dst_aip->ai_grantor = newOwnerId;
        else if (dst_aip->ai_grantor == newOwnerId)
            newpresent = true;
        if (dst_aip->ai_grantee == oldOwnerId)
            dst_aip->ai_grantee = newOwnerId;
        else if (dst_aip->ai_grantee == newOwnerId)
            newpresent = true;
    }

    /*
     * If the old ACL contained any references to the new owner, then we may
     * now have generated an ACL containing duplicate entries.	Find them and
     * merge them so that there are not duplicates.  (This is relatively
     * expensive since we use a stupid O(N^2) algorithm, but it's unlikely to
     * be the normal case.)
     *
     * To simplify deletion of duplicate entries, we temporarily leave them in
     * the array but set their privilege masks to zero; when we reach such an
     * entry it's just skipped.  (Thus, a side effect of this code will be to
     * remove privilege-free entries, should there be any in the input.)  dst
     * is the next output slot, targ is the currently considered input slot
     * (always >= dst), and src scans entries to the right of targ looking for
     * duplicates.	Once an entry has been emitted to dst it is known
     * duplicate-free and need not be considered anymore.
     */
    if (newpresent) {
        dst = 0;
        for (targ = 0, targ_aip = new_aip; targ < num; targ++, targ_aip++) {
            /* ignore if deleted in an earlier pass */
            if (ACLITEM_GET_RIGHTS(*targ_aip) == ACL_NO_RIGHTS)
                continue;
            /* find and merge any duplicates */
            for (src = targ + 1, src_aip = targ_aip + 1; src < num; src++, src_aip++) {
                if (ACLITEM_GET_RIGHTS(*src_aip) == ACL_NO_RIGHTS)
                    continue;
                if (aclitem_match(targ_aip, src_aip)) {
                    ACLITEM_SET_RIGHTS(*targ_aip, ACLITEM_GET_RIGHTS(*targ_aip) | ACLITEM_GET_RIGHTS(*src_aip));
                    /* mark the duplicate deleted */
                    ACLITEM_SET_RIGHTS(*src_aip, ACL_NO_RIGHTS);
                }
            }
            /* and emit to output */
            new_aip[dst] = *targ_aip;
            dst++;
        }
        /* Adjust array size to be 'dst' items */
        ARR_DIMS(new_acl)[0] = dst;
        SET_VARSIZE(new_acl, ACL_N_SIZE(dst));
    }

    return new_acl;
}

/*
 * When granting grant options, we must disallow attempts to set up circular
 * chains of grant options.  Suppose A (the object owner) grants B some
 * privileges with grant option, and B re-grants them to C.  If C could
 * grant the privileges to B as well, then A would be unable to effectively
 * revoke the privileges from B, since recursive_revoke would consider that
 * B still has 'em from C.
 *
 * We check for this by recursively deleting all grant options belonging to
 * the target grantee, and then seeing if the would-be grantor still has the
 * grant option or not.
 */
static void check_circularity(const Acl* old_acl, const AclItem* mod_aip, Oid ownerId)
{
    Acl* acl = NULL;
    AclItem* aip = NULL;
    int i, num;
    AclMode own_privs;
    bool is_ddl_privileges = ACLMODE_FOR_DDL(mod_aip->ai_privs);

    check_acl(old_acl);

    /*
     * For now, grant options can only be granted to roles, not PUBLIC.
     * Otherwise we'd have to work a bit harder here.
     */
    Assert(mod_aip->ai_grantee != ACL_ID_PUBLIC);

    /* The owner always has grant options, no need to check */
    if (mod_aip->ai_grantor == ownerId)
        return;

    /* Make a working copy */
    acl = allocacl(ACL_NUM(old_acl));
    errno_t errorno = EOK;
    errorno = memcpy_s(acl, ACL_SIZE(acl), old_acl, ACL_SIZE(old_acl));
    securec_check(errorno, "\0", "\0");

    /* Zap all grant options of target grantee, plus what depends on 'em */
cc_restart:
    num = ACL_NUM(acl);
    aip = ACL_DAT(acl);
    for (i = 0; i < num; i++) {
        if (aip[i].ai_grantee == mod_aip->ai_grantee && ACLITEM_GET_GOPTIONS(aip[i]) != ACL_NO_RIGHTS 
            && is_ddl_privileges == ACLMODE_FOR_DDL(aip[i].ai_privs)) {
            Acl* new_acl = NULL;

            /* We'll actually zap ordinary privs too, but no matter */
            new_acl = aclupdate(acl, &aip[i], ACL_MODECHG_DEL, ownerId, DROP_CASCADE);

            pfree_ext(acl);
            acl = new_acl;

            goto cc_restart;
        }
    }

    /* Now we can compute grantor's independently-derived privileges */
    AclMode mask;
    if (is_ddl_privileges) {
        mask = ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(ACLITEM_GET_GOPTIONS(*mod_aip)));
    } else {
        mask = ACL_GRANT_OPTION_FOR(ACLITEM_GET_GOPTIONS(*mod_aip));
    }
    own_privs = aclmask(acl, mod_aip->ai_grantor, ownerId, mask, ACLMASK_ALL);
    own_privs = ACL_OPTION_TO_PRIVS(own_privs);

    if ((ACLITEM_GET_GOPTIONS(*mod_aip) & ~own_privs) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                errmsg("grant options cannot be granted back to your own grantor")));

    pfree_ext(acl);
}

/*
 * Ensure that no privilege is "abandoned".  A privilege is abandoned
 * if the user that granted the privilege loses the grant option.  (So
 * the chain through which it was granted is broken.)  Either the
 * abandoned privileges are revoked as well, or an error message is
 * printed, depending on the drop behavior option.
 *
 *	acl: the input ACL list
 *	grantee: the user from whom some grant options have been revoked
 *	revoke_privs: the grant options being revoked
 *	ownerId: Oid of object owner
 *	behavior: RESTRICT or CASCADE behavior for recursive removal
 *
 * The input Acl object is pfree'd if replaced.
 */
static Acl* recursive_revoke(Acl* acl, Oid grantee, AclMode revoke_privs, Oid ownerId, DropBehavior behavior)
{
    AclMode still_has;
    AclItem* aip = NULL;
    int i, num;

    check_acl(acl);

    /* The owner can never truly lose grant options, so short-circuit */
    if (grantee == ownerId)
        return acl;

    bool is_ddl_privileges = ACLMODE_FOR_DDL(revoke_privs);
    /* remove ddl privileges flag from Aclitem */
    revoke_privs = REMOVE_DDL_FLAG(revoke_privs);

    /* The grantee might still have some grant options via another grantor */
    if (is_ddl_privileges) {
        still_has = aclmask(acl, grantee, ownerId, ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(revoke_privs)), ACLMASK_ALL);
    } else {
        still_has = aclmask(acl, grantee, ownerId, ACL_GRANT_OPTION_FOR(revoke_privs), ACLMASK_ALL);
    }
    
    revoke_privs &= ~ACL_OPTION_TO_PRIVS(still_has);
    if (revoke_privs == ACL_NO_RIGHTS)
        return acl;

restart:
    num = ACL_NUM(acl);
    aip = ACL_DAT(acl);
    for (i = 0; i < num; i++) {
        if (aip[i].ai_grantor == grantee &&
            (REMOVE_DDL_FLAG(ACLITEM_GET_PRIVS(aip[i])) & revoke_privs) != 0 &&
            is_ddl_privileges == ACLMODE_FOR_DDL(aip[i].ai_privs)) {
            AclItem mod_acl;
            Acl* new_acl = NULL;

            if (behavior == DROP_RESTRICT)
                ereport(ERROR,
                    (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                        errmsg("dependent privileges exist"),
                        errhint("Use CASCADE to revoke them too.")));

            mod_acl.ai_grantor = grantee;
            mod_acl.ai_grantee = aip[i].ai_grantee;
            ACLITEM_SET_PRIVS_GOPTIONS(mod_acl, revoke_privs, revoke_privs);

            /* add ddl privileges flag to Aclitem */
            if (is_ddl_privileges) {
                mod_acl.ai_privs = ADD_DDL_FLAG(mod_acl.ai_privs);
            }

            new_acl = aclupdate(acl, &mod_acl, ACL_MODECHG_DEL, ownerId, behavior);

            pfree_ext(acl);
            acl = new_acl;

            goto restart;
        }
    }

    return acl;
}
/*
 * When 'how' = ACLMASK_ALL, see if all of a set of privileges are held,
 * return true if result == mask.
 *
 * When 'how' = ACLMASK_ANY, see if any of a set of privileges are held,
 * return true if result != 0.
 */
static bool MaskMatchHow(AclMode mask, AclMode result, AclMaskHow how)
{
    if (how == ACLMASK_ALL) {
        return result == mask;
    } else {
        return result != 0;
    }
}

/*
 * aclmask --- compute bitmask of all privileges held by roleid.
 *
 * When 'how' = ACLMASK_ALL, this simply returns the privilege bits
 * held by the given roleid according to the given ACL list, ANDed
 * with 'mask'.  (The point of passing 'mask' is to let the routine
 * exit early if all privileges of interest have been found.)
 *
 * When 'how' = ACLMASK_ANY, returns as soon as any bit in the mask
 * is known true.  (This lets us exit soonest in cases where the
 * caller is only going to test for zero or nonzero result.)
 *
 * Usage patterns:
 *
 * To see if any of a set of privileges are held:
 *		if (aclmask(acl, roleid, ownerId, privs, ACLMASK_ANY) != 0)
 *
 * To see if all of a set of privileges are held:
 *		if (aclmask(acl, roleid, ownerId, privs, ACLMASK_ALL) == privs)
 *
 * To determine exactly which of a set of privileges are held:
 *		heldprivs = aclmask(acl, roleid, ownerId, privs, ACLMASK_ALL);
 */
AclMode aclmask(const Acl* acl, Oid roleid, Oid ownerId, AclMode mask, AclMaskHow how)
{
    AclMode result;
    AclMode remaining;
    AclItem* aidat = NULL;
    int i, num;

    /*
     * Null ACL should not happen, since caller should have inserted
     * appropriate default
     */
    if (acl == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null ACL")));

    check_acl(acl);
    bool is_ddl_privileges = ACLMODE_FOR_DDL(mask);
    /* remove ddl privileges flag from Aclitem */
    mask = REMOVE_DDL_FLAG(mask);

    /* Quick exit for mask == 0 */
    if (mask == 0)
        return 0;

    result = 0;

    /* Owner always implicitly has all grant options */
    if ((mask & ACLITEM_ALL_GOPTION_BITS) && has_privs_of_role(roleid, ownerId)) {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if (MaskMatchHow(mask, result, how))
            return result;
    }

    num = ACL_NUM(acl);
    aidat = ACL_DAT(acl);

    /*
     * Check privileges granted directly to roleid or to public
     */
    for (i = 0; i < num; i++) {
        AclItem* aidata = &aidat[i];

        if ((aidata->ai_grantee == ACL_ID_PUBLIC || aidata->ai_grantee == roleid) &&
            is_ddl_privileges == ACLMODE_FOR_DDL(aidata->ai_privs)) {
            result |= aidata->ai_privs & mask;
            if (MaskMatchHow(mask, result, how))
                return result;
        }
    }

    /*
     * Check privileges granted indirectly via role memberships. We do this in
     * a separate pass to minimize expensive indirect membership tests.  In
     * particular, it's worth testing whether a given ACL entry grants any
     * privileges still of interest before we perform the has_privs_of_role
     * test.
     */
    remaining = mask & ~result;
    for (i = 0; i < num; i++) {
        AclItem* aidata = &aidat[i];

        if (aidata->ai_grantee == ACL_ID_PUBLIC || aidata->ai_grantee == roleid)
            continue; /* already checked it */

        if ((aidata->ai_privs & remaining) && has_privs_of_role(roleid, aidata->ai_grantee) &&
            is_ddl_privileges == ACLMODE_FOR_DDL(aidata->ai_privs)) {
            result |= aidata->ai_privs & mask;
            if (MaskMatchHow(mask, result, how))
                return result;
            remaining = mask & ~result;
        }
    }

    return result;
}


/*
 * Does member have the privileges of role (directly or indirectly)
 * without thinking about sysadmin.
 */
static bool has_privs_of_role_without_sysadmin(Oid member, Oid role)
{
    /* Fast path for simple case */
    if (member == role)
        return true;

    /* Initial user is part of every role, except independent role. */
    if (member == INITIAL_USER_ID && (!is_role_independent(role)))
        return true;

    /* Find all the roles that member has the privileges of, including  multi-level recursion. */
    return list_member_oid(roles_has_privs_of(member), role);
}


/*
 * aclmask_without_sysadmin --- compute bitmask of all privileges of held by roleid
 * when related to schema dbe_perf, snapshot and pg_catalog.
 */
AclMode aclmask_without_sysadmin(const Acl *acl, Oid roleid, Oid ownerId, AclMode mask, AclMaskHow how)
{
    AclMode result;
    AclMode remaining;
    AclItem* aidat = NULL;
    int i, num;

    if (acl == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null ACL")));
    check_acl(acl);
    bool is_ddl_privileges = ACLMODE_FOR_DDL(mask);
    /* remove ddl privileges flag from Aclitem */
    mask = REMOVE_DDL_FLAG(mask);

    /* Quick exit for mask == 0 */
    if (mask == 0)
        return 0;

    result = 0;

    /* Owner always implicitly has all grant options */
    if ((mask & ACLITEM_ALL_GOPTION_BITS) && has_privs_of_role_without_sysadmin(roleid, ownerId)) {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if (MaskMatchHow(mask, result, how))
            return result;
    }

    num = ACL_NUM(acl);
    aidat = ACL_DAT(acl);

    /* Check privileges granted directly to roleid or to public */
    for (i = 0; i < num; i++) {
        AclItem* aidata = &aidat[i];

        if ((aidata->ai_grantee == ACL_ID_PUBLIC || aidata->ai_grantee == roleid) &&
            is_ddl_privileges == ACLMODE_FOR_DDL(aidata->ai_privs)) {
            result |= aidata->ai_privs & mask;
            if (MaskMatchHow(mask, result, how))
                return result;
        }
    }

    /* Check privileges granted indirectly via role memberships. */
    remaining = mask & ~result;
    for (i = 0; i < num; i++) {
        AclItem* aidata = &aidat[i];

        if (aidata->ai_grantee == ACL_ID_PUBLIC || aidata->ai_grantee == roleid)
            continue; /* already checked it */

        if ((aidata->ai_privs & remaining) && has_privs_of_role_without_sysadmin(roleid, aidata->ai_grantee) && 
            is_ddl_privileges == ACLMODE_FOR_DDL(aidata->ai_privs)) {
            result |= aidata->ai_privs & mask;
            if (MaskMatchHow(mask, result, how))
                return result;
            remaining = mask & ~result;
        }
    }

    return result;
}


/*
 * aclmask_direct --- compute bitmask of all privileges held by roleid.
 *
 * This is exactly like aclmask() except that we consider only privileges
 * held *directly* by roleid, not those inherited via role membership.
 */
static AclMode aclmask_direct(const Acl* acl, Oid roleid, Oid ownerId, AclMode mask, AclMaskHow how)
{
    AclMode result;
    AclItem* aidat = NULL;
    int i, num;

    /*
     * Null ACL should not happen, since caller should have inserted
     * appropriate default
     */
    if (acl == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null ACL")));

    check_acl(acl);
    bool is_ddl_privileges = ACLMODE_FOR_DDL(mask);
    /* remove ddl privileges flag from Aclitem */
    mask = REMOVE_DDL_FLAG(mask);

    /* Quick exit for mask == 0 */
    if (mask == 0)
        return 0;

    result = 0;

    /* Owner always implicitly has all grant options */
    if ((mask & ACLITEM_ALL_GOPTION_BITS) && roleid == ownerId) {
        result = mask & ACLITEM_ALL_GOPTION_BITS;
        if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
            return result;
    }

    num = ACL_NUM(acl);
    aidat = ACL_DAT(acl);

    /*
     * Check privileges granted directly to roleid (and not to public)
     */
    for (i = 0; i < num; i++) {
        AclItem* aidata = &aidat[i];

        if (aidata->ai_grantee == roleid && is_ddl_privileges == ACLMODE_FOR_DDL(aidata->ai_privs)) {
            result |= aidata->ai_privs & mask;
            if ((how == ACLMASK_ALL) ? (result == mask) : (result != 0))
                return result;
        }
    }

    return result;
}

/*
 * aclmembers
 *		Find out all the roleids mentioned in an Acl.
 *		Note that we do not distinguish grantors from grantees.
 *
 * *roleids is set to point to a palloc'd array containing distinct OIDs
 * in sorted order.  The length of the array is the function result.
 */
int aclmembers(const Acl* acl, Oid** roleids)
{
    Oid* list = NULL;
    const AclItem* acldat = NULL;
    int i, j, k;

    if (acl == NULL || ACL_NUM(acl) == 0) {
        *roleids = NULL;
        return 0;
    }

    check_acl(acl);

    /* Allocate the worst-case space requirement */
    list = (Oid*)palloc(ACL_NUM(acl) * 2 * sizeof(Oid));
    acldat = ACL_DAT(acl);

    /*
     * Walk the ACL collecting mentioned RoleIds.
     */
    j = 0;
    for (i = 0; i < ACL_NUM(acl); i++) {
        const AclItem* ai = &acldat[i];

        if (ai->ai_grantee != ACL_ID_PUBLIC)
            list[j++] = ai->ai_grantee;
        /* grantor is currently never PUBLIC, but let's check anyway */
        if (ai->ai_grantor != ACL_ID_PUBLIC)
            list[j++] = ai->ai_grantor;
    }

    /* Sort the array */
    qsort(list, j, sizeof(Oid), oidComparator);

    /* Remove duplicates from the array */
    k = 0;
    for (i = 1; i < j; i++) {
        if (list[k] != list[i])
            list[++k] = list[i];
    }

    /*
     * We could repalloc the array down to minimum size, but it's hardly worth
     * it since it's only transient memory.
     */
    *roleids = list;

    return k + 1;
}

/*
 * oidComparator
 *		qsort comparison function for Oids
 */
static int oidComparator(const void* arg1, const void* arg2)
{
    Oid oid1 = *(const Oid*)arg1;
    Oid oid2 = *(const Oid*)arg2;

    if (oid1 > oid2)
        return 1;
    if (oid1 < oid2)
        return -1;
    return 0;
}

/*
 * aclinsert (exported function)
 */
Datum aclinsert(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("aclinsert is no longer supported")));

    PG_RETURN_NULL(); /* keep compiler quiet */
}

Datum aclremove(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("aclremove is no longer supported")));

    PG_RETURN_NULL(); /* keep compiler quiet */
}

Datum aclcontains(PG_FUNCTION_ARGS)
{
    Acl* acl = PG_GETARG_ACL_P(0);
    AclItem* aip = PG_GETARG_ACLITEM_P(1);
    AclItem* aidat = NULL;
    int i, num;

    check_acl(acl);
    num = ACL_NUM(acl);
    aidat = ACL_DAT(acl);
    for (i = 0; i < num; ++i) {
        if (aip->ai_grantee == aidat[i].ai_grantee && aip->ai_grantor == aidat[i].ai_grantor &&
            (ACLITEM_GET_RIGHTS(*aip) & ACLITEM_GET_RIGHTS(aidat[i])) == ACLITEM_GET_RIGHTS(*aip))
            PG_RETURN_BOOL(true);
    }
    PG_RETURN_BOOL(false);
}

Datum makeaclitem(PG_FUNCTION_ARGS)
{
    Oid grantee = PG_GETARG_OID(0);
    Oid grantor = PG_GETARG_OID(1);
    text* privtext = PG_GETARG_TEXT_P(2);
    bool goption = PG_GETARG_BOOL(3);
    AclItem* result = NULL;
    AclMode priv;

    priv = convert_priv_string(privtext);

    result = (AclItem*)palloc(sizeof(AclItem));

    result->ai_grantee = grantee;
    result->ai_grantor = grantor;

    ACLITEM_SET_PRIVS_GOPTIONS(*result, priv, (goption ? priv : ACL_NO_RIGHTS));

    PG_RETURN_ACLITEM_P(result);
}

static AclMode convert_priv_string(text* priv_type_text)
{
    priv_map priv_map[] = {
        {"SELECT", ACL_SELECT}, {"INSERT", ACL_INSERT}, {"UPDATE", ACL_UPDATE},
        {"DELETE", ACL_DELETE}, {"TRUNCATE", ACL_TRUNCATE}, {"REFERENCES", ACL_REFERENCES}, {"TRIGGER", ACL_TRIGGER},
        {"EXECUTE", ACL_EXECUTE}, {"USAGE", ACL_USAGE}, {"CREATE", ACL_CREATE}, {"TEMP", ACL_CREATE_TEMP},
        {"TEMPORARY", ACL_CREATE_TEMP}, {"CONNECT", ACL_CONNECT}, {"COMPUTE", ACL_COMPUTE}, {"READ", ACL_READ},
        {"WRITE", ACL_WRITE}, {"ALTER", ACL_ALTER}, {"DROP", ACL_DROP}, {"COMMENT", ACL_COMMENT}, {"INDEX", ACL_INDEX},
        {"VACUUM", ACL_VACUUM}, {"RULE", 0} /* ignore old RULE privileges */
    };

    char* priv_type = text_to_cstring(priv_type_text);
    for (unsigned int i = 0; i < lengthof(priv_map); i++) {
        if (pg_strcasecmp(priv_type, priv_map[i].name) == 0) {
            return priv_map[i].value;
        }
    }

    ereport(
        ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unrecognized privilege type: \"%s\"", priv_type)));
    return ACL_NO_RIGHTS; /* keep compiler quiet */
}

/*
 * convert_any_priv_string: recognize privilege strings for has_foo_privilege
 *
 * We accept a comma-separated list of case-insensitive privilege names,
 * producing a bitmask of the OR'd privilege bits.  We are liberal about
 * whitespace between items, not so much about whitespace within items.
 * The allowed privilege names are given as an array of priv_map structs,
 * terminated by one with a NULL name pointer.
 */
static AclMode convert_any_priv_string(text* priv_type_text, const priv_map* privileges)
{
    AclMode result = 0;
    char* priv_type = text_to_cstring(priv_type_text);
    char* chunk = NULL;
    char* next_chunk = NULL;

    /* We rely on priv_type being a private, modifiable string */
    for (chunk = priv_type; chunk; chunk = next_chunk) {
        int chunk_len;
        const priv_map* this_priv = NULL;

        /* Split string at commas */
        next_chunk = strchr(chunk, ',');
        if (next_chunk != NULL)
            *next_chunk++ = '\0';

        /* Drop leading/trailing whitespace in this chunk */
        while (*chunk != 0 && isspace((unsigned char)*chunk)) {
            chunk++;
        }
        chunk_len = strlen(chunk);
        while (chunk_len > 0 && isspace((unsigned char)chunk[chunk_len - 1])) {
            chunk_len--;
        }
        chunk[chunk_len] = '\0';

        /* Match to the privileges list */
        for (this_priv = privileges; this_priv->name; this_priv++) {
            if (pg_strcasecmp(this_priv->name, chunk) == 0) {
                result |= this_priv->value;
                break;
            }
        }
        if (this_priv->name == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unrecognized privilege type: \"%s\"", chunk)));
    }

    pfree_ext(priv_type);
    return result;
}

static const char* convert_aclright_to_string(AclMode aclright)
{
    priv_map priv_map[] = {
        {"INSERT", ACL_INSERT}, {"SELECT", ACL_SELECT}, {"UPDATE", ACL_UPDATE},
        {"DELETE", ACL_DELETE}, {"TRUNCATE", ACL_TRUNCATE}, {"REFERENCES", ACL_REFERENCES}, {"TRIGGER", ACL_TRIGGER},
        {"EXECUTE", ACL_EXECUTE}, {"USAGE", ACL_USAGE}, {"CREATE", ACL_CREATE}, {"TEMPORARY", ACL_CREATE_TEMP},
        {"CONNECT", ACL_CONNECT}, {"COMPUTE", ACL_COMPUTE}, {"READ", ACL_READ}, {"WRITE", ACL_WRITE},
        {"ALTER", ACL_ALTER}, {"DROP", ACL_DROP}, {"COMMENT", ACL_COMMENT}, {"INDEX", ACL_INDEX}, {"VACUUM", ACL_VACUUM}
    };

    for (unsigned int i = 0; i < lengthof(priv_map); i++) {
        if (aclright == priv_map[i].value) {
            return priv_map[i].name;
        }
    }

    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized aclright: %d", aclright)));
    return NULL;
}

/* ----------
 * Convert an aclitem[] to a table.
 *
 * Example:
 *
 * aclexplode('{=r/joe,foo=a*w/joe}'::aclitem[])
 *
 * returns the table
 *
 * {{ OID(joe), 0::OID,   'SELECT', false },
 *	{ OID(joe), OID(foo), 'INSERT', true },
 *	{ OID(joe), OID(foo), 'UPDATE', false }}
 * ----------
 */
Datum aclexplode(PG_FUNCTION_ARGS)
{
    Acl* acl = PG_GETARG_ACL_P(0);
    FuncCallContext* funcctx = NULL;
    int* idx = NULL;
    AclItem* aidat = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        check_acl(acl);

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /*
         * build tupdesc for result tuples (matches out parameters in pg_proc
         * entry)
         */
        tupdesc = CreateTemplateTupleDesc(4, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "grantor", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "grantee", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "privilege_type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "is_grantable", BOOLOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* allocate memory for user context */
        idx = (int*)palloc(sizeof(int[2]));
        idx[0] = 0;  /* ACL array item index */
        idx[1] = -1; /* privilege type counter */
        funcctx->user_fctx = (void*)idx;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    idx = (int*)funcctx->user_fctx;
    aidat = ACL_DAT(acl);

    /* need test here in case acl has no items */
    while (idx[0] < ACL_NUM(acl)) {
        AclItem* aidata = NULL;
        AclMode priv_bit;
        errno_t ss_rc = 0;

        idx[1]++;
        if (idx[1] == N_ACL_RIGHTS) {
            idx[1] = 0;
            idx[0]++;
            if (idx[0] >= ACL_NUM(acl)) /* done */
                break;
        }
        aidata = &aidat[idx[0]];
        priv_bit = 1 << idx[1];

        if (ACLITEM_GET_PRIVS(*aidata) & priv_bit) {
            Datum result;
            Datum values[4];
            bool nulls[4] = {false};
            HeapTuple tuple = NULL;

            values[0] = ObjectIdGetDatum(aidata->ai_grantor);
            values[1] = ObjectIdGetDatum(aidata->ai_grantee);

            if (ACLMODE_FOR_DDL(ACLITEM_GET_PRIVS(*aidata))) {
                priv_bit = ADD_DDL_FLAG(priv_bit);
            }
            values[2] = CStringGetTextDatum(convert_aclright_to_string(priv_bit));
            values[3] = BoolGetDatum((ACLITEM_GET_GOPTIONS(*aidata) & REMOVE_DDL_FLAG(priv_bit)) != 0);

            ss_rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
            securec_check(ss_rc, "\0", "\0");

            tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
            result = HeapTupleGetDatum(tuple);

            SRF_RETURN_NEXT(funcctx, result);
        }
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * has_table_privilege variants
 *		These are all named "has_table_privilege" at the SQL level.
 *		They take various combinations of relation name, relation OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not.  The variants that take a relation OID
 *		return NULL if the OID doesn't exist (rather than failing, as
 *		they did before Postgres 8.4).
 */

/*
 * has_table_privilege_name_name
 *		Check user privileges on a table given
 *		name username, text tablename, and text priv name.
 */
Datum has_table_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name rolename = PG_GETARG_NAME(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid tableoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*rolename));
    tableoid = convert_table_name(tablename);
    mode = convert_table_priv_string(priv_type_text);

    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_table_privilege_name
 *		Check user privileges on a table given
 *		text tablename and text priv name.
 *		current_user is assumed
 */
Datum has_table_privilege_name(PG_FUNCTION_ARGS)
{
    text* tablename = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid tableoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    tableoid = convert_table_name(tablename);
    mode = convert_table_priv_string(priv_type_text);

    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_table_privilege_name_id
 *		Check user privileges on a table given
 *		name usename, table oid, and text priv name.
 */
Datum has_table_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid tableoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_table_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tableoid)))
        PG_RETURN_NULL();

    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_table_privilege_id
 *		Check user privileges on a table given
 *		table oid, and text priv name.
 *		current_user is assumed
 */
Datum has_table_privilege_id(PG_FUNCTION_ARGS)
{
    Oid tableoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_table_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tableoid)))
        PG_RETURN_NULL();

    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_table_privilege_id_name
 *		Check user privileges on a table given
 *		roleid, text tablename, and text priv name.
 */
Datum has_table_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid tableoid;
    AclMode mode;
    AclResult aclresult;

    tableoid = convert_table_name(tablename);
    mode = convert_table_priv_string(priv_type_text);

    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_table_privilege_id_id
 *		Check user privileges on a table given
 *		roleid, table oid, and text priv name.
 */
Datum has_table_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid tableoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_table_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tableoid)))
        PG_RETURN_NULL();

    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_table_privilege family.
 */

/*
 * Given a table name expressed as a string, look it up and return Oid
 */
static Oid convert_table_name(text* tablename)
{
    RangeVar* relrv = NULL;

    relrv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));

    /* We might not even have permissions on this relation; don't lock it. */
    return RangeVarGetRelid(relrv, NoLock, false);
}

/*
 * convert_table_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_table_priv_string(text* priv_type_text)
{
    static const priv_map table_priv_map[] = {{"SELECT", ACL_SELECT},
        {"SELECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_SELECT)},
        {"INSERT", ACL_INSERT},
        {"INSERT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_INSERT)},
        {"UPDATE", ACL_UPDATE},
        {"UPDATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_UPDATE)},
        {"DELETE", ACL_DELETE},
        {"DELETE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_DELETE)},
        {"TRUNCATE", ACL_TRUNCATE},
        {"TRUNCATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_TRUNCATE)},
        {"REFERENCES", ACL_REFERENCES},
        {"REFERENCES WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_REFERENCES)},
        {"TRIGGER", ACL_TRIGGER},
        {"TRIGGER WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_TRIGGER)},
        {"RULE", 0}, /* ignore old RULE privileges */
        {"RULE WITH GRANT OPTION", 0},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {"INDEX", ACL_INDEX},
        {"INDEX WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_INDEX)))},
        {"VACUUM", ACL_VACUUM},
        {"VACUUM WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_VACUUM)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, table_priv_map);
}

/*
 * has_sequence_privilege variants
 *		These are all named "has_sequence_privilege" at the SQL level.
 *		They take various combinations of relation name, relation OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not.  The variants that take a relation OID
 *		return NULL if the OID doesn't exist.
 */

/*
 * has_sequence_privilege_name_name
 *		Check user privileges on a sequence given
 *		name username, text sequencename, and text priv name.
 */
Datum has_sequence_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name rolename = PG_GETARG_NAME(0);
    text* sequencename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid sequenceoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*rolename));
    mode = convert_sequence_priv_string(priv_type_text);
    sequenceoid = convert_table_name(sequencename);
    if (!(RELKIND_IS_SEQUENCE(get_rel_relkind(sequenceoid))))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a (large) sequence",
                text_to_cstring(sequencename))));

    aclresult = pg_class_aclcheck(sequenceoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_sequence_privilege_name
 *		Check user privileges on a sequence given
 *		text sequencename and text priv name.
 *		current_user is assumed
 */
Datum has_sequence_privilege_name(PG_FUNCTION_ARGS)
{
    text* sequencename = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid sequenceoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_sequence_priv_string(priv_type_text);
    sequenceoid = convert_table_name(sequencename);
    if (!RELKIND_IS_SEQUENCE(get_rel_relkind(sequenceoid)))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a (large) sequence",
                text_to_cstring(sequencename))));

    aclresult = pg_class_aclcheck(sequenceoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_sequence_privilege_name_id
 *		Check user privileges on a sequence given
 *		name usename, sequence oid, and text priv name.
 */
Datum has_sequence_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid sequenceoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;
    char relkind;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_sequence_priv_string(priv_type_text);
    relkind = get_rel_relkind(sequenceoid);
    if (relkind == '\0')
        PG_RETURN_NULL();
    else if (!RELKIND_IS_SEQUENCE(relkind))
        ereport(
            ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a (large) sequence",
                get_rel_name(sequenceoid))));

    aclresult = pg_class_aclcheck(sequenceoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_sequence_privilege_id
 *		Check user privileges on a sequence given
 *		sequence oid, and text priv name.
 *		current_user is assumed
 */
Datum has_sequence_privilege_id(PG_FUNCTION_ARGS)
{
    Oid sequenceoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;
    char relkind;

    roleid = GetUserId();
    mode = convert_sequence_priv_string(priv_type_text);
    relkind = get_rel_relkind(sequenceoid);
    if (relkind == '\0')
        PG_RETURN_NULL();
    else if (!RELKIND_IS_SEQUENCE(relkind))
        ereport(
            ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a (large) sequence",
                get_rel_name(sequenceoid))));

    aclresult = pg_class_aclcheck(sequenceoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_sequence_privilege_id_name
 *		Check user privileges on a sequence given
 *		roleid, text sequencename, and text priv name.
 */
Datum has_sequence_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* sequencename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid sequenceoid;
    AclMode mode;
    AclResult aclresult;

    mode = convert_sequence_priv_string(priv_type_text);
    sequenceoid = convert_table_name(sequencename);
    if (!RELKIND_IS_SEQUENCE(get_rel_relkind(sequenceoid)))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a (large) sequence",
                text_to_cstring(sequencename))));

    aclresult = pg_class_aclcheck(sequenceoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_sequence_privilege_id_id
 *		Check user privileges on a sequence given
 *		roleid, sequence oid, and text priv name.
 */
Datum has_sequence_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid sequenceoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;
    char relkind;

    mode = convert_sequence_priv_string(priv_type_text);
    relkind = get_rel_relkind(sequenceoid);
    if (relkind == '\0')
        PG_RETURN_NULL();
    else if (!RELKIND_IS_SEQUENCE(relkind))
        ereport(
            ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a (large) sequence",
                get_rel_name(sequenceoid))));

    aclresult = pg_class_aclcheck(sequenceoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * convert_sequence_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_sequence_priv_string(text* priv_type_text)
{
    static const priv_map sequence_priv_map[] = {{"USAGE", ACL_USAGE},
        {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {"SELECT", ACL_SELECT},
        {"SELECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_SELECT)},
        {"UPDATE", ACL_UPDATE},
        {"UPDATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_UPDATE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, sequence_priv_map);
}

/*
 * has_any_column_privilege variants
 *		These are all named "has_any_column_privilege" at the SQL level.
 *		They take various combinations of relation name, relation OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege for any column of the table, false if not.  The variants
 *		that take a relation OID return NULL if the OID doesn't exist.
 */

/*
 * has_any_column_privilege_name_name
 *		Check user privileges on any column of a table given
 *		name username, text tablename, and text priv name.
 */
Datum has_any_column_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name rolename = PG_GETARG_NAME(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid tableoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*rolename));
    tableoid = convert_table_name(tablename);
    mode = convert_column_priv_string(priv_type_text);

    /* First check at table level, then examine each column if needed */
    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);
    if (aclresult != ACLCHECK_OK)
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_any_column_privilege_name
 *		Check user privileges on any column of a table given
 *		text tablename and text priv name.
 *		current_user is assumed
 */
Datum has_any_column_privilege_name(PG_FUNCTION_ARGS)
{
    text* tablename = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid tableoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    tableoid = convert_table_name(tablename);
    mode = convert_column_priv_string(priv_type_text);

    /* First check at table level, then examine each column if needed */
    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);
    if (aclresult != ACLCHECK_OK)
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_any_column_privilege_name_id
 *		Check user privileges on any column of a table given
 *		name usename, table oid, and text priv name.
 */
Datum has_any_column_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid tableoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_column_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tableoid)))
        PG_RETURN_NULL();

    /* First check at table level, then examine each column if needed */
    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);
    if (aclresult != ACLCHECK_OK)
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_any_column_privilege_id
 *		Check user privileges on any column of a table given
 *		table oid, and text priv name.
 *		current_user is assumed
 */
Datum has_any_column_privilege_id(PG_FUNCTION_ARGS)
{
    Oid tableoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_column_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tableoid)))
        PG_RETURN_NULL();

    /* First check at table level, then examine each column if needed */
    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);
    if (aclresult != ACLCHECK_OK)
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_any_column_privilege_id_name
 *		Check user privileges on any column of a table given
 *		roleid, text tablename, and text priv name.
 */
Datum has_any_column_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid tableoid;
    AclMode mode;
    AclResult aclresult;

    tableoid = convert_table_name(tablename);
    mode = convert_column_priv_string(priv_type_text);

    /* First check at table level, then examine each column if needed */
    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);
    if (aclresult != ACLCHECK_OK)
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_any_column_privilege_id_id
 *		Check user privileges on any column of a table given
 *		roleid, table oid, and text priv name.
 */
Datum has_any_column_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid tableoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_column_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tableoid)))
        PG_RETURN_NULL();

    /* First check at table level, then examine each column if needed */
    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);
    if (aclresult != ACLCHECK_OK)
        aclresult = pg_attribute_aclcheck_all(tableoid, roleid, mode, ACLMASK_ANY);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_column_privilege variants
 *		These are all named "has_column_privilege" at the SQL level.
 *		They take various combinations of relation name, relation OID,
 *		column name, column attnum, user name, user OID, or
 *		implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not.  The variants that take a relation OID
 *		and an integer attnum return NULL (rather than throwing an error)
 *		if the column doesn't exist or is dropped.
 */

/*
 * column_privilege_check: check column privileges, but don't throw an error
 *		for dropped column or table
 *
 * Returns 1 if have the privilege, 0 if not, -1 if dropped column/table.
 */
static int column_privilege_check(Oid tableoid, AttrNumber attnum, Oid roleid, AclMode mode)
{
    AclResult aclresult;
    HeapTuple attTuple = NULL;
    Form_pg_attribute attributeForm;

    /*
     * First check if we have the privilege at the table level.  We check
     * existence of the pg_class row before risking calling pg_class_aclcheck.
     * Note: it might seem there's a race condition against concurrent DROP,
     * but really it's safe because there will be no syscache flush between
     * here and there.	So if we see the row in the syscache, so will
     * pg_class_aclcheck.
     */
    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tableoid)))
        return -1;

    aclresult = pg_class_aclcheck(tableoid, roleid, mode, false);

    if (aclresult == ACLCHECK_OK)
        return true;

    /*
     * No table privilege, so try per-column privileges.  Again, we have to
     * check for dropped attribute first, and we rely on the syscache not to
     * notice a concurrent drop before pg_attribute_aclcheck fetches the row.
     */
    attTuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(tableoid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(attTuple))
        return -1;
    attributeForm = (Form_pg_attribute)GETSTRUCT(attTuple);
    if (attributeForm->attisdropped) {
        ReleaseSysCache(attTuple);
        return -1;
    }
    ReleaseSysCache(attTuple);

    aclresult = pg_attribute_aclcheck(tableoid, attnum, roleid, mode);

    return (aclresult == ACLCHECK_OK);
}

/*
 * has_column_privilege_name_name_name
 *		Check user privileges on a column given
 *		name username, text tablename, text colname, and text priv name.
 */
Datum has_column_privilege_name_name_name(PG_FUNCTION_ARGS)
{
    Name rolename = PG_GETARG_NAME(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    text* column = PG_GETARG_TEXT_P(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    Oid roleid;
    Oid tableoid;
    AttrNumber colattnum;
    AclMode mode;
    int privresult;

    roleid = get_role_oid_or_public(NameStr(*rolename));
    tableoid = convert_table_name(tablename);
    colattnum = convert_column_name(tableoid, column);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_name_name_attnum
 *		Check user privileges on a column given
 *		name username, text tablename, int attnum, and text priv name.
 */
Datum has_column_privilege_name_name_attnum(PG_FUNCTION_ARGS)
{
    Name rolename = PG_GETARG_NAME(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    AttrNumber colattnum = PG_GETARG_INT16(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    Oid roleid;
    Oid tableoid;
    AclMode mode;
    int privresult;

    roleid = get_role_oid_or_public(NameStr(*rolename));
    tableoid = convert_table_name(tablename);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_name_id_name
 *		Check user privileges on a column given
 *		name username, table oid, text colname, and text priv name.
 */
Datum has_column_privilege_name_id_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid tableoid = PG_GETARG_OID(1);
    text* column = PG_GETARG_TEXT_P(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    Oid roleid;
    AttrNumber colattnum;
    AclMode mode;
    int privresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    colattnum = convert_column_name(tableoid, column);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_name_id_attnum
 *		Check user privileges on a column given
 *		name username, table oid, int attnum, and text priv name.
 */
Datum has_column_privilege_name_id_attnum(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid tableoid = PG_GETARG_OID(1);
    AttrNumber colattnum = PG_GETARG_INT16(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    Oid roleid;
    AclMode mode;
    int privresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_id_name_name
 *		Check user privileges on a column given
 *		oid roleid, text tablename, text colname, and text priv name.
 */
Datum has_column_privilege_id_name_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    text* column = PG_GETARG_TEXT_P(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    Oid tableoid;
    AttrNumber colattnum;
    AclMode mode;
    int privresult;

    tableoid = convert_table_name(tablename);
    colattnum = convert_column_name(tableoid, column);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_id_name_attnum
 *		Check user privileges on a column given
 *		oid roleid, text tablename, int attnum, and text priv name.
 */
Datum has_column_privilege_id_name_attnum(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* tablename = PG_GETARG_TEXT_P(1);
    AttrNumber colattnum = PG_GETARG_INT16(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    Oid tableoid;
    AclMode mode;
    int privresult;

    tableoid = convert_table_name(tablename);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_id_id_name
 *		Check user privileges on a column given
 *		oid roleid, table oid, text colname, and text priv name.
 */
Datum has_column_privilege_id_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid tableoid = PG_GETARG_OID(1);
    text* column = PG_GETARG_TEXT_P(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    AttrNumber colattnum;
    AclMode mode;
    int privresult;

    colattnum = convert_column_name(tableoid, column);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_id_id_attnum
 *		Check user privileges on a column given
 *		oid roleid, table oid, int attnum, and text priv name.
 */
Datum has_column_privilege_id_id_attnum(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid tableoid = PG_GETARG_OID(1);
    AttrNumber colattnum = PG_GETARG_INT16(2);
    text* priv_type_text = PG_GETARG_TEXT_P(3);
    AclMode mode;
    int privresult;

    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_name_name
 *		Check user privileges on a column given
 *		text tablename, text colname, and text priv name.
 *		current_user is assumed
 */
Datum has_column_privilege_name_name(PG_FUNCTION_ARGS)
{
    text* tablename = PG_GETARG_TEXT_P(0);
    text* column = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid tableoid;
    AttrNumber colattnum;
    AclMode mode;
    int privresult;

    roleid = GetUserId();
    tableoid = convert_table_name(tablename);
    colattnum = convert_column_name(tableoid, column);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_name_attnum
 *		Check user privileges on a column given
 *		text tablename, int attnum, and text priv name.
 *		current_user is assumed
 */
Datum has_column_privilege_name_attnum(PG_FUNCTION_ARGS)
{
    text* tablename = PG_GETARG_TEXT_P(0);
    AttrNumber colattnum = PG_GETARG_INT16(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid tableoid;
    AclMode mode;
    int privresult;

    roleid = GetUserId();
    tableoid = convert_table_name(tablename);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_id_name
 *		Check user privileges on a column given
 *		table oid, text colname, and text priv name.
 *		current_user is assumed
 */
Datum has_column_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid tableoid = PG_GETARG_OID(0);
    text* column = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AttrNumber colattnum;
    AclMode mode;
    int privresult;

    roleid = GetUserId();
    colattnum = convert_column_name(tableoid, column);
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 * has_column_privilege_id_attnum
 *		Check user privileges on a column given
 *		table oid, int attnum, and text priv name.
 *		current_user is assumed
 */
Datum has_column_privilege_id_attnum(PG_FUNCTION_ARGS)
{
    Oid tableoid = PG_GETARG_OID(0);
    AttrNumber colattnum = PG_GETARG_INT16(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    int privresult;

    roleid = GetUserId();
    mode = convert_column_priv_string(priv_type_text);

    privresult = column_privilege_check(tableoid, colattnum, roleid, mode);
    if (privresult < 0)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(privresult);
}

/*
 *		Support routines for has_column_privilege family.
 */

/*
 * Given a table OID and a column name expressed as a string, look it up
 * and return the column number
 */
static AttrNumber convert_column_name(Oid tableoid, text* column)
{
    AttrNumber attnum;
    char* colname = NULL;

    colname = text_to_cstring(column);
    attnum = get_attnum(tableoid, colname);
    if (attnum == InvalidAttrNumber)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("column \"%s\" of relation \"%s\" does not exist", colname, get_rel_name(tableoid))));
    pfree_ext(colname);
    return attnum;
}

/*
 * convert_column_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_column_priv_string(text* priv_type_text)
{
    static const priv_map column_priv_map[] = {{"SELECT", ACL_SELECT},
        {"SELECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_SELECT)},
        {"INSERT", ACL_INSERT},
        {"INSERT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_INSERT)},
        {"UPDATE", ACL_UPDATE},
        {"UPDATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_UPDATE)},
        {"REFERENCES", ACL_REFERENCES},
        {"REFERENCES WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_REFERENCES)},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, column_priv_map);
}

/*
 * has_database_privilege variants
 *		These are all named "has_database_privilege" at the SQL level.
 *		They take various combinations of database name, database OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not, or NULL if object doesn't exist.
 */

/*
 * has_database_privilege_name_name
 *		Check user privileges on a database given
 *		name username, text databasename, and text priv name.
 */
Datum has_database_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* databasename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid databaseoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    databaseoid = convert_database_name(databasename);
    mode = convert_database_priv_string(priv_type_text);

    aclresult = pg_database_aclcheck(databaseoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_database_privilege_name
 *		Check user privileges on a database given
 *		text databasename and text priv name.
 *		current_user is assumed
 */
Datum has_database_privilege_name(PG_FUNCTION_ARGS)
{
    text* databasename = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid databaseoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    databaseoid = convert_database_name(databasename);
    mode = convert_database_priv_string(priv_type_text);

    aclresult = pg_database_aclcheck(databaseoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_database_privilege_name_id
 *		Check user privileges on a database given
 *		name usename, database oid, and text priv name.
 */
Datum has_database_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid databaseoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_database_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(databaseoid)))
        PG_RETURN_NULL();

    aclresult = pg_database_aclcheck(databaseoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_database_privilege_id
 *		Check user privileges on a database given
 *		database oid, and text priv name.
 *		current_user is assumed
 */
Datum has_database_privilege_id(PG_FUNCTION_ARGS)
{
    Oid databaseoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_database_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(databaseoid)))
        PG_RETURN_NULL();

    aclresult = pg_database_aclcheck(databaseoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_database_privilege_id_name
 *		Check user privileges on a database given
 *		roleid, text databasename, and text priv name.
 */
Datum has_database_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* databasename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid databaseoid;
    AclMode mode;
    AclResult aclresult;

    databaseoid = convert_database_name(databasename);
    mode = convert_database_priv_string(priv_type_text);

    aclresult = pg_database_aclcheck(databaseoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_database_privilege_id_id
 *		Check user privileges on a database given
 *		roleid, database oid, and text priv name.
 */
Datum has_database_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid databaseoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_database_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(databaseoid)))
        PG_RETURN_NULL();

    aclresult = pg_database_aclcheck(databaseoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_database_privilege family.
 */

/*
 * Given a database name expressed as a string, look it up and return Oid
 */
static Oid convert_database_name(text* databasename)
{
    char* dbname = text_to_cstring(databasename);

    return get_database_oid(dbname, false);
}

/*
 * convert_database_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_database_priv_string(text* priv_type_text)
{
    static const priv_map database_priv_map[] = {{"CREATE", ACL_CREATE},
        {"CREATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {"TEMPORARY", ACL_CREATE_TEMP},
        {"TEMPORARY WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE_TEMP)},
        {"TEMP", ACL_CREATE_TEMP},
        {"TEMP WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE_TEMP)},
        {"CONNECT", ACL_CONNECT},
        {"CONNECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CONNECT)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, database_priv_map);
}

/*
 * has_foreign_data_wrapper_privilege variants
 *		These are all named "has_foreign_data_wrapper_privilege" at the SQL level.
 *		They take various combinations of foreign-data wrapper name,
 *		fdw OID, user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not.
 */

/*
 * has_foreign_data_wrapper_privilege_name_name
 *		Check user privileges on a foreign-data wrapper given
 *		name username, text fdwname, and text priv name.
 */
Datum has_foreign_data_wrapper_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* fdwname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid fdwid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    fdwid = convert_foreign_data_wrapper_name(fdwname);
    mode = convert_foreign_data_wrapper_priv_string(priv_type_text);

    aclresult = pg_foreign_data_wrapper_aclcheck(fdwid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_foreign_data_wrapper_privilege_name
 *		Check user privileges on a foreign-data wrapper given
 *		text fdwname and text priv name.
 *		current_user is assumed
 */
Datum has_foreign_data_wrapper_privilege_name(PG_FUNCTION_ARGS)
{
    text* fdwname = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid fdwid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    fdwid = convert_foreign_data_wrapper_name(fdwname);
    mode = convert_foreign_data_wrapper_priv_string(priv_type_text);

    aclresult = pg_foreign_data_wrapper_aclcheck(fdwid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_foreign_data_wrapper_privilege_name_id
 *		Check user privileges on a foreign-data wrapper given
 *		name usename, foreign-data wrapper oid, and text priv name.
 */
Datum has_foreign_data_wrapper_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid fdwid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_foreign_data_wrapper_priv_string(priv_type_text);

    aclresult = pg_foreign_data_wrapper_aclcheck(fdwid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_foreign_data_wrapper_privilege_id
 *		Check user privileges on a foreign-data wrapper given
 *		foreign-data wrapper oid, and text priv name.
 *		current_user is assumed
 */
Datum has_foreign_data_wrapper_privilege_id(PG_FUNCTION_ARGS)
{
    Oid fdwid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_foreign_data_wrapper_priv_string(priv_type_text);

    aclresult = pg_foreign_data_wrapper_aclcheck(fdwid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_foreign_data_wrapper_privilege_id_name
 *		Check user privileges on a foreign-data wrapper given
 *		roleid, text fdwname, and text priv name.
 */
Datum has_foreign_data_wrapper_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* fdwname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid fdwid;
    AclMode mode;
    AclResult aclresult;

    fdwid = convert_foreign_data_wrapper_name(fdwname);
    mode = convert_foreign_data_wrapper_priv_string(priv_type_text);

    aclresult = pg_foreign_data_wrapper_aclcheck(fdwid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_foreign_data_wrapper_privilege_id_id
 *		Check user privileges on a foreign-data wrapper given
 *		roleid, fdw oid, and text priv name.
 */
Datum has_foreign_data_wrapper_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid fdwid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_foreign_data_wrapper_priv_string(priv_type_text);

    aclresult = pg_foreign_data_wrapper_aclcheck(fdwid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_foreign_data_wrapper_privilege family.
 */

/*
 * Given a FDW name expressed as a string, look it up and return Oid
 */
static Oid convert_foreign_data_wrapper_name(text* fdwname)
{
    char* fdwstr = text_to_cstring(fdwname);

    return get_foreign_data_wrapper_oid(fdwstr, false);
}

/*
 * convert_foreign_data_wrapper_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_foreign_data_wrapper_priv_string(text* priv_type_text)
{
    static const priv_map foreign_data_wrapper_priv_map[] = {
        {"USAGE", ACL_USAGE}, {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)}, {NULL, 0}};

    return convert_any_priv_string(priv_type_text, foreign_data_wrapper_priv_map);
}

/*
 * has_function_privilege variants
 *		These are all named "has_function_privilege" at the SQL level.
 *		They take various combinations of function name, function OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not, or NULL if object doesn't exist.
 */

/*
 * has_function_privilege_name_name
 *		Check user privileges on a function given
 *		name username, text functionname, and text priv name.
 */
Datum has_function_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* functionname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid functionoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    functionoid = convert_function_name(functionname);
    mode = convert_function_priv_string(priv_type_text);

    aclresult = pg_proc_aclcheck(functionoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_function_privilege_name
 *		Check user privileges on a function given
 *		text functionname and text priv name.
 *		current_user is assumed
 */
Datum has_function_privilege_name(PG_FUNCTION_ARGS)
{
    text* functionname = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid functionoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    functionoid = convert_function_name(functionname);
    mode = convert_function_priv_string(priv_type_text);

    aclresult = pg_proc_aclcheck(functionoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_function_privilege_name_id
 *		Check user privileges on a function given
 *		name usename, function oid, and text priv name.
 */
Datum has_function_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid functionoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_function_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(functionoid)))
        PG_RETURN_NULL();

    aclresult = pg_proc_aclcheck(functionoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_function_privilege_id
 *		Check user privileges on a function given
 *		function oid, and text priv name.
 *		current_user is assumed
 */
Datum has_function_privilege_id(PG_FUNCTION_ARGS)
{
    Oid functionoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_function_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(functionoid)))
        PG_RETURN_NULL();

    aclresult = pg_proc_aclcheck(functionoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_function_privilege_id_name
 *		Check user privileges on a function given
 *		roleid, text functionname, and text priv name.
 */
Datum has_function_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* functionname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid functionoid;
    AclMode mode;
    AclResult aclresult;

    functionoid = convert_function_name(functionname);
    mode = convert_function_priv_string(priv_type_text);

    aclresult = pg_proc_aclcheck(functionoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_function_privilege_id_id
 *		Check user privileges on a function given
 *		roleid, function oid, and text priv name.
 */
Datum has_function_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid functionoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_function_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(functionoid)))
        PG_RETURN_NULL();

    aclresult = pg_proc_aclcheck(functionoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_function_privilege family.
 */

/*
 * Given a function name expressed as a string, look it up and return Oid
 */
static Oid convert_function_name(text* functionname)
{
    char* funcname = text_to_cstring(functionname);
    Oid oid;

    oid = DatumGetObjectId(DirectFunctionCall1(regprocedurein, CStringGetDatum(funcname)));

    if (!OidIsValid(oid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("function \"%s\" does not exist", funcname)));

    return oid;
}

/*
 * convert_function_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_function_priv_string(text* priv_type_text)
{
    static const priv_map function_priv_map[] = {
        {"EXECUTE", ACL_EXECUTE}, {"EXECUTE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_EXECUTE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, function_priv_map);
}

/*
 * has_cmk_privilege variants
 * 		These are all named "has_cmk_privilege" at the SQL level.
 * 		They take various combinations of global setting name, global setting OID,
 * 		user name, user OID, or implicit user = current_user.
 *
 * 		The result is a boolean value: true if user has the indicated
 * 		privilege, false if not, or NULL if object doesn't exist.
 */

/*
 * has_cmk_privilege_name_name
 * 		Check user privileges on a global setting given
 * 		name username, text keyname, and text priv name.
 */
Datum has_cmk_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text *keyname = PG_GETARG_TEXT_P(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid keyoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    keyoid = convert_cmk_name(keyname);
    mode = convert_cmk_priv_string(priv_type_text);

    aclresult = gs_sec_cmk_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cmk_privilege_name
 * 		Check user privileges on a global setting given
 * 		text keyname and text priv name.
 * 		current_user is assumed
 */
Datum has_cmk_privilege_name(PG_FUNCTION_ARGS)
{
    text *keyname = PG_GETARG_TEXT_P(0);
    text *priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid keyoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    keyoid = convert_cmk_name(keyname);
    mode = convert_cmk_priv_string(priv_type_text);

    aclresult = gs_sec_cmk_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cmk_privilege_name_id
 * 		Check user privileges on a global setting given
 * 		name usename, key oid, and text priv name.
 */
Datum has_cmk_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid keyoid = PG_GETARG_OID(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_cmk_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(GLOBALSETTINGOID, ObjectIdGetDatum(keyoid)))
        PG_RETURN_NULL();

    aclresult = gs_sec_cmk_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cmk_privilege_id
 * 		Check user privileges on a global setting given
 * 		key oid, and text priv name.
 * 		current_user is assumed
 */
Datum has_cmk_privilege_id(PG_FUNCTION_ARGS)
{
    Oid keyoid = PG_GETARG_OID(0);
    text *priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_cmk_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(GLOBALSETTINGOID, ObjectIdGetDatum(keyoid)))
        PG_RETURN_NULL();

    aclresult = gs_sec_cmk_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cmk_privilege_id_name
 * 		Check user privileges on a global setting given
 * 		roleid, text keyname, and text priv name.
 */
Datum has_cmk_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text *keyname = PG_GETARG_TEXT_P(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    Oid keyoid;
    AclMode mode;
    AclResult aclresult;

    keyoid = convert_cmk_name(keyname);
    mode = convert_cmk_priv_string(priv_type_text);

    aclresult = gs_sec_cmk_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cmk_privilege_id_id
 * 		Check user privileges on a global setting given
 * 		roleid, key oid, and text priv name.
 */
Datum has_cmk_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid keyoid = PG_GETARG_OID(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_cmk_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(GLOBALSETTINGOID, ObjectIdGetDatum(keyoid)))
        PG_RETURN_NULL();

    aclresult = gs_sec_cmk_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * 		Support routines for has_cek_privilege family.
 */
/*
 * has_cek_privilege variants
 * 		These are all named "has_cek_privilege" at the SQL level.
 * 		They take various combinations of key name, key OID,
 * 		user name, user OID, or implicit user = current_user.
 *
 * 		The result is a boolean value: true if user has the indicated
 * 		privilege, false if not, or NULL if object doesn't exist.
 */

/*
 * has_cek_privilege_name_name
 * 		Check user privileges on a column setting given
 * 		name username, text keyname, and text priv name.
 */
Datum has_cek_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text *keyname = PG_GETARG_TEXT_P(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid keyoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    keyoid = convert_column_key_name(keyname);
    mode = convert_cek_priv_string(priv_type_text);

    aclresult = gs_sec_cek_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cek_privilege_name
 * 		Check user privileges on a global setting given
 * 		text keyname and text priv name.
 * 		current_user is assumed
 */
Datum has_cek_privilege_name(PG_FUNCTION_ARGS)
{
    text *keyname = PG_GETARG_TEXT_P(0);
    text *priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid keyoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    keyoid = convert_column_key_name(keyname);
    mode = convert_cek_priv_string(priv_type_text);

    aclresult = gs_sec_cek_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cek_privilege_name_id
 * 		Check user privileges on a global setting given
 * 		name usename, key oid, and text priv name.
 */
Datum has_cek_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid keyoid = PG_GETARG_OID(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_cek_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(COLUMNSETTINGOID, ObjectIdGetDatum(keyoid))) {
        PG_RETURN_NULL();
    }

    aclresult = gs_sec_cek_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cek_privilege_id
 * 		Check user privileges on a global setting given
 * 		key oid, and text priv name.
 * 		current_user is assumed
 */
Datum has_cek_privilege_id(PG_FUNCTION_ARGS)
{
    Oid keyoid = PG_GETARG_OID(0);
    text *priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_cek_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(COLUMNSETTINGOID, ObjectIdGetDatum(keyoid))) {
        PG_RETURN_NULL();
    }

    aclresult = gs_sec_cek_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cek_privilege_id_name
 * 		Check user privileges on a global setting given
 * 		roleid, text keyname, and text priv name.
 */
Datum has_cek_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text *keyname = PG_GETARG_TEXT_P(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    Oid keyoid;
    AclMode mode;
    AclResult aclresult;

    keyoid = convert_column_key_name(keyname);
    mode = convert_cek_priv_string(priv_type_text);

    aclresult = gs_sec_cek_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_cek_privilege_id_id
 * 		Check user privileges on a global setting given
 * 		roleid, key oid, and text priv name.
 */
Datum has_cek_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid keyoid = PG_GETARG_OID(1);
    text *priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_cek_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(COLUMNSETTINGOID, ObjectIdGetDatum(keyoid)))
        PG_RETURN_NULL();

    aclresult = gs_sec_cek_aclcheck(keyoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_language_privilege variants
 *		These are all named "has_language_privilege" at the SQL level.
 *		They take various combinations of language name, language OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not, or NULL if object doesn't exist.
 */

/*
 * has_language_privilege_name_name
 *		Check user privileges on a language given
 *		name username, text languagename, and text priv name.
 */
Datum has_language_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* languagename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid languageoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    languageoid = convert_language_name(languagename);
    mode = convert_language_priv_string(priv_type_text);

    aclresult = pg_language_aclcheck(languageoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_language_privilege_name
 *		Check user privileges on a language given
 *		text languagename and text priv name.
 *		current_user is assumed
 */
Datum has_language_privilege_name(PG_FUNCTION_ARGS)
{
    text* languagename = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid languageoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    languageoid = convert_language_name(languagename);
    mode = convert_language_priv_string(priv_type_text);

    aclresult = pg_language_aclcheck(languageoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_language_privilege_name_id
 *		Check user privileges on a language given
 *		name usename, language oid, and text priv name.
 */
Datum has_language_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid languageoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_language_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(LANGOID, ObjectIdGetDatum(languageoid)))
        PG_RETURN_NULL();

    aclresult = pg_language_aclcheck(languageoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_language_privilege_id
 *		Check user privileges on a language given
 *		language oid, and text priv name.
 *		current_user is assumed
 */
Datum has_language_privilege_id(PG_FUNCTION_ARGS)
{
    Oid languageoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_language_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(LANGOID, ObjectIdGetDatum(languageoid)))
        PG_RETURN_NULL();

    aclresult = pg_language_aclcheck(languageoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_language_privilege_id_name
 *		Check user privileges on a language given
 *		roleid, text languagename, and text priv name.
 */
Datum has_language_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* languagename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid languageoid;
    AclMode mode;
    AclResult aclresult;

    languageoid = convert_language_name(languagename);
    mode = convert_language_priv_string(priv_type_text);

    aclresult = pg_language_aclcheck(languageoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_language_privilege_id_id
 *		Check user privileges on a language given
 *		roleid, language oid, and text priv name.
 */
Datum has_language_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid languageoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_language_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(LANGOID, ObjectIdGetDatum(languageoid)))
        PG_RETURN_NULL();

    aclresult = pg_language_aclcheck(languageoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_language_privilege family.
 */

/*
 * Given a language name expressed as a string, look it up and return Oid
 */
static Oid convert_language_name(text* languagename)
{
    char* langname = text_to_cstring(languagename);

    return get_language_oid(langname, false);
}

/*
 * convert_language_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_language_priv_string(text* priv_type_text)
{
    static const priv_map language_priv_map[] = {
        {"USAGE", ACL_USAGE}, {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)}, {NULL, 0}};

    return convert_any_priv_string(priv_type_text, language_priv_map);
}

/*
 * @Description :  get node group oid by name
 * @in group_name - node group name
 * @return - node group oid
 */
static Oid convert_nodegroup_name(text* group_name)
{
    char* nodegroup = text_to_cstring(group_name);
    return get_pgxc_groupoid(nodegroup, false);
}

/*
 * @Description :  Convert text node group privilege string to AclMode value.
 * @in priv_type_text - privilege text
 * @return - AclMode value
 */
static AclMode convert_nodegroup_priv_string(text* priv_type_text)
{
    static const priv_map node_group_priv_map[] = {{"CREATE", ACL_CREATE},
        {"CREATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {"USAGE", ACL_USAGE},
        {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {"COMPUTE", ACL_COMPUTE},
        {"COMPUTE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_COMPUTE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, node_group_priv_map);
}

/*
 * @Description :  Check user privileges on a node group given
 * @in username - user name
 * @in nodegroup - node group name
 * @in priv_type_text - privilege name
 * @return - bool
 */
Datum has_nodegroup_privilege_name_name(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    Name username = PG_GETARG_NAME(0);
    text* nodegroup = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid groupoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    groupoid = convert_nodegroup_name(nodegroup);
    mode = convert_nodegroup_priv_string(priv_type_text);

    aclresult = pg_nodegroup_aclcheck(groupoid, roleid, mode);
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * @Description :  Check user privileges on a node group given
 * @in nodegroup - node group name
 * @in priv_type_text - privilege name
 * @return - bool
 */
Datum has_nodegroup_privilege_name(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    text* nodegroup = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid groupoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    groupoid = convert_nodegroup_name(nodegroup);
    mode = convert_nodegroup_priv_string(priv_type_text);

    aclresult = pg_nodegroup_aclcheck(groupoid, roleid, mode);
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * @Description :  Check user privileges on a node group given
 * @in username - user name
 * @in groupoid - node group oid
 * @in priv_type_text - privilege name
 * @return - bool
 */
Datum has_nodegroup_privilege_name_id(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    Name username = PG_GETARG_NAME(0);
    Oid groupoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_nodegroup_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(PGXCGROUPOID, ObjectIdGetDatum(groupoid)))
        PG_RETURN_NULL();

    aclresult = pg_nodegroup_aclcheck(groupoid, roleid, mode);
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * @Description :  Check user privileges on a node group given
 * @in groupoid - node group oid
 * @in priv_type_text - privilege name
 * @return - bool
 */
Datum has_nodegroup_privilege_id(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    Oid groupoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_nodegroup_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(PGXCGROUPOID, ObjectIdGetDatum(groupoid)))
        PG_RETURN_NULL();

    aclresult = pg_nodegroup_aclcheck(groupoid, roleid, mode);
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * @Description :  Check user privileges on a node group given
 * @in roleid - user oid
 * @in groupname - node group name
 * @in priv_type_text - privilege name
 * @return - bool
 */
Datum has_nodegroup_privilege_id_name(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    Oid roleid = PG_GETARG_OID(0);
    text* groupname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid groupoid;
    AclMode mode;
    AclResult aclresult;

    groupoid = convert_nodegroup_name(groupname);
    mode = convert_nodegroup_priv_string(priv_type_text);

    aclresult = pg_nodegroup_aclcheck(groupoid, roleid, mode);
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * @Description :  Check user privileges on a node group given
 * @in roleid - user oid
 * @in groupoid - node group oid
 * @in priv_type_text - privilege name
 * @return - bool
 */
Datum has_nodegroup_privilege_id_id(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    Oid roleid = PG_GETARG_OID(0);
    Oid groupoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    if (!OidIsValid(groupoid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("node group with OID %u does not exist", groupoid)));
    }

    mode = convert_nodegroup_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(PGXCGROUPOID, ObjectIdGetDatum(groupoid)))
        PG_RETURN_NULL();

    aclresult = pg_nodegroup_aclcheck(groupoid, roleid, mode);
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_schema_privilege variants
 *		These are all named "has_schema_privilege" at the SQL level.
 *		They take various combinations of schema name, schema OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not, or NULL if object doesn't exist.
 */

/*
 * has_schema_privilege_name_name
 *		Check user privileges on a schema given
 *		name username, text schemaname, and text priv name.
 */
Datum has_schema_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* schemaname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid schemaoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    schemaoid = convert_schema_name(schemaname);
    mode = convert_schema_priv_string(priv_type_text);

    aclresult = pg_namespace_aclcheck(schemaoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_schema_privilege_name
 *		Check user privileges on a schema given
 *		text schemaname and text priv name.
 *		current_user is assumed
 */
Datum has_schema_privilege_name(PG_FUNCTION_ARGS)
{
    text* schemaname = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid schemaoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    schemaoid = convert_schema_name(schemaname);
    mode = convert_schema_priv_string(priv_type_text);

    aclresult = pg_namespace_aclcheck(schemaoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_schema_privilege_name_id
 *		Check user privileges on a schema given
 *		name usename, schema oid, and text priv name.
 */
Datum has_schema_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid schemaoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_schema_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(schemaoid)))
        PG_RETURN_NULL();

    aclresult = pg_namespace_aclcheck(schemaoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_schema_privilege_id
 *		Check user privileges on a schema given
 *		schema oid, and text priv name.
 *		current_user is assumed
 */
Datum has_schema_privilege_id(PG_FUNCTION_ARGS)
{
    Oid schemaoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_schema_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(schemaoid)))
        PG_RETURN_NULL();

    aclresult = pg_namespace_aclcheck(schemaoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_schema_privilege_id_name
 *		Check user privileges on a schema given
 *		roleid, text schemaname, and text priv name.
 */
Datum has_schema_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* schemaname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid schemaoid;
    AclMode mode;
    AclResult aclresult;

    schemaoid = convert_schema_name(schemaname);
    mode = convert_schema_priv_string(priv_type_text);

    aclresult = pg_namespace_aclcheck(schemaoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_schema_privilege_id_id
 *		Check user privileges on a schema given
 *		roleid, schema oid, and text priv name.
 */
Datum has_schema_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid schemaoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_schema_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(schemaoid)))
        PG_RETURN_NULL();

    aclresult = pg_namespace_aclcheck(schemaoid, roleid, mode, false);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_schema_privilege family.
 */

/*
 * Given a schema name expressed as a string, look it up and return Oid
 */
static Oid convert_schema_name(text* schemaname)
{
    char* nspname = text_to_cstring(schemaname);

    return get_namespace_oid(nspname, false);
}

/*
 * convert_schema_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_schema_priv_string(text* priv_type_text)
{
    static const priv_map schema_priv_map[] = {{"CREATE", ACL_CREATE},
        {"CREATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {"USAGE", ACL_USAGE},
        {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, schema_priv_map);
}

/*
 * has_server_privilege variants
 *		These are all named "has_server_privilege" at the SQL level.
 *		They take various combinations of foreign server name,
 *		server OID, user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not.
 */

/*
 * has_server_privilege_name_name
 *		Check user privileges on a foreign server given
 *		name username, text servername, and text priv name.
 */
Datum has_server_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* servername = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid serverid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    serverid = convert_server_name(servername);
    mode = convert_server_priv_string(priv_type_text);

    aclresult = pg_foreign_server_aclcheck(serverid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_server_privilege_name
 *		Check user privileges on a foreign server given
 *		text servername and text priv name.
 *		current_user is assumed
 */
Datum has_server_privilege_name(PG_FUNCTION_ARGS)
{
    text* servername = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid serverid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    serverid = convert_server_name(servername);
    mode = convert_server_priv_string(priv_type_text);

    aclresult = pg_foreign_server_aclcheck(serverid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_server_privilege_name_id
 *		Check user privileges on a foreign server given
 *		name usename, foreign server oid, and text priv name.
 */
Datum has_server_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid serverid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_server_priv_string(priv_type_text);

    aclresult = pg_foreign_server_aclcheck(serverid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_server_privilege_id
 *		Check user privileges on a foreign server given
 *		server oid, and text priv name.
 *		current_user is assumed
 */
Datum has_server_privilege_id(PG_FUNCTION_ARGS)
{
    Oid serverid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_server_priv_string(priv_type_text);

    aclresult = pg_foreign_server_aclcheck(serverid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_server_privilege_id_name
 *		Check user privileges on a foreign server given
 *		roleid, text servername, and text priv name.
 */
Datum has_server_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* servername = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid serverid;
    AclMode mode;
    AclResult aclresult;

    serverid = convert_server_name(servername);
    mode = convert_server_priv_string(priv_type_text);

    aclresult = pg_foreign_server_aclcheck(serverid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_server_privilege_id_id
 *		Check user privileges on a foreign server given
 *		roleid, server oid, and text priv name.
 */
Datum has_server_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid serverid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_server_priv_string(priv_type_text);

    aclresult = pg_foreign_server_aclcheck(serverid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_server_privilege family.
 */

/*
 * Given a server name expressed as a string, look it up and return Oid
 */
static Oid convert_server_name(text* servername)
{
    char* serverstr = text_to_cstring(servername);

    return get_foreign_server_oid(serverstr, false);
}

/*
 * convert_server_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_server_priv_string(text* priv_type_text)
{
    static const priv_map server_priv_map[] = {
        {"USAGE", ACL_USAGE}, {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, server_priv_map);
}

/*
 * has_tablespace_privilege variants
 *		These are all named "has_tablespace_privilege" at the SQL level.
 *		They take various combinations of tablespace name, tablespace OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not.
 */

/*
 * has_tablespace_privilege_name_name
 *		Check user privileges on a tablespace given
 *		name username, text tablespacename, and text priv name.
 */
Datum has_tablespace_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* tablespacename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid tablespaceoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    tablespaceoid = convert_tablespace_name(tablespacename);
    mode = convert_tablespace_priv_string(priv_type_text);

    aclresult = pg_tablespace_aclcheck(tablespaceoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_tablespace_privilege_name
 *		Check user privileges on a tablespace given
 *		text tablespacename and text priv name.
 *		current_user is assumed
 */
Datum has_tablespace_privilege_name(PG_FUNCTION_ARGS)
{
    text* tablespacename = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid tablespaceoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    tablespaceoid = convert_tablespace_name(tablespacename);
    mode = convert_tablespace_priv_string(priv_type_text);

    aclresult = pg_tablespace_aclcheck(tablespaceoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_tablespace_privilege_name_id
 *		Check user privileges on a tablespace given
 *		name usename, tablespace oid, and text priv name.
 */
Datum has_tablespace_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid tablespaceoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_tablespace_priv_string(priv_type_text);

    aclresult = pg_tablespace_aclcheck(tablespaceoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_tablespace_privilege_id
 *		Check user privileges on a tablespace given
 *		tablespace oid, and text priv name.
 *		current_user is assumed
 */
Datum has_tablespace_privilege_id(PG_FUNCTION_ARGS)
{
    Oid tablespaceoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_tablespace_priv_string(priv_type_text);

    aclresult = pg_tablespace_aclcheck(tablespaceoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_tablespace_privilege_id_name
 *		Check user privileges on a tablespace given
 *		roleid, text tablespacename, and text priv name.
 */
Datum has_tablespace_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* tablespacename = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid tablespaceoid;
    AclMode mode;
    AclResult aclresult;

    tablespaceoid = convert_tablespace_name(tablespacename);
    mode = convert_tablespace_priv_string(priv_type_text);

    aclresult = pg_tablespace_aclcheck(tablespaceoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_tablespace_privilege_id_id
 *		Check user privileges on a tablespace given
 *		roleid, tablespace oid, and text priv name.
 */
Datum has_tablespace_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid tablespaceoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_tablespace_priv_string(priv_type_text);

    aclresult = pg_tablespace_aclcheck(tablespaceoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_tablespace_privilege family.
 */

/*
 * Given a tablespace name expressed as a string, look it up and return Oid
 */
static Oid convert_tablespace_name(text* tablespacename)
{
    char* spcname = text_to_cstring(tablespacename);

    return get_tablespace_oid(spcname, false);
}

/*
 * convert_tablespace_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_tablespace_priv_string(text* priv_type_text)
{
    static const priv_map tablespace_priv_map[] = {
        {"CREATE", ACL_CREATE}, {"CREATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, tablespace_priv_map);
}

/*
 * Given a column setting name expressed as a string, look it up and return Oid
 */
static Oid convert_column_key_name(text *cekname)
{
    char *keyname = text_to_cstring(cekname);
    Oid oid;

    oid = DatumGetObjectId(DirectFunctionCall1(columnsettingin, CStringGetDatum(keyname)));

    if (!OidIsValid(oid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_KEY), errmsg("column encryption key \"%s\" does not exist", keyname)));

    return oid;
}

/*
 * convert_cek_priv_string
 * 		Convert text string to AclMode value.
 */
static AclMode convert_cek_priv_string(text *priv_type_text)
{
    static const priv_map cek_priv_map[] = {
        {"USAGE", ACL_USAGE},
        {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_DROP)},
        {NULL, 0}
    };

    return convert_any_priv_string(priv_type_text, cek_priv_map);
}


/*
 * Given a global setting name expressed as a string, look it up and return Oid
 */
static Oid convert_cmk_name(text *globalSettingName)
{
    char *keyname = text_to_cstring(globalSettingName);
    Oid oid;

    oid = DatumGetObjectId(DirectFunctionCall1(globalsettingin, CStringGetDatum(keyname)));

    if (!OidIsValid(oid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("client master key \"%s\" does not exist", keyname)));

    return oid;
}

/*
 * convert_cmk_priv_string
 * 		Convert text string to AclMode value.
 */
static AclMode convert_cmk_priv_string(text *priv_type_text)
{
    static const priv_map key_priv_map[] = {
        {"USAGE", ACL_USAGE},
        {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_DROP)},
        {NULL, 0}
    };

    return convert_any_priv_string(priv_type_text, key_priv_map);
}

/*
 * has_type_privilege variants
 *		These are all named "has_type_privilege" at the SQL level.
 *		They take various combinations of type name, type OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not, or NULL if object doesn't exist.
 */

/*
 * has_type_privilege_name_name
 *		Check user privileges on a type given
 *		name username, text typename, and text priv name.
 */
Datum has_type_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* typname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid typeoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    typeoid = convert_type_name(typname);
    mode = convert_type_priv_string(priv_type_text);

    aclresult = pg_type_aclcheck(typeoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_type_privilege_name
 *		Check user privileges on a type given
 *		text typename and text priv name.
 *		current_user is assumed
 */
Datum has_type_privilege_name(PG_FUNCTION_ARGS)
{
    text* typname = PG_GETARG_TEXT_P(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid typeoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    typeoid = convert_type_name(typname);
    mode = convert_type_priv_string(priv_type_text);

    aclresult = pg_type_aclcheck(typeoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_type_privilege_name_id
 *		Check user privileges on a type given
 *		name usename, type oid, and text priv name.
 */
Datum has_type_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid typeoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_type_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(typeoid)))
        PG_RETURN_NULL();

    aclresult = pg_type_aclcheck(typeoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_type_privilege_id
 *		Check user privileges on a type given
 *		type oid, and text priv name.
 *		current_user is assumed
 */
Datum has_type_privilege_id(PG_FUNCTION_ARGS)
{
    Oid typeoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_type_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(typeoid)))
        PG_RETURN_NULL();

    aclresult = pg_type_aclcheck(typeoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_type_privilege_id_name
 *		Check user privileges on a type given
 *		roleid, text typename, and text priv name.
 */
Datum has_type_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* typname = PG_GETARG_TEXT_P(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid typeoid;
    AclMode mode;
    AclResult aclresult;

    typeoid = convert_type_name(typname);
    mode = convert_type_priv_string(priv_type_text);

    aclresult = pg_type_aclcheck(typeoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_type_privilege_id_id
 *		Check user privileges on a type given
 *		roleid, type oid, and text priv name.
 */
Datum has_type_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid typeoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_type_priv_string(priv_type_text);

    if (!SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(typeoid)))
        PG_RETURN_NULL();

    aclresult = pg_type_aclcheck(typeoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_type_privilege family.
 */

/*
 * Given a type name expressed as a string, look it up and return Oid
 */
static Oid convert_type_name(text* typeName)
{
    char* typname = text_to_cstring(typeName);
    Oid oid;

    oid = DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum(typname)));

    if (!OidIsValid(oid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type \"%s\" does not exist", typname)));

    return oid;
}

/*
 * convert_type_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_type_priv_string(text* priv_type_text)
{
    static const priv_map type_priv_map[] = {
        {"USAGE", ACL_USAGE}, {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {"COMMENT", ACL_COMMENT},
        {"COMMENT WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_COMMENT)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, type_priv_map);
}

/*
 * has_directory_privilege_name_name
 *		Check user privileges on a directory given
 *		name username, text directoryname, and text priv name.
 */
Datum has_directory_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    text* dirname = PG_GETARG_TEXT_P(1);
    text* priv_dir_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid diroid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    diroid = convert_directory_name(dirname);
    mode = convert_directory_priv_string(priv_dir_text);

    aclresult = pg_directory_aclcheck(diroid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_directory_privilege_name
 *		Check user privileges on a directory given
 *		text dirname and text priv name.
 *		current_user is assumed
 */
Datum has_directory_privilege_name(PG_FUNCTION_ARGS)
{
    text* dirname = PG_GETARG_TEXT_P(0);
    text* priv_dir_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid diroid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    diroid = convert_directory_name(dirname);
    mode = convert_directory_priv_string(priv_dir_text);

    aclresult = pg_directory_aclcheck(diroid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_directory_privilege_name_id
 *		Check user privileges on a directory given
 *		name usename, directory oid, and text priv name.
 */
Datum has_directory_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid diroid = PG_GETARG_OID(1);
    text* priv_dir_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid_or_public(NameStr(*username));
    mode = convert_directory_priv_string(priv_dir_text);

    if (!SearchSysCacheExists1(DIRECTORYOID, ObjectIdGetDatum(diroid)))
        PG_RETURN_NULL();

    aclresult = pg_directory_aclcheck(diroid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_directory_privilege_id
 *		Check user privileges on a directory given
 *		directory oid, and text priv name.
 *		current_user is assumed
 */
Datum has_directory_privilege_id(PG_FUNCTION_ARGS)
{
    Oid diroid = PG_GETARG_OID(0);
    text* priv_dir_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_directory_priv_string(priv_dir_text);

    if (!SearchSysCacheExists1(DIRECTORYOID, ObjectIdGetDatum(diroid)))
        PG_RETURN_NULL();

    aclresult = pg_directory_aclcheck(diroid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_directory_privilege_id_name
 *		Check user privileges on a directory given
 *		roleid, text typename, and text priv name.
 */
Datum has_directory_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    text* dirname = PG_GETARG_TEXT_P(1);
    text* priv_dir_text = PG_GETARG_TEXT_P(2);
    Oid diroid;
    AclMode mode;
    AclResult aclresult;

    diroid = convert_directory_name(dirname);
    mode = convert_directory_priv_string(priv_dir_text);

    aclresult = pg_directory_aclcheck(diroid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * has_directory_privilege_id_id
 *		Check user privileges on a directory given
 *		roleid, directory oid, and text priv name.
 */
Datum has_directory_privilege_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid diroid = PG_GETARG_OID(1);
    text* priv_dir_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_directory_priv_string(priv_dir_text);

    if (!SearchSysCacheExists1(DIRECTORYOID, ObjectIdGetDatum(diroid)))
        PG_RETURN_NULL();

    aclresult = pg_directory_aclcheck(diroid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for has_type_privilege family.
 */

/*
 * Given a directory name expressed as a string, look it up and return Oid
 */
static Oid convert_directory_name(text* dirName)
{
    char* dirname = text_to_cstring(dirName);
    return get_directory_oid(dirname, false);
}

/*
 * convert_directory_priv_string
 *		Convert text string to AclMode value.
 */
static AclMode convert_directory_priv_string(text* priv_dir_text)
{
    static const priv_map dir_priv_map[] = {{"READ", ACL_READ},
        {"READ WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_READ)},
        {"WRITE", ACL_WRITE},
        {"WRITE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_WRITE)},
        {"ALTER", ACL_ALTER},
        {"ALTER WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_ALTER)))},
        {"DROP", ACL_DROP},
        {"DROP WITH GRANT OPTION", ADD_DDL_FLAG(ACL_GRANT_OPTION_FOR(REMOVE_DDL_FLAG(ACL_DROP)))},
        {NULL, 0}};

    return convert_any_priv_string(priv_dir_text, dir_priv_map);
}

/*
 * pg_has_role variants
 *		These are all named "pg_has_role" at the SQL level.
 *		They take various combinations of role name, role OID,
 *		user name, user OID, or implicit user = current_user.
 *
 *		The result is a boolean value: true if user has the indicated
 *		privilege, false if not.
 */

/*
 * pg_has_role_name_name
 *		Check user privileges on a role given
 *		name username, name rolename, and text priv name.
 */
Datum pg_has_role_name_name(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Name rolename = PG_GETARG_NAME(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    Oid roleoid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid(NameStr(*username), false);
    roleoid = get_role_oid(NameStr(*rolename), false);
    mode = convert_role_priv_string(priv_type_text);

    aclresult = pg_role_aclcheck(roleoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * pg_has_role_name
 *		Check user privileges on a role given
 *		name rolename and text priv name.
 *		current_user is assumed
 */
Datum pg_has_role_name(PG_FUNCTION_ARGS)
{
    Name rolename = PG_GETARG_NAME(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    Oid roleoid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    roleoid = get_role_oid(NameStr(*rolename), false);
    mode = convert_role_priv_string(priv_type_text);

    aclresult = pg_role_aclcheck(roleoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * pg_has_role_name_id
 *		Check user privileges on a role given
 *		name usename, role oid, and text priv name.
 */
Datum pg_has_role_name_id(PG_FUNCTION_ARGS)
{
    Name username = PG_GETARG_NAME(0);
    Oid roleoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = get_role_oid(NameStr(*username), false);
    mode = convert_role_priv_string(priv_type_text);

    aclresult = pg_role_aclcheck(roleoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * pg_has_role_id
 *		Check user privileges on a role given
 *		role oid, and text priv name.
 *		current_user is assumed
 */
Datum pg_has_role_id(PG_FUNCTION_ARGS)
{
    Oid roleoid = PG_GETARG_OID(0);
    text* priv_type_text = PG_GETARG_TEXT_P(1);
    Oid roleid;
    AclMode mode;
    AclResult aclresult;

    roleid = GetUserId();
    mode = convert_role_priv_string(priv_type_text);

    aclresult = pg_role_aclcheck(roleoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * pg_has_role_id_name
 *		Check user privileges on a role given
 *		roleid, name rolename, and text priv name.
 */
Datum pg_has_role_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Name rolename = PG_GETARG_NAME(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    Oid roleoid;
    AclMode mode;
    AclResult aclresult;

    roleoid = get_role_oid(NameStr(*rolename), false);
    mode = convert_role_priv_string(priv_type_text);

    aclresult = pg_role_aclcheck(roleoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 * pg_has_role_id_id
 *		Check user privileges on a role given
 *		roleid, role oid, and text priv name.
 */
Datum pg_has_role_id_id(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Oid roleoid = PG_GETARG_OID(1);
    text* priv_type_text = PG_GETARG_TEXT_P(2);
    AclMode mode;
    AclResult aclresult;

    mode = convert_role_priv_string(priv_type_text);

    aclresult = pg_role_aclcheck(roleoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}

/*
 *		Support routines for pg_has_role family.
 */

/*
 * convert_role_priv_string
 *		Convert text string to AclMode value.
 *
 * We use USAGE to denote whether the privileges of the role are accessible
 * (has_privs), MEMBER to denote is_member, and MEMBER WITH GRANT OPTION
 * (or ADMIN OPTION) to denote is_admin.  There is no ACL bit corresponding
 * to MEMBER so we cheat and use ACL_CREATE for that.  This convention
 * is shared only with pg_role_aclcheck, below.
 */
static AclMode convert_role_priv_string(text* priv_type_text)
{
    static const priv_map role_priv_map[] = {{"USAGE", ACL_USAGE},
        {"MEMBER", ACL_CREATE},
        {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {"USAGE WITH ADMIN OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {"MEMBER WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {"MEMBER WITH ADMIN OPTION", ACL_GRANT_OPTION_FOR(ACL_CREATE)},
        {NULL, 0}};

    return convert_any_priv_string(priv_type_text, role_priv_map);
}

/*
 * pg_role_aclcheck
 *		Quick-and-dirty support for pg_has_role
 */
static AclResult pg_role_aclcheck(Oid role_oid, Oid roleid, AclMode mode)
{
    if (mode & ACL_GRANT_OPTION_FOR(ACL_CREATE)) {
        /*
         * XXX For roleid == role_oid, is_admin_of_role() also examines the
         * session and call stack.  That suits two-argument pg_has_role(), but
         * it gives the three-argument version a lamentable whimsy.
         */
        if (is_admin_of_role(roleid, role_oid))
            return ACLCHECK_OK;
    }
    if (mode & ACL_CREATE) {
        if (is_member_of_role(roleid, role_oid))
            return ACLCHECK_OK;
    }
    if (mode & ACL_USAGE) {
        if (has_privs_of_role(roleid, role_oid))
            return ACLCHECK_OK;
    }
    return ACLCHECK_NO_PRIV;
}

/*
 * initialization function (called by openGauss)
 */
void initialize_acl(void)
{
    if (!IsBootstrapProcessingMode()) {
        /*
         * In normal mode, set a callback on any syscache invalidation of
         * pg_auth_members rows
         */
        CacheRegisterSessionSyscacheCallback(AUTHMEMROLEMEM, RoleMembershipCacheCallback, (Datum)0);
    }
}

/*
 * RoleMembershipCacheCallback
 *		Syscache inval callback function
 */
static void RoleMembershipCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    /* Force membership caches to be recomputed on next use */
    u_sess->cache_cxt.cached_privs_role = InvalidOid;
    u_sess->cache_cxt.cached_member_role = InvalidOid;
}

/* Check if specified role has rolinherit set */
static bool has_rolinherit(Oid roleid)
{
    bool result = false;
    HeapTuple utup = NULL;

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(utup)) {
        result = ((Form_pg_authid)GETSTRUCT(utup))->rolinherit;
        ReleaseSysCache(utup);
    }
    return result;
}

/*
 * Get a list of roles that the specified roleid has the privileges of
 *
 * This is defined not to recurse through roles that don't have rolinherit
 * set; for such roles, membership implies the ability to do SET ROLE, but
 * the privileges are not available until you've done so.
 *
 * Since indirect membership testing is relatively expensive, we cache
 * a list of memberships.  Hence, the result is only guaranteed good until
 * the next call of roles_has_privs_of()!
 *
 * For the benefit of select_best_grantor, the result is defined to be
 * in breadth-first order, ie, closer relationships earlier.
 */
List* roles_has_privs_of(Oid roleid)
{
    List* roles_list = NIL;
    ListCell* l = NULL;
    List* new_cached_privs_roles = NIL;
    MemoryContext oldctx;

    /* If cache is already valid, just return the list */
    if (OidIsValid(u_sess->cache_cxt.cached_privs_role) && u_sess->cache_cxt.cached_privs_role == roleid)
        return u_sess->cache_cxt.cached_privs_roles;

    /*
     * Find all the roles that roleid is a member of, including multi-level
     * recursion.  The role itself will always be the first element of the
     * resulting list.
     *
     * Each element of the list is scanned to see if it adds any indirect
     * memberships.  We can use a single list as both the record of
     * already-found memberships and the agenda of roles yet to be scanned.
     * This is a bit tricky but works because the foreach() macro doesn't
     * fetch the next list element until the bottom of the loop.
     */
    roles_list = list_make1_oid(roleid);

    foreach (l, roles_list) {
        Oid memberid = lfirst_oid(l);
        CatCList* memlist = NULL;
        int i;

        /* Ignore non-inheriting roles */
        if (!has_rolinherit(memberid))
            continue;

        /* Find roles that memberid is directly a member of */
        memlist = SearchSysCacheList1(AUTHMEMMEMROLE, ObjectIdGetDatum(memberid));
        for (i = 0; i < memlist->n_members; i++) {
            HeapTuple tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, i);
            Oid otherid = ((Form_pg_auth_members)GETSTRUCT(tup))->roleid;

            /*
             * Even though there shouldn't be any loops in the membership
             * graph, we must test for having already seen this role. It is
             * legal for instance to have both A->B and A->C->B.
             */
            roles_list = list_append_unique_oid(roles_list, otherid);
        }
        ReleaseSysCacheList(memlist);
    }

    /*
     * Copy the completed list into u_sess.mcxt_group[MEMORY_CONTEXT_SECURITY] so it will persist.
     */
    oldctx = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY));
    new_cached_privs_roles = list_copy(roles_list);
    MemoryContextSwitchTo(oldctx);
    list_free_ext(roles_list);

    /*
     * Now safe to assign to state variable
     */
    u_sess->cache_cxt.cached_privs_role = InvalidOid; /* just paranoia */
    list_free_ext(u_sess->cache_cxt.cached_privs_roles);
    u_sess->cache_cxt.cached_privs_roles = new_cached_privs_roles;
    u_sess->cache_cxt.cached_privs_role = roleid;

    /* And now we can return the answer */
    return u_sess->cache_cxt.cached_privs_roles;
}

/*
 * Get a list of roles that the specified roleid is a member of
 *
 * This is defined to recurse through roles regardless of rolinherit.
 *
 * Since indirect membership testing is relatively expensive, we cache
 * a list of memberships.  Hence, the result is only guaranteed good until
 * the next call of roles_is_member_of()!
 */
static List* roles_is_member_of(Oid roleid)
{
    List* roles_list = NIL;
    ListCell* l = NULL;
    List* new_cached_membership_roles = NIL;
    MemoryContext oldctx;

    /* If cache is already valid, just return the list */
    if (OidIsValid(u_sess->cache_cxt.cached_member_role) && u_sess->cache_cxt.cached_member_role == roleid)
        return u_sess->cache_cxt.cached_membership_roles;

    /*
     * Find all the roles that roleid is a member of, including multi-level
     * recursion.  The role itself will always be the first element of the
     * resulting list.
     *
     * Each element of the list is scanned to see if it adds any indirect
     * memberships.  We can use a single list as both the record of
     * already-found memberships and the agenda of roles yet to be scanned.
     * This is a bit tricky but works because the foreach() macro doesn't
     * fetch the next list element until the bottom of the loop.
     */
    roles_list = list_make1_oid(roleid);

    foreach (l, roles_list) {
        Oid memberid = lfirst_oid(l);
        CatCList* memlist = NULL;
        int i;

        /* Find roles that memberid is directly a member of */
        memlist = SearchSysCacheList1(AUTHMEMMEMROLE, ObjectIdGetDatum(memberid));
        for (i = 0; i < memlist->n_members; i++) {
            HeapTuple tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, i);
            Oid otherid = ((Form_pg_auth_members)GETSTRUCT(tup))->roleid;

            /*
             * Even though there shouldn't be any loops in the membership
             * graph, we must test for having already seen this role. It is
             * legal for instance to have both A->B and A->C->B.
             */
            roles_list = list_append_unique_oid(roles_list, otherid);
        }
        ReleaseSysCacheList(memlist);
    }

    /*
     * Copy the completed list into u_sess.mcxt_group[MEMORY_CONTEXT_SECURITY] so it will persist.
     */
    oldctx = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY));
    new_cached_membership_roles = list_copy(roles_list);
    MemoryContextSwitchTo(oldctx);
    list_free_ext(roles_list);

    /*
     * Now safe to assign to state variable
     */
    u_sess->cache_cxt.cached_member_role = InvalidOid; /* just paranoia */
    list_free_ext(u_sess->cache_cxt.cached_membership_roles);
    u_sess->cache_cxt.cached_membership_roles = new_cached_membership_roles;
    u_sess->cache_cxt.cached_member_role = roleid;

    /* And now we can return the answer */
    return u_sess->cache_cxt.cached_membership_roles;
}

Oid get_nodegroup_of_roleslist(Oid roleid, List* roles_list)
{
    const ListCell* cell = NULL;
    Oid groupid;
    Oid group_oid = InvalidOid;

    foreach (cell, roles_list) {
        groupid = get_pgxc_logic_groupoid(lfirst_oid(cell));
        if (groupid != InvalidOid) {
            if (group_oid == InvalidOid) {
                group_oid = groupid;
            } else if (group_oid != groupid) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                        errmsg("Role \"%s\" can only attach to one node group.", GetUserNameFromId(roleid))));
            }
        }
    }

    return group_oid;
}

Oid get_nodegroup_member_of(Oid roleid)
{
    List* roles_list = NIL;
    Oid group_oid;

    roles_list = roles_is_member_of(roleid);
    group_oid = get_nodegroup_of_roleslist(roleid, roles_list);

    return group_oid;
}

Oid get_nodegroup_privs_of(Oid roleid)
{
    List* roles_list = NIL;
    Oid group_oid;

    roles_list = roles_has_privs_of(roleid);
    group_oid = get_nodegroup_of_roleslist(roleid, roles_list);

    return group_oid;
}

/*
 * Does member have the privileges of role (directly or indirectly)?
 *
 * This is defined not to recurse through roles that don't have rolinherit
 * set; for such roles, membership implies the ability to do SET ROLE, but
 * the privileges are not available until you've done so.
 */
bool has_privs_of_role(Oid member, Oid role)
{
    /* Fast path for simple case */
    if (member == role)
        return true;

    /* Superusers have every privilege, so are part of every role, except independent role. */
    if (superuser_arg(member) && (!is_role_independent(role)))
        return true;

    /*
     * Find all the roles that member has the privileges of, including
     * multi-level recursion, then see if target role is any one of them.
     */
    return list_member_oid(roles_has_privs_of(member), role);
}

/*
 * Is member a member of role (directly or indirectly)?
 *
 * This is defined to recurse through roles regardless of rolinherit.
 */
bool is_member_of_role(Oid member, Oid role)
{
    /* Fast path for simple case */
    if (member == role)
        return true;

    /* Superusers have every privilege, so are part of every role */
    if (superuser_arg(member))
        return true;

    /*
     * Find all the roles that member is a member of, including multi-level
     * recursion, then see if target role is any one of them.
     */
    return list_member_oid(roles_is_member_of(member), role);
}

/*
 * check_is_member_of_role
 *		is_member_of_role with a standard permission-violation error if not
 */
void check_is_member_of_role(Oid member, Oid role)
{
    if (!is_member_of_role(member, role))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be member of role \"%s\"", GetUserNameFromId(role))));
}

/*
 * Is member a member of role, not considering superuserness?
 *
 * This is identical to is_member_of_role except we ignore superuser
 * status.
 */
bool is_member_of_role_nosuper(Oid member, Oid role)
{
    /* Fast path for simple case */
    if (member == role)
        return true;

    /*
     * Find all the roles that member is a member of, including multi-level
     * recursion, then see if target role is any one of them.
     */
    return list_member_oid(roles_is_member_of(member), role);
}

/*
 * Is member an admin of role?  That is, is member the role itself (subject to
 * restrictions below), a member (directly or indirectly) WITH ADMIN OPTION,
 * or a superuser?
 */
bool is_admin_of_role(Oid member, Oid role)
{
    bool result = false;
    List* roles_list = NIL;
    ListCell* l = NULL;

    if (superuser_arg(member))
        return true;

    if (member == role)
        /*
         * A role can admin itself when it matches the session user and we're
         * outside any security-restricted operation, SECURITY DEFINER or
         * similar context.  SQL-standard roles cannot self-admin.  However,
         * SQL-standard users are distinct from roles, and they are not
         * grantable like roles: openGauss's role-user duality extends the
         * standard.  Checking for a session user match has the effect of
         * letting a role self-admin only when it's conspicuously behaving
         * like a user.  Note that allowing self-admin under a mere SET ROLE
         * would make WITH ADMIN OPTION largely irrelevant; any member could
         * SET ROLE to issue the otherwise-forbidden command.
         *
         * Withholding self-admin in a security-restricted operation prevents
         * object owners from harnessing the session user identity during
         * administrative maintenance.  Suppose Alice owns a database, has
         * issued "GRANT alice TO bob", and runs a daily ANALYZE.  Bob creates
         * an alice-owned SECURITY DEFINER function that issues "REVOKE alice
         * FROM carol".  If he creates an expression index calling that
         * function, Alice will attempt the REVOKE during each ANALYZE.
         * Checking InSecurityRestrictedOperation() thwarts that attack.
         *
         * Withholding self-admin in SECURITY DEFINER functions makes their
         * behavior independent of the calling user.  There's no security or
         * SQL-standard-conformance need for that restriction, though.
         *
         * A role cannot have actual WITH ADMIN OPTION on itself, because that
         * would imply a membership loop.  Therefore, we're done either way.
         */
        return member == GetSessionUserId() && !InLocalUserIdChange() && !InSecurityRestrictedOperation();

    /*
     * Find all the roles that member is a member of, including multi-level
     * recursion.  We build a list in the same way that is_member_of_role does
     * to track visited and unvisited roles.
     */
    roles_list = list_make1_oid(member);

    foreach (l, roles_list) {
        Oid memberid = lfirst_oid(l);
        CatCList* memlist = NULL;
        int i;

        /* Find roles that memberid is directly a member of */
        memlist = SearchSysCacheList1(AUTHMEMMEMROLE, ObjectIdGetDatum(memberid));
        for (i = 0; i < memlist->n_members; i++) {
            HeapTuple tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(memlist, i);
            Oid otherid = ((Form_pg_auth_members)GETSTRUCT(tup))->roleid;

            if (otherid == role && ((Form_pg_auth_members)GETSTRUCT(tup))->admin_option) {
                /* Found what we came for, so can stop searching */
                result = true;
                break;
            }

            roles_list = list_append_unique_oid(roles_list, otherid);
        }
        ReleaseSysCacheList(memlist);
        if (result)
            break;
    }

    list_free_ext(roles_list);

    return result;
}

/* does what it says ... */
static int count_one_bits(AclMode mask)
{
    int nbits = 0;

    /* this code relies on AclMode being an unsigned type */
    while (mask) {
        if (mask & 1)
            nbits++;
        mask >>= 1;
    }
    return nbits;
}

/*
 * Select the effective grantor ID for a GRANT or REVOKE operation.
 *
 * The grantor must always be either the object owner or some role that has
 * been explicitly granted grant options.  This ensures that all granted
 * privileges appear to flow from the object owner, and there are never
 * multiple "original sources" of a privilege.	Therefore, if the would-be
 * grantor is a member of a role that has the needed grant options, we have
 * to do the grant as that role instead.
 *
 * It is possible that the would-be grantor is a member of several roles
 * that have different subsets of the desired grant options, but no one
 * role has 'em all.  In this case we pick a role with the largest number
 * of desired options.	Ties are broken in favor of closer ancestors.
 *
 * roleId: the role attempting to do the GRANT/REVOKE
 * privileges: the privileges to be granted/revoked
 * acl: the ACL of the object in question
 * ownerId: the role owning the object in question
 * *grantorId: receives the OID of the role to do the grant as
 * *grantOptions: receives the grant options actually held by grantorId
 * isDbePerf: if the object in question belonging to schema dbe_perf
 * isPgCatalog: if the object in question belonging to schema pg_catalog
 *
 * If no grant options exist, we set grantorId to roleId, grantOptions to 0.
 */
void select_best_grantor(
    Oid roleId, AclMode privileges, AclMode ddlPrivileges, const Acl* acl, Oid ownerId,
    Oid* grantorId, AclMode* grantOptions, AclMode* grantDdlOptions, bool isDbePerf, bool isPgCatalog)
{
    /* remove ddl privileges flag from Aclitem */
    ddlPrivileges = REMOVE_DDL_FLAG(ddlPrivileges);

    AclMode needed_goptions = ACL_GRANT_OPTION_FOR(privileges);
    AclMode ddl_needed_goptions = ACL_GRANT_OPTION_FOR(ddlPrivileges);
    List* roles_list = NIL;
    int nrights;
    ListCell* l = NULL;

    /*
     * The object owner is always treated as having all grant options, so if roleId is the owner it's easy.
     * Also, if roleId is a superuser it's easy: superusers are implicitly members of every role
     * except independent role, so they act as the object owner
     * (except independent role's objects and objects in schema dbe_perf).
     * For objects in schema dbe_perf, monitoradmin is always treated as having all grant options.
     */
    if (isDbePerf) {
        if (roleId == ownerId || roleId == INITIAL_USER_ID || isMonitoradmin(roleId)) {
            *grantorId = ownerId;
            *grantOptions = needed_goptions;
            *grantDdlOptions = ddl_needed_goptions;
            return;
        }
    } else if (isPgCatalog) {
        if (roleId == ownerId || roleId == INITIAL_USER_ID) {
            *grantorId = ownerId;
            *grantOptions = needed_goptions;
            *grantDdlOptions = ddl_needed_goptions;
            return;
        }
    } else {
        if (roleId == ownerId || (superuser_arg(roleId) && !is_role_independent(ownerId))) {
            *grantorId = ownerId;
            *grantOptions = needed_goptions;
            *grantDdlOptions = ddl_needed_goptions;
            return;
        }
    }

    /*
     * Otherwise we have to do a careful search to see if roleId has the
     * privileges of any suitable role.  Note: we can hang onto the result of
     * roles_has_privs_of() throughout this loop, because aclmask_direct()
     * doesn't query any role memberships.
     */
    roles_list = roles_has_privs_of(roleId);

    /* initialize candidate result as default */
    *grantorId = roleId;
    *grantOptions = ACL_NO_RIGHTS;
    *grantDdlOptions = ACL_NO_RIGHTS;
    nrights = 0;

    foreach (l, roles_list) {
        Oid otherrole = lfirst_oid(l);
        AclMode otherprivs;
        AclMode ddl_otherprivs;

        otherprivs = aclmask_direct(acl, otherrole, ownerId, needed_goptions, ACLMASK_ALL);
        ddl_otherprivs = aclmask_direct(acl, otherrole, ownerId, ADD_DDL_FLAG(ddl_needed_goptions), ACLMASK_ALL);
        if (otherprivs == needed_goptions && ddl_otherprivs == ddl_needed_goptions) {
            /* Found a suitable grantor */
            *grantorId = otherrole;
            *grantOptions = otherprivs;
            *grantDdlOptions = ddl_otherprivs;
            return;
        }

        /*
         * If it has just some of the needed privileges, remember best
         * candidate.
         */
        if (otherprivs != ACL_NO_RIGHTS || ddl_otherprivs != ACL_NO_RIGHTS) {
            int nnewrights = count_one_bits(otherprivs) + count_one_bits(ddl_otherprivs);

            if (nnewrights > nrights) {
                *grantorId = otherrole;
                *grantOptions = otherprivs;
                *grantDdlOptions = ddl_otherprivs;
                nrights = nnewrights;
            }
        }
    }
}

/*
 * get_role_oid - Given a role name, look up the role's OID.
 *
 * If missing_ok is false, throw an error if tablespace name not found.  If
 * true, just return InvalidOid.
 */
Oid get_role_oid(const char* rolname, bool missing_ok)
{
    Oid oid;

    /* Functions which use cache in clientauth need hold interrupts for safe. */
    HOLD_INTERRUPTS();
    oid = GetSysCacheOid1(AUTHNAME, CStringGetDatum(rolname));
    RESUME_INTERRUPTS();
    CHECK_FOR_INTERRUPTS();
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", rolname)));
    return oid;
}

/*
 * get_role_oid_or_public - As above, but return ACL_ID_PUBLIC if the
 *		role name is "public".
 */
static Oid get_role_oid_or_public(const char* rolname)
{
    if (strcmp(rolname, "public") == 0)
        return ACL_ID_PUBLIC;

    return get_role_oid(rolname, false);
}

/*
 * @Description: check whether role is independent role.
 * @in roleid : the role need to be check.
 * @return : true for independent and false for noindependent.
 */
bool is_role_independent(Oid roleid)
{
    HeapTuple rtup = NULL;
    bool isNull = false;
    bool flag = false;

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);

    TupleDesc pg_authid_dsc = RelationGetDescr(relation);

    /* Look up the information in pg_authid. */
    rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(rtup)) {
        /*
         * For upgrade reason,  we must get field value through heap_getattr function
         * although it is a char type value.
         */
        Datum authidrolkindDatum = heap_getattr(rtup, Anum_pg_authid_rolkind, pg_authid_dsc, &isNull);

        if (DatumGetChar(authidrolkindDatum) == ROLKIND_INDEPENDENT)
            flag = true;
        else
            flag = false;

        ReleaseSysCache(rtup);
    }

    heap_close(relation, AccessShareLock);

    return flag;
}

/*
 * @Description: check whether role is iamauth role whose password has been disabled.
 * @in roleid : the role need to be check.
 * @return : true for iamauth and false for noiamauth role.
 */
bool is_role_iamauth(Oid roleid)
{

    bool isNull = true;
    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);
    HeapTuple tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    Datum datum = BoolGetDatum(false);

    if (HeapTupleIsValid(tuple)) {
        datum = heap_getattr(tuple, Anum_pg_authid_rolpassword, RelationGetDescr(relation), &isNull);
        ReleaseSysCache(tuple);
    }
    heap_close(relation, AccessShareLock);

    return isNull;
}
