/* -------------------------------------------------------------------------
 *
 * user.h
 *	  Commands for manipulating roles (formerly called users).
 *
 *
 * src/include/commands/user.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef USER_H
#define USER_H

#include "nodes/parsenodes.h"
#include "utils/timestamp.h"

/* Hook to check passwords in CreateRole() and AlterRole() */
#define PASSWORD_TYPE_PLAINTEXT 0
#define PASSWORD_TYPE_MD5 1
typedef struct LockInfoBuck {
    ThreadId pid;
    Oid relation;
    Oid database;
    Oid nspoid;
} LockInfoBuck;

typedef struct AccountLockHashEntry {
    Oid roleoid;
    int4 failcount;
    TimestampTz locktime;
    int2 rolstatus;
    slock_t mutex;
} AccountLockHashEntry;

#define PASSWORD_TYPE_SHA256 2
#define PASSWORD_TYPE_SM3 3
#define UNLOCK_FLAG -1

/* status of the account */
typedef enum { UNLOCK_STATUS = 0, LOCK_STATUS, SUPERLOCK_STATUS } USER_STATUS;
typedef enum { UNEXPIRED_STATUS = 0, EXPIRED_STATUS } PASSWORD_STATUS;

typedef void (*check_password_hook_type)(
    const char* username, const char* password, int password_type, Datum validuntil_time, bool validuntil_null);

extern THR_LOCAL PGDLLIMPORT check_password_hook_type check_password_hook;

extern void CreateRole(CreateRoleStmt* stmt);
extern void AlterRole(AlterRoleStmt* stmt);
extern void AlterRoleSet(AlterRoleSetStmt* stmt);
extern void DropRole(DropRoleStmt* stmt);
extern void GrantRole(GrantRoleStmt* stmt);
extern void RenameRole(const char* oldname, const char* newname);
extern void DropOwnedObjects(DropOwnedStmt* stmt);
extern void ReassignOwnedObjects(ReassignOwnedStmt* stmt);
extern void TryLockAccount(Oid roleID, int extrafails, bool superlock);
extern bool TryUnlockAccount(Oid roleID, bool superunlock, bool isreset);
void TryUnlockAllAccounts(void);
extern USER_STATUS GetAccountLockedStatus(Oid roleID);
extern void DropUserStatus(Oid roleID);
extern Oid GetRoleOid(const char* username);
extern bool IsRoleExist(const char* username);
void CheckLockPrivilege(Oid roleID, HeapTuple tuple, bool is_opradmin);
extern bool is_role_persistence(Oid roleid);
void CheckAlterAuditadminPrivilege(Oid roleid, bool isOnlyAlterPassword);
extern char* GetRoleName(Oid rolid, char* rolname, size_t size);
extern char* GetSuperUserName(char* username);
extern int decode_iteration(const char* auth_iteration_string);
extern void initSqlCount();
extern void initWaitCount(Oid userid);
extern PASSWORD_STATUS GetAccountPasswordExpired(Oid roleID);

void ReportLockAccountMessage(bool locked, const char *rolename);
bool LockAccountParaValid(Oid roleID, int extrafails, bool superlock);
void UpdateFailCountToHashTable(Oid roleid, int extrafails, bool superlock);
bool UnlockAccountToHashTable(Oid roleid, bool superlock, bool isreset);
int64 SearchAllAccounts();
void InitAccountLockHashTable();
extern USER_STATUS GetAccountLockedStatusFromHashTable(Oid roleid);
extern void UpdateAccountInfoFromHashTable();

extern inline void str_reset(char* str)
{
    if (str != NULL) {
        memset(str, 0, strlen(str));
    }

    return;
}

#ifdef PGXC
extern void PreCleanAndCheckUserConns(const char* username, bool missing_ok);
#endif /* PGXC */

#endif /* USER_H */
