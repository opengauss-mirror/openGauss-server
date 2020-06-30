/* -------------------------------------------------------------------------
 *
 * security.c
 *	  Microsoft Windows Win32 Security Support Functions
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/common/backend/port/win32/security.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "securec.h"

static BOOL pgwin32_get_dynamic_tokeninfo(
    HANDLE token, TOKEN_INFORMATION_CLASS class, char** InfoBuffer, char* errbuf, int errsize);

/*
 * Returns nonzero if the current user has administrative privileges,
 * or zero if not.
 *
 * Note: this cannot use ereport() because it's called too early during
 * startup.
 */
int pgwin32_is_admin(void)
{
    HANDLE AccessToken = NULL;
    char* InfoBuffer = NULL;
    char errbuf[256];
    PTOKEN_GROUPS Groups;
    PSID AdministratorsSid;
    PSID PowerUsersSid;
    SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
    UINT x;
    BOOL success = false;

    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_READ, &AccessToken)) {
        write_stderr("could not open process token: error code %lu\n", GetLastError());
        exit(1);
    }

    if (!pgwin32_get_dynamic_tokeninfo(AccessToken, TokenGroups, &InfoBuffer, errbuf, sizeof(errbuf))) {
        write_stderr("%s", errbuf);
        exit(1);
    }

    Groups = (PTOKEN_GROUPS)InfoBuffer;

    CloseHandle(AccessToken);

    if (!AllocateAndInitializeSid(&NtAuthority,
        2,
        SECURITY_BUILTIN_DOMAIN_RID,
        DOMAIN_ALIAS_RID_ADMINS,
        0,
        0,
        0,
        0,
        0,
        0,
        &AdministratorsSid)) {
        write_stderr("could not get SID for Administrators group: error code %lu\n", GetLastError());
        exit(1);
    }

    if (!AllocateAndInitializeSid(&NtAuthority,
        2,
        SECURITY_BUILTIN_DOMAIN_RID,
        DOMAIN_ALIAS_RID_POWER_USERS,
        0,
        0,
        0,
        0,
        0,
        0,
        &PowerUsersSid)) {
        write_stderr("could not get SID for PowerUsers group: error code %lu\n", GetLastError());
        exit(1);
    }

    success = FALSE;

    for (x = 0; x < Groups->GroupCount; x++) {
        if ((EqualSid(AdministratorsSid, Groups->Groups[x].Sid) && (Groups->Groups[x].Attributes & SE_GROUP_ENABLED)) ||
            (EqualSid(PowerUsersSid, Groups->Groups[x].Sid) && (Groups->Groups[x].Attributes & SE_GROUP_ENABLED))) {
            success = TRUE;
            break;
        }
    }

    free(InfoBuffer);
    InfoBuffer = NULL;
    FreeSid(AdministratorsSid);
    FreeSid(PowerUsersSid);
    return success;
}

/*
 * We consider ourselves running as a service if one of the following is
 * true:
 *
 * 1) We are running as Local System (only used by services)
 * 2) Our token contains SECURITY_SERVICE_RID (automatically added to the
 *    process token by the SCM when starting a service)
 *
 * Return values:
 *    0 : Not service
 *    1 : Service
 *    -1 : Error
 *
 * Note: we can't report errors via either ereport (we're called too early)
 * or write_stderr (because that calls this).  We are therefore reduced to
 * writing directly on stderr, which sucks, but we have few alternatives.
 */
int pgwin32_is_service(void)
{
    static int _is_service = -1;
    HANDLE AccessToken = NULL;
    char* InfoBuffer = NULL;
    char errbuf[256];
    PTOKEN_GROUPS Groups;
    PTOKEN_USER User;
    PSID ServiceSid;
    PSID LocalSystemSid;
    SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
    UINT x;

    /* Only check the first time */
    if (_is_service != -1) {
        return _is_service;
    }

    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_READ, &AccessToken)) {
        fprintf(stderr, "could not open process token: error code %lu\n", GetLastError());
        return -1;
    }

    /* First check for local system */
    if (!pgwin32_get_dynamic_tokeninfo(AccessToken, TokenUser, &InfoBuffer, errbuf, sizeof(errbuf))) {
        fprintf(stderr, "%s", errbuf);
        return -1;
    }

    User = (PTOKEN_USER)InfoBuffer;

    if (!AllocateAndInitializeSid(&NtAuthority, 1, SECURITY_LOCAL_SYSTEM_RID, 0, 0, 0, 0, 0, 0, 0, &LocalSystemSid)) {
        fprintf(stderr, "could not get SID for local system account\n");
        free(InfoBuffer);
        InfoBuffer = NULL;
        CloseHandle(AccessToken);
        return -1;
    }

    if (EqualSid(LocalSystemSid, User->User.Sid)) {
        FreeSid(LocalSystemSid);
        free(InfoBuffer);
        InfoBuffer = NULL;
        CloseHandle(AccessToken);
        _is_service = 1;
        return _is_service;
    }

    FreeSid(LocalSystemSid);
    free(InfoBuffer);
    InfoBuffer = NULL;

    /* Now check for group SID */
    if (!pgwin32_get_dynamic_tokeninfo(AccessToken, TokenGroups, &InfoBuffer, errbuf, sizeof(errbuf))) {
        fprintf(stderr, "%s", errbuf);
        return -1;
    }

    Groups = (PTOKEN_GROUPS)InfoBuffer;

    if (!AllocateAndInitializeSid(&NtAuthority, 1, SECURITY_SERVICE_RID, 0, 0, 0, 0, 0, 0, 0, &ServiceSid)) {
        fprintf(stderr, "could not get SID for service group\n");
        free(InfoBuffer);
        InfoBuffer = NULL;
        CloseHandle(AccessToken);
        return -1;
    }

    _is_service = 0;
    for (x = 0; x < Groups->GroupCount; x++) {
        if (EqualSid(ServiceSid, Groups->Groups[x].Sid)) {
            _is_service = 1;
            break;
        }
    }

    free(InfoBuffer);
    InfoBuffer = NULL;
    FreeSid(ServiceSid);

    CloseHandle(AccessToken);

    return _is_service;
}

/*
 * Call GetTokenInformation() on a token and return a dynamically sized
 * buffer with the information in it. This buffer must be free():d by
 * the calling function!
 */
static BOOL pgwin32_get_dynamic_tokeninfo(
    HANDLE token, TOKEN_INFORMATION_CLASS class, char** InfoBuffer, char* errbuf, int errsize)
{
    DWORD InfoBufferSize;
    int rc = 0;

    if (GetTokenInformation(token, class, NULL, 0, &InfoBufferSize)) {
        rc = snprintf_s(errbuf, errsize, errsize - 1, "could not get token information: got zero size\n");
        securec_check_ss(rc, "\0", "\0");
        return FALSE;
    }

    if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        rc = snprintf_s(
            errbuf, errsize, errsize - 1, "could not get token information: error code %lu\n", GetLastError());
        securec_check_ss(rc, "\0", "\0");
        return FALSE;
    }

    if (InfoBufferSize == 0) {
        return FALSE;
    }
    *InfoBuffer = malloc(InfoBufferSize);
    if (*InfoBuffer == NULL) {
        rc = snprintf_s(
            errbuf, errsize, errsize - 1, "could not allocate %d bytes for token information\n", (int)InfoBufferSize);
        securec_check_ss(rc, "\0", "\0");
        return FALSE;
    }

    if (!GetTokenInformation(token, class, *InfoBuffer, InfoBufferSize, &InfoBufferSize)) {
        free(*InfoBuffer);
        *InfoBuffer = NULL;
        rc = snprintf_s(
            errbuf, errsize, errsize - 1, "could not get token information: error code %lu\n", GetLastError());
        securec_check_ss(rc, "\0", "\0");
        return FALSE;
    }

    return TRUE;
}
