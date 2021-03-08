/* -------------------------------------------------------------------------
 *
 * auth.cpp
 *	  Routines to handle network authentication
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/libpq/auth.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/param.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h>

#include "libpq/auth.h"
#include "libpq/crypt.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/md5.h"
#include "libpq/sha2.h"
#include "miscadmin.h"
#include "auditfuncs.h"
#include "replication/walsender.h"
#include "replication/datasender.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "commands/user.h"
#include "utils/guc.h"
#include "utils/syscall_lock.h"
#include "utils/inval.h"
#include "access/xact.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "portability/instr_time.h"
#include "postmaster/postmaster.h"
#include "utils/acl.h"
#include "gs_policy/policy_common.h"

#include "cipher.h"
#include "openssl/rand.h"
#include "pgstat.h"

extern bool dummyStandbyMode;
extern GlobalNodeDefinition* global_node_definition;

/* ----------------------------------------------------------------
 * Global authentication functions
 * ----------------------------------------------------------------
 */
static void sendAuthRequest(Port* port, AuthRequest areq);
static void auth_failed(Port* port, int status);
static char* recv_password_packet(Port* port);
static int recv_and_check_password_packet(Port* port);
static void clear_gss_info(pg_gssinfo* gss);

/* ----------------------------------------------------------------
 * Ident authentication
 * ----------------------------------------------------------------
 */
/* Max size of username ident server can return */
#define IDENT_USERNAME_MAX 512

/* Standard TCP port number for Ident service.	Assigned by IANA */
#define IDENT_PORT 113

#define CLEAR_AND_FREE_PASSWORD(passwd, len) \
    do {\
        int _rc = memset_s(passwd, len, 0, len);\
        securec_check(_rc, "", "");\
        pfree(passwd);\
        (passwd) = NULL;\
    } while (0)

#ifdef USE_IDENT
static int ident_inet(hbaPort* port);
#endif

#ifdef HAVE_UNIX_SOCKETS
static int auth_peer(hbaPort* port);
#endif

/* ----------------------------------------------------------------
 * PAM authentication
 * ----------------------------------------------------------------
 */
#ifdef USE_PAM
#ifdef HAVE_PAM_PAM_APPL_H
#include <pam/pam_appl.h>
#endif
#ifdef HAVE_SECURITY_PAM_APPL_H
#include <security/pam_appl.h>
#endif

#define PGSQL_PAM_SERVICE "postgresql" /* Service name passed to PAM */

static int CheckPAMAuth(Port* port, char* user, char* password);
static int pam_passwd_conv_proc(
    int num_msg, const struct pam_message** msg, struct pam_response** resp, void* appdata_ptr);

static struct pam_conv pam_passw_conv = {&pam_passwd_conv_proc, NULL};

#endif /* USE_PAM */

/* ----------------------------------------------------------------
 * LDAP authentication
 * ----------------------------------------------------------------
 */
#ifdef USE_LDAP
#ifndef WIN32
/* We use a deprecated function to keep the codepath the same as win32. */
#define LDAP_DEPRECATED 1
#include <ldap.h>
#else
#include <winldap.h>

/* Correct header from the Platform SDK */
typedef ULONG (*__ldap_start_tls_sA)(IN PLDAP ExternalHandle, OUT PULONG ServerReturnValue, OUT LDAPMessage** result,
    IN PLDAPControlA* ServerControls, IN PLDAPControlA* ClientControls);
#endif

static int CheckLDAPAuth(Port* port);
#endif /* USE_LDAP */

/* ----------------------------------------------------------------
 * Cert authentication
 * ----------------------------------------------------------------
 */
#ifdef USE_SSL
static int CheckCertAuth(Port* port);
#endif

static int CheckIAMAuth(Port* port);

/* ----------------------------------------------------------------
 * MIT Kerberos authentication system - protocol version 5
 * ----------------------------------------------------------------
 */
#ifdef KRB5
static int pg_krb5_recvauth(Port* port);

#include <krb5.h>
/* Some old versions of Kerberos do not include <com_err.h> in <krb5.h> */
#if !defined(__COM_ERR_H) && !defined(__COM_ERR_H__)
#include <com_err.h>
#endif
#endif /* KRB5 */

/* ----------------------------------------------------------------
 * GSSAPI Authentication
 * ----------------------------------------------------------------
 */
#ifdef ENABLE_GSS
#if defined(HAVE_GSSAPI_H)
#include <gssapi.h>
#else
#include <gssapi/gssapi.h>
#endif

static int pg_GSS_recvauth(Port* port);
#endif /* ENABLE_GSS */

/* ----------------------------------------------------------------
 * SSPI Authentication
 * ----------------------------------------------------------------
 */
#ifdef ENABLE_SSPI
typedef SECURITY_STATUS(WINAPI* QUERY_SECURITY_CONTEXT_TOKEN_FN)(PCtxtHandle, void**);
static int pg_SSPI_recvauth(Port* port);
#endif

/*
 * Maximum accepted size of GSS and SSPI authentication tokens.
 *
 * Kerberos tickets are usually quite small, but the TGTs issued by Windows
 * domain controllers include an authorization field known as the Privilege
 * Attribute Certificate (PAC), which contains the user's Windows permissions
 * (group memberships etc.). The PAC is copied into all tickets obtained on
 * the basis of this TGT (even those issued by Unix realms which the Windows
 * realm trusts), and can be several kB in size. The maximum token size
 * accepted by Windows systems is determined by the MaxAuthToken Windows
 * registry setting. Microsoft recommends that it is not set higher than
 * 65535 bytes, so that seems like a reasonable limit for us as well.
 */
#define PG_MAX_AUTH_TOKEN_LENGTH 65535

/* ----------------------------------------------------------------
 * Global authentication functions
 * ----------------------------------------------------------------
 */

/*
 * This hook allows plugins to get control following client authentication,
 * but before the user has been informed about the results.  It could be used
 * to record login events, insert a delay after failed authentication, etc.
 */
THR_LOCAL ClientAuthentication_hook_type ClientAuthentication_hook = NULL;

/*
 * Tell the user the authentication failed, but not (much about) why.
 *
 * There is a tradeoff here between security concerns and making life
 * unnecessarily difficult for legitimate users.  We would not, for example,
 * want to report the password we were expecting to receive...
 * But it seems useful to report the username and authorization method
 * in use, and these are items that must be presumed known to an attacker
 * anyway.
 * Note that many sorts of failure report additional information in the
 * postmaster log, which we hope is only readable by good guys.
 */
static void auth_failed(Port* port, int status)
{
    const char* errstr = NULL;
    int errcode_return = ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION;
    /* Database Security: Support database audit */
    char details[PGAUDIT_MAXLENGTH] = {0};
    errno_t rc = EOK;
    /*
     * If we failed due to EOF from client, just quit; there's no point in
     * trying to send a message to the client, and not much point in logging
     * the failure in the postmaster log.  (Logging the failure might be
     * desirable, were it not for the fact that libpq closes the connection
     * unceremoniously if challenged for a password when it hasn't got one to
     * send.  We'll get a useless log entry for every psql connection under
     * password auth, even if it's perfectly successful, if we log STATUS_EOF
     * events.)
     */
    if (status == STATUS_EOF)
        proc_exit(0);

    if (status == STATUS_EXPIRED) {
        errstr = gettext_noop("The account is not within the period of validity.");
    } else {
        switch (port->hba->auth_method) {
            case uaReject:
            case uaImplicitReject:
                errstr = gettext_noop("authentication failed for user \"%s\": host rejected");
                break;
            case uaKrb5:
                errstr = gettext_noop("Kerberos 5 authentication failed for user \"%s\"");
                break;
            case uaTrust:
                errstr = gettext_noop("\"trust\" authentication failed for user \"%s\"");
                break;
            case uaIdent:
                errstr = gettext_noop("Ident authentication failed for user \"%s\"");
                break;
            case uaPeer:
                errstr = gettext_noop("Peer authentication failed for user \"%s\"");
                break;
            case uaMD5:
            case uaSHA256:
            case uaIAM:
                errstr = gettext_noop("Invalid username/password,login denied.");
                /* We use it to indicate if a .pgpass password failed. */
                errcode_return = ERRCODE_INVALID_PASSWORD;
                break;
            case uaGSS:
                errstr = gettext_noop("GSSAPI authentication failed for user \"%s\"");
                break;
            case uaSSPI:
                errstr = gettext_noop("SSPI authentication failed for user \"%s\"");
                break;
            case uaPAM:
                errstr = gettext_noop("PAM authentication failed for user \"%s\"");
                break;
            case uaLDAP:
                errstr = gettext_noop("LDAP authentication failed for user \"%s\"");
                break;
            case uaCert:
                errstr = gettext_noop("certificate authentication failed for user \"%s\"");
                break;
            default:
                errstr = gettext_noop("authentication failed for user \"%s\": invalid authentication method");
                break;
        }
    }
    /* Database Security: Support database audit */
    rc = snprintf_s(details,
        PGAUDIT_MAXLENGTH,
        PGAUDIT_MAXLENGTH - 1,
        "login db(%s)failed,authentication for user(%s)failed",
        port->database_name,
        port->user_name);
    securec_check_ss(rc, "\0", "\0");


    /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
    if (!(g_instance.status > NoShutdown) && user_login_hook) {
        user_login_hook(port->database_name, port->user_name, false, true);
	}
    pgaudit_user_login(FALSE, port->database_name, details);
    ereport(FATAL, (errcode(errcode_return), errmsg(errstr, port->user_name)));
    /* doesn't return */
}

/*
 * Is dummyStandby or walsender but not for logicaldecoding
 */
bool IsDSorHaWalSender()
{
    return (dummyStandbyMode == true || (AM_WAL_SENDER && AM_WAL_DB_SENDER == false));
}

bool isRemoteInitialUser(Port* port)
{
    return (strcmp(port->remote_host, "[local]") != 0 && !IsLoopBackAddr(port) &&
            get_role_oid(port->user_name, true) == INITIAL_USER_ID);
}

/*
 * Client authentication starts here.  If there is an error, this
 * function does not return and the backend process is terminated.
 */
void ClientAuthentication(Port* port)
{
    int status = STATUS_ERROR;
    /* Database Security: Support password complexity */
    char details[PGAUDIT_MAXLENGTH] = {0};
    char token[TOKEN_LENGTH + 1] = {0};
    errno_t rc = EOK;
    int retval = 0;

    /*
     * Get the authentication method to use for this frontend/database
     * combination.  Note: we do not parse the file at this point; this has
     * already been done elsewhere.  hba.c dropped an error message into the
     * server logfile if parsing the hba config file failed.
     */
    hba_getauthmethod(port);

    /*
     * Enable immediate response to SIGTERM/SIGINT/timeout interrupts. (We
     * don't want this during hba_getauthmethod() because it might have to do
     * database access, eg for role membership checks.)
     */
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    /* And don't forget to detect one that already arrived */
    CHECK_FOR_INTERRUPTS();

    /*
     * This is the first point where we have access to the hba record for the
     * current connection, so perform any verifications based on the hba
     * options field that should be done *before* the authentication here.
     */
    if (port->hba->clientcert) {
        /*
         * When we parse pg_hba.conf, we have already made sure that we have
         * been able to load a certificate store. Thus, if a certificate is
         * present on the client, it has been verified against our root
         * certificate store, and the connection would have been aborted
         * already if it didn't verify ok.
         */
#ifdef USE_SSL
        if (port->peer == NULL) {
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                    errmsg("connection requires a valid client certificate")));
        }
#else

        /*
         * hba.c makes sure hba->clientcert can't be set unless OpenSSL is
         * present.
         */
        Assert(false);
#endif
    }

    /*
     * To prevent external applications from being spoofing coordinators, 
     * here we check auth_method from coordinators. Connection from coor-
     * dinators must use trust auth method or Kerberos(uaGss) auth method.
     */
    if (IS_PGXC_DATANODE && IsConnFromCoord()) {
#ifdef ENABLE_GSS
        if (port->hba->auth_method != uaTrust && port->hba->auth_method != uaGSS) {
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                        errmsg("Connection from CN must use trust or gss auth method.")));
        }
#else
        if (port->hba->auth_method != uaTrust) {
            ereport(FATAL,
                    (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                        errmsg("Connection from CN must use trust auth method.")));
        }
#endif
    }
    /*
     * Now proceed to do the actual authentication check
     */
    switch (port->hba->auth_method) {
        case uaReject:

            /*
             * An explicit "reject" entry in pg_hba.conf.  This report exposes
             * the fact that there's an explicit reject entry, which is
             * perhaps not so desirable from a security standpoint; but the
             * message for an implicit reject could confuse the DBA a lot when
             * the true situation is a match to an explicit reject.  And we
             * don't want to change the message for an implicit reject.  As
             * noted below, the additional information shown here doesn't
             * expose anything not known to an attacker.
             */
            {
                char hostinfo[NI_MAXHOST];
                /* 
                 * Audit user login
                 * it's unsafe to deal with plugins hooks as dynamic lib may be released 
                 */
                if (!(g_instance.status > NoShutdown) && user_login_hook) {
                    user_login_hook(port->database_name, port->user_name, false, true);
                }
                rc = snprintf_s(details,
                    PGAUDIT_MAXLENGTH,
                    PGAUDIT_MAXLENGTH - 1,
                    "login db(%s)failed,pg_hba.conf rejects connection for user(%s)",
                    port->database_name,
                    port->user_name);
                securec_check_ss(rc, "\0", "\0");
                pgaudit_user_login(FALSE, port->database_name, details);

                (void)pg_getnameinfo_all(
                    &port->raddr.addr, port->raddr.salen, hostinfo, sizeof(hostinfo), NULL, 0, NI_NUMERICHOST);
                if (AM_WAL_SENDER) {
#ifdef USE_SSL
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("pg_hba.conf rejects replication connection for host \"%s\", user \"%s\", %s",
                                hostinfo,
                                port->user_name,
                                port->ssl ? _("SSL on") : _("SSL off"))));
#else
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("pg_hba.conf rejects replication connection for host \"%s\", user \"%s\"",
                                hostinfo,
                                port->user_name)));
#endif
                } else {
#ifdef USE_SSL
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("pg_hba.conf rejects connection for host \"%s\", user \"%s\", database \"%s\", %s",
                                hostinfo,
                                port->user_name,
                                port->database_name,
                                port->ssl ? _("SSL on") : _("SSL off"))));
#else
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("pg_hba.conf rejects connection for host \"%s\", user \"%s\", database \"%s\"",
                                hostinfo,
                                port->user_name,
                                port->database_name)));
#endif
                }
                break;
            }

        case uaImplicitReject:

            /*
             * No matching entry, so tell the user we fell through.
             *
             * NOTE: the extra info reported here is not a security breach,
             * because all that info is known at the frontend and must be
             * assumed known to bad guys.  We're merely helping out the less
             * clueful good guys.
             */
            {
                char hostinfo[NI_MAXHOST];

                (void)pg_getnameinfo_all(
                    &port->raddr.addr, port->raddr.salen, hostinfo, sizeof(hostinfo), NULL, 0, NI_NUMERICHOST);

#define HOSTNAME_LOOKUP_DETAIL(port)                                                                                   \
    ((port)->remote_hostname                                                                                           \
            ? (((port)->remote_hostname_resolv == +1)                                                                  \
                      ? errdetail_log(                                                                                 \
                            "Client IP address resolved to \"%s\", forward lookup matches.", (port)->remote_hostname)  \
                      : (((port)->remote_hostname_resolv == 0)                                                         \
                                ? errdetail_log("Client IP address resolved to \"%s\", forward lookup not checked.",   \
                                      (port)->remote_hostname)                                                         \
                                : (((port)->remote_hostname_resolv == -1)                                              \
                                          ? errdetail_log("Client IP address resolved to \"%s\", forward lookup does " \
                                                          "not match.",                                                \
                                                (port)->remote_hostname)                                               \
                                          : 0)))                                                                       \
            : 0)

                if (AM_WAL_SENDER) {
#ifdef USE_SSL
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("no pg_hba.conf entry for replication connection from host \"%s\", user \"%s\", %s",
                                hostinfo,
                                port->user_name,
                                port->ssl ? _("SSL on") : _("SSL off")),
                            HOSTNAME_LOOKUP_DETAIL(port)));
#else
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("no pg_hba.conf entry for replication connection from host \"%s\", user \"%s\"",
                                hostinfo,
                                port->user_name),
                            HOSTNAME_LOOKUP_DETAIL(port)));
#endif
                } else {
#ifdef USE_SSL
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("no pg_hba.conf entry for host \"%s\", user \"%s\", database \"%s\", %s",
                                hostinfo,
                                port->user_name,
                                port->database_name,
                                port->ssl ? _("SSL on") : _("SSL off")),
                            HOSTNAME_LOOKUP_DETAIL(port)));
#else
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("no pg_hba.conf entry for host \"%s\", user \"%s\", database \"%s\"",
                                hostinfo,
                                port->user_name,
                                port->database_name),
                            HOSTNAME_LOOKUP_DETAIL(port)));
#endif
                }
                break;
            }

        case uaKrb5:
#ifdef KRB5
            sendAuthRequest(port, AUTH_REQ_KRB5);
            status = pg_krb5_recvauth(port);
#else
            Assert(false);
#endif
            break;

        case uaGSS:
#ifdef ENABLE_GSS
            sendAuthRequest(port, AUTH_REQ_GSS);
            status = pg_GSS_recvauth(port);
#else
            Assert(false);
#endif
            break;

        case uaSSPI:
#ifdef ENABLE_SSPI
            sendAuthRequest(port, AUTH_REQ_SSPI);
            status = pg_SSPI_recvauth(port);
#else
            Assert(false);
#endif
            break;

        case uaPeer:
#ifdef HAVE_UNIX_SOCKETS
            status = auth_peer(port);
#else
            Assert(false);
#endif
            break;

        case uaIdent:
#ifdef USE_IDENT
            status = ident_inet(port);
#else
            Assert(false);
#endif
            break;

        case uaMD5:
            /*Forbid remote connection with initial user.*/
            if (isRemoteInitialUser(port)) {
                ereport(FATAL,
                    (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                        errmsg("Forbid remote connection with initial user.")));
            }
            sendAuthRequest(port, AUTH_REQ_MD5);
            status = recv_and_check_password_packet(port);
            break;
        /* Database Security:  Support SHA256.*/
        case uaSHA256:
            /*Forbid remote connection with initial user.*/
            if (isRemoteInitialUser(port)) {
                ereport(FATAL,
                    (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                        errmsg("Forbid remote connection with initial user.")));
            }

            rc = memset_s(port->token, TOKEN_LENGTH * 2 + 1, 0, TOKEN_LENGTH * 2 + 1);
            securec_check(rc, "\0", "\0");
            /* Functions which alloc memory need hold interrupts for safe. */
            HOLD_INTERRUPTS();
            retval = RAND_priv_bytes((GS_UCHAR*)token, (GS_UINT32)TOKEN_LENGTH);
            RESUME_INTERRUPTS();
            CHECK_FOR_INTERRUPTS();
            if (retval != 1) {
                ereport(ERROR, (errmsg("Failed to Generate the random number,errcode:%d", retval)));
            }
            sha_bytes_to_hex8((uint8*)token, port->token);
            port->token[TOKEN_LENGTH * 2] = '\0';
            sendAuthRequest(port, AUTH_REQ_SHA256);
            status = recv_and_check_password_packet(port);
            break;
        case uaPAM:
#ifdef USE_PAM
            status = CheckPAMAuth(port, port->user_name, "");
#else
            Assert(false);
#endif /* USE_PAM */
            break;

        case uaLDAP:
#ifdef USE_LDAP
            status = CheckLDAPAuth(port);
#else
            Assert(false);
#endif
            break;

        case uaCert:
#ifdef USE_SSL
            status = CheckCertAuth(port);
#else
            Assert(false);
#endif
            break;
        case uaIAM:
            status = CheckIAMAuth(port);
            break;
        case uaTrust:
            status = STATUS_OK;
            break;
        default:
            // default is rdundance.
            break;
    }

    /* Database Security: Support lock/unlock account */
    if (!AM_WAL_SENDER) {
        /*
         * Disable immediate response to SIGTERM/SIGINT/timeout interrupts as there are
         * some cache and memory operations which can not be interrupted. And nothing will
         * block here, so disable the interrupts is ok.
         */
        t_thrd.int_cxt.ImmediateInterruptOK = false;

        /* We will not check lock status for initial user. */
        if (IsRoleExist(port->user_name) && GetRoleOid(port->user_name) != INITIAL_USER_ID) {
            Oid roleid = GetRoleOid(port->user_name);
            USER_STATUS rolestatus;
            if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                rolestatus = GetAccountLockedStatusFromHashTable(roleid);
            } else {
                rolestatus = GetAccountLockedStatus(roleid);
            }
            if (UNLOCK_STATUS != rolestatus) {
                errno_t errorno = EOK;
                bool unlocked = false;
                if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                    unlocked = UnlockAccountToHashTable(roleid, false, false);
                } else {
                    unlocked = TryUnlockAccount(roleid, false, false);
                }
                if (!unlocked && status != STATUS_EOF) { 
                    /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
                    if (!(g_instance.status > NoShutdown) && user_login_hook) {
                        user_login_hook(port->database_name, port->user_name, false, true);
                    }
                    errorno = snprintf_s(details,
                        PGAUDIT_MAXLENGTH,
                        PGAUDIT_MAXLENGTH - 1,
                        "login db(%s)failed,the account(%s)has been locked",
                        port->database_name,
                        port->user_name);
                    securec_check_ss(errorno, "\0", "\0");
                    pgaudit_user_login(FALSE, port->database_name, details);

                    /* Show locked errror messages when the account has been locked. */
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION), errmsg("The account has been locked.")));
                }
            } else if (status == STATUS_OK) {
                if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                    (void)UnlockAccountToHashTable(roleid, false, true);
                } else {
                    (void)TryUnlockAccount(roleid, false, true);
                }
            }

            /* if password is not right, send signal to try lock the account*/
            if (status == STATUS_WRONG_PASSWORD) {
                if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
                    UpdateFailCountToHashTable(roleid, 1, false);
                } else {
                    TryLockAccount(roleid, 1, false);
                }
            }

            if (status == STATUS_OK) {
                status = CheckUserValid(port, port->user_name);
            }
        }

        /* Reset t_thrd.int_cxt.ImmediateInterruptOK and check for interrupt here. */
        t_thrd.int_cxt.ImmediateInterruptOK = true;
    }

    if (ClientAuthentication_hook)
        (*ClientAuthentication_hook)(port, status);

    if (status == STATUS_OK)
        sendAuthRequest(port, AUTH_REQ_OK);
    else {
        auth_failed(port, status);
    }
    /* Done with authentication, so we should turn off immediate interrupts */
    t_thrd.int_cxt.ImmediateInterruptOK = false;
}

/*
 * Generate a string of fake salt bytes as user is not existed.
 */
void GenerateFakeSaltBytes(const char* user_name, char* fake_salt_bytes, int salt_len)
{
    SHA256_CTX2 ctx;
    int combine_string_length = POSTFIX_LENGTH + strlen(user_name);
    char *combine_string = (char*)palloc0(sizeof(char) * (combine_string_length + 1));
    char postfix[POSTFIX_LENGTH + 1] = {0};
    char superuser_name[NAMEDATALEN] = {0};
    unsigned char buf[SHA256_DIGEST_LENGTH] = {0};
    char encrypt_string[ENCRYPTED_STRING_LENGTH + 1] = {0};
    int32 superuser_stored_method;
    errno_t rc = EOK;
    int rcs = 0;

    (void)GetSuperUserName((char*)superuser_name);
    superuser_stored_method = get_password_stored_method(superuser_name, encrypt_string, ENCRYPTED_STRING_LENGTH + 1);
    if (superuser_stored_method == SHA256_PASSWORD || superuser_stored_method == COMBINED_PASSWORD) {
        rc = strncpy_s(postfix, POSTFIX_LENGTH + 1, encrypt_string + SALT_STRING_LENGTH, POSTFIX_LENGTH);
        securec_check(rc, "\0", "\0");
    }

    rcs = snprintf_s(combine_string, combine_string_length + 1, combine_string_length, "%s%s", user_name, postfix);
    securec_check_ss(rcs, "\0", "\0");
    SHA256_Init2(&ctx);
    SHA256_Update2(&ctx, (const uint8*)combine_string, combine_string_length);
    SHA256_Final2(buf, &ctx);
    rc = memcpy_s(fake_salt_bytes, salt_len, buf, (salt_len < SHA256_DIGEST_LENGTH) ? salt_len : SHA256_DIGEST_LENGTH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(encrypt_string, ENCRYPTED_STRING_LENGTH + 1, 0, ENCRYPTED_STRING_LENGTH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(combine_string, combine_string_length + 1, 0, combine_string_length);
    securec_check(rc, "\0", "\0");
    pfree(combine_string);
}

/*
 * Send an authentication request packet to the frontend.
 */
static void sendAuthRequest(Port* port, AuthRequest areq)
{
    /* Database Security:  Support SHA256.*/
    int32 stored_method = 0;
    StringInfoData buf;
    char encrypt_string[ENCRYPTED_STRING_LENGTH + 1] = {0};
    char token[TOKEN_LENGTH + 1] = {0};
    char sever_key_string[HMAC_LENGTH * 2 + 1] = {0};
    char sever_key[HMAC_LENGTH + 1] = {0};
    char sever_signature[HMAC_LENGTH + 1] = {0};
    char sever_signature_string[HMAC_LENGTH * 2 + 1] = {0};
    int sever_signature_length = HMAC_LENGTH;
#ifdef USE_ASSERT_CHECKING
    GS_UINT32 CRYPT_hmac_result;
#endif
    char salt[SALT_LENGTH * 2 + 1] = {0};
    char fake_salt_bytes[SALT_LENGTH + 1] = {0};
    char fake_salt[SALT_LENGTH * 2 + 1] = {0};
    char fake_serverkey_bytes[HMAC_LENGTH + 1] = {0};
    char fake_serverkey[HMAC_LENGTH * 2 + 1] = {0};
    char fake_storedkey_bytes[STORED_KEY_LENGTH + 1] = {0};
    char fake_storedkey[STORED_KEY_LENGTH * 2 + 1] = {0};
    int auth_iteration_count = 0;
    errno_t rc = EOK;
    bool save_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

    /*
     * Disable immediate response to SIGTERM/SIGINT/timeout interrupts as there are
     * some cache and memory operations which can not be interrupted. We just set
     * t_thrd.int_cxt.ImmediateInterruptOK to true in some block condition and reset it at the end.
     */
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    
#ifdef ENABLE_MULTIPLE_NODES
    if (IsDSorHaWalSender()) {
#else        
    if (IsDSorHaWalSender() && is_node_internal_connection(port)) {
#endif        
        stored_method = SHA256_PASSWORD;
    } else {
        if (!IsRoleExist(port->user_name)) {
            int retval = 0;

            /*
             * When login failed, let the server quit at the same place regardless of right or wrong
             * username. We construct a fake encrypted password here and send it the client.
             */
            GenerateFakeSaltBytes(port->user_name, fake_salt_bytes, SALT_LENGTH);
            retval = RAND_priv_bytes((GS_UCHAR*)fake_serverkey_bytes, (GS_UINT32)(HMAC_LENGTH));
            if (retval != 1) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                        errmsg("Failed to Generate the random serverkey,errcode:%d", retval)));
            }
            retval = RAND_priv_bytes((GS_UCHAR*)fake_storedkey_bytes, (GS_UINT32)(STORED_KEY_LENGTH));
            if (retval != 1) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                        errmsg("Failed to Generate the random storedkey,errcode:%d", retval)));
            }
            sha_bytes_to_hex64((uint8*)fake_salt_bytes, fake_salt);
            sha_bytes_to_hex64((uint8*)fake_serverkey_bytes, fake_serverkey);
            sha_bytes_to_hex64((uint8*)fake_storedkey_bytes, fake_storedkey);
            rc = snprintf_s(encrypt_string,
                ENCRYPTED_STRING_LENGTH + 1,
                ENCRYPTED_STRING_LENGTH,
                "%s%s%s",
                fake_salt,
                fake_serverkey,
                fake_storedkey);
            securec_check_ss(rc, "\0", "\0");

            /* Construct the stored method. */
            if (PG_PROTOCOL_MINOR(FrontendProtocol) < PG_PROTOCOL_GAUSS_BASE) {
                stored_method = MD5_PASSWORD;
            } else {
                stored_method = SHA256_PASSWORD;
            }
        } else {
            /* Only need to send the necessary information here when use the iamauth role for authenication. */
            if (IS_PGXC_COORDINATOR && IsConnFromApp()) {
                bool is_passwd_disable = is_role_iamauth(GetRoleOid(port->user_name));

                if (port->hba->auth_method == uaIAM && is_passwd_disable) {
                    pq_beginmessage(&buf, 'R');
                    pq_sendint32(&buf, (int32)areq);

                    /* Keep block function pq_endmessage and pq_flush can be interrupted. */
                    t_thrd.int_cxt.ImmediateInterruptOK = true;
                    pq_endmessage(&buf);
                    pq_flush();
                    t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
                    return;
                }

#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)

                /*
                 * Add the authentication data for the next step of the GSSAPI or SSPI
                 * negotiation.
                 */
                if (port->hba->auth_method == uaGSS && GetRoleOid(port->user_name) != INITIAL_USER_ID) {
                    if (is_passwd_disable) {
                        if (areq == AUTH_REQ_GSS_CONT) {
                            if (port->gss->outbuf.length > 0) {
                                elog(DEBUG4, "sending GSS token of length %u", (unsigned int)port->gss->outbuf.length);
                                pq_beginmessage(&buf, 'R');
                                pq_sendint32(&buf, (int32)areq);
                                pq_sendbytes(&buf, (char*)port->gss->outbuf.value, port->gss->outbuf.length);
                                t_thrd.int_cxt.ImmediateInterruptOK = true;
                                pq_endmessage(&buf);
                            }
                        } else if (areq == AUTH_REQ_GSS || areq == AUTH_REQ_OK) {
                            pq_beginmessage(&buf, 'R');
                            pq_sendint32(&buf, (int32)areq);
                            /* Keep block function pq_endmessage and pq_flush can be interrupted. */
                            t_thrd.int_cxt.ImmediateInterruptOK = true;
                            pq_endmessage(&buf);
                        }
                        pq_flush();
                        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
                        return;
                    } else {
                        if (!is_cluster_internal_connection(port)) {
                            ereport(FATAL,
                                (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                                    errmsg("GSS authentication method is not allowed because %s user password is not "
                                           "disabled.",
                                        port->user_name)));
                        }
                    }
                }
#endif
            }

            /*Get the stored method and the encrypted string, the first part is the salt(also named token1,length is
            SALT_LENGTH), the second part is the sever key(length is HMAC_LENGTH), the third part is the stored
            key(length is STORED_KEY_LENGTH)*/
            stored_method = get_password_stored_method(port->user_name, encrypt_string, ENCRYPTED_STRING_LENGTH + 1);
        }
    }

    /* Check password stored method and there is no need to check for send only AUTH_REQ_OK. */
    if (areq != AUTH_REQ_OK && MD5_PASSWORD != stored_method && SHA256_PASSWORD != stored_method &&
        stored_method != COMBINED_PASSWORD && port->hba->auth_method != uaGSS) {
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        rc = memset_s(encrypt_string, ENCRYPTED_STRING_LENGTH + 1, 0, ENCRYPTED_STRING_LENGTH + 1);
        securec_check(rc, "\0", "\0");
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION), errmsg("Invalid username/password,login denied.")));
    }

    pq_beginmessage(&buf, 'R');

    /*
     * The following block will determine the actual authentication method, sha256 or md5.
     * AUTH_REQ_MD5_SHA256 is only for stored sha256 and req md5 condition.
     */
    if (AUTH_REQ_MD5 == areq && SHA256_PASSWORD == stored_method) {
        pq_sendint32(&buf, (int32)AUTH_REQ_MD5_SHA256);

        rc = strncpy_s(salt, sizeof(salt), encrypt_string, sizeof(salt) - 1);
        securec_check(rc, "\0", "\0");
        salt[sizeof(salt) - 1] = '\0';

        /* Here send the sha256 salt for authenication. */
        pq_sendbytes(&buf, salt, SALT_LENGTH * 2);
    } else {
        /* Be careful about areq == AUTH_REQ_OK condition. */
        if (AUTH_REQ_SHA256 == areq && stored_method != SHA256_PASSWORD) {
            /*
             * New compatibility : Whenever FrontendProtocol is less than PG_PROTOCOL_GAUSS_BASE,
             * it's mean the connection is from PG client or old gauss client.
             * Old  compatibility : PGUSER stored MD5_PASSWORD here.
             */
            if (PG_PROTOCOL_MINOR(FrontendProtocol) < PG_PROTOCOL_GAUSS_BASE || MD5_PASSWORD == stored_method) {
                areq = AUTH_REQ_MD5;
                port->hba->auth_method = uaMD5;
            }
        }
        pq_sendint32(&buf, (int32)areq);
    }

    /*
     * Notify the user to change the password, as PG client don't support sha256 Encryption Algorithm.
     * We also suggest user change the password if they use old gauss client for unified password
     * storage format to conbined format,  although this is not mandatory.
     */
    if (PG_PROTOCOL_MINOR(FrontendProtocol) < PG_PROTOCOL_GAUSS_BASE &&
        (AUTH_REQ_MD5_SHA256 == areq || AUTH_REQ_SHA256 == areq)) {
        ereport(LOG, (errmsg("Please change user password when connect through old client or PG client.")));
    }

    /* Add the salt for encrypted passwords. */
    if (areq == AUTH_REQ_MD5) {
        /* Here send the md5 salt for authenication. */
        pq_sendbytes(&buf, port->md5Salt, 4);
    }

    if (areq == AUTH_REQ_SHA256 && (stored_method == SHA256_PASSWORD || stored_method == COMBINED_PASSWORD)) {
        /*calculate SeverSignature */
        rc = strncpy_s(
            sever_key_string, sizeof(sever_key_string), &encrypt_string[SALT_LENGTH * 2], sizeof(sever_key_string) - 1);
        securec_check(rc, "\0", "\0");
        sever_key_string[sizeof(sever_key_string) - 1] = '\0';
        sha_hex_to_bytes32(sever_key, sever_key_string);
        sha_hex_to_bytes4(token, port->token);
#ifdef USE_ASSERT_CHECKING
        CRYPT_hmac_result =
#else
        (void)
#endif
            CRYPT_hmac(NID_hmacWithSHA256,
                (GS_UCHAR*)sever_key,
                HMAC_LENGTH,
                (GS_UCHAR*)token,
                TOKEN_LENGTH,
                (GS_UCHAR*)sever_signature,
                (GS_UINT32*)&sever_signature_length);
#ifdef USE_ASSERT_CHECKING
        Assert(!CRYPT_hmac_result);
#endif

        rc = strncpy_s(salt, sizeof(salt), encrypt_string, sizeof(salt) - 1);
        securec_check(rc, "\0", "\0");
        salt[sizeof(salt) - 1] = '\0';
        sha_bytes_to_hex64((uint8*)sever_signature, sever_signature_string);
        pq_sendint32(&buf, (int32)SHA256_PASSWORD);
        pq_sendbytes(&buf, salt, SALT_LENGTH * 2);
        pq_sendbytes(&buf, port->token, TOKEN_LENGTH * 2);

        /*
         * Send the sever_signature before client_signature is not safe.
         * The rfc5802 authentication protocol need be enhanced.
         */
        /* For M.N, N < 50  means older version of Gauss200 or Postgres versions */
        if (PG_PROTOCOL_MINOR(FrontendProtocol) < PG_PROTOCOL_GAUSS_BASE)
            pq_sendbytes(&buf, sever_signature_string, HMAC_LENGTH * 2);

        /* Get iteration and send it to client. */
        if (PG_PROTOCOL_MINOR(FrontendProtocol) > PG_PROTOCOL_GAUSS_BASE) {
            auth_iteration_count = get_stored_iteration(port->user_name);

            /*
             * We can't ereport error here when auth_iteration_count is wrong, as
             * we should confirm the authenication between client and server even
             * the role,iteration or some other thing is wrong to prevent guessing.
             */
            if (auth_iteration_count == -1)
                auth_iteration_count = ITERATION_COUNT;
            pq_sendint32(&buf, (int32)auth_iteration_count);
        }

    }

#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)

    /*
     * Add the authentication data for the next step of the GSSAPI or SSPI
     * negotiation.
     */
    else if (areq == AUTH_REQ_GSS_CONT) {
        if (port->gss->outbuf.length > 0) {
            elog(DEBUG4, "sending GSS token of length %u", (unsigned int)port->gss->outbuf.length);

            pq_sendbytes(&buf, (char*)port->gss->outbuf.value, port->gss->outbuf.length);
        }
    }
#endif

    /* Keep block function pq_endmessage and pq_flush can be interrupted. */
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    pq_endmessage(&buf);

    /*
     * Flush message so client will see it, except for AUTH_REQ_OK, which need
     * not be sent until we are ready for queries.
     */
    if (areq != AUTH_REQ_OK)
        pq_flush();

    t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
    rc = memset_s(encrypt_string, ENCRYPTED_STRING_LENGTH + 1, 0, ENCRYPTED_STRING_LENGTH + 1);
    securec_check(rc, "\0", "\0");
}

/*
 * Collect password response packet from frontend.
 *
 * Returns NULL if couldn't get password, else palloc'd string.
 */
static char* recv_password_packet(Port* port)
{
    StringInfoData buf;

    if (PG_PROTOCOL_MAJOR(port->proto) >= 3) {
        /* Expect 'p' message type */
        int mtype;

        mtype = pq_getbyte();
        if (mtype != 'p') {
            /*
             * If the client just disconnects without offering a password,
             * don't make a log entry.  This is legal per protocol spec and in
             * fact commonly done by psql, so complaining just clutters the
             * log.
             */
            if (mtype != EOF)
                ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg("expected password response, got message type %d", mtype)));
            return NULL; /* EOF or bad message type */
        }
    } else {
        /* For pre-3.0 clients, avoid log entry if they just disconnect */
        if (pq_peekbyte() == EOF)
            return NULL; /* EOF */
    }

    initStringInfo(&buf);
    if (pq_getmessage(&buf, 16384)) /* receive password */
    {
        /* EOF - pq_getmessage already logged a suitable message */
        pfree(buf.data);
        buf.data = NULL;
        return NULL;
    }

    /*
     * Apply sanity check: password packet length should agree with length of
     * contained string.  Note it is safe to use strlen here because
     * StringInfo is guaranteed to have an appended '\0'.
     */
    if (strlen(buf.data) + 1 != (size_t)buf.len)
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid password packet size")));

    /*
     * Don't allow an empty password. Libpq treats an empty password the same
     * as no password at all, and won't even try to authenticate. But other
     * clients might, so allowing it would be confusing.
     *
     * Note that this only catches an empty password sent by the client in
     * plaintext. There's another check in md5_crypt_verify to prevent an
     * empty password from being used with MD5 authentication.
     */
    if (buf.data[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("empty password returned by client")));

    /* Do not echo password to logs, for security. */
    ereport(DEBUG5, (errmsg("received password packet")));

    /*
     * Return the received string.	Note we do not attempt to do any
     * character-set conversion on it; since we don't yet know the client's
     * encoding, there wouldn't be much point.
     */
    return buf.data;
}

/* ----------------------------------------------------------------
 * MD5 authentication
 * ----------------------------------------------------------------
 */

/*
 * Called when we have sent an authorization request for a password.
 * Get the response and check it.
 */
static int recv_and_check_password_packet(Port* port)
{
    int result;
    int rcs = 0;
    errno_t rc = EOK;
    char details[PGAUDIT_MAXLENGTH] = {0};
    char* passwd = recv_password_packet(port);
    bool save_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

    /*
     * Disable immediate response to SIGTERM/SIGINT/timeout interrupts as there are
     * some cache and memory operations which can not be interrupted. We just set
     * t_thrd.int_cxt.ImmediateInterruptOK to true in some block condition and reset it at the end.
     */
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    /* client wouldn't send password */
    if (passwd == NULL) {
        /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
        if (!(g_instance.status > NoShutdown) && user_login_hook) {
            user_login_hook(port->database_name, port->user_name, false, true);
        }
        /* Record the audit log for password-null condition. */
        rc = snprintf_s(details,
            PGAUDIT_MAXLENGTH,
            PGAUDIT_MAXLENGTH - 1,
            "login db(%s)failed with no password",
            port->database_name);
        securec_check_ss(rc, "\0", "\0");
        pgaudit_user_login(FALSE, port->database_name, details);
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return STATUS_EOF;
    }

    result = crypt_verify(port, port->user_name, passwd);

    rcs = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
    securec_check(rcs, "\0", "\0");

    pfree(passwd);
    passwd = NULL;

    /* Resume t_thrd.int_cxt.ImmediateInterruptOK. */
    t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;

    return result;
}

/* ----------------------------------------------------------------
 * MIT Kerberos authentication system - protocol version 5
 * ----------------------------------------------------------------
 */
#ifdef KRB5

static int pg_krb5_init(Port* port)
{
    krb5_error_code retval;
    char* khostname = NULL;

    if (g_instance.libpq_cxt.pg_krb5_initialised)
        return STATUS_OK;

    retval = krb5_init_context(&g_instance.libpq_cxt.pg_krb5_context);
    if (retval) {
        ereport(LOG, (errmsg("Kerberos initialization returned error %d", retval)));
        com_err("postgres", retval, "while initializing krb5");
        return STATUS_ERROR;
    }

    retval = krb5_kt_resolve(g_instance.libpq_cxt.pg_krb5_context,
        u_sess->attr.attr_security.pg_krb_server_keyfile,
        &g_instance.libpq_cxt.pg_krb5_keytab);
    if (retval) {
        ereport(LOG, (errmsg("Kerberos keytab resolving returned error %d", retval)));
        com_err(
            "postgres", retval, "while resolving keytab file \"%s\"", u_sess->attr.attr_security.pg_krb_server_keyfile);
        krb5_free_context(g_instance.libpq_cxt.pg_krb5_context);
        return STATUS_ERROR;
    }

    /*
     * If no hostname was specified, pg_krb_server_hostname is already NULL.
     * If it's set to blank, force it to NULL.
     */
    khostname = port->hba->krb_server_hostname;
    if (khostname != NULL && khostname[0] == '\0')
        khostname = NULL;

    retval = krb5_sname_to_principal(g_instance.libpq_cxt.pg_krb5_context,
        khostname,
        u_sess->attr.attr_security.pg_krb_srvnam,
        KRB5_NT_SRV_HST,
        &g_instance.libpq_cxt.pg_krb5_server);
    if (retval) {
        ereport(LOG,
            (errmsg("Kerberos sname_to_principal(\"%s\", \"%s\") returned error %d",
                khostname ? khostname : "server hostname",
                u_sess->attr.attr_security.pg_krb_srvnam,
                retval)));
        com_err("postgres",
            retval,
            "while getting server principal for server \"%s\" for service \"%s\"",
            khostname ? khostname : "server hostname",
            u_sess->attr.attr_security.pg_krb_srvnam);
        krb5_kt_close(g_instance.libpq_cxt.pg_krb5_context, g_instance.libpq_cxt.pg_krb5_keytab);
        krb5_free_context(g_instance.libpq_cxt.pg_krb5_context);
        return STATUS_ERROR;
    }

    g_instance.libpq_cxt.pg_krb5_initialised = 1;
    return STATUS_OK;
}

/*
 * pg_krb5_recvauth -- server routine to receive authentication information
 *					   from the client
 *
 * We still need to compare the username obtained from the client's setup
 * packet to the authenticated name.
 *
 * We have our own keytab file because postgres is unlikely to run as root,
 * and so cannot read the default keytab.
 */
static int pg_krb5_recvauth(Port* port)
{
    krb5_error_code retval;
    int ret;
    krb5_auth_context auth_context = NULL;
    krb5_ticket* ticket = NULL;
    char* kusername = NULL;
    char* cp = NULL;

    ret = pg_krb5_init(port);
    if (ret != STATUS_OK)
        return ret;

    retval = krb5_recvauth(g_instance.libpq_cxt.pg_krb5_context,
        &auth_context,
        (krb5_pointer)&port->sock,
        u_sess->attr.attr_security.pg_krb_srvnam,
        g_instance.libpq_cxt.pg_krb5_server,
        0,
        g_instance.libpq_cxt.pg_krb5_keytab,
        &ticket);
    if (retval) {
        ereport(LOG, (errmsg("Kerberos recvauth returned error %d", retval)));
        com_err("postgres", retval, "from krb5_recvauth");
        return STATUS_ERROR;
    }

    /*
     * The "client" structure comes out of the ticket and is therefore
     * authenticated.  Use it to check the username obtained from the
     * postmaster startup packet.
     */
#if defined(HAVE_KRB5_TICKET_ENC_PART2)
    retval = krb5_unparse_name(g_instance.libpq_cxt.pg_krb5_context, ticket->enc_part2->client, &kusername);
#elif defined(HAVE_KRB5_TICKET_CLIENT)
    retval = krb5_unparse_name(g_instance.libpq_cxt.pg_krb5_context, ticket->client, &kusername);
#else
#error "bogus configuration"
#endif
    if (retval) {
        ereport(LOG, (errmsg("Kerberos unparse_name returned error %d", retval)));
        com_err("postgres", retval, "while unparsing client name");
        krb5_free_ticket(g_instance.libpq_cxt.pg_krb5_context, ticket);
        krb5_auth_con_free(g_instance.libpq_cxt.pg_krb5_context, auth_context);
        return STATUS_ERROR;
    }

    cp = strchr(kusername, '@');
    if (cp != NULL) {
        /*
         * If we are not going to include the realm in the username that is
         * passed to the ident map, destructively modify it here to remove the
         * realm. Then advance past the separator to check the realm.
         */
        if (!port->hba->include_realm)
            *cp = '\0';
        cp++;

        if (port->hba->krb_realm != NULL && strlen(port->hba->krb_realm)) {
            /* Match realm against configured */
            if (u_sess->attr.attr_security.pg_krb_caseins_users)
                ret = pg_strcasecmp(port->hba->krb_realm, cp);
            else
                ret = strcmp(port->hba->krb_realm, cp);

            if (ret) {
                elog(DEBUG2, "krb5 realm (%s) and configured realm (%s) don't match", cp, port->hba->krb_realm);

                krb5_free_ticket(g_instance.libpq_cxt.pg_krb5_context, ticket);
                krb5_auth_con_free(g_instance.libpq_cxt.pg_krb5_context, auth_context);
                return STATUS_ERROR;
            }
        }
    } else if (port->hba->krb_realm != NULL && strlen(port->hba->krb_realm)) {
        elog(DEBUG2, "krb5 did not return realm but realm matching was requested");

        krb5_free_ticket(g_instance.libpq_cxt.pg_krb5_context, ticket);
        krb5_auth_con_free(g_instance.libpq_cxt.pg_krb5_context, auth_context);
        return STATUS_ERROR;
    }

    ret =
        check_usermap(port->hba->usermap, port->user_name, kusername, u_sess->attr.attr_security.pg_krb_caseins_users);

    krb5_free_ticket(g_instance.libpq_cxt.pg_krb5_context, ticket);
    krb5_auth_con_free(g_instance.libpq_cxt.pg_krb5_context, auth_context);
    pfree(kusername);

    return ret;
}
#endif /* KRB5 */

/* ----------------------------------------------------------------
 * GSSAPI authentication system
 * ----------------------------------------------------------------
 */
#ifdef ENABLE_GSS

#if defined(WIN32) && !defined(WIN32_ONLY_COMPILER)
/*
 * MIT Kerberos GSSAPI DLL doesn't properly export the symbols for MingW
 * that contain the OIDs required. Redefine here, values copied
 * from src/athena/auth/krb5/src/lib/gssapi/generic/gssapi_generic.c
 */
static const gss_OID_desc GSS_C_NT_USER_NAME_desc = {10, (void*)"\x2a\x86\x48\x86\xf7\x12\x01\x02\x01\x02"};
static GSS_DLLIMP gss_OID GSS_C_NT_USER_NAME = &GSS_C_NT_USER_NAME_desc;
#endif

/*
 * Generate an error for GSSAPI authentication.  The caller should apply
 * _() to errmsg to make it translatable.
 */
static void pg_GSS_error(int severity, char* errmsg, OM_uint32 maj_stat, OM_uint32 min_stat)
{
    gss_buffer_desc gmsg;
    OM_uint32 lmin_s, msg_ctx;
    char msg_major[128], msg_minor[128];

    /* Fetch major status message */
    msg_ctx = 0;
    gss_display_status(&lmin_s, maj_stat, GSS_C_GSS_CODE, GSS_C_NO_OID, &msg_ctx, &gmsg);

    int rc = strcpy_s(msg_major, sizeof(msg_major), (char*)gmsg.value);
    securec_check(rc, "\0", "\0");

    gss_release_buffer(&lmin_s, &gmsg);

    if (msg_ctx)

        /*
         * More than one message available. XXX: Should we loop and read all
         * messages? (same below)
         */
        ereport(WARNING, (errmsg_internal("incomplete GSS error report")));

    /* Fetch mechanism minor status message */
    msg_ctx = 0;
    gss_display_status(&lmin_s, min_stat, GSS_C_MECH_CODE, GSS_C_NO_OID, &msg_ctx, &gmsg);

    rc = strcpy_s(msg_minor, sizeof(msg_minor), (char*)gmsg.value);
    securec_check(rc, "\0", "\0");
    gss_release_buffer(&lmin_s, &gmsg);

    if (msg_ctx)
        ereport(WARNING, (errmsg_internal("incomplete GSS minor error report")));

    /*
     * errmsg_internal, since translation of the first part must be done
     * before calling this function anyway.
     */
    ereport(severity, (errmsg_internal("%s", errmsg), errdetail_internal("%s: %s", msg_major, msg_minor)));
}

static int pg_GSS_recvauth(Port* port)
{
    OM_uint32 maj_stat, min_stat, gflags;
    int mtype;
    int ret = 0;
    StringInfoData buf;
    gss_buffer_desc gbuf;
    errno_t rc = EOK;
    bool save_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

    /*
     * Disable immediate response to SIGTERM/SIGINT/timeout interrupts as there are
     * some cache and memory operations which can not be interrupted. We just set
     * t_thrd.int_cxt.ImmediateInterruptOK to true in some block condition and reset it at the end.
     */
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    /*
     * GSS auth is not supported for protocol versions before 3, because it
     * relies on the overall message length word to determine the GSS payload
     * size in AuthenticationGSSContinue and PasswordMessage messages. (This
     * is, in fact, a design error in our GSS support, because protocol
     * messages are supposed to be parsable without relying on the length
     * word; but it's not worth changing it now.)
     */
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
        ereport(
            FATAL, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("GSSAPI is not supported in protocol version 2")));

    if (u_sess->attr.attr_security.pg_krb_server_keyfile != NULL &&
        strlen(u_sess->attr.attr_security.pg_krb_server_keyfile) > 0) {
        /*
         * Set default Kerberos keytab file for the Krb5 mechanism.
         *
         * setenv("KRB5_KTNAME", pg_krb_server_keyfile, 0); except setenv()
         * not always available.
         */
        if (gs_getenv_r("KRB5_KTNAME") == NULL) {
            size_t kt_len = strlen(u_sess->attr.attr_security.pg_krb_server_keyfile) + 14;
            char* kt_path = (char*)malloc(kt_len);

            if (kt_path == NULL) {
                ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
                t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
                return STATUS_ERROR;
            }
            rc = snprintf_s(
                kt_path, kt_len, kt_len - 1, "KRB5_KTNAME=%s", u_sess->attr.attr_security.pg_krb_server_keyfile);
            securec_check_ss(rc, "", "");
            if (rc != (int)(kt_len - 2) || gs_putenv_r(kt_path) != 0) {
                ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
                t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
                return STATUS_ERROR;
            }
            ereport(DEBUG2, (errmsg("Set the KRB5_KTNAME to %s", kt_path)));

            /*
             * Notice: Do not free(kt_path).
             */
        } else {
            /* Most "key table entry not found" error caused by wrong KRB5_KTNAME. */
            ereport(DEBUG2, (errmsg("KRB5_KTNAME is %s.", gs_getenv_r("KRB5_KTNAME"))));
        }
    } else {
        /* We log the pg_krb_server_keyfile when it's value is abnormal. */
        ereport(LOG, (errmsg("pg_krb_server_keyfile is null")));
    }

    /*
     * We accept any service principal that's present in our keytab. This
     * increases interoperability between kerberos implementations that see
     * for example case sensitivity differently, while not really opening up
     * any vector of attack.
     */
    port->gss->cred = GSS_C_NO_CREDENTIAL;

    /*
     * Initialize sequence with an empty context
     */
    port->gss->ctx = GSS_C_NO_CONTEXT;

    /*
     * Loop through GSSAPI message exchange. This exchange can consist of
     * multiple messags sent in both directions. First message is always from
     * the client. All messages from client to server are password packets
     * (type 'p').
     */
    do {
        char* krbconfig = NULL;

        /* Keep pq_getbyte can be interrupted. */
        t_thrd.int_cxt.ImmediateInterruptOK = true;
        mtype = pq_getbyte();
        t_thrd.int_cxt.ImmediateInterruptOK = false;
        if (mtype != 'p') {
            /* Only log error if client didn't disconnect. */
            if (mtype != EOF) {
                ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("expected GSS response, got message type %d", mtype)));
            } else {
                elog(LOG,
                    "options:%s user:%s remotehost:%s remote port:%s",
                    port->cmdline_options,
                    port->user_name,
                    port->remote_host,
                    port->remote_port);
                ereport(LOG, (errmsg("pq_getbyte returns EOF")));
            }
            t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
            return STATUS_ERROR;
        }

        /* Get the actual GSS token */
        initStringInfo(&buf);

        /* Keep pq_getmessage can be interrupted. */
        t_thrd.int_cxt.ImmediateInterruptOK = true;
        if (pq_getmessage(&buf, PG_MAX_AUTH_TOKEN_LENGTH)) {
            t_thrd.int_cxt.ImmediateInterruptOK = false;
            /* EOF - pq_getmessage already logged error */
            pfree(buf.data);
            buf.data = NULL;
            ereport(LOG, (errmsg("Received message is larger than %d", PG_MAX_AUTH_TOKEN_LENGTH)));
            t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
            return STATUS_ERROR;
        }
        t_thrd.int_cxt.ImmediateInterruptOK = false;

        /* Map to GSSAPI style buffer */
        gbuf.length = buf.len;
        gbuf.value = buf.data;

        elog(DEBUG4, "Processing received GSS token of length %u", (unsigned int)gbuf.length);

        /* Clean the config cache and ticket cache set by hadoop remote read. */
        krb5_clean_cache_profile_path();

        /* Krb5 config file priority : setpath > env(MPPDB_KRB5_FILE_PATH) > default(/etc/krb5.conf).*/
        if ((krbconfig = gs_getenv_r("MPPDB_KRB5_FILE_PATH")) != NULL) {
            check_backend_env(krbconfig);
            krb5_set_profile_path(krbconfig);
        }

        maj_stat = gss_accept_sec_context(&min_stat,
            &port->gss->ctx,
            port->gss->cred,
            &gbuf,
            GSS_C_NO_CHANNEL_BINDINGS,
            &port->gss->name,
            NULL,
            &port->gss->outbuf,
            &gflags,
            NULL,
            NULL);

        /* gbuf no longer used */
        pfree(buf.data);
        buf.data = NULL;

        elog(DEBUG5,
            "gss_accept_sec_context major: %u, "
            "minor: %u, outlen: %u, outflags: %x",
            maj_stat,
            min_stat,
            (unsigned int)port->gss->outbuf.length,
            gflags);

        if (port->gss->outbuf.length != 0) {
            /*
             * Negotiation generated data to be sent to the client.
             */
            OM_uint32 lmin_s = 0;
            elog(DEBUG4, "sending GSS response token of length %u", (unsigned int)port->gss->outbuf.length);
            sendAuthRequest(port, AUTH_REQ_GSS_CONT);
            gss_release_buffer(&lmin_s, &port->gss->outbuf);
        }

        /* wrong status, release resource here and print error */
        if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
            clear_gss_info(port->gss);
            pg_GSS_error(ERROR, _("accepting GSS security context failed"), maj_stat, min_stat);
        }

        if (maj_stat == GSS_S_CONTINUE_NEEDED)
            elog(DEBUG4, "GSS continue needed");

    } while (maj_stat == GSS_S_CONTINUE_NEEDED);

    /*
     * GSS_S_COMPLETE indicates that authentication is now complete.
     * Get the name of the user that authenticated, and compare it to the pg
     * username that was specified for the connection.
     */
    maj_stat = gss_display_name(&min_stat, port->gss->name, &gbuf, NULL);
    if (maj_stat != GSS_S_COMPLETE) {
        clear_gss_info(port->gss);
        pg_GSS_error(ERROR, _("retrieving GSS user name failed"), maj_stat, min_stat);
    }

    clear_gss_info(port->gss);
    /*
     * Split the username at the realm separator
     */
    char* cp = strchr((char*)gbuf.value, '@');
    if (cp != NULL) {
        /*
         * If we are not going to include the realm in the username that is
         * passed to the ident map, destructively modify it here to remove the
         * realm. Then advance past the separator to check the realm.
         */
        if (!port->hba->include_realm)
            *cp = '\0';
        cp++;

        if (port->hba->krb_realm != NULL && strlen(port->hba->krb_realm)) {
            /*
             * Match the realm part of the name first
             */
            if (u_sess->attr.attr_security.pg_krb_caseins_users)
                ret = pg_strcasecmp(port->hba->krb_realm, cp);
            else
                ret = strcmp(port->hba->krb_realm, cp);

            if (ret) {
                /* GSS realm does not match */
                OM_uint32 lmin_s = 0;
                elog(LOG, "GSSAPI realm (%s) and configured realm (%s) don't match", cp, port->hba->krb_realm);
                gss_release_buffer(&lmin_s, &gbuf);
                t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
                return STATUS_ERROR;
            }
        }

        if (IS_PGXC_COORDINATOR && IsConnFromApp()) {
            elog(DEBUG4, "options:%s auth_user:%s", port->cmdline_options, (char*)gbuf.value);
            if (IsConnPortFromCoord(port)) {
                /* process_startup_options() parse later, so when setup connect, wo do not kown remoteConnType.
                    we need check remotetype here. if remotetype is CN, no need check.
                    if remotetype is gsql/jdbc, we need check user info*/
            } else {
                /*
                 *  a principal is divides into three parts: primary(usernmae), the instance and the realm,
                 *  and the format of a typical kerberos principal is "primary/instance@realm"
                 *  now we will compare auth_name with username getting from principal.
                 */
                cp = strchr((char*)gbuf.value, '/');
                char* namebuf = (char*)palloc0(strlen((char*)gbuf.value) + 2);
                int len = (int)((intptr_t)cp - (intptr_t)gbuf.value);
                int rcs = 0;

                rcs = memcpy_s(namebuf, strlen((char*)gbuf.value) + 2, (char*)gbuf.value, len);
                securec_check(rcs, "\0", "\0");
                ret = check_usermap(
                    port->hba->usermap, port->user_name, namebuf, u_sess->attr.attr_security.pg_krb_caseins_users);
                pfree(namebuf);
                namebuf = NULL;
            }
        }

    } else if (port->hba->krb_realm != NULL && strlen(port->hba->krb_realm)) {
        elog(LOG, "GSSAPI did not return realm but realm matching was requested");
        OM_uint32 lmin_s = 0;
        gss_release_buffer(&lmin_s, &gbuf);
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return STATUS_ERROR;
    }

    /*
     * Currently only for internal authentication, no need to verify the database
     * user information.
     */
    OM_uint32 lmin_s = 0;
    gss_release_buffer(&lmin_s, &gbuf);

    /* Resume t_thrd.int_cxt.ImmediateInterruptOK.*/
    t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
    return ret;
}

/*
 * @Description: Generic api for send data.
 * @in fd : the sock used to send.
 * @in data : the data need to be sent.
 * @in size : the size of data which need to be sent.
 * @return : the number of bytes sent or -1 for error.
 */
static int GssInternalSend(int fd, const void* data, int size)
{
    ssize_t nbytes;
    ssize_t nSend = 0;

    /*  Several errors are OK. When speculative write is being done we may not
     *  be able to write a single byte to the socket. Also, SIGSTOP issued
     *  by a debugging tool can result in EINTR error.
     */
    while (nSend != size) {
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        nbytes = send(fd, (const void*)((char*)data + nSend), size - nSend, 0);
        END_NET_SEND_INFO(nbytes);

        if (nbytes <= 0) {
            if (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR || errno == ENOBUFS)) {
                (void)usleep(100);
                continue;
            }
            return -1;
        } else
            nSend += nbytes;
    }

    return (size_t)nSend;
}

/*
 * @Description: Generic api for recv data.
 * @in fd : the sock used to recv.
 * @in data : the buffer for stored the recv data.
 * @in size : the size of data which need recv.
 * @return : the number of bytes received or -1 for error.
 */
static int GssInternalRecv(int fd, void* data, int size)
{
#define MSG_NOTIFICATION 0x8000

    int error = 0;
    int recv_bytes = 0;

    int flags = MSG_WAITALL;
    struct iovec iov;
    struct msghdr inmsg;

    while (1) {
        iov.iov_base = (char*)data + recv_bytes;
        iov.iov_len = size - recv_bytes;

        inmsg.msg_name = NULL;
        inmsg.msg_namelen = 0;
        inmsg.msg_iov = &iov;
        inmsg.msg_iovlen = 1;
        inmsg.msg_control = NULL;
        inmsg.msg_controllen = 0;
        error = recvmsg(fd, &inmsg, flags);

        /* We should ignore MSG_NOTIFICATION msg of SCTP here. */
        if (MSG_NOTIFICATION & (unsigned int)inmsg.msg_flags)
            continue;

        if (error > 0)
            recv_bytes += error;

        else if (error == 0)
            break;

        else if (error < 0 && errno != EAGAIN && errno != EINTR)
            break;

        if (recv_bytes == size)
            break;
    }

    return ((recv_bytes == size) ? 0 : -1);
}

/*
 * @Description: Send gss kerberos token for the security context.
 * @in gss_conn : the struct contain the outbuf need to be sent.
 * @in type : the data type for which need to be sent.
 * @return : 0 for success and -1 for failed.
 */
static int GssSendWithType(GssConn* gss_conn, char type)
{
#define MSG_HEAD_LEN 5 /* sizeof(char)+sizeof(int) */

    char msg_head[MSG_HEAD_LEN + 1] = {0};
    int socket = gss_conn->sock;
    char* buf = (char*)gss_conn->goutbuf.value;
    int buf_len = gss_conn->goutbuf.length;
    int re = 0;
    OM_uint32 lmin_s;

    msg_head[0] = type;

    int* msg_len = (int*)&msg_head[1];
    *msg_len = htonl((uint32)(buf_len));

    /* Send 5 bytes msg_head with type and buf_len messages first. */
    re = GssInternalSend(socket, msg_head, MSG_HEAD_LEN);

    if (re == MSG_HEAD_LEN && buf_len > 0) {
        /* Send the buf which contain the token in outbuf. */
        re = GssInternalSend(socket, buf, buf_len);
    }

    gss_release_buffer(&lmin_s, &gss_conn->goutbuf);

    if (re < 0)
        return -1;

    return 0;
}

/*
 * @Description: Recv gss kerberos token for the security context.
 * @in gss_conn : the struct contain the inbuf to stored token.
 * @in type : the data type need to recv.
 * @return : 0 for success and -1 for failed.
 */
static int GssRecvWithType(GssConn* gss_conn, char type)
{
#define MSG_HEAD_LEN 5 /* sizeof(char)+sizeof(int) */

    int re;
    int socket = gss_conn->sock;
    char msg_head[MSG_HEAD_LEN + 1] = {0};

    /* Recv 5 bytes msg_head with type and buf_len messages first. */
    if (GssInternalRecv(socket, msg_head, MSG_HEAD_LEN) < 0) {
        return -1;
    }

    if (msg_head[0] != type) {
        errno = ENOMSG;
        return -1;
    }

    int buf_len = *((int*)(&msg_head[1]));

    /* Calculate the buf_len for recv length. */
    buf_len = ntohl(buf_len);

    if (buf_len < 0 || buf_len > PG_MAX_AUTH_TOKEN_LENGTH) {
        errno = ENOMSG;
        return -1;
    }

    if (gss_conn->ginbuf.value != NULL) {
        free(gss_conn->ginbuf.value);
        gss_conn->ginbuf.value = NULL;
        gss_conn->ginbuf.length = 0;
    }

    if (buf_len == 0)
        return 0;

    /* malloc ginbuf for GssContinue. */
    gss_conn->ginbuf.value = (char*)malloc(buf_len);
    if (gss_conn->ginbuf.value == NULL) {
        errno = ENOMEM;
        return -1;
    }

    /* Recv the data which contain the token in inbuf. */
    re = GssInternalRecv(socket, gss_conn->ginbuf.value, buf_len);
    if (re < 0) {
        free(gss_conn->ginbuf.value);
        gss_conn->ginbuf.value = NULL;
        gss_conn->ginbuf.length = 0;
        return -1;
    } else
        gss_conn->ginbuf.length = buf_len;

    return 0;
}

/*
 * @Description: the function for gss log.
 * @in errmsg : the error string need to log.
 * @in maj_stat : major execute info in gss
 * @in min_stat : auxiliary execute info in gss
 */
static void GSSLog(const char* errmsg, OM_uint32 maj_stat, OM_uint32 min_stat)
{
#define TIMELEN 128

    gss_buffer_desc gmsg;
    OM_uint32 lmin_s, msg_ctx;
    char msg_major[128], msg_minor[128];
    time_t now;
    struct tm* timenow = NULL;
    errno_t rc = EOK;

    /* Fetch major status message */
    msg_ctx = 0;
    gss_display_status(&lmin_s, maj_stat, GSS_C_GSS_CODE, GSS_C_NO_OID, &msg_ctx, &gmsg);

    rc = strcpy_s(msg_major, sizeof(msg_major), (char*)gmsg.value);
    securec_check(rc, "\0", "\0");
    gss_release_buffer(&lmin_s, &gmsg);

    /* Fetch mechanism minor status message */
    msg_ctx = 0;
    gss_display_status(&lmin_s, min_stat, GSS_C_MECH_CODE, GSS_C_NO_OID, &msg_ctx, &gmsg);
    rc = strcpy_s(msg_minor, sizeof(msg_minor), (char*)gmsg.value);
    securec_check(rc, "\0", "\0");
    gss_release_buffer(&lmin_s, &gmsg);

    /* make time string end */
    (void)time(&now);

    timenow = localtime(&now);
    char timebuf[TIMELEN] = {0};

    if (timenow != NULL) {
        // format date and time.
        (void)strftime(timebuf, TIMELEN, "%Y-%m-%d %H:%M:%S %Z", timenow);
    }

    /*
     * errmsg_internal, since translation of the first part must be done
     * before calling this function anyway.
     */
    fprintf(stdout,
        "%s %lu %lu LOG: [GSSAPI] %s, detail:%s:%s.\n",
        timebuf,
        gs_thread_self(),
        u_sess->debug_query_id,
        errmsg,
        msg_major,
        msg_minor);
    (void)fflush(stdout);
}

/*
 * @Description: Get the kerberos server name.
 * @return : the kerberos server name.
 */
static char* GssGetKerberosServerName()
{
    static char* g_krbsrvname = NULL;
    char* krbsrvname = gs_getenv_r("PGKRBSRVNAME");

    if (NULL != krbsrvname) {
        check_backend_env(krbsrvname);
        g_krbsrvname = krbsrvname;
    }

    return g_krbsrvname;
}

/*
 * @Description: Get the kerberos host name.
 * @return : the kerberos host name.
 */
static char* GssGetKerberosHostName()
{
    static char* g_krbhostname = NULL;
    char* krbhostname = gs_getenv_r("KRBHOSTNAME");

    if (NULL != krbhostname) {
        check_backend_env(krbhostname);
        g_krbhostname = krbhostname;
    }

    return g_krbhostname;
}

/*
 * @Description: get the kerberos server name.
 * @in gss_conn : to stored the target message.
 * @in server_host : the server ip.
 * @return : 0 for success and -1 for error.
 */
static int GssImportName(GssConn* gss_conn, char* server_host)
{
#define MAXENVLEN 1024

    OM_uint32 maj_stat, min_stat;
    gss_buffer_desc temp_gbuf;
    char* krbsrvname = GssGetKerberosServerName();
    char* krbhostname = GssGetKerberosHostName();
    errno_t rc = EOK;

    if (krbsrvname == NULL || krbhostname == NULL) {
        errno = EINVAL;
        return -1;
    }

    int gbuf_len = strlen(krbsrvname) + strlen(krbhostname) + 2;
    temp_gbuf.value = malloc(gbuf_len);
    if (temp_gbuf.value == NULL) {
        errno = ENOMEM;
        return -1;
    }

    rc = snprintf_s((char*)temp_gbuf.value, gbuf_len, gbuf_len - 1, "%s/%s", krbsrvname, krbhostname);
    securec_check_ss(rc, "", "");
    temp_gbuf.length = strlen((char*)temp_gbuf.value);

    maj_stat = gss_import_name(&min_stat, &temp_gbuf, (gss_OID)GSS_KRB5_NT_PRINCIPAL_NAME, &gss_conn->gtarg_nam);

    free(temp_gbuf.value);
    temp_gbuf.value = NULL;

    if (maj_stat != GSS_S_COMPLETE) {
        errno = EPERM;
        GSSLog("client import name fail!\n", maj_stat, min_stat);
        return -1;
    }

    return 0;
}

/*
 * @Description: init the security context.
 * @in gss_conn : stored the messages used in authentication.
 * @return : 0 for success and -1 for error.
 */
static int GssClientInit(GssConn* gss_conn)
{
#define MAX_KERBEROS_CAPACITY 3000      /* The max capacity of kerberos is 3000 per second */
#define RESERVED_KERBEROS_CAPACITY 1000 /* Reserved 1000 capacity of kerberos for other service */

    OM_uint32 maj_stat, min_stat, lmin_s;
    char* krbconfig = NULL;
    instr_time before, after;
    double elapsed_msec = 0;
    int retry_count = 0;

retry_init:
    /*
     * 1. Get lock here as krb5 lib used non-thread safe function like getenv.
     * 2. The lock can prevent big concurrent access to kerberos, as once get ticket from
     *	kerberos(TGS), we will cache it, and no need to contact kerberos every time.
     */
    (void)syscalllockAcquire(&kerberos_conn_lock);

    if (log_min_messages <= DEBUG2)
        INSTR_TIME_SET_CURRENT(before);

    /* Clean the config cache and ticket cache set by hadoop remote read. */
    krb5_clean_cache_profile_path();

    /* Krb5 config file priority : setpath > env(MPPDB_KRB5_FILE_PATH) > default(/etc/krb5.conf).*/
    krbconfig = gs_getenv_r("MPPDB_KRB5_FILE_PATH");
    if (krbconfig != NULL) {
        check_backend_env(krbconfig);
        krb5_set_profile_path(krbconfig);
    }

    int num_nodes = global_node_definition ? global_node_definition->num_nodes : (u_sess->pgxc_cxt.NumDataNodes);
    /*
     * 1.The max capacity of kerberos is 3000 per second, we  reserved 1000 for other
     *	use beside gaussdb inner authenication.
     * 2.The max parallel connection and authenication count is dn_num + cn_num
     *	for lock and cahce reasion which means one process can only require one REQ
     *	to kerberos server once.
     * 3.Once datanode is more than MAX_KERBEROS_CAPACITY - RESERVED_KERBEROS_CAPACITY,
     *	we sleep a little bit time here to reduce the pressure on parallel authentication for kerberos.
     */
    if (num_nodes > (MAX_KERBEROS_CAPACITY - RESERVED_KERBEROS_CAPACITY))
        pg_usleep(100);

    /*
     * The first time come here(with no tickent cache), gss_init_sec_context will send TGS_REQ
     * to kerberos server to get ticket and then cache it in default_ccache_name which configured
     * in MPPDB_KRB5_FILE_PATH.
     */
    maj_stat = gss_init_sec_context(&min_stat,
        GSS_C_NO_CREDENTIAL,
        &gss_conn->gctx,
        gss_conn->gtarg_nam,
        GSS_C_NO_OID,
        GSS_C_MUTUAL_FLAG,
        0,
        GSS_C_NO_CHANNEL_BINDINGS,
        (gss_conn->gctx == GSS_C_NO_CONTEXT) ? GSS_C_NO_BUFFER : &gss_conn->ginbuf,
        NULL,
        &gss_conn->goutbuf,
        NULL,
        NULL);

    if (log_min_messages <= DEBUG2) {
        INSTR_TIME_SET_CURRENT(after);
        INSTR_TIME_SUBTRACT(after, before);
        elapsed_msec = INSTR_TIME_GET_MILLISEC(after);
        ereport(DEBUG2,
            (errmsg("Stream gss init time is %.3f ms and datanode num is %d.", elapsed_msec, num_nodes),
                ignore_interrupt(true)));
    }

    (void)syscalllockRelease(&kerberos_conn_lock);

    if (gss_conn->ginbuf.value != NULL) {
        free(gss_conn->ginbuf.value);
        gss_conn->ginbuf.value = NULL;
        gss_conn->ginbuf.length = 0;
    }

    if (gss_conn->goutbuf.length > 0) {
        /*
         * GSS generated data to send to the server. We don't care if it's the
         * first or subsequent packet, just send the same kind of password
         * packet.
         */
        if (GssSendWithType(gss_conn, 'p') < 0)
            return -1;
    }

    if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
        GSSLog("client init fail", maj_stat, min_stat);

        /* Retry 10 times for init context responding to scenarios such as cache renewed by kinit. */
        if (retry_count < 10) {
            (void)usleep(1000);
            retry_count++;
            goto retry_init;
        }

        gss_release_name(&lmin_s, &gss_conn->gtarg_nam);
        if (gss_conn->gctx)
            gss_delete_sec_context(&lmin_s, &gss_conn->gctx, GSS_C_NO_BUFFER);

        errno = EPERM;
        return -1;
    }

    if (maj_stat == GSS_S_CONTINUE_NEEDED)
        return 1;

    gss_release_name(&lmin_s, &gss_conn->gtarg_nam);

    return 0;
}

/*
 * @Description: Continue GSS authentication with next token as needed..
 * @in gss_conn : to stored the messages used in create security context.
 * @return : 0 for success and -1 for error.
 */
int GssClientContinue(GssConn* gss_conn)
{
    int re = -1;

    while (1) {
        /* ginbuf free in GssClientInit */
        re = GssClientInit(gss_conn);
        if (re < 0)
            return -1;

        re = GssRecvWithType(gss_conn, 'R');
        if (re < 0) {
            if (gss_conn->ginbuf.value != NULL) {
                free(gss_conn->ginbuf.value);
                gss_conn->ginbuf.value = NULL;
                gss_conn->ginbuf.length = 0;
            }
            return -1;
        }

        /* return GSS_S_COMPLETE */
        if (gss_conn->ginbuf.length == 0)
            return 0;
    }
}

/*
 * @Description: the main function for gss client authentication.
 * @in socket : the socket used to send and recv authentication messages.
 * @in server_host : server host ip
 * @return : 0 for success and -1 for error.
 */
int GssClientAuth(int socket, char* server_host)
{
    errno_t rc = EOK;

    if (socket < 0) {
        errno = EBADF;
        return -1;
    }

    if (server_host == NULL || strlen(server_host) <= 0) {
        errno = EINVAL;
        return -1;
    }

    GssConn gss_conn;
    rc = memset_s(&gss_conn, sizeof(gss_conn), 0, sizeof(gss_conn));
    securec_check(rc, "\0", "\0");

    gss_conn.sock = socket;
    gss_conn.gctx = GSS_C_NO_CONTEXT;

    /* Get the target name in corresponding format*/
    if (GssImportName(&gss_conn, server_host) < 0) {
        return -1;
    }

    /* Start and continue the authentication. */
    return GssClientContinue(&gss_conn);
}

/*
 * @Description: he main function for gss client authentication.
 * @in gss_conn : to stored the messages used in accept security context.
 * @return : 0 for success and -1 for error.
 */
static int GssServerAccept(GssConn* gss_conn)
{
    OM_uint32 maj_stat, min_stat, lmin_s, gflags;
    char* krbconfig = NULL;

    /* Clean the config cache and ticket cache set by hadoop remote read. */
    krb5_clean_cache_profile_path();

    /* Krb5 config file priority : setpath > env(MPPDB_KRB5_FILE_PATH) > default(/etc/krb5.conf).*/
    krbconfig = gs_getenv_r("MPPDB_KRB5_FILE_PATH");
    if (NULL != krbconfig) {
        check_backend_env(krbconfig);
        krb5_set_profile_path(krbconfig);
    }

    maj_stat = gss_accept_sec_context(&min_stat,
        &gss_conn->gctx,
        GSS_C_NO_CREDENTIAL,
        &gss_conn->ginbuf,
        GSS_C_NO_CHANNEL_BINDINGS,
        &gss_conn->gtarg_nam,
        NULL,
        &gss_conn->goutbuf,
        &gflags,
        NULL,
        NULL);

    if (gss_conn->ginbuf.value != NULL) {
        free(gss_conn->ginbuf.value);
        gss_conn->ginbuf.value = NULL;
        gss_conn->ginbuf.length = 0;
    }

    if (gss_conn->goutbuf.length > 0) {
        /*
         * GSS generated data to send to the server. We don't care if it's the
         * first or subsequent packet, just send the same kind of password
         * packet.
         */
        if (GssSendWithType(gss_conn, 'R') < 0)
            return -1;
    }

    if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
        GSSLog("server accept fail", maj_stat, min_stat);
        gss_delete_sec_context(&lmin_s, &gss_conn->gctx, GSS_C_NO_BUFFER);

        errno = EPERM;
        return -1;
    }

    if (maj_stat == GSS_S_CONTINUE_NEEDED)
        return 1;

    return GssSendWithType(gss_conn, 'R');
}

/*
 * @Description: Continue GSS authentication with next token as needed..
 * @in gss_conn : to stored the messages used in gss authentication.
 * @return : 0 for success and -1 for error.
 */
static int GssServerContinue(GssConn* gss_conn)
{
    int re = -1;

    while (1) {
        if (GssRecvWithType(gss_conn, 'p') < 0)
            return -1;

        /* ginbuf free in GssServerAccept */
        re = GssServerAccept(gss_conn);
        if (re == 1)
            continue;
        else
            break;
    }

    return re;
}

/*
 * @Description: the main function for gss server authentication.
 * @in socket : the socket used to send and recv authentication messages.
 * @in krb_keyfile : the kerberos keytab file.
 * @return : 0 for success and -1 for error.
 */
int GssServerAuth(int socket, const char* krb_keyfile)
{

    errno_t rc = EOK;
    GssConn gss_conn;

    if (socket < 0) {
        errno = EBADF;
        return -1;
    }

    if (krb_keyfile == NULL || strlen(krb_keyfile) <= 0) {
        errno = EINVAL;
        return -1;
    }

    /*
     * Set default Kerberos keytab file for the Krb5 mechanism.
     *
     * setenv("KRB5_KTNAME", pg_krb_server_keyfile, 0); except setenv()
     * not always available.
     */
    if (gs_getenv_r("KRB5_KTNAME") == NULL) {
        size_t kt_len = strlen(krb_keyfile) + 14;
        char* kt_path = (char*)malloc(kt_len);

        if (kt_path == NULL) {
            errno = ENOMEM;
            return -1;
        }
        int res = snprintf_s(kt_path, kt_len, kt_len - 1, "KRB5_KTNAME=%s", krb_keyfile);
        securec_check_ss(res, "", "");
        gs_putenv_r(kt_path);
    }

    /*
     * Notice: Do not free(kt_path).
     */

    rc = memset_s(&gss_conn, sizeof(gss_conn), 0, sizeof(gss_conn));
    securec_check(rc, "\0", "\0");

    gss_conn.sock = socket;
    gss_conn.gctx = GSS_C_NO_CONTEXT;

    /* Continue the authentication process. */
    return GssServerContinue(&gss_conn);
}

#endif /* ENABLE_GSS */

/* ----------------------------------------------------------------
 * SSPI authentication system
 * ----------------------------------------------------------------
 */
#ifdef ENABLE_SSPI
/*
 * Generate an error for SSPI authentication.  The caller should apply
 * _() to errmsg to make it translatable.
 */
static void pg_SSPI_error(int severity, const char* errmsg, SECURITY_STATUS r)
{
    char sysmsg[256];

    if (FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, NULL, r, 0, sysmsg, sizeof(sysmsg), NULL) == 0)
        ereport(severity, (errmsg_internal("%s", errmsg), errdetail_internal("SSPI error %x", (unsigned int)r)));
    else
        ereport(severity, (errmsg_internal("%s", errmsg), errdetail_internal("%s (%x)", sysmsg, (unsigned int)r)));
}

static int pg_SSPI_recvauth(Port* port)
{
    int mtype;
    int rcs = 0;
    StringInfoData buf;
    SECURITY_STATUS r;
    CredHandle sspicred;
    CtxtHandle *sspictx = NULL, newctx;
    TimeStamp expiry;
    ULONG contextattr;
    SecBufferDesc inbuf;
    SecBufferDesc outbuf;
    SecBuffer OutBuffers[1];
    SecBuffer InBuffers[1];
    HANDLE token = NULL;
    TOKEN_USER* tokenuser = NULL;
    DWORD retlen;
    char accountname[MAXPGPATH];
    char domainname[MAXPGPATH];
    DWORD accountnamesize = sizeof(accountname);
    DWORD domainnamesize = sizeof(domainname);
    SID_NAME_USE accountnameuse;
    HMODULE secur32 = NULL;
    QUERY_SECURITY_CONTEXT_TOKEN_FN _QuerySecurityContextToken = NULL;

    /*
     * SSPI auth is not supported for protocol versions before 3, because it
     * relies on the overall message length word to determine the SSPI payload
     * size in AuthenticationGSSContinue and PasswordMessage messages. (This
     * is, in fact, a design error in our SSPI support, because protocol
     * messages are supposed to be parsable without relying on the length
     * word; but it's not worth changing it now.)
     */
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
        ereport(FATAL, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SSPI is not supported in protocol version 2")));

    /*
     * Acquire a handle to the server credentials.
     */
    r = AcquireCredentialsHandle(NULL, "negotiate", SECPKG_CRED_INBOUND, NULL, NULL, NULL, NULL, &sspicred, &expiry);
    if (r != SEC_E_OK)
        pg_SSPI_error(ERROR, _("could not acquire SSPI credentials"), r);

    /*
     * Loop through SSPI message exchange. This exchange can consist of
     * multiple messags sent in both directions. First message is always from
     * the client. All messages from client to server are password packets
     * (type 'p').
     */
    do {
        mtype = pq_getbyte();
        if (mtype != 'p') {
            /* Only log error if client didn't disconnect. */
            if (mtype != EOF)
                ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg("expected SSPI response, got message type %d", mtype)));
            return STATUS_ERROR;
        }

        /* Get the actual SSPI token */
        initStringInfo(&buf);
        if (pq_getmessage(&buf, PG_MAX_AUTH_TOKEN_LENGTH)) {
            /* EOF - pq_getmessage already logged error */
            pfree(buf.data);
            buf.data = NULL;
            return STATUS_ERROR;
        }

        /* Map to SSPI style buffer */
        inbuf.ulVersion = SECBUFFER_VERSION;
        inbuf.cBuffers = 1;
        inbuf.pBuffers = InBuffers;
        InBuffers[0].pvBuffer = buf.data;
        InBuffers[0].cbBuffer = buf.len;
        InBuffers[0].BufferType = SECBUFFER_TOKEN;

        /* Prepare output buffer */
        OutBuffers[0].pvBuffer = NULL;
        OutBuffers[0].BufferType = SECBUFFER_TOKEN;
        OutBuffers[0].cbBuffer = 0;
        outbuf.cBuffers = 1;
        outbuf.pBuffers = OutBuffers;
        outbuf.ulVersion = SECBUFFER_VERSION;

        elog(DEBUG4, "Processing received SSPI token of length %u", (unsigned int)buf.len);

        r = AcceptSecurityContext(&sspicred,
            sspictx,
            &inbuf,
            ASC_REQ_ALLOCATE_MEMORY,
            SECURITY_NETWORK_DREP,
            &newctx,
            &outbuf,
            &contextattr,
            NULL);

        /* input buffer no longer used */
        pfree(buf.data);
        buf.data = NULL;

        if (outbuf.cBuffers > 0 && outbuf.pBuffers[0].cbBuffer > 0) {
            /*
             * Negotiation generated data to be sent to the client.
             */
            elog(DEBUG4, "sending SSPI response token of length %u", (unsigned int)outbuf.pBuffers[0].cbBuffer);

            port->gss->outbuf.length = outbuf.pBuffers[0].cbBuffer;
            port->gss->outbuf.value = outbuf.pBuffers[0].pvBuffer;

            sendAuthRequest(port, AUTH_REQ_GSS_CONT);

            FreeContextBuffer(outbuf.pBuffers[0].pvBuffer);
        }

        if (r != SEC_E_OK && r != SEC_I_CONTINUE_NEEDED) {
            if (sspictx != NULL) {
                DeleteSecurityContext(sspictx);
                pfree(sspictx);
            }
            FreeCredentialsHandle(&sspicred);
            pg_SSPI_error(ERROR, _("could not accept SSPI security context"), r);
        }

        /*
         * Overwrite the current context with the one we just received. If
         * sspictx is NULL it was the first loop and we need to allocate a
         * buffer for it. On subsequent runs, we can just overwrite the buffer
         * contents since the size does not change.
         */
        if (sspictx == NULL) {
            sspictx = MemoryContextAlloc(
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY), sizeof(CtxtHandle));
            if (sspictx == NULL)
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }

        rcs = memcpy_s(sspictx, sizeof(CtxtHandle), &newctx, sizeof(CtxtHandle));
        securec_check(rcs, "\0", "\0");
        if (r == SEC_I_CONTINUE_NEEDED)
            elog(DEBUG4, "SSPI continue needed");

    } while (r == SEC_I_CONTINUE_NEEDED);

    /*
     * Release service principal credentials
     */
    FreeCredentialsHandle(&sspicred);

    /*
     * SEC_E_OK indicates that authentication is now complete.
     *
     * Get the name of the user that authenticated, and compare it to the pg
     * username that was specified for the connection.
     *
     * MingW is missing the export for QuerySecurityContextToken in the
     * secur32 library, so we have to load it dynamically.
     */

    secur32 = LoadLibrary("SECUR32.DLL");
    if (secur32 == NULL)
        ereport(ERROR, (errmsg_internal("could not load secur32.dll: error code %lu", GetLastError())));

    _QuerySecurityContextToken = (QUERY_SECURITY_CONTEXT_TOKEN_FN)GetProcAddress(secur32, "QuerySecurityContextToken");
    if (_QuerySecurityContextToken == NULL) {
        FreeLibrary(secur32);
        ereport(ERROR,
            (errmsg_internal(
                "could not locate QuerySecurityContextToken in secur32.dll: error code %lu", GetLastError())));
    }

    r = (_QuerySecurityContextToken)(sspictx, &token);
    if (r != SEC_E_OK) {
        FreeLibrary(secur32);
        pg_SSPI_error(ERROR, _("could not get token from SSPI security context"), r);
    }

    FreeLibrary(secur32);

    /*
     * No longer need the security context, everything from here on uses the
     * token instead.
     */
    DeleteSecurityContext(sspictx);
    pfree(sspictx);

    if (!GetTokenInformation(token, TokenUser, NULL, 0, &retlen) && GetLastError() != 122)
        ereport(ERROR, (errmsg_internal("could not get token user size: error code %lu", GetLastError())));

    tokenuser = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY), retlen);
    if (tokenuser == NULL)
        ereport(ERROR, (errmsg("out of memory")));

    if (!GetTokenInformation(token, TokenUser, tokenuser, retlen, &retlen)) {
        pfree(tokenuser);
        ereport(ERROR, (errmsg_internal("could not get user token: error code %lu", GetLastError())));
    }

    if (!LookupAccountSid(
            NULL, tokenuser->User.Sid, accountname, &accountnamesize, domainname, &domainnamesize, &accountnameuse)) {
        pfree(tokenuser);
        ereport(ERROR, (errmsg_internal("could not look up account SID: error code %lu", GetLastError())));
    }

    pfree(tokenuser);

    /*
     * Compare realm/domain if requested. In SSPI, always compare case
     * insensitive.
     */
    if (port->hba->krb_realm != NULL && strlen(port->hba->krb_realm)) {
        if (pg_strcasecmp(port->hba->krb_realm, domainname) != 0) {
            elog(DEBUG2, "SSPI domain (%s) and configured domain (%s) don't match", domainname, port->hba->krb_realm);

            return STATUS_ERROR;
        }
    }

    /*
     * We have the username (without domain/realm) in accountname, compare to
     * the supplied value. In SSPI, always compare case insensitive.
     *
     * If set to include realm, append it in <username>@<realm> format.
     */
    if (port->hba->include_realm) {
        char* namebuf = NULL;
        int retval;

        namebuf = palloc(strlen(accountname) + strlen(domainname) + 2);
        retval = sprintf_s(namebuf, strlen(accountname) + strlen(domainname) + 2, "%s@%s", accountname, domainname);
        securec_check_ss(retval, "\0", "\0");
        retval = check_usermap(port->hba->usermap, port->user_name, namebuf, true);
        pfree(namebuf);
        namebuf = NULL;
        return retval;
    } else
        return check_usermap(port->hba->usermap, port->user_name, accountname, true);
}
#endif /* ENABLE_SSPI */

#ifdef USE_IDENT

/* ----------------------------------------------------------------
 * Ident authentication system
 * ----------------------------------------------------------------
 */

/*
 *	Parse the string "*ident_response" as a response from a query to an Ident
 *	server.  If it's a normal response indicating a user name, return true
 *	and store the user name at *ident_user. If it's anything else,
 *	return false.
 */
static bool interpret_ident_response(const char* ident_response, char* ident_user)
{
    const char* cursor = ident_response; /* Cursor into *ident_response */

    /*
     * Ident's response, in the telnet tradition, should end in crlf (\r\n).
     */
    if (strlen(ident_response) < 2)
        return false;
    else if (ident_response[strlen(ident_response) - 2] != '\r')
        return false;
    else {
        while (*cursor != ':' && *cursor != '\r')
            cursor++; /* skip port field */

        if (*cursor != ':')
            return false;
        else {
            /* We're positioned to colon before response type field */
            char response_type[80];
            int i; /* Index into *response_type */

            cursor++; /* Go over colon */
            while (pg_isblank(*cursor))
                cursor++; /* skip blanks */
            i = 0;
            while (*cursor != ':' && *cursor != '\r' && !pg_isblank(*cursor) && i < (int)(sizeof(response_type) - 1))
                response_type[i++] = *cursor++;
            response_type[i] = '\0';
            while (pg_isblank(*cursor))
                cursor++; /* skip blanks */
            if (strcmp(response_type, "USERID") != 0)
                return false;
            else {
                /*
                 * It's a USERID response.  Good.  "cursor" should be pointing
                 * to the colon that precedes the operating system type.
                 */
                if (*cursor != ':')
                    return false;
                else {
                    cursor++; /* Go over colon */
                    /* Skip over operating system field. */
                    while (*cursor != ':' && *cursor != '\r')
                        cursor++;
                    if (*cursor != ':')
                        return false;
                    else {
                        int i; /* Index into *ident_user */

                        cursor++; /* Go over colon */
                        while (pg_isblank(*cursor))
                            cursor++; /* skip blanks */
                        /* Rest of line is user name.  Copy it over. */
                        i = 0;
                        while (*cursor != '\r' && i < IDENT_USERNAME_MAX)
                            ident_user[i++] = *cursor++;
                        ident_user[i] = '\0';
                        return true;
                    }
                }
            }
        }
    }
}

/*
 *	Talk to the ident server on host "remote_ip_addr" and find out who
 *	owns the tcp connection from his port "remote_port" to port
 *	"local_port_addr" on host "local_ip_addr".	Return the user name the
 *	ident server gives as "*ident_user".
 *
 *	IP addresses and port numbers are in network byte order.
 *
 *	But iff we're unable to get the information from ident, return false.
 */
static int ident_inet(hbaPort* port)
{
    const SockAddr remote_addr = port->raddr;
    const SockAddr local_addr = port->laddr;
    char ident_user[IDENT_USERNAME_MAX + 1];
    pgsocket sock_fd = PGINVALID_SOCKET; /* for talking to Ident server */
    int rc;                              /* Return code from a locally called function */
    int rcs = 0;
    bool ident_return = false;
    char remote_addr_s[NI_MAXHOST];
    char remote_port[NI_MAXSERV];
    char local_addr_s[NI_MAXHOST];
    char local_port[NI_MAXSERV];
    char ident_port[NI_MAXSERV];
    char ident_query[80];
    char ident_response[80 + IDENT_USERNAME_MAX];
    struct addrinfo *ident_serv = NULL, *la = NULL, hints;

    /*
     * Might look a little weird to first convert it to text and then back to
     * sockaddr, but it's protocol independent.
     */
    (void)pg_getnameinfo_all(&remote_addr.addr,
        remote_addr.salen,
        remote_addr_s,
        sizeof(remote_addr_s),
        remote_port,
        sizeof(remote_port),
        NI_NUMERICHOST | NI_NUMERICSERV);
    (void)pg_getnameinfo_all(&local_addr.addr,
        local_addr.salen,
        local_addr_s,
        sizeof(local_addr_s),
        local_port,
        sizeof(local_port),
        NI_NUMERICHOST | NI_NUMERICSERV);

    rcs = snprintf_s(ident_port, NI_MAXSERV, NI_MAXSERV - 1, "%d", IDENT_PORT);
    securec_check_ss(rcs, "\0", "\0");
    hints.ai_flags = AI_NUMERICHOST;
    hints.ai_family = remote_addr.addr.ss_family;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;
    hints.ai_addrlen = 0;
    hints.ai_canonname = NULL;
    hints.ai_addr = NULL;
    hints.ai_next = NULL;
    rc = pg_getaddrinfo_all(remote_addr_s, ident_port, &hints, &ident_serv);
    if (rc || ident_serv == NULL) {
        /* we don't expect this to happen */
        ident_return = false;
        goto ident_inet_done;
    }

    hints.ai_flags = AI_NUMERICHOST;
    hints.ai_family = local_addr.addr.ss_family;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;
    hints.ai_addrlen = 0;
    hints.ai_canonname = NULL;
    hints.ai_addr = NULL;
    hints.ai_next = NULL;
    rc = pg_getaddrinfo_all(local_addr_s, NULL, &hints, &la);
    if (rc || la == NULL) {
        /* we don't expect this to happen */
        ident_return = false;
        goto ident_inet_done;
    }

    sock_fd = socket(ident_serv->ai_family, ident_serv->ai_socktype, ident_serv->ai_protocol);
    if (sock_fd < 0) {
        ereport(LOG, (errcode_for_socket_access(), errmsg("could not create socket for Ident connection: %m")));
        ident_return = false;
        goto ident_inet_done;
    }

#ifdef F_SETFD
    if (fcntl(sock_fd, F_SETFD, FD_CLOEXEC) == -1) {
        ereport(LOG, (errcode_for_socket_access(), errmsg("setsockopt(FD_CLOEXEC) failed: %m")));
        ident_return = false;
        goto ident_inet_done;
    }
#endif /* F_SETFD */

    /*
     * Bind to the address which the client originally contacted, otherwise
     * the ident server won't be able to match up the right connection. This
     * is necessary if the PostgreSQL server is running on an IP alias.
     */
    rc = bind(sock_fd, la->ai_addr, la->ai_addrlen);
    if (rc != 0) {
        ereport(LOG, (errcode_for_socket_access(), errmsg("could not bind to local address \"%s\": %m", local_addr_s)));
        ident_return = false;
        goto ident_inet_done;
    }

    rc = connect(sock_fd, ident_serv->ai_addr, ident_serv->ai_addrlen);
    if (rc != 0) {
        ereport(LOG,
            (errcode_for_socket_access(),
                errmsg("could not connect to Ident server at address \"%s\", port %s: %m", remote_addr_s, ident_port)));
        ident_return = false;
        goto ident_inet_done;
    }

    /* The query we send to the Ident server */
    rcs = snprintf_s(ident_query, sizeof(ident_query), sizeof(ident_query) - 1, "%s,%s\r\n", remote_port, local_port);
    securec_check_ss(rcs, "\0", "\0");

    /* loop in case send is interrupted */
    do {
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        rc = send(sock_fd, ident_query, strlen(ident_query), 0);
        END_NET_SEND_INFO(rc);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        ereport(LOG,
            (errcode_for_socket_access(),
                errmsg(
                    "could not send query to Ident server at address \"%s\", port %s: %m", remote_addr_s, ident_port)));
        ident_return = false;
        goto ident_inet_done;
    }

    do {
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        rc = recv(sock_fd, ident_response, sizeof(ident_response) - 1, 0);
        END_NET_RECV_INFO(rc);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        ereport(LOG,
            (errcode_for_socket_access(),
                errmsg("could not receive response from Ident server at address \"%s\", port %s: %m",
                    remote_addr_s,
                    ident_port)));
        ident_return = false;
        goto ident_inet_done;
    }

    ident_response[rc] = '\0';
    ident_return = interpret_ident_response(ident_response, ident_user);
    if (!ident_return)
        ereport(LOG, (errmsg("invalidly formatted response from Ident server: \"%s\"", ident_response)));

ident_inet_done:
    if (sock_fd >= 0)
        closesocket(sock_fd);
    if (ident_serv != NULL)
        pg_freeaddrinfo_all(remote_addr.addr.ss_family, ident_serv);
    if (la != NULL)
        pg_freeaddrinfo_all(local_addr.addr.ss_family, la);

    if (ident_return)
        /* Success! Check the usermap */
        return check_usermap(port->hba->usermap, port->user_name, ident_user, false);
    return STATUS_ERROR;
}

#endif

/*
 *	Ask kernel about the credentials of the connecting process,
 *	determine the symbolic name of the corresponding user, and check
 *	if valid per the usermap.
 *
 *	Iff authorized, return STATUS_OK, otherwise return STATUS_ERROR.
 */
#ifdef HAVE_UNIX_SOCKETS

static int auth_peer(hbaPort* port)
{
    char ident_user[IDENT_USERNAME_MAX + 1];
    uid_t uid;
    gid_t gid;
    struct passwd* pass = NULL;

    errno = 0;
    if (getpeereid(port->sock, &uid, &gid) != 0) {
        /* Provide special error message if getpeereid is a stub */
        if (errno == ENOSYS)
            ereport(LOG,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("peer authentication is not supported on this platform")));
        else
            ereport(LOG, (errcode_for_socket_access(), errmsg("could not get peer credentials: %m")));
        return STATUS_ERROR;
    }

    syscalllockAcquire(&getpwuid_lock);
    pass = getpwuid(uid);

    if (pass == NULL) {
        syscalllockRelease(&getpwuid_lock);
        ereport(LOG, (errmsg("local user with ID %d does not exist", (int)uid)));
        return STATUS_ERROR;
    }

    int rc = strcpy_s(ident_user, IDENT_USERNAME_MAX + 1, pass->pw_name);
    securec_check(rc, "\0", "\0");
    syscalllockRelease(&getpwuid_lock);

    return check_usermap(port->hba->usermap, port->user_name, ident_user, false);
}
#endif /* HAVE_UNIX_SOCKETS */

/* ----------------------------------------------------------------
 * PAM authentication system
 * ----------------------------------------------------------------
 */
#ifdef USE_PAM

/*
 * PAM conversation function
 */

static int pam_passwd_conv_proc(
    int num_msg, const struct pam_message** msg, struct pam_response** resp, void* appdata_ptr)
{
    char* passwd = NULL;
    struct pam_response* reply = NULL;
    int i;
    int rcs = 0;

    if (appdata_ptr != NULL)
        passwd = (char*)appdata_ptr;
    else {
        /*
         * Workaround for Solaris 2.6 where the PAM library is broken and does
         * not pass appdata_ptr to the conversation routine
         */
        passwd = g_instance.libpq_cxt.pam_passwd;
    }

    *resp = NULL; /* in case of error exit */

    if (num_msg <= 0 || num_msg > PAM_MAX_NUM_MSG)
        return PAM_CONV_ERR;

    /*
     * Explicitly not using palloc here - PAM will free this memory in
     * pam_end()
     */
    reply = MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY), num_msg * sizeof(struct pam_response));
    if (reply == NULL) {
        ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        return PAM_CONV_ERR;
    }

    for (i = 0; i < num_msg; i++) {
        switch (msg[i]->msg_style) {
            case PAM_PROMPT_ECHO_OFF:
                if (strlen(passwd) == 0) {
                    /*
                     * Password wasn't passed to PAM the first time around -
                     * let's go ask the client to send a password, which we
                     * then stuff into PAM.
                     */
                    sendAuthRequest(g_instance.ibpq_instance_cxt.pam_port_cludge, AUTH_REQ_PASSWORD);
                    passwd = recv_password_packet(g_instance.libpq_cxt.pam_port_cludge);
                    if (passwd == NULL) {
                        /*
                         * Client didn't want to send password.  We
                         * intentionally do not log anything about this.
                         */
                        goto fail;
                    }
                }
                reply[i].resp = MemoryContextStrdup(
                    SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY), passwd);
                if (reply[i].resp == NULL)
                    goto fail;
                reply[i].resp_retcode = PAM_SUCCESS;
                break;
            case PAM_ERROR_MSG:
                ereport(LOG, (errmsg("error from underlying PAM layer: %s", msg[i]->msg)));
                /* FALL THROUGH */
            case PAM_TEXT_INFO:
                /* we don't bother to log TEXT_INFO messages */
                reply[i].resp = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY), "");
                if (reply[i].resp == NULL)
                    goto fail;
                reply[i].resp_retcode = PAM_SUCCESS;
                break;
            default:
                elog(LOG,
                    "unsupported PAM conversation %d/\"%s\"",
                    msg[i]->msg_style,
                    msg[i]->msg ? msg[i]->msg : "(none)");
                goto fail;
        }
    }

    *resp = reply;
    rcs = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
    securec_check(rcs, "\0", "\0");
    return PAM_SUCCESS;

fail:
    /* clear sensitive info */
    if (passwd != NULL) {
        int len = strlen(passwd);
        rcs = memset_s(passwd, len, 0, len);
        securec_check(rcs, "", "");
    }
    /* free up whatever we allocated */
    for (i = 0; i < num_msg; i++) {
        if (reply[i].resp != NULL)
            pfree(reply[i].resp);
    }
    pfree(reply);

    return PAM_CONV_ERR;
}

/*
 * Check authentication against PAM.
 */
static int CheckPAMAuth(Port* port, char* user, char* password)
{
    int retval;
    pam_handle_t* pamh = NULL;

    /*
     * We can't entirely rely on PAM to pass through appdata --- it appears
     * not to work on at least Solaris 2.6.  So use these ugly static
     * variables instead.
     */
    g_instance.libpq_cxt.pam_passwd = password;
    g_instance.libpq_cxt.pam_port_cludge = port;

    /*
     * Set the application data portion of the conversation struct This is
     * later used inside the PAM conversation to pass the password to the
     * authentication module.
     */
    pam_passw_conv.appdata_ptr = (char*)password; /* from password above,
                                                   * not allocated */

    /* Optionally, one can set the service name in pg_hba.conf */
    if (port->hba->pamservice != NULL && port->hba->pamservice[0] != '\0')
        retval = pam_start(port->hba->pamservice, "pgsql@", &pam_passw_conv, &pamh);
    else
        retval = pam_start(PGSQL_PAM_SERVICE, "pgsql@", &pam_passw_conv, &pamh);

    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("could not create PAM authenticator: %s", pam_strerror(pamh, retval))));
        g_instance.libpq_cxt.pam_passwd = NULL; /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    retval = pam_set_item(pamh, PAM_USER, user);

    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("pam_set_item(PAM_USER) failed: %s", pam_strerror(pamh, retval))));
        g_instance.libpq_cxt.pam_passwd = NULL; /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    retval = pam_set_item(pamh, PAM_CONV, &pam_passw_conv);

    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("pam_set_item(PAM_CONV) failed: %s", pam_strerror(pamh, retval))));
        g_instance.libpq_cxt.pam_passwd = NULL; /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    retval = pam_authenticate(pamh, 0);

    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("pam_authenticate failed: %s", pam_strerror(pamh, retval))));
        g_instance.libpq_cxt.pam_passwd = NULL; /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    retval = pam_acct_mgmt(pamh, 0);

    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("pam_acct_mgmt failed: %s", pam_strerror(pamh, retval))));
        g_instance.libpq_cxt.pam_passwd = NULL; /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    retval = pam_end(pamh, retval);

    if (retval != PAM_SUCCESS) {
        ereport(LOG, (errmsg("could not release PAM authenticator: %s", pam_strerror(pamh, retval))));
    }

    g_instance.libpq_cxt.pam_passwd = NULL; /* Unset pam_passwd */

    return ((retval == PAM_SUCCESS) ? STATUS_OK : STATUS_ERROR);
}
#endif /* USE_PAM */

/* ----------------------------------------------------------------
 * LDAP authentication system
 * ----------------------------------------------------------------
 */
#ifdef USE_LDAP

/*
 * Initialize a connection to the LDAP server, including setting up
 * TLS if requested.
 */
static int InitializeLDAPConnection(Port* port, LDAP** ldap)
{
    int ldapversion = LDAP_VERSION3;
    int r;

    *ldap = ldap_init(port->hba->ldapserver, port->hba->ldapport);
    if (*ldap == NULL) {
#ifndef WIN32
        ereport(LOG, (errmsg("could not initialize LDAP: error code %d", errno)));
#else
        ereport(LOG, (errmsg("could not initialize LDAP: error code %d", (int)LdapGetLastError())));
#endif
        return STATUS_ERROR;
    }

    if ((r = ldap_set_option(*ldap, LDAP_OPT_PROTOCOL_VERSION, &ldapversion)) != LDAP_SUCCESS) {
        ldap_unbind(*ldap);
        ereport(LOG, (errmsg("could not set LDAP protocol version: error code %d", r)));
        return STATUS_ERROR;
    }

    if (port->hba->ldaptls) {
#ifndef WIN32
        if ((r = ldap_start_tls_s(*ldap, NULL, NULL)) != LDAP_SUCCESS)
#else
        static __ldap_start_tls_sA _ldap_start_tls_sA = NULL;

        if (_ldap_start_tls_sA == NULL) {
            /*
             * Need to load this function dynamically because it does not
             * exist on Windows 2000, and causes a load error for the whole
             * exe if referenced.
             */
            HANDLE ldaphandle;

            ldaphandle = LoadLibrary("WLDAP32.DLL");
            if (ldaphandle == NULL) {
                /*
                 * should never happen since we import other files from
                 * wldap32, but check anyway
                 */
                ldap_unbind(*ldap);
                ereport(LOG, (errmsg("could not load wldap32.dll")));
                return STATUS_ERROR;
            }
            _ldap_start_tls_sA = (__ldap_start_tls_sA)GetProcAddress(ldaphandle, "ldap_start_tls_sA");
            if (_ldap_start_tls_sA == NULL) {
                ldap_unbind(*ldap);
                FreeLibrary(ldaphandle);
                ereport(LOG,
                    (errmsg("could not load function _ldap_start_tls_sA in wldap32.dll"),
                        errdetail("LDAP over SSL is not supported on this platform.")));
                return STATUS_ERROR;
            }

            /*
             * Leak LDAP handle on purpose, because we need the library to
             * stay open. This is ok because it will only ever be leaked once
             * per process and is automatically cleaned up on process exit.
             */
        }
        if ((r = _ldap_start_tls_sA(*ldap, NULL, NULL, NULL, NULL)) != LDAP_SUCCESS)
#endif
        {
            ldap_unbind(*ldap);
            ereport(LOG, (errmsg("could not start LDAP TLS session: error code %d", r)));
            return STATUS_ERROR;
        }
    }

    return STATUS_OK;
}

/*
 * Perform LDAP authentication
 */
static int CheckLDAPAuth(Port* port)
{
    char* passwd = NULL;
    LDAP* ldap = NULL;
    int r;
    char* fulluser = NULL;

    if (port->hba->ldapserver == NULL || port->hba->ldapserver[0] == '\0') {
        ereport(LOG, (errmsg("LDAP server not specified")));
        return STATUS_ERROR;
    }

    if (port->hba->ldapport == 0)
        port->hba->ldapport = LDAP_PORT;

    sendAuthRequest(port, AUTH_REQ_PASSWORD);

    passwd = recv_password_packet(port);
    if (passwd == NULL) {
        return STATUS_EOF; /* client wouldn't send password */
    }

    int passwdLen = strlen(passwd);
    if (InitializeLDAPConnection(port, &ldap) == STATUS_ERROR) {
        /* Error message already sent */
        CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
        return STATUS_ERROR;
    }

    if (port->hba->ldapbasedn != NULL) {
        /*
         * First perform an LDAP search to find the DN for the user we are
         * trying to log in as.
         */
        char* filter = NULL;
        LDAPMessage* search_message = NULL;
        LDAPMessage* entry = NULL;
        char* attributes[2];
        char* dn = NULL;
        char* c = NULL;

        /*
         * Disallow any characters that we would otherwise need to escape,
         * since they aren't really reasonable in a username anyway. Allowing
         * them would make it possible to inject any kind of custom filters in
         * the LDAP filter.
         */
        for (c = port->user_name; *c; c++) {
            if (*c == '*' || *c == '(' || *c == ')' || *c == '\\' || *c == '/') {
                ereport(LOG, (errmsg("invalid character in user name for LDAP authentication")));

                CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
                return STATUS_ERROR;
            }
        }

        /*
         * Bind with a pre-defined username/password (if available) for
         * searching. If none is specified, this turns into an anonymous bind.
         */
        r = ldap_simple_bind_s(ldap,
            port->hba->ldapbinddn ? port->hba->ldapbinddn : "",
            port->hba->ldapbindpasswd ? port->hba->ldapbindpasswd : "");
        if (r != LDAP_SUCCESS) {
            ereport(LOG,
                (errmsg("could not perform initial LDAP bind for ldapbinddn \"%s\" on server \"%s\": error code %d",
                    port->hba->ldapbinddn ? port->hba->ldapbinddn : "",
                    port->hba->ldapserver,
                    r)));

            CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
            return STATUS_ERROR;
        }

        /* Fetch just one attribute, else *all* attributes are returned */
        attributes[0] = port->hba->ldapsearchattribute ? port->hba->ldapsearchattribute : "uid";
        attributes[1] = NULL;

        filter = palloc(strlen(attributes[0]) + strlen(port->user_name) + 4);
        r = sprintf_s(
            filter, strlen(attributes[0]) + strlen(port->user_name) + 4, "(%s=%s)", attributes[0], port->user_name);
        securec_check_ss(r, "\0", "\0");

        r = ldap_search_s(ldap, port->hba->ldapbasedn, LDAP_SCOPE_SUBTREE, filter, attributes, 0, &search_message);

        if (r != LDAP_SUCCESS) {
            ereport(LOG,
                (errmsg("could not search LDAP for filter \"%s\" on server \"%s\": error code %d",
                    filter,
                    port->hba->ldapserver,
                    r)));
            pfree(filter);
            filter = NULL;

            CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
            return STATUS_ERROR;
        }

        if (ldap_count_entries(ldap, search_message) != 1) {
            if (ldap_count_entries(ldap, search_message) == 0)
                ereport(LOG,
                    (errmsg("LDAP search failed for filter \"%s\" on server \"%s\": no such user",
                        filter,
                        port->hba->ldapserver)));
            else
                ereport(LOG,
                    (errmsg("LDAP search failed for filter \"%s\" on server \"%s\": user is not unique (%ld matches)",
                        filter,
                        port->hba->ldapserver,
                        (long)ldap_count_entries(ldap, search_message))));

            pfree(filter);
            filter = NULL;
            ldap_msgfree(search_message);

            CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
            return STATUS_ERROR;
        }

        entry = ldap_first_entry(ldap, search_message);
        dn = ldap_get_dn(ldap, entry);
        if (dn == NULL) {
            int error;

            (void)ldap_get_option(ldap, LDAP_OPT_ERROR_NUMBER, &error);
            ereport(LOG,
                (errmsg("could not get dn for the first entry matching \"%s\" on server \"%s\": %s",
                    filter,
                    port->hba->ldapserver,
                    ldap_err2string(error))));
            pfree(filter);
            filter = NULL;
            ldap_msgfree(search_message);

            CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
            return STATUS_ERROR;
        }
        fulluser = pstrdup(dn);

        pfree(filter);
        filter = NULL;
        ldap_memfree(dn);
        ldap_msgfree(search_message);

        /* Unbind and disconnect from the LDAP server */
        r = ldap_unbind_s(ldap);
        if (r != LDAP_SUCCESS) {
            int error;

            (void)ldap_get_option(ldap, LDAP_OPT_ERROR_NUMBER, &error);
            ereport(LOG,
                (errmsg("could not unbind after searching for user \"%s\" on server \"%s\": %s",
                    fulluser,
                    port->hba->ldapserver,
                    ldap_err2string(error))));
            pfree(fulluser);
            fulluser = NULL;

            CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
            return STATUS_ERROR;
        }

        /*
         * Need to re-initialize the LDAP connection, so that we can bind to
         * it with a different username.
         */
        if (InitializeLDAPConnection(port, &ldap) == STATUS_ERROR) {
            pfree(fulluser);
            fulluser = NULL;

            CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
            /* Error message already sent */
            return STATUS_ERROR;
        }
    } else {
        int fulluser_len = (port->hba->ldapprefix ? strlen(port->hba->ldapprefix) : 0) + strlen(port->user_name) +
                           (port->hba->ldapsuffix ? strlen(port->hba->ldapsuffix) : 0) + 1;
        fulluser = palloc(fulluser_len);
        r = sprintf_s(fulluser,
            fulluser_len,
            "%s%s%s",
            port->hba->ldapprefix ? port->hba->ldapprefix : "",
            port->user_name,
            port->hba->ldapsuffix ? port->hba->ldapsuffix : "");
        securec_check_ss(r, "\0", "\0");
    }

    r = ldap_simple_bind_s(ldap, fulluser, passwd);
    ldap_unbind(ldap);

    if (r != LDAP_SUCCESS) {
        ereport(LOG,
            (errmsg("LDAP login failed for user \"%s\" on server \"%s\": error code %d",
                fulluser,
                port->hba->ldapserver,
                r)));
        pfree(fulluser);
        fulluser = NULL;

        CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
        return STATUS_ERROR;
    }

    pfree(fulluser);
    fulluser = NULL;

    CLEAR_AND_FREE_PASSWORD(passwd, passwdLen);
    return STATUS_OK;
}
#endif /* USE_LDAP */

/* ----------------------------------------------------------------
 * SSL client certificate authentication
 * ----------------------------------------------------------------
 */
#ifdef USE_SSL
static int CheckCertAuth(Port* port)
{
    Assert(port->ssl);

    /* Make sure we have received a username in the certificate */
    if (port->peer_cn == NULL || strlen(port->peer_cn) <= 0) {
        ereport(LOG,
            (errmsg("certificate authentication failed for user \"%s\": client certificate contains no user name",
                port->user_name)));
        return STATUS_ERROR;
    }

    /* Just pass the certificate CN to the usermap check */
    return check_usermap(port->hba->usermap, port->user_name, port->peer_cn, false);
}
#endif

/*
 * @Description: the main function for iam authenication check.
 * @in port : the port which contain socket info for recv password from client.
 * @return : status of check, STATUS_OK for ok.
 */
static int CheckIAMAuth(Port* port)
{
    iam_token token;
    int rcs = 0;

    /* init the value of the token struct. */
    token.expires_at = NULL;
    token.username = NULL;
    token.role_priv = false;
    token.cluster_id = NULL;
    token.tenant_id = NULL;
    bool save_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

    sendAuthRequest(port, AUTH_REQ_PASSWORD);

    char* passwd = recv_password_packet(port);

    /*
     * Disable immediate response to SIGTERM/SIGINT/timeout interrupts as there are
     * some cache and memory  operations which can not be interrupted. And nothing will
     * block here, so disable the interrupts is ok.
     */
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    if (passwd == NULL) {
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return STATUS_EOF;
    }

    if (strlen(passwd) == 0) {
        ereport(LOG, (errmsg("empty password from client.")));
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return STATUS_ERROR;
    }

    /* parse the token from client. */
    if (!parse_token(passwd, &token)) {
        ereport(LOG, (errmsg("parse token failed.")));
        if (NULL != token.cluster_id) {
            pfree(token.cluster_id);
            token.cluster_id = NULL;
        }
        rcs = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
        securec_check(rcs, "\0", "\0");

        pfree(passwd);
        passwd = NULL;
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return STATUS_ERROR;
    }

    /* check the auth messages in token. */
    if (!check_token(token, port->user_name)) {
        ereport(LOG, (errmsg("check token failed.")));
        pfree(token.cluster_id);
        token.cluster_id = NULL;
        rcs = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
        securec_check(rcs, "\0", "\0");

        pfree(passwd);
        passwd = NULL;
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return STATUS_ERROR;
    }

    pfree(token.cluster_id);
    token.cluster_id = NULL;

    /* Resume t_thrd.int_cxt.ImmediateInterruptOK. */
    t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
    rcs = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
    securec_check(rcs, "\0", "\0");

    pfree(passwd);
    passwd = NULL;
    return STATUS_OK;
}

/*
 * release kerberos gss connection info 
 * if the handle to be released is specified GSS_C_NO_CREDENTIAL or GSS_C_NO_CONTEXT(which is initial status), 
 * the function will complete successfully but do nothing, so that it's safe to invoke the function without pre-judge
 */
static void clear_gss_info(pg_gssinfo* gss)
{
    /* status codes coming from gss interface */
    OM_uint32 lmin_s = 0;
    /* Release service principal credentials */
    (void)gss_release_cred(&lmin_s, &gss->cred);
    /* Release gss security context and name after server authentication finished */
    (void)gss_delete_sec_context(&lmin_s, &gss->ctx, GSS_C_NO_BUFFER);
    /* Release gss_name and gss_buf */
    (void)gss_release_name(&lmin_s, &gss->name);
}
