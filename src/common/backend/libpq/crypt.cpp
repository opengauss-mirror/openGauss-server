/* -------------------------------------------------------------------------
 *
 * crypt.cpp
 *	  Look into the password file and check the encrypted password with
 *	  the one passed in from the frontend.
 *
 * Original coding by Todd A. Brandys
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/common/backend/libpq/crypt.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef HAVE_CRYPT_H
#include <crypt.h>
#endif

#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "libpq/crypt.h"
#include "libpq/md5.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "cipher.h"
#include "openssl/evp.h"

/* IAM authenication: iam role and config path info. */
#define IAM_ROLE_WITH_DWSDB_PRIV "dws_db_acc"
#define DWS_IAMAUTH_CONFIG_PATH "/opt/dws/iamauth/"
#define CLUSTERID_LEN 36 //clusterid example:36cessba-ec3d-478f-a5b7-561a9a5f463f
#define TENANTID_LEN 32 //tenantid example:9e57dcaa89164a149f1b5f7130c49c52

static bool GetValidPeriod(const char *role, password_info *passInfo);

/*
 *Funcation  : get_password_stored_method
 *Description: get user password stored method : MD5 SHA256 COMBINED or PLAIN
 */

int32 get_password_stored_method(const char* role, char* encrypted_string, int len)
{
    char* shadow_pass = NULL;
    password_info pass_info = {NULL, 0, 0, false, false};
    int32 stored_method;
    errno_t rc = EOK;

    if (encrypted_string == NULL)
        return BAD_MEM_ADDR;

    if (get_stored_password(role, &pass_info)) {
        shadow_pass = pass_info.shadow_pass;
    } else {
        pfree_ext(pass_info.shadow_pass);
        return ERROR_PASSWORD;
    }

    if (isMD5(shadow_pass)) {
        stored_method = MD5_PASSWORD;
    } else if (isSHA256(shadow_pass)) {
        rc = strncpy_s((char*)encrypted_string, len, shadow_pass + SHA256_LENGTH, ENCRYPTED_STRING_LENGTH);
        securec_check(rc, "\0", "\0");
        stored_method = SHA256_PASSWORD;
    } else if (isSM3(shadow_pass)) {
        rc = strncpy_s((char*)encrypted_string, len, shadow_pass + SM3_LENGTH, ENCRYPTED_STRING_LENGTH);
        securec_check(rc, "\0", "\0");
        stored_method = SM3_PASSWORD;
    } else if (isCOMBINED(shadow_pass)) {
        rc = strncpy_s((char*)encrypted_string, len, shadow_pass + SHA256_LENGTH, ENCRYPTED_STRING_LENGTH);
        securec_check(rc, "\0", "\0");
        stored_method = COMBINED_PASSWORD;
    } else {
        stored_method = PLAIN_PASSWORD;
    }

    pfree_ext(pass_info.shadow_pass);
    return stored_method;
}

/*
 *Funcation  : get_stored_password
 *Description: get user password information
 */
bool get_stored_password(const char* role, password_info* pass_info)
{
    HeapTuple roleTup = NULL;
    Datum datum1;
    Datum datum2;
    bool save_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

    /*
     * Disable immediate interrupts while doing database access.  (Note we
     * don't bother to turn this back on if we hit one of the failure
     * conditions, since we can expect we'll just exit right away anyway.)
     */
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    /* Get role info from pg_authid */
    if (strchr(role, '@') && !OidIsValid(u_sess->proc_cxt.MyDatabaseId))
        ereport(ERROR,(errcode(ERRCODE_INVALID_NAME),errmsg("@ can't be allowed in username")));
    roleTup = SearchUserHostName(role, NULL);

    if (!HeapTupleIsValid(roleTup)) {
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return false; /* no such user */
    }

    /*
     * Since pass_info->isnull_begin will be reset later, it is only
     * used as a tag here, with no real meaning.
     */
    datum1 = SysCacheGetAttr(AUTHNAME, roleTup, Anum_pg_authid_rolpassword, &(pass_info->isnull_begin));
    if (pass_info->isnull_begin) {
        ReleaseSysCache(roleTup);
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return false; /* user has no password */
    }
    pass_info->shadow_pass = TextDatumGetCString(datum1);

    /*Get the date validation of password, including starting and expiration time */
    datum1 = SysCacheGetAttr(AUTHNAME, roleTup, Anum_pg_authid_rolvalidbegin, &(pass_info->isnull_begin));

    if (!(pass_info->isnull_begin))
        pass_info->vbegin = DatumGetTimestampTz(datum1);

    datum2 = SysCacheGetAttr(AUTHNAME, roleTup, Anum_pg_authid_rolvaliduntil, &(pass_info->isnull_until));

    if (!(pass_info->isnull_until))
        pass_info->vuntil = DatumGetTimestampTz(datum2);

    ReleaseSysCache(roleTup);

    if (*(pass_info->shadow_pass) == '\0') {
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return false; /* empty password */
    }

    /* Resume response to SIGTERM/SIGINT/timeout interrupts */
    t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
    /* And don't forget to detect one that already arrived */
    CHECK_FOR_INTERRUPTS();

    return true;
}

/*
*Funcation  : GetValidPeriod
*Description: ONLY get user valid peroid information
*/
static bool GetValidPeriod(const char *role, password_info *passInfo)
{
    bool save_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    
    /*
     * Disable immediate interrupts while doing database access.  (Note we
     * don't bother to turn this back on if we hit one of the failure
     * conditions, since we can expect we'll just exit right away anyway.)
     */
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    
    /* Get role info from pg_authid */
    if (strchr(role, '@') && !OidIsValid(u_sess->proc_cxt.MyDatabaseId))
        ereport(ERROR,(errcode(ERRCODE_INVALID_NAME),errmsg("@ can't be allowed in username")));
    HeapTuple roleTup = SearchUserHostName(role, NULL);
    if (!HeapTupleIsValid(roleTup)) {
        t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
        return false;	/* no such user */
    }
    
    /*Get the date validation of password, including starting and expiration time */
    Datum datum1 = SysCacheGetAttr(AUTHNAME, roleTup,
                                   Anum_pg_authid_rolvalidbegin, &(passInfo->isnull_begin));
    
    if (!(passInfo->isnull_begin)) {
        passInfo->vbegin = DatumGetTimestampTz(datum1);
    }
    
    Datum datum2 = SysCacheGetAttr(AUTHNAME, roleTup,
                                   Anum_pg_authid_rolvaliduntil, &(passInfo->isnull_until));
    
    if (!(passInfo->isnull_until)) {
        passInfo->vuntil = DatumGetTimestampTz(datum2);
    }
    
    ReleaseSysCache(roleTup);
    
    /* Resume response to SIGTERM/SIGINT/timeout interrupts */
    t_thrd.int_cxt.ImmediateInterruptOK = save_ImmediateInterruptOK;
    /* And don't forget to detect one that already arrived */
    CHECK_FOR_INTERRUPTS();
    
    return true;
}


/* Database Security: check user valid state */
int CheckUserValid(Port* port, const char* role)
{
    TimestampTz vbegin = 0;
    TimestampTz vuntil = 0;
    bool isnull_begin = false;
    bool isnull_until = false;
    password_info pass_info = {NULL, 0, 0, false, false};
    int retval = STATUS_ERROR;

    if (GetValidPeriod(role, &pass_info)) {
        isnull_begin = pass_info.isnull_begin;
        vbegin = pass_info.vbegin;
        isnull_until = pass_info.isnull_until;
        vuntil = pass_info.vuntil;
    } else {
        return STATUS_ERROR;
    }

    if (isnull_begin && isnull_until) {
        retval = STATUS_OK;
    } else if ((!isnull_begin && vbegin > GetCurrentTimestamp()) || (!isnull_until && vuntil < GetCurrentTimestamp())) {
        retval = STATUS_EXPIRED;
    } else {
        retval = STATUS_OK;
    }

    return retval;
}

/*
 * Brief			: whether the digest of passwd equal to the passDigest
 * Description		: the compare mainly contains the digest of password
 * Notes			:
 */
bool VerifyPasswdDigest(const char* rolename, char* passwd, char* passDigest)
{
    char encrypted_md5_password[MD5_PASSWD_LEN + 1] = {0};
    char encrypted_sha256_password[SHA256_PASSWD_LEN + 1] = {0};
    char encryptedSm3Password[SM3_PASSWD_LEN + 1] = {0};
    char encrypted_combined_password[MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1] = {0};
    char salt[SALT_LENGTH * 2 + 1] = {0};
    int iteration_count = 0;
    errno_t rc = EOK;

    if (passwd == NULL || passDigest == NULL || strlen(passDigest) == 0) {
        return false;
    }

    if (isMD5(passDigest)) {
        if (!pg_md5_encrypt(passwd, rolename, strlen(rolename), encrypted_md5_password)) {
            /* Var 'encrypted_md5_password' has not been assigned any value if the function return 'false' */
            str_reset(passwd);
            str_reset(passDigest);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("md5-password encryption failed.")));
        }
        if (strncmp(passDigest, encrypted_md5_password, MD5_PASSWD_LEN + 1) == 0) {
            rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            return true;
        }
    } else if (isSHA256(passDigest)) {
        rc = strncpy_s(salt, sizeof(salt), &passDigest[SHA256_LENGTH], sizeof(salt) - 1);
        securec_check(rc, "\0", "\0");
        salt[sizeof(salt) - 1] = '\0';

        iteration_count = get_stored_iteration(rolename);
        if (iteration_count == -1) {
            iteration_count = ITERATION_COUNT;
        }
        if (!pg_sha256_encrypt(passwd, salt, strlen(salt), encrypted_sha256_password, NULL, iteration_count)) {
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            str_reset(passwd);
            str_reset(passDigest);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("sha256-password encryption failed.")));
        }

        if (strncmp(passDigest, encrypted_sha256_password, SHA256_LENGTH + SALT_STRING_LENGTH) == 0 && 
            strncmp(passDigest + (SHA256_PASSWD_LEN - STORED_KEY_STRING_LENGTH), encrypted_sha256_password + 
                (SHA256_PASSWD_LEN - STORED_KEY_STRING_LENGTH), STORED_KEY_STRING_LENGTH) == 0) {
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            return true;
        }
    } else if (isSM3(passDigest)) {
        rc = strncpy_s(salt, sizeof(salt), &passDigest[SM3_LENGTH], sizeof(salt) - 1);
        securec_check(rc, "\0", "\0");
        salt[sizeof(salt) - 1] = '\0';

        iteration_count = get_stored_iteration(rolename);
        if (!GsSm3Encrypt(passwd, salt, strlen(salt), encryptedSm3Password, NULL, iteration_count)) {
            rc = memset_s(encryptedSm3Password, SM3_PASSWD_LEN + 1, 0, SM3_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("sm3-password encryption failed.")));
        }

        if (strncmp(passDigest, encryptedSm3Password, SM3_PASSWD_LEN) == 0) {
            rc = memset_s(encryptedSm3Password, SM3_PASSWD_LEN + 1, 0, SM3_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            return true;
        }
    } else if (isCOMBINED(passDigest)) {
        rc = strncpy_s(salt, sizeof(salt), &passDigest[SHA256_LENGTH], sizeof(salt) - 1);
        securec_check(rc, "\0", "\0");
        salt[sizeof(salt) - 1] = '\0';

        iteration_count = get_stored_iteration(rolename);
        if (iteration_count == -1) {
            iteration_count = ITERATION_COUNT;
        }
        if (!pg_sha256_encrypt(passwd, salt, strlen(salt), encrypted_sha256_password, NULL, iteration_count)) {
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            str_reset(passwd);
            str_reset(passDigest);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("first stage encryption password failed")));
        }

        if (!pg_md5_encrypt(passwd, rolename, strlen(rolename), encrypted_md5_password)) {
            /* Var 'encrypted_md5_password' has not been assigned any value if the function return 'false' */
            str_reset(passwd);
            str_reset(passDigest);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("second stage encryption password failed")));
        }

        rc = snprintf_s(encrypted_combined_password,
            MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1,
            MD5_PASSWD_LEN + SHA256_PASSWD_LEN,
            "%s%s",
            encrypted_sha256_password,
            encrypted_md5_password);
        securec_check_ss(rc, "\0", "\0");

        /*
         * When alter user's password:
         * 1. No need to compare the new iteration and old iteration.
         * 2. No need to compare MD5 password, just compare SHA256 is enough.
         */
        if (strncmp(passDigest, encrypted_combined_password, SHA256_LENGTH + SALT_STRING_LENGTH) == 0 && 
            strncmp(passDigest + (SHA256_PASSWD_LEN - STORED_KEY_STRING_LENGTH), encrypted_combined_password + 
                (SHA256_PASSWD_LEN - STORED_KEY_STRING_LENGTH), STORED_KEY_STRING_LENGTH) == 0) {
            /* clear the sensitive messages in the stack. */
            rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            rc = memset_s(encrypted_combined_password,
                MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1,
                0,
                MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1);
            securec_check(rc, "\0", "\0");
            return true;
        }
    } else {
        if (strcmp(passwd, passDigest) == 0) {
            return true;
        }
    }

    /* clear sensitive messages in stack. */
    rc = memset_s(encrypted_md5_password, MD5_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(encrypted_sha256_password, SHA256_PASSWD_LEN + 1, 0, SHA256_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(encryptedSm3Password, SM3_PASSWD_LEN + 1, 0, SM3_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(
        encrypted_combined_password, MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1, 0, MD5_PASSWD_LEN + SHA256_PASSWD_LEN + 1);
    securec_check(rc, "\0", "\0");

    return false;
}

/* Database Security:  Support SHA256.*/
int crypt_verify(const Port* port, const char* role, char* client_pass)
{
    int retval = STATUS_ERROR;
    char* shadow_pass = NULL;
    char* crypt_pwd = NULL;
    char combined_shadow_pass[SHA256_PASSWD_LEN + MD5_PASSWD_LEN + ITERATION_STRING_LEN + 1] = {0};
    char* crypt_client_pass = client_pass;
    int CRYPT_hmac_ret;
    int CRYPT_digest_ret;
    bool RFC5802_pass = false;

    password_info pass_info = {NULL, 0, 0, false, false};
    errno_t errorno = EOK;

    if (get_stored_password(role, &pass_info)) {
        /*
         * We just extract appropriate password encrypted information in upper layer
         * according to the authentication method. And there is no need to check
         * whether it is combined password later.
         */
        if (isCOMBINED(pass_info.shadow_pass)) {
            if (port->hba->auth_method == uaMD5) {
                errorno = memcpy_s(combined_shadow_pass,
                    SHA256_PASSWD_LEN + MD5_PASSWD_LEN + ITERATION_STRING_LEN + 1,
                    &pass_info.shadow_pass[SHA256_PASSWD_LEN],
                    MD5_PASSWD_LEN);
                securec_check(errorno, "\0", "\0");
            }
            if (port->hba->auth_method == uaSHA256) {
                errorno = memcpy_s(combined_shadow_pass,
                    SHA256_PASSWD_LEN + MD5_PASSWD_LEN + ITERATION_STRING_LEN + 1,
                    pass_info.shadow_pass,
                    SHA256_PASSWD_LEN);
                securec_check(errorno, "\0", "\0");
                errorno = memcpy_s(combined_shadow_pass + SHA256_PASSWD_LEN,
                    MD5_PASSWD_LEN + ITERATION_STRING_LEN + 1,
                    &pass_info.shadow_pass[SHA256_PASSWD_LEN + MD5_PASSWD_LEN],
                    ITERATION_STRING_LEN);
                securec_check(errorno, "\0", "\0");
            }

            shadow_pass = combined_shadow_pass;
        } else {
            shadow_pass = pass_info.shadow_pass;
        }
    } else {
        return STATUS_ERROR;
    }

    /*
     * Don't allow an empty password. Libpq treats an empty password the same
     * as no password at all, and won't even try to authenticate. But other
     * clients might, so allowing it would be confusing.
     *
     * For a plaintext password, we can simply check that it's not an empty
     * string. For an encrypted password, check that it does not match the MD5
     * hash of an empty string.
     */
    if (shadow_pass == NULL || *shadow_pass == '\0')
        return STATUS_ERROR; /* empty password */

    if (VerifyPasswdDigest(port->user_name, "\0", shadow_pass)) {
        return STATUS_ERROR;
    }

    /*
     * Compare with the encrypted or plain password depending on the
     * authentication method being used for this connection.
     */
    switch (port->hba->auth_method) {
        case uaMD5:
            crypt_pwd = (char*)palloc(MD5_PASSWD_LEN + 1);
            if (crypt_pwd != NULL) {
                errorno = memset_s(crypt_pwd, (MD5_PASSWD_LEN + 1), 0, (MD5_PASSWD_LEN + 1));
                securec_check(errorno, "\0", "\0");
            }else {
                return STATUS_ERROR;
            }

            if (isMD5(shadow_pass)) {
                /* stored password already md5 encrypted, only do md5 salt */
                if (!pg_md5_encrypt(shadow_pass + strlen("md5"), port->md5Salt, sizeof(port->md5Salt), crypt_pwd)) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
            }
            /* Database Security:  Support SHA256.*/
            else if (isSHA256(shadow_pass)) {
                /* stored password already sha256 encrypted, only do md5 salt */
                if (!pg_md5_encrypt(shadow_pass + SHA256_LENGTH, port->md5Salt, sizeof(port->md5Salt), crypt_pwd)) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
            } else {
                /* stored password is plain, double-md5 encrypt */
                char* crypt_pwd2 = (char*)palloc(MD5_PASSWD_LEN + 1);
                errorno = memset_s(crypt_pwd2, (MD5_PASSWD_LEN + 1), 0, (MD5_PASSWD_LEN + 1));
                securec_check(errorno, "\0", "\0");

                if (!pg_md5_encrypt(shadow_pass, port->user_name, strlen(port->user_name), crypt_pwd2)) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(crypt_pwd2);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
                if (!pg_md5_encrypt(crypt_pwd2 + strlen("md5"), port->md5Salt, sizeof(port->md5Salt), crypt_pwd)) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(crypt_pwd2);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
                pfree_ext(crypt_pwd2);
            }
            break;

        /* Database Security:  Support SHA256.*/
        case uaSHA256: {
            crypt_pwd = (char*)palloc(SHA256_MD5_ENCRY_PASSWD_LEN + 1);
            if (crypt_pwd == NULL) {
                pfree_ext(pass_info.shadow_pass);
                return STATUS_ERROR;
            }
            errorno = memset_s(crypt_pwd, (SHA256_MD5_ENCRY_PASSWD_LEN + 1), 0, (SHA256_MD5_ENCRY_PASSWD_LEN + 1));
            securec_check(errorno, "\0", "\0");

            if (isMD5(shadow_pass)) {
                /* stored password already md5 encrypted, only do sha256 salt */
                if (!pg_sha256_encrypt_for_md5(
                        shadow_pass + strlen("md5"), port->md5Salt, sizeof(port->md5Salt), crypt_pwd)) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
            } else if (isSHA256(shadow_pass)) {
                char stored_key[STORED_KEY_BYTES_LENGTH + 1] = {0};
                char stored_key_string[STORED_KEY_STRING_LENGTH + 1] = {0};
                char hmac_result[HMAC_LENGTH + 1] = {0};
                char xor_result[HMAC_LENGTH + 1] = {0};
                char token[TOKEN_LENGTH + 1] = {0};
                char hash_result[HMAC_LENGTH + 1] = {0};
                char hash_result_string[HMAC_LENGTH * 2 + 1] = {0};
                char client_pass_bytes[HMAC_LENGTH + 1] = {0};
                int hmac_length = HMAC_LENGTH;

                if (H_LENGTH != strlen(client_pass)) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_WRONG_PASSWORD;
                }

                errorno = strncpy_s(stored_key_string,
                    sizeof(stored_key_string),
                    &shadow_pass[SHA256_LENGTH + SALT_STRING_LENGTH + HMAC_STRING_LENGTH],
                    sizeof(stored_key_string) - 1);
                securec_check(errorno, "\0", "\0");
                stored_key_string[sizeof(stored_key_string) - 1] = '\0';
                sha_hex_to_bytes32(stored_key, stored_key_string);
                sha_hex_to_bytes4(token, (char*)port->token);
                CRYPT_hmac_ret = CRYPT_hmac(NID_hmacWithSHA256,
                    (GS_UCHAR*)stored_key,
                    STORED_KEY_LENGTH,
                    (GS_UCHAR*)token,
                    TOKEN_LENGTH,
                    (GS_UCHAR*)hmac_result,
                    (GS_UINT32*)&hmac_length);

                if (CRYPT_hmac_ret) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }

                sha_hex_to_bytes32(client_pass_bytes, client_pass);
                if (XOR_between_password(hmac_result, client_pass_bytes, xor_result, HMAC_LENGTH)) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }

                CRYPT_digest_ret = EVP_Digest((GS_UCHAR*)xor_result,
                    HMAC_LENGTH,
                    (GS_UCHAR*)hash_result,
                    (GS_UINT32*)&hmac_length,
                    EVP_sha256(),
                    NULL);

                if (!CRYPT_digest_ret) {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }

                sha_bytes_to_hex64((uint8*)hash_result, hash_result_string);
                if (strncmp(hash_result_string, stored_key_string, HMAC_LENGTH * 2) == 0) {
                    RFC5802_pass = true;
                } else {
                    pfree_ext(crypt_pwd);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_WRONG_PASSWORD;
                }
            } else {
                pfree_ext(crypt_pwd);
                pfree_ext(pass_info.shadow_pass);
                return STATUS_ERROR;
            }
            break;
        }
        case uaSM3: {
            char stored_key_sm3[STORED_KEY_BYTES_LENGTH + 1] = {0};
            char stored_key_string_sm3[STORED_KEY_STRING_LENGTH + 1] = {0};
            char hmac_result_sm3[HMAC_LENGTH + 1] = {0};
            char xor_result_sm3[HMAC_LENGTH + 1] = {0};
            char token_sm3[TOKEN_LENGTH + 1] = {0};
            char hash_result_sm3[HMAC_LENGTH + 1] = {0};
            char hash_result_string_sm3[HMAC_LENGTH * 2 + 1] = {0};
            char client_pass_bytes_sm3[HMAC_LENGTH + 1] = {0};
            int hmac_length_sm3 = HMAC_LENGTH;

            if (H_LENGTH != strlen(client_pass)) {
                pfree_ext(pass_info.shadow_pass);
                return STATUS_WRONG_PASSWORD;
            }

            errorno = strncpy_s(stored_key_string_sm3,
                sizeof(stored_key_string_sm3),
                &shadow_pass[SM3_LENGTH + SALT_STRING_LENGTH + HMAC_STRING_LENGTH],
                sizeof(stored_key_string_sm3) - 1);
            securec_check(errorno, "\0", "\0");
            stored_key_string_sm3[sizeof(stored_key_string_sm3) - 1] = '\0';
            sha_hex_to_bytes32(stored_key_sm3, stored_key_string_sm3);
            sha_hex_to_bytes4(token_sm3, (char*)port->token);
            CRYPT_hmac_ret = CRYPT_hmac(NID_hmacWithSHA256,
                (GS_UCHAR*)stored_key_sm3,
                STORED_KEY_LENGTH,
                (GS_UCHAR*)token_sm3,
                TOKEN_LENGTH,
                (GS_UCHAR*)hmac_result_sm3,
                (GS_UINT32*)&hmac_length_sm3);

            if (CRYPT_hmac_ret) {
                pfree_ext(pass_info.shadow_pass);
                return STATUS_ERROR;
            }

            sha_hex_to_bytes32(client_pass_bytes_sm3, client_pass);
            if (XOR_between_password(hmac_result_sm3, client_pass_bytes_sm3, xor_result_sm3, HMAC_LENGTH)) {
                pfree_ext(pass_info.shadow_pass);
                return STATUS_ERROR;
            }

            CRYPT_digest_ret = EVP_Digest((GS_UCHAR*)xor_result_sm3,
                HMAC_LENGTH,
                (GS_UCHAR*)hash_result_sm3,
                (GS_UINT32*)&hmac_length_sm3,
                EVP_sm3(),
                NULL);

            if (!CRYPT_digest_ret) {
                pfree_ext(pass_info.shadow_pass);
                return STATUS_ERROR;
            }

            sha_bytes_to_hex64((uint8*)hash_result_sm3, hash_result_string_sm3);
            if (strncmp(hash_result_string_sm3, stored_key_string_sm3, HMAC_LENGTH * 2) == 0) {
                RFC5802_pass = true;
            } else {
                pfree_ext(pass_info.shadow_pass);
                return STATUS_WRONG_PASSWORD;
            }
            break;
        }

        default:
            if (isMD5(shadow_pass)) {
                /*Encrypt user-supplied password to match stored MD5 */
                crypt_client_pass = (char*)palloc(MD5_PASSWD_LEN + 1);
                if (crypt_client_pass != NULL) {
                    errorno = memset_s(crypt_client_pass, (MD5_PASSWD_LEN + 1), 0, (MD5_PASSWD_LEN + 1));
                    securec_check(errorno, "\0", "\0");
                }
                if (!pg_md5_encrypt(client_pass, port->user_name, strlen(port->user_name), crypt_client_pass)) {
                    pfree_ext(crypt_client_pass);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
            }
            /* Database Security:  Support SHA256.*/
            if (isSHA256(shadow_pass)) {
                /*Encrypt user-supplied password to match stored SHA256 */
                crypt_client_pass = (char*)palloc(SHA256_PASSWD_LEN + 1);
                if (crypt_client_pass != NULL) {
                    errorno = memset_s(crypt_client_pass, (SHA256_PASSWD_LEN + 1), 0, (SHA256_PASSWD_LEN + 1));
                    securec_check(errorno, "\0", "\0");
                }
                if (!pg_sha256_encrypt(
                        client_pass, port->user_name, strlen(port->user_name), crypt_client_pass, NULL)) {
                    pfree_ext(crypt_client_pass);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
            }
    
            if (isSM3(shadow_pass)) {
                /*Encrypt user-supplied password to match stored SHA256 */
                crypt_client_pass = (char*)palloc(SM3_PASSWD_LEN + 1);
                if (crypt_client_pass != NULL) {
                    errorno = memset_s(crypt_client_pass, (SM3_PASSWD_LEN + 1), 0, (SM3_PASSWD_LEN + 1));
                    securec_check(errorno, "\0", "\0");
                }
                if (!GsSm3Encrypt(
                    client_pass, port->user_name, strlen(port->user_name), crypt_client_pass, NULL)) {
                    pfree_ext(crypt_client_pass);
                    pfree_ext(pass_info.shadow_pass);
                    return STATUS_ERROR;
                }
            }

            crypt_pwd = shadow_pass;

            break;
    }

    if (RFC5802_pass || strcmp(crypt_client_pass, crypt_pwd) == 0) {
        retval = STATUS_OK;
    } else {
        retval = STATUS_WRONG_PASSWORD;
    }

    if (port->hba->auth_method == uaMD5 || port->hba->auth_method == uaSHA256) {
        pfree_ext(crypt_pwd);
    }

    if (crypt_client_pass != client_pass) {
        pfree_ext(crypt_client_pass);
    }

    pfree_ext(pass_info.shadow_pass);
    return retval;
}
/*
 * Target		:get iteration from the stored password.
 * Description	:under the latest password storage format, iteration is stored in pg_authid.
 * 				we get the iteration if there is ,if not we return the default iteration value.
 * Input		:role name.
 * Return		:auth iteration integer.
 */
int get_stored_iteration(const char* role)
{
    password_info pass_info = {NULL, 0, 0, false, false};
    int iteration_count = 0;

    if (!get_stored_password(role, &pass_info)) {
        pfree_ext(pass_info.shadow_pass);
        return -1;
    }
    iteration_count = get_iteration_by_password(pass_info.shadow_pass);

    pfree_ext(pass_info.shadow_pass);
    return iteration_count;
}

/* get iteration from the encrypted password */
int get_iteration_by_password(char* encrypted_password)
{
    int iteration_count = 0;
    if (isCOMBINED(encrypted_password)) {
        iteration_count = decode_iteration(&encrypted_password[SHA256_PASSWD_LEN + MD5_PASSWD_LEN]);
    } else if (isSHA256(encrypted_password)) {
        iteration_count = decode_iteration(&encrypted_password[SHA256_PASSWD_LEN]);
    } else if (isSM3(encrypted_password)) {
        iteration_count = decode_iteration(&encrypted_password[SM3_PASSWD_LEN]);
    } else {
        iteration_count = ITERATION_COUNT;
    }
    return iteration_count;
}

#ifdef USE_IAM
/*
 * @Description: read auth file to buffer for iam token authenication.
 * @in file_path : the file path with filename.
 * @return : string buffer contained the contents of the file.
 * @NOTICE : the return buffer need be free'd by caller.
 */
char* read_auth_file(const char* file_path)
{
    char* file_buf = NULL;
    int file_len = 0;
    int fd = -1;
    struct stat statbuf;

    fd = open(file_path, O_RDONLY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(LOG, (errmsg("could not open file : %m")));
        return NULL;
    }
    if (fstat(fd, &statbuf) < 0) {
        (void)close(fd);
        ereport(LOG, (errmsg("could not stat file : %m")));
        return NULL;
    }

    /* the operate file must be less than 600 for security. */
    if (!S_ISREG(statbuf.st_mode) || (statbuf.st_mode & (S_IRWXG | S_IRWXO))) {
        (void)close(fd);
        ereport(LOG, (errmsg("file permissions can't be more than 600.")));
        return NULL;
    }

    /* malloc a buffer to store the file content.*/
    file_buf = (char*)palloc0(statbuf.st_size + 1);
    file_len = (int)read(fd, file_buf, statbuf.st_size);
    if (file_len != statbuf.st_size) {
        (void)close(fd);
        pfree_ext(file_buf);
        ereport(LOG, (errmsg("could not read file : %m")));
        return NULL;
    }

    if (close(fd) != 0)
        ereport(LOG, (errmsg("close file failed : %m")));

    return file_buf;
}

/*
 * @Description: convert the token to cms format for verify.
 * @in token_string : the token need be converted.
 * @return : the filename stored the cms token.
 * @NOTICE : the return buffer need be free'd by caller.
 */
char* convert_cms_token(const char* token_string)
{
#define ROW_LENGTH 64

    char* temp_string = NULL;
    char* source_string = NULL;
    char* cms_file = NULL;
    FILE* fp = NULL;
    int token_length = 0;
    int multiple_num = 0;
    int remainder_num = 0;
    int i = 0;
    errno_t rc = 0;

    token_length = strlen(token_string);
    multiple_num = token_length / ROW_LENGTH;
    remainder_num = token_length % ROW_LENGTH;

    temp_string = pstrdup(token_string);
    cms_file = (char*)palloc0(MAXPGPATH);

    /* convert-1 : replace '-' to '/' as cms need. */
    source_string = temp_string;
    while (*temp_string != '\0') {
        if (*temp_string == '-') {
            *temp_string = '/';
        }
        temp_string++;
    }

    /* construct the cms file name through random and nowtime to avoid same name. */
    rc = snprintf_s(cms_file,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%sgaussdb_cms_token_%d_%ld",
        DWS_IAMAUTH_CONFIG_PATH,
        gs_random(),
        GetCurrentTimestamp());
    securec_check_ss(rc, "\0", "\0");

    /* create the cms file for write. */
    if ((fp = fopen(cms_file, "w")) == NULL) {
        ereport(LOG, (errmsg("create cms file failed.")));
        rc = memset_s(source_string, strlen(source_string), 0, strlen(source_string));
        securec_check(rc, "\0", "\0");
        pfree_ext(source_string);
        pfree_ext(cms_file);
        return NULL;
    }

#ifdef WIN32
    rc = _chmod(cms_file, 0600);
#else
    rc = fchmod(fp->_fileno, 0600);
#endif
    if (rc != 0) {
        ereport(LOG, (errmsg("could not set permissions of file.")));
        rc = memset_s(source_string, strlen(source_string), 0, strlen(source_string));
        securec_check(rc, "\0", "\0");
        pfree_ext(source_string);
        pfree_ext(cms_file);
        (void)fclose(fp);
        return NULL;
    }

    /* convert-2 : add '-----BEGIN CMS-----' at the head. */
    if (fputs("-----BEGIN CMS-----\n", fp) < 0) {
        ereport(LOG, (errmsg("add cms head failed.")));
        rc = memset_s(source_string, strlen(source_string), 0, strlen(source_string));
        securec_check(rc, "\0", "\0");
        pfree_ext(source_string);
        pfree_ext(cms_file);
        (void)fclose(fp);
        return NULL;
    }

    /* convert-3 : add '\n' at each 64 chars. */
    for (i = 0; i <= multiple_num; i++) {
        int data_size;
        if (i < multiple_num) {
            data_size = ROW_LENGTH;
        } else {
            data_size = remainder_num;
        }
        if (i == multiple_num && remainder_num == 0) {
            break;
        }
        if (fwrite(source_string + ROW_LENGTH * i, data_size, 1, fp) != 1) {
            ereport(LOG, (errmsg("add cms body failed.")));
            rc = memset_s(source_string, strlen(source_string), 0, strlen(source_string));
            securec_check(rc, "\0", "\0");
            pfree_ext(source_string);
            pfree_ext(cms_file);
            (void)fclose(fp);
            return NULL;
        }
        (void)fputs("\n", fp);
    }

    /* convert-4 : add '-----END CMS-----' at the end. */
    if (fputs("-----END CMS-----\n", fp) < 0) {
        ereport(LOG, (errmsg("add cms end failed.")));
        rc = memset_s(source_string, strlen(source_string), 0, strlen(source_string));
        securec_check(rc, "\0", "\0");
        pfree_ext(source_string);
        pfree_ext(cms_file);
        (void)fclose(fp);
        return NULL;
    }

    rc = memset_s(source_string, strlen(source_string), 0, strlen(source_string));
    securec_check(rc, "\0", "\0");
    pfree_ext(source_string);
    (void)fclose(fp);
    return cms_file;
}

/*
 * @Description: verify the token used signature cert and ca.
 * @in token_string : the token need be verified.
 * @return : string of the verified token.
 * @NOTICE : the return buffer need be free'd by caller.
 */
char* verify_cms_token(char* token_string)
{
#define TOKEN_FILE_MAX_LEN 256
    char command[MAXPGPATH] = {0};
    char* cms_token_file = NULL;
    char* verified_token_file = NULL;
    const char* cert_file = "signing_cert.pem";
    const char* ca_file = "ca.pem";
    errno_t rc = EOK;

    /* convert the token to cms format for unsignature. */
    cms_token_file = convert_cms_token(token_string);
    if (cms_token_file == NULL) {
        ereport(LOG, (errmsg("convert to cms failed.")));
        return NULL;
    }

    verified_token_file = (char*)palloc0(TOKEN_FILE_MAX_LEN);
    rc = snprintf_s(verified_token_file,
        TOKEN_FILE_MAX_LEN,
        TOKEN_FILE_MAX_LEN - 1,
        "%s_%ld",
        cms_token_file,
        GetCurrentTimestamp());
    securec_check_ss(rc, "\0", "\0");

    rc = snprintf_s(command,
        MAXPGPATH,
        MAXPGPATH - 1,
        "openssl cms -verify -certfile %s%s -CAfile %s%s -inform PEM -nosmimecap -nodetach -nocerts -noattr < %s 1> %s "
        "2> /dev/null",
        DWS_IAMAUTH_CONFIG_PATH,
        cert_file,
        DWS_IAMAUTH_CONFIG_PATH,
        ca_file,
        cms_token_file,
        verified_token_file);
    securec_check_ss(rc, "\0", "\0");

    /* verify the token string through openssl tools. */
    rc = system(command);
    if (rc != 0) {
        ereport(LOG, (errmsg("openssl cms command failed : [%d]", rc)));
        (void)unlink(cms_token_file);
        (void)unlink(verified_token_file);
        pfree_ext(cms_token_file);
        pfree_ext(verified_token_file);

        return NULL;
    }

    /* rm cms token file and free the memory malloced in convert_cms_token. */
    (void)unlink(cms_token_file);
    pfree_ext(cms_token_file);

    return verified_token_file;
}

/*
 * @Description: parse the roles array in json body and check whether include dws privilege role.
 * @in array_object : the role array object in json body need be parsed .
 * @return : false for not including the dws_db_acc role privileges.
 */
bool parse_token_roles(cJSON* array_object)
{
    int obj_size = 0;
    int i = 0;
    cJSON* arr_object = NULL;
    cJSON* object = NULL;
    char* rol_name = NULL;

    obj_size = cJSON_GetArraySize(array_object);

    for (i = 0; i < obj_size; i++) {
        arr_object = cJSON_GetArrayItem(array_object, i);

        /* get the roles name in the array object. */
        object = cJSON_GetObjectItem(arr_object, "name");
        if (object == NULL) {
            ereport(LOG, (errmsg("get name in token failed : [%s].", (char*)cJSON_GetErrorPtr)));
            return false;
        }
        rol_name = object->valuestring;
        if (rol_name == NULL) {
            ereport(LOG, (errmsg("get role in token failed : [%s].", (char*)cJSON_GetErrorPtr)));
            return false;
        }

        if (0 == strcmp(rol_name, IAM_ROLE_WITH_DWSDB_PRIV))
            return true;
    }
    return false;
}

/*
 * @Description: parse the token in json format.
 * @in token_string : the token string need be parsed.
 * @out token : the token messages after parsed.
 * @return : false for parse failed.
 * @NOTICE : the return token and messages in token need be free'd by caller.
 */
bool parse_token(const char* token_string, iam_token* token)
{
    char* verified_token_file = NULL;
    char* clusterid = NULL;
    char* real_token = NULL;
    char* json_string = NULL;
    cJSON* root_obj = NULL;
    cJSON* first_level_obj = NULL;
    cJSON* second_level_obj = NULL;
    cJSON* third_level_obj = NULL;
    cJSON* fourth_level_obj = NULL;
    int token_str_len = 0;
    int token_len = 0;
    errno_t rc = EOK;

    /* token string length must longer than CLUSTERID_LEN. */
    token_str_len = strlen(token_string);
    if (token_str_len <= CLUSTERID_LEN) {
        return false;
    }

    token_len = token_str_len - CLUSTERID_LEN;
    clusterid = (char*)palloc0(CLUSTERID_LEN + 1);
    real_token = (char*)palloc0(token_len + 1);

    /* the token_string include the real token and clusterid, get their values separately here. */
    rc = memcpy_s(real_token, token_len + 1, token_string, token_len);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(clusterid, CLUSTERID_LEN + 1, token_string + token_str_len - CLUSTERID_LEN, CLUSTERID_LEN);
    securec_check(rc, "\0", "\0");

    token->cluster_id = clusterid;

    /* verify the token for check token's info. */
    verified_token_file = verify_cms_token(real_token);
    pfree_ext(real_token);
    if (verified_token_file == NULL) {
        return false;
    }

    /* read the verified token messages. */
    json_string = read_auth_file(verified_token_file);
    if (json_string == NULL) {
        ereport(LOG, (errmsg("read verified token file failed.")));
        (void)unlink(verified_token_file);
        pfree_ext(verified_token_file);
        return false;
    }

    /* rm verified_token_file and free the memory malloced in verify_cms_token. */
    (void)unlink(verified_token_file);
    pfree_ext(verified_token_file);

    /* start parse the json_string through cJSON lib function. */
    root_obj = cJSON_Parse(json_string);
    if (root_obj == NULL) {
        ereport(LOG, (errmsg("get root object failed : [%s].", (char*)cJSON_GetErrorPtr)));
        pfree_ext(json_string);
        return false;
    }

    /* free the memory malloced in read_auth_file. */
    pfree_ext(json_string);

    first_level_obj = cJSON_GetObjectItem(root_obj, "token");
    if (first_level_obj == NULL) {
        ereport(LOG, (errmsg("get first level object failed : [%s].", (char*)cJSON_GetErrorPtr)));
        cJSON_Delete(root_obj);
        return false;
    }

    /* 1. get the token expired time info. */
    second_level_obj = cJSON_GetObjectItem(first_level_obj, "expires_at");
    if (second_level_obj == NULL) {
        ereport(LOG, (errmsg("get token expires_at failed : [%s].", (char*)cJSON_GetErrorPtr)));
        return false;
    } else {
        token->expires_at = second_level_obj->valuestring;
        ereport(DEBUG5,
            (errmsg("token expires object messages: type=%d, key=%s, value=%s.",
                second_level_obj->type,
                second_level_obj->string,
                second_level_obj->valuestring)));
    }

    /* 2. get the token user info. */
    second_level_obj = cJSON_GetObjectItem(first_level_obj, "user");
    if (second_level_obj == NULL) {
        ereport(LOG, (errmsg("get token user failed : [%s].", (char*)cJSON_GetErrorPtr)));
        return false;
    } else {
        /* get the user name of token. */
        third_level_obj = cJSON_GetObjectItem(second_level_obj, "name");
        if (third_level_obj == NULL) {
            ereport(LOG, (errmsg("get token user name failed : [%s].", (char*)cJSON_GetErrorPtr)));
            return false;
        }
        token->username = third_level_obj->valuestring;
        ereport(DEBUG5,
            (errmsg("token user object messages: type=%d, key=%s, value=%s.",
                third_level_obj->type,
                third_level_obj->string,
                third_level_obj->valuestring)));

        /* get the tenant id of the token. */
        third_level_obj = cJSON_GetObjectItem(second_level_obj, "domain");
        if (third_level_obj == NULL) {
            ereport(LOG, (errmsg("get token user domain failed : [%s].", (char*)cJSON_GetErrorPtr)));
            return false;
        }
        fourth_level_obj = cJSON_GetObjectItem(third_level_obj, "id");
        if (fourth_level_obj == NULL) {
            ereport(LOG, (errmsg("get token user tenantid failed : [%s].", (char*)cJSON_GetErrorPtr)));
            return false;
        }
        token->tenant_id = fourth_level_obj->valuestring;
    }

    /* 3. get the token roles of privileges info. */
    second_level_obj = cJSON_GetObjectItem(first_level_obj, "roles");
    if (second_level_obj == NULL) {
        ereport(LOG, (errmsg("get token roles failed : [%s].", (char*)cJSON_GetErrorPtr)));
        return false;
    } else {
        token->role_priv = parse_token_roles(second_level_obj);
        ereport(DEBUG5, (errmsg("token role messages: %d.", token->role_priv)));
    }

    cJSON_Delete(root_obj);
    return true;
}

/*
 * @Description: check whether the token is expired.
 * @in token_expired_time : the expired time of the token.
 * @return : false for token expired.
 */
bool check_token_expired(char* token_expired_time)
{
    struct tm token_time;
    struct tm now_time;
    time_t now = time(NULL);

    /* change to 'UTC' time and use gmtime_r here for thread safe. */
    if (NULL == gmtime_r(&now, &now_time)) {
        ereport(LOG, (errmsg("Call gmtime_r failed.")));
        return false;
    }

    /* change the time string to tm struct. */
    (void)strptime(token_expired_time, "%Y-%m-%dT%H:%M:%SZ", &token_time);
    /* compare the time through mktime to check whether expired. */
    if (mktime(&token_time) <= mktime(&now_time)) {
        ereport(LOG, (errmsg("Invalid token for expired : %s.", token_expired_time)));
        return false;
    } else {
        return true;
    }
}

/*
 * @Description: check the clusterid and tenantId to determine unique cluster information.
 * @in token : the token need to be check.
 * @return : false for wrong properties.
 */
bool check_cluster_properties(iam_token token)
{
    char* cluster_properties = NULL;
    char* clusterid_position = NULL;
    char* tenantid_position = NULL;
    char clusterid_value[CLUSTERID_LEN + 1] = {0};
    char tenantid_value[TENANTID_LEN + 1] = {0};
    const char* cluserid_file = "cluster.properties";
    char cluserid_file_path[MAXPGPATH] = {0};
    errno_t rc = EOK;

    /* read the cluserid in file cluster.properties. */
    rc = snprintf_s(cluserid_file_path, MAXPGPATH, MAXPGPATH - 1, "%s%s", DWS_IAMAUTH_CONFIG_PATH, cluserid_file);
    securec_check_ss(rc, "\0", "\0");
    cluster_properties = read_auth_file(cluserid_file_path);
    if (cluster_properties == NULL) {
        ereport(LOG, (errmsg("read file cluster.properties failed.")));
        return false;
    }

    /* get the clusterid and tenantid from cluster.properties. */
    clusterid_position = strstr(cluster_properties, "cluterId=");
    tenantid_position = strstr(cluster_properties, "tenantId=");
    if (clusterid_position == NULL || tenantid_position == NULL) {
        ereport(LOG, (errmsg("wrong content for cluster.properties.")));
        pfree_ext(cluster_properties);
        return false;
    }
    rc = memcpy_s(clusterid_value, CLUSTERID_LEN + 1, clusterid_position + strlen("cluterId="), CLUSTERID_LEN);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(tenantid_value, TENANTID_LEN + 1, tenantid_position + strlen("tenantId="), TENANTID_LEN);
    securec_check(rc, "\0", "\0");

    /* free the memory malloced in read_auth_file after used. */
    pfree_ext(cluster_properties);

    if (token.cluster_id == NULL || token.tenant_id == NULL) {
        ereport(LOG, (errmsg("wrong clusterid or tenantid in token.")));
        return false;
    }
    /* check the clusterid and tenantid between cluster.properties and token. */
    if (0 != strcmp(clusterid_value, token.cluster_id) || 0 != strcmp(tenantid_value, token.tenant_id)) {
        ereport(LOG, (errmsg("check clusterid and tenantid failed.")));
        return false;
    } else {
        return true;
    }
}

/*
 * @Description: check the token from iam.
 * @in token : the token messages.
 * @in rolname : the db role name used for login.
 * @return : false for wrong token.
 */
bool check_token(iam_token token, char* rolname)
{
    if (!check_cluster_properties(token))
        return false;

    /* check the token validity period. */
    if (!check_token_expired(token.expires_at))
        return false;

    /* check the token user message. */
    if (0 != strcmp(token.username, rolname)) {
        ereport(LOG, (errmsg("Invalid iam username %s for %s.", token.username, rolname)));
        return false;
    }

    /* check the token role privilege. */
    if (!token.role_priv) {
        ereport(LOG, (errmsg("Invalid iam role for login.")));
        return false;
    }

    return true;
}
#endif
