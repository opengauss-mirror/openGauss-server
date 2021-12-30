/* -------------------------------------------------------------------------
 *
 * crypt.h
 *	  Interface to libpq/crypt.c
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/crypt.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_CRYPT_H
#define PG_CRYPT_H

#include "libpq/libpq-be.h"

#include "libpq/sha2.h"

/* the struct for iam token */
typedef struct iam_token {
    char* expires_at;
    char* username;
    bool role_priv;
    char* cluster_id;
    char* tenant_id;
} iam_token;

typedef struct password_info {
    char* shadow_pass;
    TimestampTz vbegin;
    TimestampTz vuntil;
    bool isnull_begin;
    bool isnull_until;
} password_info;

extern int32 get_password_stored_method(const char* role, char* encrypted_string, int len);
extern bool get_stored_password(const char *role, password_info *pass_info);
extern int CheckUserValid(Port* port, const char* role);
extern bool VerifyPasswdDigest(const char* roleID, char* passwd, char* passDigest);
extern int crypt_verify(const Port* port, const char* user, char* client_pass);
extern int get_stored_iteration(const char* role);
#ifdef USE_IAM
extern char* verify_cms_token(char* token_string);
extern bool parse_token(const char* token_string, iam_token* token);
extern bool check_token(iam_token token, char* rolname);
#endif
#endif
