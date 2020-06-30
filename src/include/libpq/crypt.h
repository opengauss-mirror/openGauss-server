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

extern int32 get_password_stored_method(const char* role, char* encrypted_string, int len);
extern int CheckUserValid(Port* port, const char* role);
extern int crypt_verify(const Port* port, const char* user, char* client_pass);
extern int get_stored_iteration(const char* role);
extern char* verify_cms_token(char* token_string);
extern bool parse_token(const char* token_string, iam_token* token);
extern bool check_token(iam_token token, char* rolname);
#endif
