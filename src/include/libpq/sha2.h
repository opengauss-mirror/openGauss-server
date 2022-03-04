/*	contrib/pgcrypto/sha2.h */
/*	$OpenBSD: sha2.h,v 1.2 2004/04/28 23:11:57 millert Exp $	*/

/*
 * FILE:	sha2.h
 * AUTHOR:	Aaron D. Gifford <me@aarongifford.com>
 *
 * Copyright (c) 2000-2001, Aaron D. Gifford
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in the
 *	  documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of contributors
 *	  may be used to endorse or promote products derived from this software
 *	  without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTOR(S) ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.	IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTOR(S) BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $From: sha2.h,v 1.1 2001/11/08 00:02:01 adg Exp adg $
 */

#ifndef _SHA2_H
#define _SHA2_H

/* avoid conflict with OpenSSL */
#define SHA256_Init2 gs_SHA256_Init
#define SHA256_Update2 gs_SHA256_Update
#define SHA256_Final2 gs_SHA256_Final

/*** SHA-256 Various Length Definitions ***********************/
#define SHA256_BLOCK_LENGTH 64
#define SHA256_DIGEST_LENGTH 32
#define SHA256_DIGEST_STRING_LENGTH (SHA256_DIGEST_LENGTH * 2 + 1)

/*** SHA-256 Context Structures *******************************/
#define K_LENGTH 32
#define ITERATION_COUNT 10000
#ifdef ENABLE_LITE_MODE
#define ITERATION_COUNT_V1 2048
#endif
#define CLIENT_STRING_LENGTH 11
#define SEVER_STRING_LENGTH 10
#define SEVER_STRING_LENGTH_SM3 11
#define HMAC_LENGTH 32
#define ENCRY_LENGTH_DOUBLE 2

#define HMAC_BYTES_LENGTH 32
#define HMAC_STRING_LENGTH (HMAC_LENGTH * 2)
#define STORED_KEY_LENGTH 32
#define STORED_KEY_BYTES_LENGTH STORED_KEY_LENGTH
#define STORED_KEY_STRING_LENGTH (STORED_KEY_LENGTH * 2)
#define CLIENT_KEY_BYTES_LENGTH 32
#define CLIENT_KEY_STRING_LENGTH (CLIENT_KEY_BYTES_LENGTH * 2)
#define SALT_LENGTH 32
#define SALT_BYTE_LENGTH 32
#define SALT_STRING_LENGTH (SALT_LENGTH * 2)
#define H_LENGTH 64
#define ENCRYPTED_STRING_LENGTH (HMAC_STRING_LENGTH + STORED_KEY_STRING_LENGTH + SALT_STRING_LENGTH)
#define TOKEN_LENGTH 4
#define SHA256_LENGTH 6
#define SHA256_PASSWD_LEN (ENCRYPTED_STRING_LENGTH + SHA256_LENGTH)
#define SHA256_MD5_ENCRY_PASSWD_LEN 70
#define ITERATION_STRING_LEN 11 /* The length of INT_MAX(2147483647) */
#define SHA256_MD5_COMBINED_LEN (SHA256_PASSWD_LEN + MD5_PASSWD_LEN + ITERATION_STRING_LEN)

#define SM3_LENGTH 3
#define SM3_PASSWD_LEN (ENCRYPTED_STRING_LENGTH + SM3_LENGTH)

#define isSM3(passwd) \
    (strncmp(passwd, "sm3", SM3_LENGTH) == 0 && strlen(passwd) == SM3_PASSWD_LEN + ITERATION_STRING_LEN)

#define isSHA256(passwd) \
    (strncmp(passwd, "sha256", SHA256_LENGTH) == 0 && strlen(passwd) == SHA256_PASSWD_LEN + ITERATION_STRING_LEN)

/* Check combined password for compatible with PG. */
#define isCOMBINED(passwd) \
    (strncmp(passwd, "sha256", SHA256_LENGTH) == 0 && strncmp(passwd + SHA256_PASSWD_LEN, "md5", 3) == 0)

/* Check whether it is encrypted password. */
#define isPWDENCRYPTED(passwd) (isMD5(passwd) || isSHA256(passwd) || isSM3(passwd) || isCOMBINED(passwd))

/* The current password stored method are sha256, md5 and combined. */
#define PLAIN_PASSWORD 0
#define MD5_PASSWORD 1
#define SHA256_PASSWORD 2
#define SM3_PASSWORD 3
#define ERROR_PASSWORD 4
#define BAD_MEM_ADDR 5
#ifdef ENABLE_LITE_MODE
#define SHA256_PASSWORD_RFC 6
#endif
#define COMBINED_PASSWORD 7

typedef struct _SHA256_CTX2 {
    uint32 state[8];
    uint64 bitcount;
    uint8 buffer[SHA256_BLOCK_LENGTH];
} SHA256_CTX2;

void SHA256_Init2(SHA256_CTX2*);
void SHA256_Update2(SHA256_CTX2*, const uint8*, size_t);
void SHA256_Final2(uint8[SHA256_DIGEST_LENGTH], SHA256_CTX2*);

/* Use the old iteration ITERATION_COUNT as the default iteraion count. */
extern bool pg_sha256_encrypt(const char* passwd, const char* salt_s, size_t salt_len, char* buf, char* client_key_buf,
    int iteration_count = ITERATION_COUNT);
#ifdef ENABLE_LITE_MODE
extern bool pg_sha256_encrypt_v1(const char* passwd, const char* salt_s, size_t salt_len, char* buf, char* client_key_buf);
#endif
extern int XOR_between_password(const char* password1, const char* password2, char* r, int length);
extern void sha_hex_to_bytes32(char* s, const char b[64]);
extern void sha_hex_to_bytes4(char* s, const char b[8]);
extern void sha_bytes_to_hex8(uint8 b[4], char* s);
extern void sha_bytes_to_hex64(uint8 b[32], char* s);
extern bool pg_sha256_encrypt_for_md5(const char* passwd, const char* salt, size_t salt_len, char* buf);
extern bool GsSm3Encrypt(const char* passwd, const char* salt_s, size_t salt_len, char* buf, char* client_key_buf,
    int iteration_count = ITERATION_COUNT);

#endif /* _SHA2_H */

