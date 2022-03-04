/*	$OpenBSD: sha2.cpp,v 1.6 2004/05/03 02:57:36 millert Exp $	*/

/*
 * FILE:	sha2.cpp
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
 * $From: sha2.cpp,v 1.1 2001/11/08 00:01:51 adg Exp adg $
 *
 */
#include "postgres_fe.h"
#ifndef FRONTEND_PARSER
#endif /* FRONTEND_PARSER */
#include <sys/param.h>
#include "libpq/sha2.h"
#include "openssl/evp.h"
#include "cipher.h"

/* UNROLLED TRANSFORM LOOP NOTE:
 * You can define SHA2_UNROLL_TRANSFORM to use the unrolled transform
 * loop version for the hash transform rounds (defined using macros
 * later in this file).  Either define on the command line, for example:
 *
 *	 cc -DSHA2_UNROLL_TRANSFORM -o sha2 sha2.c sha2prog.c
 *
 * or define below:
 *
 *	 #define SHA2_UNROLL_TRANSFORM
 *
 */
/*** SHA-256/384/512 Various Length Definitions ***********************/
/* NOTE: Most of these are in sha2.h */
#define SHA256_SHORT_BLOCK_LENGTH (SHA256_BLOCK_LENGTH - 8)

/*** ENDIAN REVERSAL MACROS *******************************************/
#ifndef WORDS_BIGENDIAN
#define REVERSE32(w, x)                                                  \
    {                                                                    \
        uint32 tmp = (w);                                                \
        tmp = (tmp >> 16) | (tmp << 16);                                 \
        (x) = ((tmp & 0xff00ff00UL) >> 8) | ((tmp & 0x00ff00ffUL) << 8); \
    }
#define REVERSE64(w, x)                                                                      \
    {                                                                                        \
        uint64 tmp = (w);                                                                    \
        tmp = (tmp >> 32) | (tmp << 32);                                                     \
        tmp = ((tmp & 0xff00ff00ff00ff00ULL) >> 8) | ((tmp & 0x00ff00ff00ff00ffULL) << 8);   \
        (x) = ((tmp & 0xffff0000ffff0000ULL) >> 16) | ((tmp & 0x0000ffff0000ffffULL) << 16); \
    }
#endif /* not bigendian */

/*
 * Macro for incrementally adding the unsigned 64-bit integer n to the
 * unsigned 128-bit integer (represented using a two-element array of
 * 64-bit words):
 */
#define ADDINC128(w, n)        \
    do {                       \
        (w)[0] += (uint64)(n); \
        if ((w)[0] < (n)) {    \
            (w)[1]++;          \
        }                      \
    } while (0)

/* Notice: sha2.cpp can not compile and link to kernel(backend).
 * So we can't use securec_check, securec_check_ss and elog.
 */
#define SECUREC_CHECK(rc) securec_check_c(rc, "", "")
#define SECUREC_CHECK_SS(rc) securec_check_ss_c(rc, "", "")

/*** THE SIX LOGICAL FUNCTIONS ****************************************/
/*
 * Bit shifting and rotation (used by the six SHA-XYZ logical functions:
 *
 *	 NOTE:	The naming of R and S appears backwards here (R is a SHIFT and
 *	 S is a ROTATION) because the SHA-256/384/512 description document
 *	 (see http://www.iwar.org.uk/comsec/resources/cipher/sha256-384-512.pdf)
 *	 uses this same "backwards" definition.
 */
/* Shift-right (used in SHA-256, SHA-384, and SHA-512): */
#define R(b, x) ((x) >> (b))
/* 32-bit Rotate-right (used in SHA-256): */
#define S32(b, x) (((x) >> (b)) | ((x) << (32 - (b))))

/* Two of six logical functions used in SHA-256, SHA-384, and SHA-512: */
#define Ch(x, y, z) (((x) & (y)) ^ ((~(x)) & (z)))
#define Maj(x, y, z) (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))

/* Four of six logical functions used in SHA-256: */
#define Sigma0_256(x) (S32(2, (x)) ^ S32(13, (x)) ^ S32(22, (x)))
#define Sigma1_256(x) (S32(6, (x)) ^ S32(11, (x)) ^ S32(25, (x)))
#define sigma0_256(x) (S32(7, (x)) ^ S32(18, (x)) ^ R(3, (x)))
#define sigma1_256(x) (S32(17, (x)) ^ S32(19, (x)) ^ R(10, (x)))

/*** INTERNAL FUNCTION PROTOTYPES *************************************/
/* NOTE: These should not be accessed directly from outside this
 * library -- they are intended for private internal visibility/use
 * only.
 */
static void SHA256_Transform(SHA256_CTX2*, const uint8*);

/*** SHA-XYZ INITIAL HASH VALUES AND CONSTANTS ************************/
/* Hash constant words K for SHA-256: */
static const uint32 K256[64] = {0x428a2f98UL,
    0x71374491UL,
    0xb5c0fbcfUL,
    0xe9b5dba5UL,
    0x3956c25bUL,
    0x59f111f1UL,
    0x923f82a4UL,
    0xab1c5ed5UL,
    0xd807aa98UL,
    0x12835b01UL,
    0x243185beUL,
    0x550c7dc3UL,
    0x72be5d74UL,
    0x80deb1feUL,
    0x9bdc06a7UL,
    0xc19bf174UL,
    0xe49b69c1UL,
    0xefbe4786UL,
    0x0fc19dc6UL,
    0x240ca1ccUL,
    0x2de92c6fUL,
    0x4a7484aaUL,
    0x5cb0a9dcUL,
    0x76f988daUL,
    0x983e5152UL,
    0xa831c66dUL,
    0xb00327c8UL,
    0xbf597fc7UL,
    0xc6e00bf3UL,
    0xd5a79147UL,
    0x06ca6351UL,
    0x14292967UL,
    0x27b70a85UL,
    0x2e1b2138UL,
    0x4d2c6dfcUL,
    0x53380d13UL,
    0x650a7354UL,
    0x766a0abbUL,
    0x81c2c92eUL,
    0x92722c85UL,
    0xa2bfe8a1UL,
    0xa81a664bUL,
    0xc24b8b70UL,
    0xc76c51a3UL,
    0xd192e819UL,
    0xd6990624UL,
    0xf40e3585UL,
    0x106aa070UL,
    0x19a4c116UL,
    0x1e376c08UL,
    0x2748774cUL,
    0x34b0bcb5UL,
    0x391c0cb3UL,
    0x4ed8aa4aUL,
    0x5b9cca4fUL,
    0x682e6ff3UL,
    0x748f82eeUL,
    0x78a5636fUL,
    0x84c87814UL,
    0x8cc70208UL,
    0x90befffaUL,
    0xa4506cebUL,
    0xbef9a3f7UL,
    0xc67178f2UL};

/* Initial hash value H for SHA-256: */
static const uint32 sha256_initial_hash_value[8] = {
    0x6a09e667UL, 0xbb67ae85UL, 0x3c6ef372UL, 0xa54ff53aUL, 0x510e527fUL, 0x9b05688cUL, 0x1f83d9abUL, 0x5be0cd19UL};

/*** SHA-256: *********************************************************/
void SHA256_Init2(SHA256_CTX2* context)
{
    errno_t rc = 0;
    if (context == NULL)
        return;
    rc = memcpy_s(context->state, SHA256_DIGEST_LENGTH, sha256_initial_hash_value, sizeof(sha256_initial_hash_value));
    SECUREC_CHECK(rc);

    rc = memset_s(context->buffer, SHA256_BLOCK_LENGTH, 0, SHA256_BLOCK_LENGTH);
    SECUREC_CHECK(rc);

    context->bitcount = 0;
}

#ifdef SHA2_UNROLL_TRANSFORM

/* Unrolled SHA-256 round macros: */
#define ROUND256_0_TO_15(a, b, c, d, e, f, g, h)                                                                \
    do {                                                                                                        \
        W256[j] = (uint32)data[3] | ((uint32)data[2] << 8) | ((uint32)data[1] << 16) | ((uint32)data[0] << 24); \
        data += 4;                                                                                              \
        T1 = (h) + Sigma1_256((e)) + Ch((e), (f), (g)) + K256[j] + W256[j];                                     \
        (d) += T1;                                                                                              \
        (h) = T1 + Sigma0_256((a)) + Maj((a), (b), (c));                                                        \
        j++;                                                                                                    \
    } while (0)

#define ROUND256(a, b, c, d, e, f, g, h)                                                                               \
    do {                                                                                                               \
        s0 = W256[(j + 1) & 0x0f];                                                                                     \
        s0 = sigma0_256(s0);                                                                                           \
        s1 = W256[(j + 14) & 0x0f];                                                                                    \
        s1 = sigma1_256(s1);                                                                                           \
        T1 = (h) + Sigma1_256((e)) + Ch((e), (f), (g)) + K256[j] + (W256[j & 0x0f] += s1 + W256[(j + 9) & 0x0f] + s0); \
        (d) += T1;                                                                                                     \
        (h) = T1 + Sigma0_256((a)) + Maj((a), (b), (c));                                                               \
        j++;                                                                                                           \
    } while (0)

static void SHA256_Transform(SHA256_CTX2* context, const uint8* data)
{
    uint32 a, b, c, d, e, f, g, h, s0, s1;
    uint32 T1, *W256 = NULL;
    int j;

    W256 = (uint32*)context->buffer;

    /* Initialize registers with the prev. intermediate value */
    a = context->state[0];
    b = context->state[1];
    c = context->state[2];
    d = context->state[3];
    e = context->state[4];
    f = context->state[5];
    g = context->state[6];
    h = context->state[7];

    j = 0;
    do {
        /* Rounds 0 to 15 (unrolled): */
        ROUND256_0_TO_15(a, b, c, d, e, f, g, h);
        ROUND256_0_TO_15(h, a, b, c, d, e, f, g);
        ROUND256_0_TO_15(g, h, a, b, c, d, e, f);
        ROUND256_0_TO_15(f, g, h, a, b, c, d, e);
        ROUND256_0_TO_15(e, f, g, h, a, b, c, d);
        ROUND256_0_TO_15(d, e, f, g, h, a, b, c);
        ROUND256_0_TO_15(c, d, e, f, g, h, a, b);
        ROUND256_0_TO_15(b, c, d, e, f, g, h, a);
    } while (j < 16);

    /* Now for the remaining rounds to 64: */
    do {
        ROUND256(a, b, c, d, e, f, g, h);
        ROUND256(h, a, b, c, d, e, f, g);
        ROUND256(g, h, a, b, c, d, e, f);
        ROUND256(f, g, h, a, b, c, d, e);
        ROUND256(e, f, g, h, a, b, c, d);
        ROUND256(d, e, f, g, h, a, b, c);
        ROUND256(c, d, e, f, g, h, a, b);
        ROUND256(b, c, d, e, f, g, h, a);
    } while (j < 64);

    /* Compute the current intermediate hash value */
    context->state[0] += a;
    context->state[1] += b;
    context->state[2] += c;
    context->state[3] += d;
    context->state[4] += e;
    context->state[5] += f;
    context->state[6] += g;
    context->state[7] += h;

    /* Clean up */
    a = b = c = d = e = f = g = h = T1 = 0;
}
#else  /* SHA2_UNROLL_TRANSFORM */

static void SHA256_Transform(SHA256_CTX2* context, const uint8* data)
{
    uint32 a, b, c, d, e, f, g, h, temp_s0, temp_s1;
    uint32 T1, T2;
    uint32 *W256 = NULL;
    unsigned int j;

    W256 = (uint32*)context->buffer;

    /* Initialize registers with the prev. intermediate value */
    a = context->state[0];
    b = context->state[1];
    c = context->state[2];
    d = context->state[3];
    e = context->state[4];
    f = context->state[5];
    g = context->state[6];
    h = context->state[7];

    j = 0;
    do {
        W256[j] = (uint32)data[3] | ((uint32)data[2] << 8) | ((uint32)data[1] << 16) | ((uint32)data[0] << 24);
        data += 4;
        /* Apply the SHA-256 compression function to update a..h */
        T1 = h + Sigma1_256(e) + Ch(e, f, g) + K256[j] + W256[j];
        T2 = Sigma0_256(a) + Maj(a, b, c);
        h = g;
        g = f;
        f = e;
        e = d + T1;
        d = c;
        c = b;
        b = a;
        a = T1 + T2;

        j++;
    } while (j < 16);

    do {
        /* Part of the message block expansion: */
        temp_s0 = W256[(j + 1) & 0x0f];
        temp_s0 = sigma0_256(temp_s0);
        temp_s1 = W256[(j + 14) & 0x0f];
        temp_s1 = sigma1_256(temp_s1);

        /* Apply the SHA-256 compression function to update a..h */
        T1 = h + Sigma1_256(e) + Ch(e, f, g) + K256[j] + (W256[j & 0x0f] += temp_s1 + W256[(j + 9) & 0x0f] + temp_s0);
        T2 = Sigma0_256(a) + Maj(a, b, c);
        h = g;
        g = f;
        f = e;
        e = d + T1;
        d = c;
        c = b;
        b = a;
        a = T1 + T2;

        j++;
    } while (j < 64);

    /* Compute the current intermediate hash value */
    context->state[0] += a;
    context->state[1] += b;
    context->state[2] += c;
    context->state[3] += d;
    context->state[4] += e;
    context->state[5] += f;
    context->state[6] += g;
    context->state[7] += h;
}
#endif /* SHA2_UNROLL_TRANSFORM */

void SHA256_Update2(SHA256_CTX2* context, const uint8* data, size_t len)
{
    size_t freespace, usedspace;
    errno_t rc = 0;

    /* Calling with no data is valid (we do nothing) */
    if (len == 0) {
        return;
    }

    usedspace = (context->bitcount >> 3) % SHA256_BLOCK_LENGTH;
    if (usedspace > 0) {
        /* Calculate how much free space is available in the buffer */
        freespace = SHA256_BLOCK_LENGTH - usedspace;

        if (len >= freespace) {
            /* Fill the buffer completely and process it */
            rc = memcpy_s(&context->buffer[usedspace], freespace, data, freespace);
            SECUREC_CHECK(rc);

            context->bitcount += freespace << 3;
            len -= freespace;
            data += freespace;
            SHA256_Transform(context, context->buffer);
        } else {
            /* The buffer is not yet full */
            rc = memcpy_s(&context->buffer[usedspace], len, data, len);
            SECUREC_CHECK(rc);

            context->bitcount += len << 3;
            /* Clean up: */
            return;
        }
    }
    while (len >= SHA256_BLOCK_LENGTH) {
        /* Process as many complete blocks as we can */
        SHA256_Transform(context, data);
        context->bitcount += SHA256_BLOCK_LENGTH << 3;
        len -= SHA256_BLOCK_LENGTH;
        data += SHA256_BLOCK_LENGTH;
    }
    if (len > 0) {
        /* There's left-overs, so save 'em */
        rc = memcpy_s(context->buffer, len, data, len);
        SECUREC_CHECK(rc);

        context->bitcount += len << 3;
    }
}

static void SHA256_Last(SHA256_CTX2* context)
{
    unsigned int usedspace;
    errno_t rc = 0;

    usedspace = (context->bitcount >> 3) % SHA256_BLOCK_LENGTH;
#ifndef WORDS_BIGENDIAN
    /* Convert FROM host byte order */
    REVERSE64(context->bitcount, context->bitcount);
#endif
    if (usedspace > 0) {
        /* Begin padding with a 1 bit: */
        context->buffer[usedspace++] = 0x80;

        if (usedspace <= SHA256_SHORT_BLOCK_LENGTH) {
            /* Set-up for the last transform: */
            rc = memset_s(&context->buffer[usedspace],
                SHA256_SHORT_BLOCK_LENGTH - usedspace,
                0,
                SHA256_SHORT_BLOCK_LENGTH - usedspace);
            SECUREC_CHECK(rc);
        } else {
            if (usedspace < SHA256_BLOCK_LENGTH) {
                rc = memset_s(
                    &context->buffer[usedspace], SHA256_BLOCK_LENGTH - usedspace, 0, SHA256_BLOCK_LENGTH - usedspace);
                SECUREC_CHECK(rc);
            }
            /* Do second-to-last transform: */
            SHA256_Transform(context, context->buffer);

            /* And set-up for the last transform: */
            rc = memset_s(context->buffer, SHA256_SHORT_BLOCK_LENGTH, 0, SHA256_SHORT_BLOCK_LENGTH);
            SECUREC_CHECK(rc);
        }
    } else {
        /* Set-up for the last transform: */
        rc = memset_s(context->buffer, SHA256_SHORT_BLOCK_LENGTH, 0, SHA256_SHORT_BLOCK_LENGTH);
        SECUREC_CHECK(rc);

        /* Begin padding with a 1 bit: */
        *context->buffer = 0x80;
    }
    /* Set the bit count: */
    *(uint64*)&context->buffer[SHA256_SHORT_BLOCK_LENGTH] = context->bitcount;

    /* Final transform: */
    SHA256_Transform(context, context->buffer);
}

void SHA256_Final2(uint8 digest[], SHA256_CTX2* context)
{
    errno_t rc = 0;
    /* If no digest buffer is passed, we don't bother doing this: */
    if (digest != NULL) {
        SHA256_Last(context);

#ifndef WORDS_BIGENDIAN
        {
            /* Convert TO host byte order */
            int j;

            for (j = 0; j < 8; j++) {
                REVERSE32(context->state[j], context->state[j]);
            }
        }
#endif

        rc = memcpy_s(digest, SHA256_DIGEST_LENGTH, context->state, sizeof(context->state));
        SECUREC_CHECK(rc);
    }

    /* Clean up state data: */
    rc = memset_s(context, sizeof(*context), 0, sizeof(*context));
    SECUREC_CHECK(rc);
}

/* Tranform binary(32Bytes) to string(64Bytes) */
void sha_bytes_to_hex64(uint8 b[32], char* s)
{
    static const char* hex = "0123456789abcdef";
    int q = 0;
    int w = 0;

    for (q = 0, w = 0; q < 32; q++) {
        s[w++] = hex[(b[q] >> 4) & 0x0F];
        s[w++] = hex[b[q] & 0x0F];
    }
    s[w] = '\0';
}

/* Tranform binary(4Bytes) to string(8Bytes) */
void sha_bytes_to_hex8(uint8 b[4], char* s)
{
    static const char* hex = "0123456789abcdef";
    int q = 0;
    int w = 0;

    for (q = 0, w = 0; q < 4; q++) {
        s[w++] = hex[(b[q] >> 4) & 0x0F];
        s[w++] = hex[b[q] & 0x0F];
    }
    s[w] = '\0';
}

/* Tranform string(64Bytes) to binary(32Bytes) */
void sha_hex_to_bytes32(char* s, const char b[64])
{
    int i = 0;
    uint8 v1, v2;
    for (i = 0; i < 64; i += 2) {
        v1 = (b[i] >= 'a') ? (b[i] - 'a' + 10) : (b[i] - '0');
        v2 = (*((b + i) + 1)) >= 'a' ? (*((b + i) + 1)) - 'a' + 10 : (*((b + i) + 1)) - '0';

        s[i / 2] = (v1 << 4) + v2;
    }
}

/* Tranform string(8Bytes) to binary(4Bytes) */
void sha_hex_to_bytes4(char* s, const char b[8])
{
    int i = 0;
    uint8 v1, v2;
    for (i = 0; i < 8; i += 2) {
        v1 = (b[i] >= 'a') ? (b[i] - 'a' + 10) : (b[i] - '0');
        v2 = (*((b + i) + 1)) >= 'a' ? (*((b + i) + 1)) - 'a' + 10 : (*((b + i) + 1)) - '0';

        s[i / 2] = (v1 << 4) + v2;
    }
}

bool pg_sha256_encrypt(
    const char* password, const char* salt_s, size_t salt_len, char* buf, char* client_key_buf, int iteration_count)
{
    size_t password_len = 0;
    char k[K_LENGTH + 1] = {0};
    char client_key[CLIENT_KEY_BYTES_LENGTH + 1] = {0};
    char sever_key[HMAC_LENGTH + 1] = {0};
    char stored_key[STORED_KEY_LENGTH + 1] = {0};
    char salt[SALT_LENGTH + 1] = {0};
    char sever_key_string[HMAC_LENGTH * 2 + 1] = {0};
    char stored_key_string[STORED_KEY_LENGTH * 2 + 1] = {0};
    int pkcs_ret;
    int sever_ret;
    int client_ret;
    int hash_ret;
    int hmac_length = HMAC_LENGTH;
    int stored_key_length = STORED_KEY_LENGTH;
    int total_encrypt_length;
    char sever_string[SEVER_STRING_LENGTH] = "Sever Key";
    char client_string[CLIENT_STRING_LENGTH] = "Client Key";
    errno_t rc = 0;

    if (password == NULL || buf == NULL) {
        return false;
    }

    password_len = strlen(password);
    sha_hex_to_bytes32(salt, (char*)salt_s);
    /* calculate k */
    pkcs_ret = PKCS5_PBKDF2_HMAC((char*)password,
        password_len,
        (unsigned char*)salt,
        SALT_LENGTH,
        iteration_count,
        (EVP_MD*)EVP_sha1(),
        K_LENGTH,
        (unsigned char*)k);
    if (!pkcs_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        return false;
    }

    /* We have already get k ,then we calculate client key and sever key,
     * then calculate stored key by using client key */

    /* calculate sever key */
    sever_ret = CRYPT_hmac(NID_hmacWithSHA256,
        (GS_UCHAR*)k,
        K_LENGTH,
        (GS_UCHAR*)sever_string,
        SEVER_STRING_LENGTH - 1,
        (GS_UCHAR*)sever_key,
        (GS_UINT32*)&hmac_length);
    if (sever_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
        SECUREC_CHECK(rc);
        return false;
    }

    /* calculate client key */
    client_ret = CRYPT_hmac(NID_hmacWithSHA256,
        (GS_UCHAR*)k,
        K_LENGTH,
        (GS_UCHAR*)client_string,
        CLIENT_STRING_LENGTH - 1,
        (GS_UCHAR*)client_key,
        (GS_UINT32*)&hmac_length);
    if (client_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(client_key, CLIENT_KEY_BYTES_LENGTH + 1, 0, CLIENT_KEY_BYTES_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
        SECUREC_CHECK(rc);

        return false;
    }

    if (NULL != client_key_buf) {
        sha_bytes_to_hex64((uint8*)client_key, client_key_buf);
    }

#ifndef WIN32
    hash_ret = EVP_Digest(
        (GS_UCHAR*)client_key, HMAC_LENGTH, (GS_UCHAR*)stored_key, (GS_UINT32*)&stored_key_length, EVP_sha256(), NULL);
#else
    hash_ret = EVP_Digest(
        (GS_UCHAR*)client_key, HMAC_LENGTH, (GS_UCHAR*)stored_key, (unsigned int *)&stored_key_length,
        EVP_sha256(), NULL);
#endif
    if (!hash_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(client_key, CLIENT_KEY_BYTES_LENGTH + 1, 0, CLIENT_KEY_BYTES_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(stored_key, STORED_KEY_LENGTH + 1, 0, STORED_KEY_LENGTH + 1);
        SECUREC_CHECK(rc);

        return false;
    }

    // Mark the type in the stored string
    rc = strncpy_s(buf, SHA256_LENGTH + 1, "sha256", SHA256_LENGTH);
    SECUREC_CHECK(rc);

    buf[SHA256_LENGTH] = '\0';

    total_encrypt_length = SALT_LENGTH * 2 + HMAC_LENGTH * 2 + STORED_KEY_LENGTH * 2;

    sha_bytes_to_hex64((uint8*)sever_key, sever_key_string);
    sha_bytes_to_hex64((uint8*)stored_key, stored_key_string);

    rc = snprintf_s(buf + SHA256_LENGTH,
        total_encrypt_length + 1,
        total_encrypt_length,
        "%s%s%s",
        salt_s,
        sever_key_string,
        stored_key_string);
    SECUREC_CHECK_SS(rc);

    buf[(unsigned int)(SHA256_LENGTH + total_encrypt_length)] = '\0';

    // We must clear the mem before we free it for the safe
    rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
    SECUREC_CHECK(rc);

    rc = memset_s(client_key, CLIENT_KEY_BYTES_LENGTH + 1, 0, CLIENT_KEY_BYTES_LENGTH + 1);
    SECUREC_CHECK(rc);

    rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
    SECUREC_CHECK(rc);

    rc = memset_s(stored_key, STORED_KEY_LENGTH + 1, 0, STORED_KEY_LENGTH + 1);
    SECUREC_CHECK(rc);

    return true;
}

#ifdef ENABLE_LITE_MODE
bool pg_sha256_encrypt_v1(
    const char* password, const char* salt_s, size_t salt_len, char* buf, char* client_key_buf)
{
    return pg_sha256_encrypt(password, salt_s, salt_len, buf, client_key_buf, ITERATION_COUNT_V1);
}
#endif

/* Caller must ensure that the length of the password1 and password2 are the same, and equal to the length */
int XOR_between_password(const char* password1, const char* password2, char* r, int length)
{
    int i;

    if (password1 == NULL || password2 == NULL || length < 0) {
        return -1;
    }

    for (i = 0; i < length; i++) {
        *r++ = (unsigned char)*password1++ ^ (unsigned char)*password2++;
    }
    return 0;
}

bool pg_sha256_encrypt_for_md5(const char* password, const char* salt, size_t salt_len, char* buf)
{
    size_t password_len = 0;
    SHA256_CTX2 sha_contest;
    uint8 digest[SHA256_DIGEST_LENGTH] = {0};
    char* crypt_buf = NULL;
    errno_t rc = 0;

    if (password == NULL || salt == NULL || buf == NULL) {
        return false;
    }
        

    password_len = strlen(password);
    /* +1 here is just to avoid risk of unportable malloc(0) */
    crypt_buf = (char*)malloc(password_len + salt_len + 1);
    if (crypt_buf == NULL) {
        return false;
    }

    rc = memset_s(&sha_contest, sizeof(sha_contest), 0, sizeof(sha_contest));
    securec_check_c(rc, crypt_buf, "");

    /*
     * Place salt at the end because it may be known by users trying to crack
     * the SHA256 output.
     */
    rc = memcpy_s(crypt_buf, password_len + 1, password, password_len);
    securec_check_c(rc, crypt_buf, "");

    rc = memcpy_s(crypt_buf + password_len, salt_len + 1, salt, salt_len);
    securec_check_c(rc, crypt_buf, "");

    rc = strncpy_s(buf, SHA256_LENGTH + 1, "sha256", SHA256_LENGTH);
    SECUREC_CHECK(rc);

    buf[SHA256_LENGTH] = '\0';

    SHA256_Init2(&sha_contest);
    SHA256_Update2(&sha_contest, (const uint8*)crypt_buf, password_len + salt_len);
    SHA256_Final2(digest, &sha_contest);

    sha_bytes_to_hex64(digest, buf + 6);

    rc = memset_s(crypt_buf, password_len + salt_len + 1, 0, password_len + salt_len + 1);
    securec_check_c(rc, crypt_buf, "");
    free(crypt_buf);
    crypt_buf = NULL;

    return true;
}

bool GsSm3Encrypt(
    const char* password, const char* salt_s, size_t salt_len, char* buf, char* client_key_buf, int iteration_count)
{
    size_t password_len = 0;
    char k[K_LENGTH + 1] = {0};
    char client_key[CLIENT_KEY_BYTES_LENGTH + 1] = {0};
    char sever_key[HMAC_LENGTH + 1] = {0};
    char stored_key[STORED_KEY_LENGTH + 1] = {0};
    char salt[SALT_LENGTH + 1] = {0};
    char serverKeyString[HMAC_LENGTH * ENCRY_LENGTH_DOUBLE + 1] = {0};
    char stored_key_string[STORED_KEY_LENGTH * 2 + 1] = {0};
    int pkcs_ret;
    int sever_ret;
    int client_ret;
    int hash_ret;
    int hmac_length = HMAC_LENGTH;
    int stored_key_length = STORED_KEY_LENGTH;
    int total_encrypt_length;
    char server_string[SEVER_STRING_LENGTH_SM3] = "Server Key";
    char client_string[CLIENT_STRING_LENGTH] = "Client Key";
    errno_t rc = 0;

    if (NULL == password || NULL == buf) {
        return false;
    }

    password_len = strlen(password);
    sha_hex_to_bytes32(salt, (char*)salt_s);
    /* calculate k */
    pkcs_ret = PKCS5_PBKDF2_HMAC((char*)password,
        password_len,
        (unsigned char*)salt,
        SALT_LENGTH,
        iteration_count,
        (EVP_MD*)EVP_sha1(),
        K_LENGTH,
        (unsigned char*)k);
    if (!pkcs_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        return false;
    }

    /* We have already get k ,then we calculate client key and server key,
     * then calculate stored key by using client key */

    /* calculate server rkey */
    sever_ret = CRYPT_hmac(NID_hmacWithSHA256,
        (GS_UCHAR*)k,
        K_LENGTH,
        (GS_UCHAR*)server_string,
        SEVER_STRING_LENGTH_SM3 - 1,
        (GS_UCHAR*)sever_key,
        (GS_UINT32*)&hmac_length);
    if (sever_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
        SECUREC_CHECK(rc);
        return false;
    }

    /* calculate client key */
    client_ret = CRYPT_hmac(NID_hmacWithSHA256,
        (GS_UCHAR*)k,
        K_LENGTH,
        (GS_UCHAR*)client_string,
        CLIENT_STRING_LENGTH - 1,
        (GS_UCHAR*)client_key,
        (GS_UINT32*)&hmac_length);
    if (client_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(client_key, CLIENT_KEY_BYTES_LENGTH + 1, 0, CLIENT_KEY_BYTES_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
        SECUREC_CHECK(rc);

        return false;
    }

    if (NULL != client_key_buf) {
        sha_bytes_to_hex64((uint8*)client_key, client_key_buf);
    }

    hash_ret = EVP_Digest(
        (GS_UCHAR*)client_key, HMAC_LENGTH, (GS_UCHAR*)stored_key, (GS_UINT32*)&stored_key_length, EVP_sm3(), NULL);

    if (!hash_ret) {
        rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(client_key, CLIENT_KEY_BYTES_LENGTH + 1, 0, CLIENT_KEY_BYTES_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
        SECUREC_CHECK(rc);

        rc = memset_s(stored_key, STORED_KEY_LENGTH + 1, 0, STORED_KEY_LENGTH + 1);
        SECUREC_CHECK(rc);

        return false;
    }

    /* Mark the type in the stored string */
    rc = strncpy_s(buf, SM3_LENGTH + 1, "sm3", SM3_LENGTH);
    SECUREC_CHECK(rc);

    buf[SM3_LENGTH] = '\0';

    total_encrypt_length = SALT_LENGTH * 2 + HMAC_LENGTH * 2 + STORED_KEY_LENGTH * 2;

    sha_bytes_to_hex64((uint8*)sever_key, serverKeyString);
    sha_bytes_to_hex64((uint8*)stored_key, stored_key_string);

    rc = snprintf_s(buf + SM3_LENGTH,
        total_encrypt_length + 1,
        total_encrypt_length,
        "%s%s%s",
        salt_s,
        serverKeyString,
        stored_key_string);
    SECUREC_CHECK_SS(rc);

    buf[(unsigned int)(SM3_LENGTH + total_encrypt_length)] = '\0';

    /* We must clear the mem before we free it for the safe */
    rc = memset_s(k, K_LENGTH + 1, 0, K_LENGTH + 1);
    SECUREC_CHECK(rc);

    rc = memset_s(client_key, CLIENT_KEY_BYTES_LENGTH + 1, 0, CLIENT_KEY_BYTES_LENGTH + 1);
    SECUREC_CHECK(rc);

    rc = memset_s(sever_key, HMAC_LENGTH + 1, 0, HMAC_LENGTH + 1);
    SECUREC_CHECK(rc);

    rc = memset_s(stored_key, STORED_KEY_LENGTH + 1, 0, STORED_KEY_LENGTH + 1);
    SECUREC_CHECK(rc);

    return true;
}

