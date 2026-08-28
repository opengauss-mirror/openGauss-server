/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * backup_encrypt.cpp: streaming encryption of backup files
 *
 * Encryption is applied at the IO layer, right after page level compression
 * and before the data reaches the disk, so it inherits the parallelism of
 * the backup threads instead of being a serial post processing pass.
 *
 * Every backup owns a random data encryption key (DEK) which is stored,
 * wrapped by a key encryption key (KEK), in backup.keyinfo. The KEK is
 * derived from a passphrase with PBKDF2-HMAC-SHA256, the same primitive
 * gs_dump uses. Each backup file is written as a sequence of independently
 * authenticated AES-128-GCM chunks, which keeps random access (restore only
 * decrypts the chunks it reads) and detects reordering or truncation.
 *
 * IDENTIFICATION
 *     src/bin/pg_probackup/backup_encrypt.cpp
 *
 *-------------------------------------------------------------------------
 */

/* postgres_fe.h, pulled in here, has to come before any system header */
#include "pg_probackup.h"

#include <fcntl.h>
#include <pthread.h>
#include <termios.h>
#include <unistd.h>
#include <sys/stat.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include "backup_encrypt.h"
#include "common/fe_memutils.h"

bool  g_encryptEnabled = false;
char *g_encryptAlgorithmStr = NULL;
char *g_encryptKeySourceStr = NULL;
char *g_encryptKeyArg = NULL;
char *g_encryptKeyFile = NULL;
char *g_encryptChunkSizeStr = NULL;
char *g_newEncryptKeyArg = NULL;
char *g_newEncryptKeyFile = NULL;

/* key material of a single backup */
typedef struct BackupEncKey {
    char            root[MAXPGPATH];
    size_t          rootLen;
    bool            encrypted;
    bool            loading;
    uint16          alg;
    uint32          chunkSize;
    unsigned char   dek[GSPB_ENC_DEK_LEN];
    unsigned char   kData[GSPB_ENC_KEY_LEN];
    unsigned char   kMac[GSPB_ENC_MAC_LEN];
} BackupEncKey;

/* container stream state */
typedef struct EncStream {
    FILE           *raw;                     /* real file underneath */
    FILE           *self;                    /* cookie stream handed to caller */
    BackupEncKey   *key;
    bool            writeMode;
    bool            stagedMode;
    unsigned char   header[GSPB_ENC_HDR_LEN];
    uint32          chunkSize;
    uint64          fileNonce;
    off_t           plainPos;
    off_t           plainSize;              /* read mode only */
    /* write side */
    char           *wrBuf;
    uint32          wrFill;
    uint32          wrChunk;
    /* read side: one decrypted chunk is kept around */
    char           *rdBuf;
    int64           rdChunk;
    uint32          rdLen;
    unsigned char  *cipherBuf;
    char            path[MAXPGPATH];
    char            stage_path[MAXPGPATH];
} EncStream;

#define ENC_MAX_KEYS        64
#define ENC_MAX_STREAMS     1024

static BackupEncKey *g_keyCache[ENC_MAX_KEYS];
static int           g_keyCacheNum = 0;
static pthread_mutex_t g_keyCacheMutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_keyCacheCond = PTHREAD_COND_INITIALIZER;

static EncStream    *g_streamRegistry[ENC_MAX_STREAMS];
static int           g_streamRegistryNum = 0;
static pthread_mutex_t g_streamRegistryMutex = PTHREAD_MUTEX_INITIALIZER;

static char         *g_cachedPassphrase = NULL;
static bool          g_passphraseResolved = false;
static pthread_mutex_t g_passphraseMutex = PTHREAD_MUTEX_INITIALIZER;

static uint32 g_configuredChunkSize = GSPB_ENC_DEFAULT_CHUNK;

/*-------------------------------------------------------------------------
 * small helpers
 *-------------------------------------------------------------------------
 */

static void EncWipe(void *p, size_t len)
{
    if (p != NULL && len > 0) {
        OPENSSL_cleanse(p, len);
    }
}

static void StoreUint16(unsigned char *dst, uint16 v)
{
    dst[0] = (unsigned char) (v & 0xff);
    dst[1] = (unsigned char) ((v >> GSPB_ENC_BITS_PER_BYTE) & 0xff);
}

static uint16 LoadUint16(const unsigned char *src)
{
    return (uint16) src[0] | ((uint16) src[1] << GSPB_ENC_BITS_PER_BYTE);
}

static void StoreUint32(unsigned char *dst, uint32 v)
{
    for (size_t i = 0; i < sizeof(uint32); i++) {
        dst[i] = (unsigned char) ((v >> (i * GSPB_ENC_BITS_PER_BYTE)) & 0xff);
    }
}

static uint32 LoadUint32(const unsigned char *src)
{
    uint32 v = 0;

    for (size_t i = 0; i < sizeof(uint32); i++) {
        v |= ((uint32) src[i]) << (i * GSPB_ENC_BITS_PER_BYTE);
    }
    return v;
}

static void StoreUint64(unsigned char *dst, uint64 v)
{
    for (size_t i = 0; i < sizeof(uint64); i++) {
        dst[i] = (unsigned char) ((v >> (i * GSPB_ENC_BITS_PER_BYTE)) & 0xff);
    }
}

static uint64 LoadUint64(const unsigned char *src)
{
    uint64 v = 0;

    for (size_t i = 0; i < sizeof(uint64); i++) {
        v |= ((uint64) src[i]) << (i * GSPB_ENC_BITS_PER_BYTE);
    }
    return v;
}

/* base64 alphabet layout: 26 upper, 26 lower, 10 digits, '+', '/' */
#define B64_LOWER_BASE      26
#define B64_DIGIT_BASE      52
#define B64_PLUS_VALUE      62
#define B64_SLASH_VALUE     63
#define B64_GROUP_BYTES     3      /* 3 input bytes map to 4 output chars */
#define B64_GROUP_CHARS     4
#define B64_BITS_PER_CHAR   6
#define B64_CHAR_MASK       0x3f
#define B64_BITS_PER_BYTE   8

static const char B64_ALPHABET[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static char *EncBase64Encode(const unsigned char *src, size_t len)
{
    size_t  outLen = ((len + B64_GROUP_BYTES - 1) / B64_GROUP_BYTES) * B64_GROUP_CHARS;
    char   *out = (char *) pgut_malloc(outLen + 1);
    size_t  i = 0;
    size_t  j = 0;

    while (i < len) {
        uint32 triple = (uint32) src[i++] << (2 * B64_BITS_PER_BYTE);

        triple |= (i < len ? (uint32) src[i++] << B64_BITS_PER_BYTE : 0);
        triple |= (i < len ? (uint32) src[i++] : 0);

        out[j++] = B64_ALPHABET[(triple >> (3 * B64_BITS_PER_CHAR)) & B64_CHAR_MASK];
        out[j++] = B64_ALPHABET[(triple >> (2 * B64_BITS_PER_CHAR)) & B64_CHAR_MASK];
        out[j++] = B64_ALPHABET[(triple >> B64_BITS_PER_CHAR) & B64_CHAR_MASK];
        out[j++] = B64_ALPHABET[triple & B64_CHAR_MASK];
    }

    /* pad according to the number of bytes in the final group */
    if (len % B64_GROUP_BYTES == 1) {
        out[outLen - 2] = '=';
        out[outLen - 1] = '=';
    } else if (len % B64_GROUP_BYTES == 2) {
        out[outLen - 1] = '=';
    }

    out[outLen] = '\0';
    return out;
}

static int B64Value(char c)
{
    if (c >= 'A' && c <= 'Z') {
        return c - 'A';
    }
    if (c >= 'a' && c <= 'z') {
        return c - 'a' + B64_LOWER_BASE;
    }
    if (c >= '0' && c <= '9') {
        return c - '0' + B64_DIGIT_BASE;
    }
    if (c == '+') {
        return B64_PLUS_VALUE;
    }
    if (c == '/') {
        return B64_SLASH_VALUE;
    }
    return -1;
}

/* returns number of decoded bytes, or -1 on malformed input */
static int EncBase64Decode(const char *src, unsigned char *dst, size_t dstSize)
{
    uint32 buf = 0;
    int    bits = 0;
    size_t out = 0;

    for (const char *p = src; *p != '\0'; p++) {
        int v;

        if (*p == '=' || *p == '\n' || *p == '\r' || *p == ' ') {
            continue;
        }

        v = B64Value(*p);
        if (v < 0) {
            return -1;
        }

        buf = (buf << B64_BITS_PER_CHAR) | (uint32) v;
        bits += B64_BITS_PER_CHAR;
        if (bits >= B64_BITS_PER_BYTE) {
            bits -= B64_BITS_PER_BYTE;
            if (out >= dstSize) {
                return -1;
            }
            dst[out++] = (unsigned char) ((buf >> bits) & 0xff);
        }
    }

    return (int) out;
}

static void EncRandomBytes(unsigned char *buf, int len)
{
    if (RAND_priv_bytes(buf, len) != 1) {
        elog(ERROR, "Cannot obtain random bytes for backup encryption");
    }
}

static void HmacSha256(const unsigned char *key, size_t keyLen,
            const unsigned char *data, size_t dataLen,
            unsigned char *out)
{
    unsigned int outLen = GSPB_ENC_MAC_LEN;
    if (HMAC(EVP_sha256(), key, (int) keyLen, data, dataLen, out, &outLen) == NULL ||
        outLen != GSPB_ENC_MAC_LEN) {
        elog(ERROR, "HMAC-SHA256 computation failed");
    }
}

/* RFC 5869 HKDF with an all-zero salt, which is enough for a random DEK */
static void HkdfSha256(const unsigned char *ikm, size_t ikmLen,
            const char *info, unsigned char *out, size_t outLen)
{
    unsigned char zeroSalt[GSPB_ENC_MAC_LEN] = {0};
    unsigned char prk[GSPB_ENC_MAC_LEN];
    unsigned char block[GSPB_ENC_MAC_LEN];
    unsigned char buf[GSPB_ENC_MAC_LEN + GSPB_ENC_HKDF_INFO_MAX + 1];
    size_t        infoLen = strlen(info);
    size_t        done = 0;
    unsigned char counter = 1;
    size_t        blockLen = 0;
    errno_t       rc;

    if (infoLen > GSPB_ENC_HKDF_INFO_MAX) {
        elog(ERROR, "HKDF info label is too long");
    }

    HmacSha256(zeroSalt, sizeof(zeroSalt), ikm, ikmLen, prk);

    while (done < outLen) {
        size_t pos = 0;
        size_t take;

        if (blockLen > 0) {
            rc = memcpy_s(buf, sizeof(buf), block, blockLen);
            securec_check_c(rc, "", "");
            pos += blockLen;
        }
        rc = memcpy_s(buf + pos, sizeof(buf) - pos, info, infoLen);
        securec_check_c(rc, "", "");
        pos += infoLen;
        buf[pos++] = counter;

        HmacSha256(prk, sizeof(prk), buf, pos, block);
        blockLen = sizeof(block);

        take = Min(outLen - done, blockLen);
        rc = memcpy_s(out + done, outLen - done, block, take);
        securec_check_c(rc, "", "");
        done += take;
        counter++;
    }

    EncWipe(prk, sizeof(prk));
    EncWipe(block, sizeof(block));
    EncWipe(buf, sizeof(buf));
}

/*
 * AES-GCM in one shot. keyLen selects AES-128 or AES-256.
 * Returns false when authentication fails on decrypt.
 */
static bool AesGcmCrypt(bool encrypt, const unsigned char *key, int keyLen,
              const unsigned char *nonce,
              const unsigned char *aad, int aadLen,
              const unsigned char *in, int inLen,
              unsigned char *out, unsigned char *tag)
{
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    const EVP_CIPHER *cipher = (keyLen == 32) ? EVP_aes_256_gcm() : EVP_aes_128_gcm();
    int  len = 0;
    bool ok = false;

    if (ctx == NULL) {
        elog(ERROR, "Cannot allocate cipher context");
    }

    do {
        if (EVP_CipherInit_ex(ctx, cipher, NULL, NULL, NULL, encrypt ? 1 : 0) != 1) {
            break;
        }
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, GSPB_ENC_NONCE_LEN, NULL) != 1) {
            break;
        }
        if (EVP_CipherInit_ex(ctx, NULL, NULL, key, nonce, encrypt ? 1 : 0) != 1) {
            break;
        }
        if (aad != NULL && aadLen > 0 &&
            EVP_CipherUpdate(ctx, NULL, &len, aad, aadLen) != 1) {
            break;
        }
        if (!encrypt &&
            EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, GSPB_ENC_TAG_LEN, tag) != 1) {
            break;
        }
        if (inLen > 0 && EVP_CipherUpdate(ctx, out, &len, in, inLen) != 1) {
            break;
        }
        if (EVP_CipherFinal_ex(ctx, out + len, &len) != 1) {
            break;
        }
        if (encrypt &&
            EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, GSPB_ENC_TAG_LEN, tag) != 1) {
            break;
        }
        ok = true;
    } while (0);

    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

/*-------------------------------------------------------------------------
 * passphrase handling
 *-------------------------------------------------------------------------
 */

/*
 * Overwrite the key given on the command line, so that it stops showing up
 * in ps(1) output, the same way the password options are handled.
 */
void EncryptScrubArgv(int argc, char **argv)
{
    replace_password(argc, argv, "--encrypt-key");
    replace_password(argc, argv, "--new-encrypt-key");
    replace_password(argc, argv, "--with-key");
}

static char *ReadPassphraseFromFile(const char *path)
{
    struct stat st;
    FILE       *fp;
    char        buf[GSPB_ENC_TEXT_BUF_LEN];
    size_t      len;

    if (stat(path, &st) != 0) {
        elog(ERROR, "Cannot stat encryption key file \"%s\": %s", path, gs_strerror(errno));
    }

    if (!S_ISREG(st.st_mode)) {
        elog(ERROR, "Encryption key file \"%s\" is not a regular file", path);
    }

    if ((st.st_mode & ACCESSPERMS) != (S_IRUSR | S_IWUSR)) {
        elog(ERROR, "Encryption key file \"%s\" must have permissions 0600", path);
    }
    if (st.st_size <= 0 || st.st_size >= GSPB_ENC_TEXT_BUF_LEN) {
        elog(ERROR, "Encryption key file \"%s\" must contain 1..%d bytes", path,
             GSPB_ENC_TEXT_BUF_LEN - 1);
    }

    fp = fopen(path, PG_BINARY_R);
    if (fp == NULL) {
        elog(ERROR, "Cannot open encryption key file \"%s\": %s", path, gs_strerror(errno));
    }

    len = fread(buf, 1, sizeof(buf) - 1, fp);
    (void) fclose(fp);

    if (len == 0) {
        elog(ERROR, "Encryption key file \"%s\" is empty", path);
    }

    /* a trailing newline is almost always an artifact of the editor */
    while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
        len--;
    }

    buf[len] = '\0';
    if (len == 0) {
        elog(ERROR, "Encryption key file \"%s\" contains no key material", path);
    }

    return pgut_strdup(buf);
}

static char *PromptPassphrase(void)
{
    struct termios oldFlags;
    struct termios newFlags;
    char           buf[GSPB_ENC_TEXT_BUF_LEN];
    bool           ttyChanged = false;
    size_t         len;

    if (!isatty(fileno(stdin))) {
        elog(ERROR, "Backup encryption key is required but no key was provided. "
             "Use --encrypt-key, --encrypt-key-file or the %s environment variable",
             GSPB_PASSPHRASE_ENV);
    }

    (void) fprintf(stderr, "Encryption key: ");
    (void) fflush(stderr);

    if (tcgetattr(fileno(stdin), &oldFlags) == 0) {
        newFlags = oldFlags;
        newFlags.c_lflag &= ~ECHO;
        if (tcsetattr(fileno(stdin), TCSAFLUSH, &newFlags) == 0) {
            ttyChanged = true;
        }
    }

    if (fgets(buf, sizeof(buf), stdin) == NULL) {
        buf[0] = '\0';
    }

    if (ttyChanged) {
        (void) tcsetattr(fileno(stdin), TCSAFLUSH, &oldFlags);
    }

    (void) fputc('\n', stderr);

    len = strlen(buf);
    while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r')) {
        buf[--len] = '\0';
    }

    if (len == 0) {
        elog(ERROR, "Empty encryption key");
    }

    return pgut_strdup(buf);
}

/* wipe every cached key and the passphrase; safe to call more than once */
void EncryptCleanup(void)
{
    pthread_mutex_lock(&g_keyCacheMutex);
    for (int i = 0; i < g_keyCacheNum; i++) {
        EncWipe(g_keyCache[i], sizeof(BackupEncKey));
        pg_free(g_keyCache[i]);
    }
    g_keyCacheNum = 0;
    pthread_mutex_unlock(&g_keyCacheMutex);

    pthread_mutex_lock(&g_passphraseMutex);
    if (g_cachedPassphrase != NULL) {
        EncWipe(g_cachedPassphrase, strlen(g_cachedPassphrase));
        pg_free(g_cachedPassphrase);
        g_cachedPassphrase = NULL;
    }
    if (g_encryptKeyArg != NULL) {
        EncWipe(g_encryptKeyArg, strlen(g_encryptKeyArg));
    }
    if (g_newEncryptKeyArg != NULL) {
        EncWipe(g_newEncryptKeyArg, strlen(g_newEncryptKeyArg));
    }
    pthread_mutex_unlock(&g_passphraseMutex);
}

/* adapter for the pgut exit callback signature */
static void EncryptCleanupCallback(bool fatal, void *userdata)
{
    (void) fatal;
    (void) userdata;
    EncryptCleanup();
}

/* passphrase is resolved once per process and cached */
static const char *GetPassphrase(void)
{
    pthread_mutex_lock(&g_passphraseMutex);

    if (!g_passphraseResolved) {
        char *envKey = NULL;

        if (g_encryptKeyFile != NULL) {
            g_cachedPassphrase = ReadPassphraseFromFile(g_encryptKeyFile);
        } else if (g_encryptKeyArg != NULL && g_encryptKeyArg[0] != '\0') {
            g_cachedPassphrase = pgut_strdup(g_encryptKeyArg);
        } else if ((envKey = gs_getenv_r(GSPB_PASSPHRASE_ENV)) != NULL && envKey[0] != '\0') {
            g_cachedPassphrase = pgut_strdup(envKey);
            unsetenv(GSPB_PASSPHRASE_ENV);
        } else {
            g_cachedPassphrase = PromptPassphrase();
        }

        /* the copy in the option slot is not needed any more */
        if (g_encryptKeyArg != NULL) {
            EncWipe(g_encryptKeyArg, strlen(g_encryptKeyArg));
        }

        static bool cleanupRegistered = false;
        if (!cleanupRegistered) {
            pgut_atexit_push(EncryptCleanupCallback, NULL);
            cleanupRegistered = true;
        }
        g_passphraseResolved = true;
    }

    pthread_mutex_unlock(&g_passphraseMutex);
    return g_cachedPassphrase;
}

static void DeriveKekFromPassphrase(const char *pass, const unsigned char *salt,
                           uint32 iterations, unsigned char *kek)
{
    if (PKCS5_PBKDF2_HMAC(pass, (int) strlen(pass), salt, GSPB_ENC_SALT_LEN,
                          (int) iterations, EVP_sha256(),
                          GSPB_ENC_DEK_LEN, kek) != 1) {
        elog(ERROR, "Cannot derive key encryption key from the given key material");
    }
}

static void DeriveKek(const unsigned char *salt, uint32 iterations, unsigned char *kek)
{
    DeriveKekFromPassphrase(GetPassphrase(), salt, iterations, kek);
}

static char *GetNewPassphrase(void)
{
    if (g_newEncryptKeyFile != NULL) {
        return ReadPassphraseFromFile(g_newEncryptKeyFile);
    }
    if (g_newEncryptKeyArg != NULL && g_newEncryptKeyArg[0] != '\0') {
        return pgut_strdup(g_newEncryptKeyArg);
    }

    elog(ERROR, "rekey requires --new-encrypt-key or --new-encrypt-key-file");
    return NULL;
}

static void DeriveSubkeys(BackupEncKey *key)
{
    HkdfSha256(key->dek, sizeof(key->dek), "gspb-data",
                key->kData, sizeof(key->kData));
    HkdfSha256(key->dek, sizeof(key->dek), "gspb-meta-mac",
                key->kMac, sizeof(key->kMac));
}

static void ComputeKekCheck(const BackupEncKey *key, unsigned char *out)
{
    unsigned char mac[GSPB_ENC_MAC_LEN];
    errno_t       rc;

    HmacSha256(key->kMac, sizeof(key->kMac),
                (const unsigned char *) GSPB_ENC_MAGIC, GSPB_ENC_MAGIC_LEN, mac);
    rc = memcpy_s(out, GSPB_ENC_KEKCHECK_LEN, mac, GSPB_ENC_KEKCHECK_LEN);
    securec_check_c(rc, "", "");
}

