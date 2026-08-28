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

/*-------------------------------------------------------------------------
 * backup.keyinfo
 *-------------------------------------------------------------------------
 */

static void KeyinfoPath(const char *backupRoot, char *out, size_t outSize)
{
    errno_t rc = snprintf_s(out, outSize, outSize - 1, "%s/%s",
                            backupRoot, GSPB_KEYINFO_FILE);

    securec_check_ss_c(rc, "", "");
}

bool EncryptDirIsEncrypted(const char *backupRoot)
{
    char        path[MAXPGPATH];
    struct stat st;

    KeyinfoPath(backupRoot, path, sizeof(path));
    return stat(path, &st) == 0;
}

static void WriteKeyinfo(const char *backupRoot, BackupEncKey *key,
              const unsigned char *salt, uint32 iterations,
              const unsigned char *wrapped, size_t wrappedLen,
              const char *kekSource)
{
    char          path[MAXPGPATH];
    char          pathTmp[MAXPGPATH];
    FILE         *fp;
    char         *saltB64 = EncBase64Encode(salt, GSPB_ENC_SALT_LEN);
    char         *wrappedB64 = EncBase64Encode(wrapped, wrappedLen);
    unsigned char check[GSPB_ENC_KEKCHECK_LEN];
    char         *checkB64;
    errno_t       rc;

    ComputeKekCheck(key, check);
    checkB64 = EncBase64Encode(check, sizeof(check));

    KeyinfoPath(backupRoot, path, sizeof(path));
    rc = snprintf_s(pathTmp, sizeof(pathTmp), sizeof(pathTmp) - 1, "%s.tmp", path);
    securec_check_ss_c(rc, "", "");

    fp = fopen(pathTmp, PG_BINARY_W);
    if (fp == NULL) {
        elog(ERROR, "Cannot create \"%s\": %s", pathTmp, gs_strerror(errno));
    }

    if (chmod(pathTmp, S_IRUSR | S_IWUSR) != 0) {
        elog(ERROR, "Cannot set permissions of \"%s\": %s", pathTmp, gs_strerror(errno));
    }

    (void) fprintf(fp, "keyinfo-version = %d\n", GSPB_ENC_FORMAT_VERSION);
    (void) fprintf(fp, "encrypt-algorithm = AES128\n");
    (void) fprintf(fp, "kek-source = %s\n", kekSource);
    (void) fprintf(fp, "kdf = PBKDF2-HMAC-SHA256\n");
    (void) fprintf(fp, "kdf-iterations = %u\n", iterations);
    (void) fprintf(fp, "kdf-salt = %s\n", saltB64);
    (void) fprintf(fp, "chunk-size = %u\n", key->chunkSize);
    (void) fprintf(fp, "wrapped-dek = %s\n", wrappedB64);
    (void) fprintf(fp, "kek-check = %s\n", checkB64);

    if (fflush(fp) != 0 || fsync(fileno(fp)) != 0) {
        elog(ERROR, "Cannot flush \"%s\": %s", pathTmp, gs_strerror(errno));
    }
    if (fclose(fp) != 0) {
        elog(ERROR, "Cannot close \"%s\": %s", pathTmp, gs_strerror(errno));
    }

    if (rename(pathTmp, path) != 0) {
        elog(ERROR, "Cannot rename \"%s\" to \"%s\": %s", pathTmp, path, gs_strerror(errno));
    }

    pg_free(saltB64);
    pg_free(wrappedB64);
    pg_free(checkB64);
}

/* strip leading and trailing blanks in place */
static char *TrimValue(char *s)
{
    char *end;

    while (*s == ' ' || *s == '\t') {
        s++;
    }

    end = s + strlen(s);
    while (end > s && (end[-1] == '\n' || end[-1] == '\r' ||
                       end[-1] == ' ' || end[-1] == '\t')) {
        end--;
    }
    *end = '\0';

    return s;
}

/* text fields of backup.keyinfo, as they appear on disk */
typedef struct KeyinfoFields {
    char   saltB64[GSPB_ENC_B64_FIELD_LEN];
    char   wrappedB64[GSPB_ENC_B64_FIELD_LEN];
    char   checkB64[GSPB_ENC_B64_SHORT_LEN];
    char   algorithm[GSPB_ENC_B64_SHORT_LEN];
    uint32 iterations;
    int    version;
} KeyinfoFields;

static void ReadKeyinfoFields(const char *path, BackupEncKey *key, KeyinfoFields *out)
{
    FILE   *fp;
    char    line[GSPB_ENC_TEXT_BUF_LEN];
    errno_t rc;

    fp = fopen(path, PG_BINARY_R);
    if (fp == NULL) {
        elog(ERROR, "Cannot open \"%s\": %s", path, gs_strerror(errno));
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *sep = strchr(line, '=');
        char *name;
        char *value;

        if (sep == NULL) {
            continue;
        }
        *sep = '\0';
        name = TrimValue(line);
        value = TrimValue(sep + 1);

        if (strcmp(name, "keyinfo-version") == 0) {
            out->version = atoi(value);
        } else if (strcmp(name, "encrypt-algorithm") == 0) {
            rc = strncpy_s(out->algorithm, sizeof(out->algorithm), value,
                           sizeof(out->algorithm) - 1);
            securec_check_c(rc, "", "");
        } else if (strcmp(name, "kdf-iterations") == 0) {
            out->iterations = (uint32) strtoul(value, NULL, GSPB_ENC_DECIMAL_BASE);
        } else if (strcmp(name, "kdf-salt") == 0) {
            rc = strncpy_s(out->saltB64, sizeof(out->saltB64), value,
                           sizeof(out->saltB64) - 1);
            securec_check_c(rc, "", "");
        } else if (strcmp(name, "chunk-size") == 0) {
            key->chunkSize = (uint32) strtoul(value, NULL, GSPB_ENC_DECIMAL_BASE);
        } else if (strcmp(name, "wrapped-dek") == 0) {
            rc = strncpy_s(out->wrappedB64, sizeof(out->wrappedB64), value,
                           sizeof(out->wrappedB64) - 1);
            securec_check_c(rc, "", "");
        } else if (strcmp(name, "kek-check") == 0) {
            rc = strncpy_s(out->checkB64, sizeof(out->checkB64), value,
                           sizeof(out->checkB64) - 1);
            securec_check_c(rc, "", "");
        }
    }
    (void) fclose(fp);
}

static void LoadKeyinfo(const char *backupRoot, BackupEncKey *key)
{
    char          path[MAXPGPATH];
    KeyinfoFields f;
    unsigned char salt[GSPB_ENC_SALT_LEN];
    unsigned char wrapped[GSPB_ENC_NONCE_LEN + GSPB_ENC_DEK_LEN + GSPB_ENC_TAG_LEN];
    unsigned char storedCheck[GSPB_ENC_KEKCHECK_LEN * 2];
    unsigned char expectedCheck[GSPB_ENC_KEKCHECK_LEN];
    unsigned char kek[GSPB_ENC_DEK_LEN];
    int           len;
    errno_t       rc;

    rc = memset_s(&f, sizeof(f), 0, sizeof(f));
    securec_check_c(rc, "", "");

    KeyinfoPath(backupRoot, path, sizeof(path));
    ReadKeyinfoFields(path, key, &f);

    if (f.version != GSPB_ENC_FORMAT_VERSION) {
        elog(ERROR, "Unsupported keyinfo version %d in \"%s\", this gs_probackup "
             "supports version %d", f.version, path, GSPB_ENC_FORMAT_VERSION);
    }

    if (f.algorithm[0] != '\0' && pg_strcasecmp(f.algorithm, "AES128") != 0) {
        elog(ERROR, "Unsupported encryption algorithm \"%s\" in \"%s\"", f.algorithm, path);
    }

    if (f.iterations == 0 || f.saltB64[0] == '\0' || f.wrappedB64[0] == '\0') {
        elog(ERROR, "Key information file \"%s\" is incomplete", path);
    }

    if (key->chunkSize < GSPB_ENC_MIN_CHUNK || key->chunkSize > GSPB_ENC_MAX_CHUNK) {
        elog(ERROR, "Invalid chunk size %u in \"%s\"", key->chunkSize, path);
    }

    if (EncBase64Decode(f.saltB64, salt, sizeof(salt)) != GSPB_ENC_SALT_LEN) {
        elog(ERROR, "Malformed kdf-salt in \"%s\"", path);
    }

    len = EncBase64Decode(f.wrappedB64, wrapped, sizeof(wrapped));
    if (len != (int) sizeof(wrapped)) {
        elog(ERROR, "Malformed wrapped-dek in \"%s\"", path);
    }

    DeriveKek(salt, f.iterations, kek);

    if (!AesGcmCrypt(false, kek, sizeof(kek), wrapped, NULL, 0,
                       wrapped + GSPB_ENC_NONCE_LEN, GSPB_ENC_DEK_LEN,
                       key->dek,
                       wrapped + GSPB_ENC_NONCE_LEN + GSPB_ENC_DEK_LEN)) {
        EncWipe(kek, sizeof(kek));
        elog(ERROR, "Cannot unwrap the data key of backup \"%s\": "
             "the encryption key is wrong or the key information file is damaged",
             backupRoot);
    }
    EncWipe(kek, sizeof(kek));

    DeriveSubkeys(key);

    if (f.checkB64[0] != '\0') {
        ComputeKekCheck(key, expectedCheck);
        if (EncBase64Decode(f.checkB64, storedCheck, sizeof(storedCheck)) != GSPB_ENC_KEKCHECK_LEN ||
            memcmp(storedCheck, expectedCheck, GSPB_ENC_KEKCHECK_LEN) != 0) {
            elog(ERROR, "Key check of backup \"%s\" failed", backupRoot);
        }
    }

    key->alg = GSPB_ENC_ALG_AES128_GCM;
    key->encrypted = true;
}

/*-------------------------------------------------------------------------
 * key cache, keyed by backup root directory
 *-------------------------------------------------------------------------
 */

/* caller must hold g_keyCacheMutex */
static BackupEncKey *KeyCacheLookup(const char *path)
{
    for (int i = 0; i < g_keyCacheNum; i++) {
        BackupEncKey *key = g_keyCache[i];
        if (strncmp(path, key->root, key->rootLen) == 0 &&
            (path[key->rootLen] == '/' || path[key->rootLen] == '\0')) {
            return key;
        }
    }
    return NULL;
}

/* caller must hold g_keyCacheMutex */
static BackupEncKey *KeyCacheAdd(const char *backupRoot)
{
    BackupEncKey *key;
    errno_t       rc;

    if (g_keyCacheNum >= ENC_MAX_KEYS) {
        elog(ERROR, "Too many encrypted backups are open at the same time");
    }

    key = (BackupEncKey *) pgut_malloc(sizeof(BackupEncKey));
    rc = memset_s(key, sizeof(BackupEncKey), 0, sizeof(BackupEncKey));
    securec_check_c(rc, "", "");

    rc = strncpy_s(key->root, sizeof(key->root), backupRoot, sizeof(key->root) - 1);
    securec_check_c(rc, "", "");
    key->rootLen = strlen(key->root);
    key->chunkSize = GSPB_ENC_DEFAULT_CHUNK;

    g_keyCache[g_keyCacheNum++] = key;
    return key;
}

/*
 * Walk up from a file to the directory of the backup that owns it. Backup
 * roots are recognized by their backup.control file and the search never
 * leaves the catalog given with -B.
 */
static bool FindBackupRoot(const char *path, char *root, size_t rootSize)
{
    char        dir[MAXPGPATH];
    char        probe[MAXPGPATH];
    struct stat st;
    size_t      backupPathLen;
    errno_t     rc;

    if (backup_path == NULL) {
        return false;
    }

    backupPathLen = strlen(backup_path);
    if (strncmp(path, backup_path, backupPathLen) != 0) {
        return false;
    }

    rc = strncpy_s(dir, sizeof(dir), path, sizeof(dir) - 1);
    securec_check_c(rc, "", "");

    for (int depth = 0; depth < GSPB_ENC_MAX_PARENT_DEPTH; depth++) {
        get_parent_directory(dir);

        if (dir[0] == '\0' || strlen(dir) <= backupPathLen) {
            return false;
        }

        rc = snprintf_s(probe, sizeof(probe), sizeof(probe) - 1,
                        "%s/%s", dir, BACKUP_CONTROL_FILE);
        securec_check_ss_c(rc, "", "");

        if (stat(probe, &st) == 0) {
            rc = strncpy_s(root, rootSize, dir, rootSize - 1);
            securec_check_c(rc, "", "");
            return true;
        }
    }

    return false;
}

/*
 * Files that stay in the clear even in an encrypted backup, because the
 * catalog has to be navigable without a key. They carry no user data:
 * backup.control holds status and chain information, backup.keyinfo holds
 * the wrapped key, and the lock files hold pids.
 */
static bool PathIsPlaintextByDesign(const char *path)
{
    static const char *const plainNames[] = {
        BACKUP_CONTROL_FILE, GSPB_KEYINFO_FILE, GSPB_CONTROL_MAC_FILE,
        BACKUP_LOCK_FILE, BACKUP_RO_LOCK_FILE, NULL
    };
    const char *name = last_dir_separator(path);
    name = (name != NULL) ? name + 1 : path;

    for (int i = 0; plainNames[i] != NULL; i++) {
        size_t len = strlen(plainNames[i]);
        /* also covers the "<name>-<pid>.tmp" and "<name>.tmp" variants */
        if (strncmp(name, plainNames[i], len) == 0) {
            return true;
        }
    }

    return false;
}

/*
 * Return the key material protecting the given file, or NULL when the file
 * does not belong to an encrypted backup.
 */
static BackupEncKey *KeyForPath(const char *path)
{
    BackupEncKey *key;
    char          root[MAXPGPATH];
    bool          mustLoad = false;

    if (PathIsPlaintextByDesign(path)) {
        return NULL;
    }

    pthread_mutex_lock(&g_keyCacheMutex);
    key = KeyCacheLookup(path);
    while (key != NULL && key->loading) {
        pthread_cond_wait(&g_keyCacheCond, &g_keyCacheMutex);
    }
    pthread_mutex_unlock(&g_keyCacheMutex);

    if (key != NULL) {
        return key->encrypted ? key : NULL;
    }

    if (!FindBackupRoot(path, root, sizeof(root))) {
        return NULL;
    }

    pthread_mutex_lock(&g_keyCacheMutex);
    key = KeyCacheLookup(path);
    if (key == NULL) {
        key = KeyCacheAdd(root);
        if (EncryptDirIsEncrypted(root)) {
            key->loading = true;
            mustLoad = true;
        }
    } else {
        while (key->loading) {
            pthread_cond_wait(&g_keyCacheCond, &g_keyCacheMutex);
        }
    }
    pthread_mutex_unlock(&g_keyCacheMutex);

    if (mustLoad) {
        LoadKeyinfo(root, key);
        pthread_mutex_lock(&g_keyCacheMutex);
        key->loading = false;
        pthread_cond_broadcast(&g_keyCacheCond);
        pthread_mutex_unlock(&g_keyCacheMutex);
    }

    return key->encrypted ? key : NULL;
}

bool EncryptPathIsEncrypted(const char *path)
{
    return KeyForPath(path) != NULL;
}

static BackupEncKey *KeyForBackupRoot(const char *backupRoot)
{
    char probe[MAXPGPATH];
    errno_t rc = snprintf_s(probe, sizeof(probe), sizeof(probe) - 1,
                            "%s/%s", backupRoot, DATABASE_FILE_LIST);
    securec_check_ss_c(rc, "", "");
    return KeyForPath(probe);
}

static void ControlMacPaths(const char *backupRoot, char *controlPath,
                  char *macPath, char *tmpPath)
{
    errno_t rc = snprintf_s(controlPath, MAXPGPATH, MAXPGPATH - 1,
                            "%s/%s", backupRoot, BACKUP_CONTROL_FILE);
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(macPath, MAXPGPATH, MAXPGPATH - 1,
                    "%s/%s", backupRoot, GSPB_CONTROL_MAC_FILE);
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(tmpPath, MAXPGPATH, MAXPGPATH - 1, "%s.tmp", macPath);
    securec_check_ss_c(rc, "", "");
}

static void ComputeFileHmac(const char *path, const BackupEncKey *key, unsigned char *mac)
{
    struct stat st;
    FILE       *fp;
    unsigned char *buffer;
    size_t      size;

    if (stat(path, &st) != 0 || st.st_size < 0 || st.st_size > GSPB_ENC_METADATA_MAX_SIZE) {
        elog(ERROR, "Invalid backup metadata file \"%s\": %s",
             path, gs_strerror(errno));
    }

    fp = fopen(path, PG_BINARY_R);
    if (fp == NULL) {
        elog(ERROR, "Cannot open \"%s\" for metadata authentication: %s",
             path, gs_strerror(errno));
    }

    size = (size_t) st.st_size;
    buffer = (unsigned char *) pgut_malloc(Max(size, (size_t) 1));
    if (size > 0 && fread(buffer, 1, size, fp) != size) {
        elog(ERROR, "Cannot read backup metadata \"%s\": %s", path, gs_strerror(errno));
    }
    (void) fclose(fp);

    HmacSha256(key->kMac, sizeof(key->kMac), buffer, size, mac);
    EncWipe(buffer, Max(size, (size_t) 1));
    pg_free(buffer);
}

void EncryptPrepareMetadataUpdate(const char *backupRoot)
{
    char key_path[MAXPGPATH];
    char macPath[MAXPGPATH];
    struct stat st;
    errno_t rc = snprintf_s(key_path, sizeof(key_path), sizeof(key_path) - 1,
                            "%s/%s", backupRoot, GSPB_KEYINFO_FILE);
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(macPath, sizeof(macPath), sizeof(macPath) - 1,
                    "%s/%s", backupRoot, GSPB_CONTROL_MAC_FILE);
    securec_check_ss_c(rc, "", "");

    if (stat(key_path, &st) != 0) {
        return;
    }

    (void) KeyForBackupRoot(backupRoot);
    if (stat(macPath, &st) == 0) {
        EncryptVerifyControlMac(backupRoot);
    }
}

void EncryptRefreshControlMac(const char *backupRoot)
{
    BackupEncKey *key;

    pthread_mutex_lock(&g_keyCacheMutex);
    key = KeyCacheLookup(backupRoot);
    pthread_mutex_unlock(&g_keyCacheMutex);
    if (key == NULL || !key->encrypted) {
        elog(ERROR, "Updating metadata of encrypted backup \"%s\" requires its encryption key",
             backupRoot);
    }
    char controlPath[MAXPGPATH];
    char macPath[MAXPGPATH];
    char tmpPath[MAXPGPATH];
    unsigned char mac[GSPB_ENC_MAC_LEN];
    FILE *fp;

    if (key == NULL) {
        return;
    }

    ControlMacPaths(backupRoot, controlPath, macPath, tmpPath);
    ComputeFileHmac(controlPath, key, mac);

    fp = fopen(tmpPath, PG_BINARY_W);
    if (fp == NULL) {
        elog(ERROR, "Cannot create \"%s\": %s", tmpPath, gs_strerror(errno));
    }
    for (size_t i = 0; i < sizeof(mac); i++) {
        (void) fprintf(fp, "%02x", mac[i]);
    }
    (void) fputc('\n', fp);

    if (fflush(fp) != 0 || fsync(fileno(fp)) != 0 || fclose(fp) != 0) {
        elog(ERROR, "Cannot flush metadata authentication file \"%s\": %s",
             tmpPath, gs_strerror(errno));
    }
    if (rename(tmpPath, macPath) != 0) {
        elog(ERROR, "Cannot rename \"%s\" to \"%s\": %s",
             tmpPath, macPath, gs_strerror(errno));
    }
}

void EncryptVerifyControlMac(const char *backupRoot)
{
    BackupEncKey *key = KeyForBackupRoot(backupRoot);
    char controlPath[MAXPGPATH];
    char macPath[MAXPGPATH];
    char tmpPath[MAXPGPATH];
    unsigned char expected[GSPB_ENC_MAC_LEN];
    unsigned char actual[GSPB_ENC_MAC_LEN];
    char hex[GSPB_ENC_MAC_LEN * 2 + 2];
    FILE *fp;

    if (key == NULL) {
        return;
    }

    ControlMacPaths(backupRoot, controlPath, macPath, tmpPath);
    ComputeFileHmac(controlPath, key, expected);

    fp = fopen(macPath, PG_BINARY_R);
    if (fp == NULL || fgets(hex, sizeof(hex), fp) == NULL) {
        elog(ERROR, "Backup metadata authentication file \"%s\" is missing or unreadable",
             macPath);
    }
    (void) fclose(fp);

    for (size_t i = 0; i < sizeof(actual); i++) {
        unsigned int byte;
        if (sscanf_s(hex + i * 2, "%2x", &byte) != 1) {
            elog(ERROR, "Backup metadata authentication file \"%s\" is malformed",
                 macPath);
        }
        actual[i] = (unsigned char) byte;
    }

    if (CRYPTO_memcmp(expected, actual, sizeof(actual)) != 0) {
        elog(ERROR, "Authentication of backup.control in \"%s\" failed: "
             "the metadata is damaged or has been tampered with", backupRoot);
    }
}

/*
 * Create the data key of a new backup and store it, wrapped, in the backup
 * directory. Must be called before any file of that backup is written.
 */
void EncryptSetupBackup(const char *backupRoot)
{
    BackupEncKey *key;
    unsigned char salt[GSPB_ENC_SALT_LEN];
    unsigned char wrapped[GSPB_ENC_NONCE_LEN + GSPB_ENC_DEK_LEN + GSPB_ENC_TAG_LEN];
    unsigned char kek[GSPB_ENC_DEK_LEN];

    if (!g_encryptEnabled) {
        return;
    }

    pthread_mutex_lock(&g_keyCacheMutex);
    key = KeyCacheAdd(backupRoot);
    key->chunkSize = g_configuredChunkSize;
    key->alg = GSPB_ENC_ALG_AES128_GCM;
    key->encrypted = true;
    EncRandomBytes(key->dek, sizeof(key->dek));
    DeriveSubkeys(key);
    pthread_mutex_unlock(&g_keyCacheMutex);

    EncRandomBytes(salt, sizeof(salt));
    EncRandomBytes(wrapped, GSPB_ENC_NONCE_LEN);

    DeriveKek(salt, GSPB_KDF_DEFAULT_ITERATIONS, kek);

    if (!AesGcmCrypt(true, kek, sizeof(kek), wrapped, NULL, 0,
                       key->dek, GSPB_ENC_DEK_LEN,
                       wrapped + GSPB_ENC_NONCE_LEN,
                       wrapped + GSPB_ENC_NONCE_LEN + GSPB_ENC_DEK_LEN)) {
        EncWipe(kek, sizeof(kek));
        elog(ERROR, "Cannot wrap the data key of the new backup");
    }
    EncWipe(kek, sizeof(kek));

    WriteKeyinfo(backupRoot, key, salt, GSPB_KDF_DEFAULT_ITERATIONS,
                  wrapped, sizeof(wrapped),
                  g_encryptKeyFile != NULL ? "keyfile" : "passphrase");
    EncryptRefreshControlMac(backupRoot);

    elog(LOG, "Backup encryption enabled: AES-128-GCM, chunk size %u bytes",
         key->chunkSize);
}

void EncryptRekeyBackup(const char *backupRoot)
{
    BackupEncKey *key = KeyForBackupRoot(backupRoot);
    unsigned char salt[GSPB_ENC_SALT_LEN];
    unsigned char wrapped[GSPB_ENC_NONCE_LEN + GSPB_ENC_DEK_LEN + GSPB_ENC_TAG_LEN];
    unsigned char kek[GSPB_ENC_DEK_LEN];
    char         *newPassphrase;

    if (key == NULL) {
        elog(ERROR, "Backup \"%s\" is not encrypted with the streaming format", backupRoot);
    }

    newPassphrase = GetNewPassphrase();
    EncRandomBytes(salt, sizeof(salt));
    EncRandomBytes(wrapped, GSPB_ENC_NONCE_LEN);
    DeriveKekFromPassphrase(newPassphrase, salt,
                               GSPB_KDF_DEFAULT_ITERATIONS, kek);

    if (!AesGcmCrypt(true, kek, sizeof(kek), wrapped, NULL, 0,
                       key->dek, GSPB_ENC_DEK_LEN,
                       wrapped + GSPB_ENC_NONCE_LEN,
                       wrapped + GSPB_ENC_NONCE_LEN + GSPB_ENC_DEK_LEN)) {
        elog(ERROR, "Cannot wrap the data key with the new encryption key");
    }

    EncWipe(kek, sizeof(kek));
    EncWipe(newPassphrase, strlen(newPassphrase));
    pg_free(newPassphrase);

    WriteKeyinfo(backupRoot, key, salt, GSPB_KDF_DEFAULT_ITERATIONS,
                  wrapped, sizeof(wrapped),
                  g_newEncryptKeyFile != NULL ? "keyfile" : "passphrase");
    EncryptRefreshControlMac(backupRoot);
}

void EncryptForgetBackup(const char *backupRoot)
{
    pthread_mutex_lock(&g_keyCacheMutex);

    for (int i = 0; i < g_keyCacheNum; i++) {
        if (strcmp(g_keyCache[i]->root, backupRoot) != 0) {
            continue;
        }

        EncWipe(g_keyCache[i], sizeof(BackupEncKey));
        pg_free(g_keyCache[i]);
        g_keyCache[i] = g_keyCache[--g_keyCacheNum];
        break;
    }

    pthread_mutex_unlock(&g_keyCacheMutex);
}

void EncryptBackupRenamed(const char *oldRoot, const char *newRoot)
{
    pthread_mutex_lock(&g_keyCacheMutex);

    BackupEncKey *renamedKey = NULL;
    for (int i = 0; i < g_keyCacheNum;) {
        BackupEncKey *key = g_keyCache[i];
        if (strcmp(key->root, oldRoot) == 0) {
            renamedKey = key;
            i++;
            continue;
        }

        if (strcmp(key->root, newRoot) == 0) {
            EncWipe(key, sizeof(BackupEncKey));
            pg_free(key);
            g_keyCache[i] = g_keyCache[--g_keyCacheNum];
            continue;
        }

        i++;
    }

    if (renamedKey != NULL) {
        errno_t rc = strncpy_s(renamedKey->root, sizeof(renamedKey->root),
                               newRoot, sizeof(renamedKey->root) - 1);
        securec_check_c(rc, "", "");
        renamedKey->rootLen = strlen(renamedKey->root);
    }

    pthread_mutex_unlock(&g_keyCacheMutex);
}

