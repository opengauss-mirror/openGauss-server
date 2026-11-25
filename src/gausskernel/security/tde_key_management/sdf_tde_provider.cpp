/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 */
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>

#include "knl/knl_instance.h"
#include "knl/knl_session.h"
#include "miscadmin.h"
#include "utils/atomic.h"
#include "securec.h"
#include "tde_key_management/data_common.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "storage/ipc.h"
#include "cjson/cJSON.h"
#include "tde_key_management/sdf_tde_provider.h"

/* SDF GM/T 0018 types and algorithm identifiers. */
typedef void* SdfHandle;
static_assert(sizeof(unsigned int) == sizeof(uint32), "SDF requires 32-bit unsigned int");

static const int SDF_SUCCESS = 0;
static const unsigned int SDF_SM4_CBC = 0x00000402;
static const unsigned int SDF_SM3_HMAC = 0x00000008;
static const unsigned int SDF_SM4_CTR = 0x00000420;
static const Size SDF_DEK_LEN = 16;
static const Size SDF_IV_LEN = 16;
static const Size SDF_HMAC_LEN = 32;
static const Size SDF_WRAPPED_KEY_MAX = 64;
static const Size SDF_ENVELOPE_HEADER_LEN = 8;
static const uint8 SDF_ENVELOPE_VERSION = 3;
static const Size SDF_PASSWORD_MAX_LEN = 128;
static const Size SDF_HANDLE_BUCKETS = 1024; /* Bucket count, not a limit on cached keys. */
static const Size SDF_HANDLE_CACHE_LIMIT = 10000;
static const unsigned int SDF_HEX_SHIFT = 4;
static const unsigned int SDF_HEX_MASK = 0x0f;
static const Size SDF_HEX_CHARS_PER_BYTE = 2;
static const int SDF_UUID_TEXT_LEN = CMK_ID_TEXT_LEN;
static const char SDF_UUID_TEMPLATE[] = "00000000-0000-0000-0000-000000000000";
static_assert(CMK_ID_TEXT_LEN == sizeof("00000000-0000-0000-0000-000000000000") - 1,
    "CMK_ID text length must describe a UUID, not its buffer capacity");
static const unsigned int SDF_KEK_INDEX_MAX = 1000;
static const unsigned int SDF_HEX_DECIMAL_BASE = 10;
static const mode_t SDF_LIBRARY_WRITE_MASK = 0022;
static const mode_t SDF_PRIVATE_FILE_MASK = 0077;
static const int SDF_SESSION_OPEN_ATTEMPTS = 3;
static const Size SDF_CLOCK_SECOND_CHANCES = 2;
static const Size SDF_PASSWORD_MAGIC_LEN = 8;
static const Size SDF_PASSWORD_NONCE_LEN = 12;
static const Size SDF_PASSWORD_TAG_LEN = 16;
static const Size SDF_PASSWORD_KEY_LEN = 32;
static const int SDF_PASSWORD_MIN_LEN = 8;
static const Size SDF_PASSWORD_CIPHER_OFFSET = SDF_PASSWORD_MAGIC_LEN + SDF_PASSWORD_NONCE_LEN;
static const Size SDF_PASSWORD_OVERHEAD = SDF_PASSWORD_CIPHER_OFFSET + SDF_PASSWORD_TAG_LEN;

struct SdfApi {
    void* library;
    int (*open_device)(SdfHandle*);
    int (*close_device)(SdfHandle);
    int (*open_session)(SdfHandle, SdfHandle*);
    int (*close_session)(SdfHandle);
    int (*get_kek_access_right)(SdfHandle, unsigned int, unsigned char*, unsigned int);
    int (*generate_key_with_kek)(SdfHandle, unsigned int, unsigned int, unsigned int,
        unsigned char*, unsigned int*, SdfHandle*);
    int (*import_key_with_kek)(SdfHandle, unsigned int, unsigned int, unsigned char*, unsigned int, SdfHandle*);
    int (*destroy_key)(SdfHandle, SdfHandle);
    int (*encrypt)(SdfHandle, SdfHandle, unsigned int, unsigned char*, unsigned char*, unsigned int,
        unsigned char*, unsigned int*);
    int (*decrypt)(SdfHandle, SdfHandle, unsigned int, unsigned char*, unsigned char*, unsigned int,
        unsigned char*, unsigned int*);
    int (*hmac_init)(SdfHandle, SdfHandle, unsigned int);
    int (*hmac_update)(SdfHandle, unsigned char*, unsigned int);
    int (*hmac_final)(SdfHandle, unsigned char*, unsigned int*);
};

typedef struct SdfEnvelopeHeader {
    uint8 version;
    uint8 total_len;
    uint8 dek_key_len;
    uint8 macKeyLen;
    uint8 reserved[3];
    uint8 macLen;
} SdfEnvelopeHeader;

static SdfApi g_sdfApi = {};
struct SdfConfig {
    unsigned int generation;
    unsigned int keyIndex;
    unsigned char password[SDF_PASSWORD_MAX_LEN];
    unsigned int passwordLen;

    bool LoadPassword(char* errorMsg, Size errorMsgSize);
    void Clear();
};
/* Read once before sessions start; only password contents can be reloaded. */
struct SdfFileConfig {
    unsigned int keyIndex;
    char libraryPath[PATH_MAX];
    char passwordFile[PATH_MAX];
};
static SdfFileConfig g_sdfFile = {};
struct SdfHandleEntry {
    uint8 cmkId[CMK_ID_LEN_PAGE];
    char envelope[DEK_CIPHER_AES128_LEN_PAGE * SDF_HEX_CHARS_PER_BYTE + 1];
    SdfHandle handle;
    pg_atomic_uint32 referenced;
    pg_atomic_uint32 users;
    SdfHandleEntry* next;
};
struct SdfHandleBucket {
    pthread_rwlock_t lock;
    SdfHandleEntry* head;
};
struct SdfSessionState {
    SdfHandle session;
    unsigned int generation;
    pg_atomic_uint32 reset_required;
};
struct SdfScratch {
    unsigned char* data;
    Size capacity;
};
static pthread_mutex_t g_sdfMutex = PTHREAD_MUTEX_INITIALIZER;
/* Readers pin the session until their entire operation has finished. */
static pthread_rwlock_t g_sdfSessionGate = PTHREAD_RWLOCK_INITIALIZER;
/* Serializes multi-step HMAC/import/retirement, never held by hot page crypto. */
static pthread_mutex_t g_sdfColdMutex = PTHREAD_MUTEX_INITIALIZER;
static SdfSessionState g_sdfShared = {};
/* Retain a failed validation-session close so shutdown can retry it safely. */
static SdfSessionState g_sdfValidation = {};
static SdfHandleBucket g_sdfHandles[SDF_HANDLE_BUCKETS] = {};
static pthread_once_t g_sdfCacheOnce = PTHREAD_ONCE_INIT;
static int g_sdfCacheInitError = 0;
static SdfHandleEntry* g_sdfRetiredHandles = NULL;
static pg_atomic_uint32 g_sdfRetiredPending = 0;
/* Protected by the cold mutex; count includes detached entries awaiting release. */
static Size g_sdfHandleCount = 0;
static Size g_sdfClockBucket = 0;
static SdfConfig* g_sdfConfig = NULL;
static SdfHandle g_sdfDevice = NULL;
static pthread_key_t g_sdfThreadKey;
static pthread_once_t g_sdfThreadKeyOnce = PTHREAD_ONCE_INIT;
static bool g_sdfRunning = false;
static bool g_sdfStopping = false;
static pg_atomic_uint32 g_sdfGeneration;

static void sdf_shutdown(int code, Datum arg);

/* Error reporting must not throw while a caller owns pthread locks. */
static void format_sdf_error(char* errorMsg, Size errorMsgSize, const char* format, ...)
{
    if (errorMsg == NULL || errorMsgSize == 0) {
        return;
    }
    va_list args;
    va_start(args, format);
    int result = vsnprintf_s(errorMsg, errorMsgSize, errorMsgSize - 1, format, args);
    va_end(args);
    if (result < 0) {
        static const char fallback[] = "SDF error details unavailable";
        Size i = 0;
        while (i + 1 < errorMsgSize && fallback[i] != '\0') {
            errorMsg[i] = fallback[i];
            ++i;
        }
        errorMsg[i] = '\0';
    }
}

static bool set_error_text(char* errorMsg, Size errorMsgSize, const char* message)
{
    format_sdf_error(errorMsg, errorMsgSize, "%s", message);
    return false;
}

/* POSIX dlsym supplies function addresses; retain the exact declared function type. */
template <typename Function>
static bool LoadSdfSymbol(void* library, const char* name, Function& function,
    char* errorMsg, Size errorMsgSize)
{
    (void)dlerror();
    void* address = dlsym(library, name);
    const char* loaderError = dlerror();
    if (loaderError != NULL || address == NULL) {
        format_sdf_error(errorMsg, errorMsgSize, "SDF library is missing %s: %s",
            name, loaderError == NULL ? "null function address" : loaderError);
        return false;
    }
    function = reinterpret_cast<Function>(address);
    return true;
}
static bool load_sdf_api(char* errorMsg, Size errorMsgSize)
{
    if (g_sdfApi.library != NULL) {
        return true;
    }
    const char* libraryPath = g_sdfFile.libraryPath;

    char resolvedPath[PATH_MAX] = {0};
    if (realpath(libraryPath, resolvedPath) == NULL) {
        format_sdf_error(errorMsg, errorMsgSize,
            "cannot resolve SDF library path %s: %s", libraryPath, gs_strerror(errno));
        return false;
    }
    struct stat libraryInfo;
    if (stat(resolvedPath, &libraryInfo) != 0) {
        format_sdf_error(errorMsg, errorMsgSize,
            "cannot inspect SDF library %s: %s", resolvedPath, gs_strerror(errno));
        return false;
    }
    if (!S_ISREG(libraryInfo.st_mode) || libraryInfo.st_uid != 0 ||
        (libraryInfo.st_mode & SDF_LIBRARY_WRITE_MASK) != 0) {
        return set_error_text(errorMsg, errorMsgSize,
            "SDF library must be a root-owned regular file not writable by group or others");
    }
    void* library = dlopen(resolvedPath, RTLD_NOW | RTLD_LOCAL);
    if (library == NULL) {
        const char* dlError = dlerror();
        format_sdf_error(errorMsg, errorMsgSize,
            "cannot load SDF library %s: %s", resolvedPath,
            dlError == NULL ? "unknown loader error" : dlError);
        return false;
    }

    SdfApi api = {};
    api.library = library;
    if (!LoadSdfSymbol(library, "SDF_OpenDevice", api.open_device, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_CloseDevice", api.close_device, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_OpenSession", api.open_session, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_CloseSession", api.close_session, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_GetKEKAccessRight", api.get_kek_access_right, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_GenerateKeyWithKEK", api.generate_key_with_kek, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_ImportKeyWithKEK", api.import_key_with_kek, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_DestroyKey", api.destroy_key, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_Encrypt", api.encrypt, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_Decrypt", api.decrypt, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_HMACInit", api.hmac_init, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_HMACUpdate", api.hmac_update, errorMsg, errorMsgSize) ||
        !LoadSdfSymbol(library, "SDF_HMACFinal", api.hmac_final, errorMsg, errorMsgSize)) {
        dlclose(library);
        return false;
    }
    g_sdfApi = api;
    return true;
}

static int HexToNibble(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + SDF_HEX_DECIMAL_BASE;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + SDF_HEX_DECIMAL_BASE;
    }
    return -1;
}

static bool parse_uuid(const char* uuid, uint8 bytes[CMK_ID_LEN_PAGE])
{
    if (uuid == NULL || strlen(uuid) != SDF_UUID_TEXT_LEN) {
        return false;
    }
    for (int i = 0, out = 0; i < SDF_UUID_TEXT_LEN;) {
        if (SDF_UUID_TEMPLATE[i] == '-') {
            if (uuid[i++] != '-') {
                return false;
            }
        } else {
            int high = HexToNibble(uuid[i++]);
            int low = HexToNibble(uuid[i++]);
            if (high < 0 || low < 0) {
                return false;
            }
            bytes[out++] = (uint8)((high << SDF_HEX_SHIFT) | low);
        }
    }
    return true;
}

/* The existing 16-byte CMK field encodes the slot directly; no UUID map is used. */
bool SdfTdeGetCmkId(char* cmkId, Size capacity)
{
    if (g_sdfFile.keyIndex < 1 || g_sdfFile.keyIndex > SDF_KEK_INDEX_MAX ||
        cmkId == NULL || capacity < (Size)SDF_UUID_TEXT_LEN + 1) {
        return false;
    }
    return snprintf_s(cmkId, capacity, capacity - 1, "00000000-0000-0000-0000-%012x",
        g_sdfFile.keyIndex) == SDF_UUID_TEXT_LEN;
}

static unsigned int decode_key_index(const char* cmkId, uint8 bytes[CMK_ID_LEN_PAGE])
{
    if (!parse_uuid(cmkId, bytes)) {
        return 0;
    }
    for (int i = 0; i < CMK_ID_LEN_PAGE - sizeof(unsigned int); ++i) {
        if (bytes[i] != 0) {
            return 0;
        }
    }
    unsigned int index = 0;
    for (Size i = CMK_ID_LEN_PAGE - sizeof(index); i < CMK_ID_LEN_PAGE; ++i) {
        index = (index << CHAR_BIT) | bytes[i];
    }
    return index >= 1 && index <= SDF_KEK_INDEX_MAX ? index : 0;
}

static int OpenSecureSdfFile(const char* path, char* errorMsg, Size errorMsgSize)
{
    if (path == NULL || path[0] == '\0') {
        set_error_text(errorMsg, errorMsgSize, "SDF configuration must specify a file path");
        return -1;
    }
    /* A misconfigured FIFO must not block before the regular-file check. */
#ifdef O_NOFOLLOW
    int fd = open(path, O_RDONLY | O_NONBLOCK | O_NOFOLLOW
#ifdef O_CLOEXEC
        | O_CLOEXEC
#endif
    );
#else
    int fd = open(path, O_RDONLY | O_NONBLOCK);
#endif
    if (fd < 0) {
        format_sdf_error(errorMsg, errorMsgSize,
            "cannot open SDF file %s: %s", path, gs_strerror(errno));
        return -1;
    }

    return fd;
}

static bool read_secure_file(const char* path, char* contents, Size capacity, Size* length,
    char* errorMsg, Size errorMsgSize)
{
    int fd = OpenSecureSdfFile(path, errorMsg, errorMsgSize);
    if (fd < 0) {
        return false;
    }
    struct stat info;
    bool valid = false;
    if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) || info.st_size <= 0 ||
        (Size)info.st_size >= capacity || (info.st_mode & SDF_PRIVATE_FILE_MASK) != 0 || info.st_uid != geteuid()) {
        format_sdf_error(errorMsg, errorMsgSize,
            "%s: SDF file must be nonempty, within the size limit, regular, owned by the database user "
            "and have no group/other permissions (mode 0600 or stricter)", path);
        (void)close(fd);
        return false;
    }

    Size used = 0;
    while (used < capacity) {
        ssize_t nread = read(fd, contents + used, capacity - used);
        if (nread < 0 && errno == EINTR) {
            continue;
        }
        if (nread < 0) {
            format_sdf_error(errorMsg, errorMsgSize,
                "cannot read SDF file %s: %s", path, gs_strerror(errno));
            break;
        }
        if (nread == 0) {
            valid = true;
            break;
        }
        used += (Size)nread;
    }
    (void)close(fd);
    if (!valid) {
        if (used >= capacity) {
            format_sdf_error(errorMsg, errorMsgSize,
                "%s: SDF file exceeds the %lu-byte size limit", path, (unsigned long)(capacity - 1));
        }
        return false;
    }
    *length = used;
    return true;
}

static bool ValidateSdfJsonFields(cJSON* root, SdfFileConfig& config,
    const char*& reason, const char*& fieldName)
{
    const char* names[] = {"kek_index", "library_path", "password_file"};
    cJSON* fields[lengthof(names)] = {NULL};
    bool valid = cJSON_IsObject(root);
    for (cJSON* item = valid ? root->child : NULL; item != NULL; item = item->next) {
        int i = 0;
        while (i < (int)lengthof(names) && strcmp(item->string, names[i]) != 0) {
            i++;
        }
        fieldName = item->string;
        if (i == (int)lengthof(names) || fields[i] != NULL) {
            reason = i == (int)lengthof(names) ? "unknown field" : "duplicate field";
            valid = false;
            break;
        }
        fields[i] = item;
    }
    for (int i = 0; valid && i < (int)lengthof(names); i++) {
        fieldName = names[i];
        cJSON* item = fields[i];
        if (item == NULL) {
            reason = "missing required field";
            valid = false;
        } else if (i == 0) {
            valid = cJSON_IsNumber(item) && item->valuedouble >= 1 && item->valuedouble <= SDF_KEK_INDEX_MAX &&
                item->valuedouble == (double)item->valueint;
            reason = "expected an integer from 1 to 1000";
            if (valid) {
                config.keyIndex = (unsigned int)item->valueint;
            }
        } else {
            valid = cJSON_IsString(item) && item->valuestring != NULL && item->valuestring[0] == '/' &&
                strlen(item->valuestring) < PATH_MAX;
            reason = "expected an absolute path shorter than PATH_MAX";
            if (!valid) {
                break;
            }
            char* destination = i == 1 ? config.libraryPath : config.passwordFile;
            errno_t result = strcpy_s(destination, PATH_MAX, item->valuestring);
            if (result != EOK) {
                securec_check(result, "", "");
            }
        }
    }
    return valid;
}

static bool ParseSdfFileConfig(char* text, Size length, const char* path,
    char* errorMsg, Size errorMsgSize)
{
    bool validText = memchr(text, '\0', length) == NULL;
    /* cJSON strings cannot preserve embedded NULs, including those in field names. */
    for (Size i = 0; validText && i < length; i++) {
        if (text[i] == '\\') {
            validText = strncmp(text + i + 1, "u0000", sizeof("u0000") - 1) != 0;
            i++;
        }
    }
    cJSON* root = validText ? cJSON_ParseWithOpts(text, NULL, true) : NULL;
    const char* reason = "expected a JSON object without NUL or trailing data";
    const char* fieldName = "";
    SdfFileConfig config = {};
    bool valid = ValidateSdfJsonFields(root, config, reason, fieldName);
    if (valid) {
        g_sdfFile = config;
    } else {
        format_sdf_error(errorMsg, errorMsgSize, "%s: %s \"%s\".",
            path, reason, fieldName);
    }
    cJSON_Delete(root);
    return valid;
}

static bool load_sdf_file_config(char* errorMsg, Size errorMsgSize)
{
    const char* dataDirectory = g_instance.attr.attr_common.data_directory;
    if (dataDirectory == NULL || dataDirectory[0] == '\0') {
        return set_error_text(errorMsg, errorMsgSize, "data_directory must be set before loading SDF configuration");
    }
    char path[PATH_MAX];
    int rc = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/tde_config/sdf_info.json",
        dataDirectory);
    if (rc < 0) {
        return set_error_text(errorMsg, errorMsgSize, "SDF configuration path is too long");
    }
    char text[4097];
    Size length = 0;
    if (!read_secure_file(path, text, sizeof(text), &length, errorMsg, errorMsgSize)) {
        return false;
    }
    text[length] = '\0';
    return ParseSdfFileConfig(text, length, path, errorMsg, errorMsgSize);
}

static bool DecryptSdfPassword(const unsigned char* key, unsigned char* contents, Size length,
    unsigned char* plain, int& plainLen, char* errorMsg, Size errorMsgSize)
{
    EVP_CIPHER_CTX* ctx = NULL;
    int ignoredLen = 0;
    int tail = 0;
    bool ok = true;
    ctx = EVP_CIPHER_CTX_new();
    ok = ctx != NULL &&
        EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, key, contents + SDF_PASSWORD_MAGIC_LEN) == 1 &&
        EVP_DecryptUpdate(ctx, NULL, &ignoredLen, contents, SDF_PASSWORD_MAGIC_LEN) == 1 &&
        EVP_DecryptUpdate(ctx, plain, &plainLen, contents + SDF_PASSWORD_CIPHER_OFFSET,
            (int)(length - SDF_PASSWORD_OVERHEAD)) == 1 &&
        EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, SDF_PASSWORD_TAG_LEN,
            contents + length - SDF_PASSWORD_TAG_LEN) == 1 &&
        EVP_DecryptFinal_ex(ctx, plain + plainLen, &tail) == 1;
    if (!ok) {
        (void)set_error_text(errorMsg, errorMsgSize,
            "cannot authenticate/decrypt SDF password; wrong companion key or damaged credential file");
    }
    plainLen += tail;
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

/* Validate before publishing bytes into the caller-owned output. */
static bool CopySdfPassword(const unsigned char* plain, int plainLen,
    unsigned char* destination, unsigned int* destinationLen, char* errorMsg, Size errorMsgSize)
{
    if (plainLen < SDF_PASSWORD_MIN_LEN || plainLen > (int)SDF_PASSWORD_MAX_LEN ||
        memchr(plain, '\n', plainLen) != NULL || memchr(plain, '\r', plainLen) != NULL ||
        memchr(plain, '\0', plainLen) != NULL) {
        return set_error_text(errorMsg, errorMsgSize, "SDF password must contain 8 to 128 bytes without newline");
    }
    errno_t rc = memcpy_s(destination, SDF_PASSWORD_MAX_LEN, plain, plainLen);
    if (rc != EOK) {
        return set_error_text(errorMsg, errorMsgSize, "cannot copy SDF password");
    }
    *destinationLen = (unsigned int)plainLen;
    return true;
}

bool SdfConfig::LoadPassword(char* errorMsg, Size errorMsgSize)
{
    /* Retain credentials for session authentication; leave them empty on failure. */
    (void)memset_s(password, sizeof(password), 0, sizeof(password));
    passwordLen = 0;
    /* Tool format: 8-byte magic (also AAD), 12-byte nonce, ciphertext, 16-byte tag. */
    unsigned char contents[SDF_PASSWORD_OVERHEAD + SDF_PASSWORD_MAX_LEN + 1] = {};
    unsigned char key[SDF_PASSWORD_KEY_LEN + 1] = {};
    unsigned char plain[SDF_PASSWORD_MAX_LEN + EVP_MAX_BLOCK_LENGTH] = {};
    Size length = 0;
    Size keyLength = 0;
    const char* path = g_sdfFile.passwordFile;
    char keyPath[PATH_MAX];
    int pathLen = snprintf_s(keyPath, sizeof(keyPath), sizeof(keyPath) - 1, "%s.key", path);
    if (pathLen < 0 || (Size)pathLen >= sizeof(keyPath)) {
        return set_error_text(errorMsg, errorMsgSize, "SDF password companion key path is too long");
    }
    bool ok = read_secure_file(path, (char*)contents, sizeof(contents), &length, errorMsg, errorMsgSize);
    if (ok && (length < SDF_PASSWORD_OVERHEAD + SDF_PASSWORD_MIN_LEN ||
        memcmp(contents, "OGSDFP01", SDF_PASSWORD_MAGIC_LEN) != 0)) {
        ok = set_error_text(errorMsg, errorMsgSize,
            "invalid encrypted SDF password file; regenerate credentials with gs_tde_sdf (no plaintext fallback)");
    }
    if (ok) {
        ok = read_secure_file(keyPath, (char*)key, sizeof(key), &keyLength, errorMsg, errorMsgSize);
    }
    if (ok && keyLength != SDF_PASSWORD_KEY_LEN) {
        ok = set_error_text(errorMsg, errorMsgSize, "SDF password companion key must contain exactly 32 bytes");
    }
    int plainLen = 0;
    if (ok) {
        ok = DecryptSdfPassword(key, contents, length, plain, plainLen, errorMsg, errorMsgSize);
    }
    if (ok) {
        ok = CopySdfPassword(plain, plainLen, password, &passwordLen, errorMsg, errorMsgSize);
    }
    OPENSSL_cleanse(key, sizeof(key));
    OPENSSL_cleanse(plain, sizeof(plain));
    if (!ok) {
        (void)memset_s(password, sizeof(password), 0, sizeof(password));
        passwordLen = 0;
    }
    return ok;
}

static void bytes_to_hex(const unsigned char* src, Size srcLen, char* dst)
{
    static const char hex[] = "0123456789abcdef";
    for (Size i = 0; i < srcLen; i++) {
        dst[i * SDF_HEX_CHARS_PER_BYTE] = hex[src[i] >> SDF_HEX_SHIFT];
        dst[i * SDF_HEX_CHARS_PER_BYTE + 1] = hex[src[i] & SDF_HEX_MASK];
    }
    dst[srcLen * SDF_HEX_CHARS_PER_BYTE] = '\0';
}

static bool decode_envelope(const char* cipherHex, unsigned char envelope[DEK_CIPHER_AES128_LEN_PAGE],
    SdfEnvelopeHeader* header, char* errorMsg, Size errorMsgSize)
{
    if (cipherHex == NULL || strlen(cipherHex) != DEK_CIPHER_AES128_LEN_PAGE * SDF_HEX_CHARS_PER_BYTE) {
        return set_error_text(errorMsg, errorMsgSize, "invalid SDF DEK envelope length");
    }
    for (Size i = 0; i < DEK_CIPHER_AES128_LEN_PAGE; i++) {
        int high = HexToNibble(cipherHex[i * SDF_HEX_CHARS_PER_BYTE]);
        int low = HexToNibble(cipherHex[i * SDF_HEX_CHARS_PER_BYTE + 1]);
        if (high < 0 || low < 0) {
            return set_error_text(errorMsg, errorMsgSize, "SDF DEK envelope is not hexadecimal");
        }
        envelope[i] = (unsigned char)((high << SDF_HEX_SHIFT) | low);
    }
    errno_t rc = memcpy_s(header, sizeof(*header), envelope, sizeof(*header));
    if (rc != EOK) {
        return set_error_text(errorMsg, errorMsgSize, "cannot copy SDF envelope header");
    }
    Size actualLen = SDF_ENVELOPE_HEADER_LEN + header->dek_key_len + header->macKeyLen + header->macLen;
    if (header->version != SDF_ENVELOPE_VERSION || header->total_len != actualLen ||
        actualLen > DEK_CIPHER_AES128_LEN_PAGE || header->dek_key_len == 0 || header->macKeyLen == 0 ||
        header->macLen != SDF_HMAC_LEN || header->reserved[0] != 0 || header->reserved[1] != 0 ||
        header->reserved[lengthof(header->reserved) - 1] != 0) {
        return set_error_text(errorMsg, errorMsgSize, "malformed or unsupported SDF DEK envelope");
    }
    for (Size i = actualLen; i < DEK_CIPHER_AES128_LEN_PAGE; i++) {
        if (envelope[i] != 0) {
            return set_error_text(errorMsg, errorMsgSize, "nonzero bytes after SDF DEK envelope");
        }
    }
    return true;
}

static bool constant_time_equal(const unsigned char* left, const unsigned char* right, Size length)
{
    unsigned char diff = 0;
    for (Size i = 0; i < length; i++) {
        diff |= left[i] ^ right[i];
    }
    return diff == 0;
}

static const unsigned char SDF_MAC_DOMAIN[16] = {
    'O', 'G', '-', 'S', 'D', 'F', '-', 'T', 'D', 'E', '-', 'V', '3', 0, 0, 0};
static_assert(sizeof(SdfEnvelopeHeader) == SDF_ENVELOPE_HEADER_LEN, "SDF envelope header must remain 8 bytes");

template <typename Destination, typename Source>
static bool CopyBytes(Destination* dst, Size capacity, const Source* src, Size length,
    char* errorMsg, Size errorMsgSize)
{
    errno_t rc = memcpy_s(dst, capacity, src, length);
    if (rc != EOK) {
        return set_error_text(errorMsg, errorMsgSize, "cannot copy SDF internal buffer");
    }
    return true;
}

static bool sdf_call(SdfSessionState* state, int ret, const char* operation,
    char* errorMsg, Size errorMsgSize)
{
    if (ret == SDF_SUCCESS) {
        return true;
    }
    if (state != NULL) {
        pg_atomic_write_u32(&state->reset_required, 1);
    }
    if (errorMsg != NULL && errorMsgSize > 0) {
        format_sdf_error(errorMsg, errorMsgSize,
            "%s failed with SDF error 0x%08x", operation, (unsigned int)ret);
    }
    return false;
}

static bool close_session(SdfSessionState* state, char* errorMsg, Size errorMsgSize)
{
    if (state->session != NULL) {
        int ret = g_sdfApi.close_session(state->session);
        if (!sdf_call(state, ret, "SDF_CloseSession", errorMsg, errorMsgSize)) {
            return false; /* Keep ownership/device reference so a later reset may retry. */
        }
        state->session = NULL;
    }
    state->generation = 0;
    pg_atomic_write_u32(&state->reset_required, 0);
    return true;
}

static void SdfScratchDestructor(void* value)
{
    SdfScratch* scratch = (SdfScratch*)value;
    if (scratch != NULL) {
        if (scratch->data != NULL) {
            OPENSSL_cleanse(scratch->data, scratch->capacity);
            free(scratch->data);
        }
        free(scratch);
    }
}

static int g_sdfTlsKeyError = 0;
static void CreateSdfTlsKey()
{
    g_sdfTlsKeyError = pthread_key_create(&g_sdfThreadKey, SdfScratchDestructor);
}

static SdfScratch* get_sdf_scratch(char* errorMsg, Size errorMsgSize)
{
    int error = pthread_once(&g_sdfThreadKeyOnce, CreateSdfTlsKey);
    if (error != 0 || g_sdfTlsKeyError != 0) {
        set_error_text(errorMsg, errorMsgSize, "cannot initialize SDF thread-local scratch");
        return NULL;
    }
    SdfScratch* scratch = (SdfScratch*)pthread_getspecific(g_sdfThreadKey);
    if (scratch == NULL) {
        scratch = (SdfScratch*)calloc(1, sizeof(*scratch));
        if (scratch == NULL) {
            set_error_text(errorMsg, errorMsgSize, "out of memory allocating SDF scratch");
            return NULL;
        }
        error = pthread_setspecific(g_sdfThreadKey, scratch);
        if (error != 0) {
            free(scratch);
            set_error_text(errorMsg, errorMsgSize, "cannot register SDF thread-local scratch");
            return NULL;
        }
    }
    return scratch;
}

static bool copy_sdf_config(SdfConfig* config, char* errorMsg, Size errorMsgSize)
{
    pthread_mutex_lock(&g_sdfMutex);
    if (!g_sdfRunning || g_sdfStopping || g_sdfConfig == NULL) {
        pthread_mutex_unlock(&g_sdfMutex);
        return set_error_text(errorMsg, errorMsgSize, "SDF TDE provider is not running");
    }
    *config = *g_sdfConfig;
    pthread_mutex_unlock(&g_sdfMutex);
    return true;
}

static bool open_session(SdfSessionState* state, const SdfConfig* config,
    char* errorMsg, Size errorMsgSize)
{
    pthread_mutex_lock(&g_sdfMutex);
    if (!g_sdfRunning || g_sdfStopping) {
        pthread_mutex_unlock(&g_sdfMutex);
        return set_error_text(errorMsg, errorMsgSize, "SDF TDE provider is shutting down");
    }
    pthread_mutex_unlock(&g_sdfMutex);

    SdfHandle session = NULL;
    int ret = g_sdfApi.open_session(g_sdfDevice, &session);
    state->session = session;
    bool ok = ret == SDF_SUCCESS && session != NULL;
    if (!ok) {
        if (ret != SDF_SUCCESS) {
            format_sdf_error(errorMsg, errorMsgSize,
                "SDF_OpenSession failed with SDF error 0x%08x", (unsigned int)ret);
        } else {
            set_error_text(errorMsg, errorMsgSize, "SDF_OpenSession returned a null session");
        }
    }
    if (ok) {
        ret = g_sdfApi.get_kek_access_right(session, config->keyIndex,
            (unsigned char*)config->password, config->passwordLen);
        ok = ret == SDF_SUCCESS;
        if (!ok) {
            format_sdf_error(errorMsg, errorMsgSize,
                "SDF_GetKEKAccessRight failed with SDF error 0x%08x", (unsigned int)ret);
        }
    }
    if (!ok) {
        char closeError[256] = {0};
        if (!close_session(state, closeError, sizeof(closeError))) {
            (void)fprintf(stderr, "WARNING: %s; SDF session retained for cleanup retry\n", closeError);
        }
        return false;
    }
    state->generation = config->generation;
    return true;
}

static void InitHandleBuckets()
{
    for (Size i = 0; i < SDF_HANDLE_BUCKETS; i++) {
        g_sdfCacheInitError = pthread_rwlock_init(&g_sdfHandles[i].lock, NULL);
        if (g_sdfCacheInitError != 0) {
            while (i > 0) {
                (void)pthread_rwlock_destroy(&g_sdfHandles[--i].lock);
            }
            return; /* No cache operation is allowed after an initialization failure. */
        }
    }
}

static void ClearSharedHandles();

/* Success leaves the read lock held. Never ereport/palloc while holding it. */
static SdfSessionState* begin_shared_session(char* errorMsg, Size errorMsgSize)
{
    int result = pthread_once(&g_sdfCacheOnce, InitHandleBuckets);
    if (result != 0 || g_sdfCacheInitError != 0) {
        set_error_text(errorMsg, errorMsgSize, "cannot initialize SDF handle-cache buckets");
        return NULL;
    }
    for (int attempt = 0; attempt < SDF_SESSION_OPEN_ATTEMPTS; attempt++) {
        pthread_rwlock_rdlock(&g_sdfSessionGate);
        uint32 generation = pg_atomic_read_u32(&g_sdfGeneration);
        if (generation != 0 && g_sdfShared.session != NULL &&
            !pg_atomic_read_u32(&g_sdfShared.reset_required) && g_sdfShared.generation == generation) {
            return &g_sdfShared;
        }
        pthread_rwlock_unlock(&g_sdfSessionGate);
        pthread_rwlock_wrlock(&g_sdfSessionGate);
        SdfConfig config = {};
        bool ok = copy_sdf_config(&config, errorMsg, errorMsgSize);
        if (ok && (g_sdfShared.session == NULL ||
            pg_atomic_read_u32(&g_sdfShared.reset_required) ||
            g_sdfShared.generation != config.generation)) {
            ClearSharedHandles(); /* All old users have drained behind the write lock. */
            ok = close_session(&g_sdfShared, errorMsg, errorMsgSize) &&
                open_session(&g_sdfShared, &config, errorMsg, errorMsgSize);
        }
        config.Clear();
        pthread_rwlock_unlock(&g_sdfSessionGate);
        if (!ok) {
            return NULL;
        }
    }
    set_error_text(errorMsg, errorMsgSize, "SDF credentials changed repeatedly while opening the shared session");
    return NULL;
}

static void EndSharedSession()
{
    pthread_rwlock_unlock(&g_sdfSessionGate);
}

static bool valid_cmk(const char* cmkId, uint8 id[CMK_ID_LEN_PAGE],
    char* errorMsg, Size errorMsgSize)
{
    unsigned int index = decode_key_index(cmkId, id);
    if (index == 0 || index != g_sdfFile.keyIndex) {
        return set_error_text(errorMsg, errorMsgSize,
            "DEK references an invalid SDF CMK_ID or a KEK slot different from the configured slot");
    }
    return true;
}

static Size handle_bucket(const uint8 cmkId[CMK_ID_LEN_PAGE], const char* cipher)
{
    uint32 hash = 2166136261U;
    for (Size i = 0; i < CMK_ID_LEN_PAGE; i++) {
        hash = (hash ^ cmkId[i]) * 16777619U;
    }
    for (Size i = 0; cipher[i] != '\0'; i++) {
        hash = (hash ^ (uint8)cipher[i]) * 16777619U;
    }
    return hash % SDF_HANDLE_BUCKETS;
}

/* Cold mutex plus bucket/session write lock held. Detached entries keep their users. */
static void RetireHandle(SdfHandleEntry** link)
{
    SdfHandleEntry* entry = *link;
    *link = entry->next;
    entry->next = g_sdfRetiredHandles;
    g_sdfRetiredHandles = entry;
    pg_atomic_write_u32(&g_sdfRetiredPending, 1);
}

/* Cold mutex held. No bucket lock is held during vendor calls. */
static void CollectRetiredHandles()
{
    SdfHandleEntry** link = &g_sdfRetiredHandles;
    while (*link != NULL) {
        SdfHandleEntry* entry = *link;
        if (pg_atomic_read_u32(&entry->users) != 0) {
            link = &entry->next;
            continue;
        }
        *link = entry->next;
        if (g_sdfApi.destroy_key(g_sdfShared.session, entry->handle) != SDF_SUCCESS) {
            pg_atomic_write_u32(&g_sdfShared.reset_required, 1);
        }
        free(entry);
        --g_sdfHandleCount;
    }
    pg_atomic_write_u32(&g_sdfRetiredPending, g_sdfRetiredHandles != NULL);
}

/* Session write lock held: no cache users or cold calls remain, even during startup failure. */
static void ClearSharedHandles()
{
    for (Size i = 0; i < SDF_HANDLE_BUCKETS; i++) {
        while (g_sdfHandles[i].head != NULL) {
            RetireHandle(&g_sdfHandles[i].head);
        }
    }
    CollectRetiredHandles();
    g_sdfClockBucket = 0;
}

/* Cold mutex held. CLOCK gives recently used keys a second chance without a global hit lock. */
static bool reserve_handle_slot(char* errorMsg, Size errorMsgSize)
{
    CollectRetiredHandles();
    if (pg_atomic_read_u32(&g_sdfShared.reset_required)) {
        return set_error_text(errorMsg, errorMsgSize, "SDF shared session requires recovery");
    }
    if (g_sdfHandleCount < SDF_HANDLE_CACHE_LIMIT) {
        return true;
    }
    /* Give second chances first; a final pass ignores renewed reference bits to ensure progress. */
    for (Size i = 0; i < SDF_HANDLE_BUCKETS * (SDF_CLOCK_SECOND_CHANCES + 1); ++i) {
        SdfHandleBucket* bucket = &g_sdfHandles[g_sdfClockBucket];
        g_sdfClockBucket = (g_sdfClockBucket + 1) % SDF_HANDLE_BUCKETS;
        pthread_rwlock_wrlock(&bucket->lock);
        SdfHandleEntry** link = &bucket->head;
        bool evicted = false;
        while (*link != NULL) {
            SdfHandleEntry* entry = *link;
            if (pg_atomic_read_u32(&entry->users) == 0 &&
                (i >= SDF_HANDLE_BUCKETS * SDF_CLOCK_SECOND_CHANCES ||
                pg_atomic_exchange_u32(&entry->referenced, 0) == 0)) {
                RetireHandle(link);
                evicted = true;
                break;
            }
            link = &entry->next;
        }
        pthread_rwlock_unlock(&bucket->lock);
        if (evicted) {
            CollectRetiredHandles(); /* Vendor calls must run outside the bucket lock. */
            if (pg_atomic_read_u32(&g_sdfShared.reset_required)) {
                return set_error_text(errorMsg, errorMsgSize,
                    "SDF handle destruction failed; shared session requires recovery");
            }
            return true;
        }
    }
    return set_error_text(errorMsg, errorMsgSize,
        "SDF DEK handle cache is full (10000 entries); no idle eviction candidate found, retry later");
}

static bool cache_handle(const uint8 cmkId[CMK_ID_LEN_PAGE], const char* cipher,
    SdfHandle handle, char* errorMsg, Size errorMsgSize)
{
    SdfHandleEntry* entry = (SdfHandleEntry*)calloc(1, sizeof(*entry));
    if (entry == NULL) {
        return set_error_text(errorMsg, errorMsgSize, "out of memory caching SDF key handle");
    }
    errno_t rc = strcpy_s(entry->envelope, sizeof(entry->envelope), cipher);
    if (rc != EOK || !CopyBytes(entry->cmkId, sizeof(entry->cmkId), cmkId, CMK_ID_LEN_PAGE,
        errorMsg, errorMsgSize)) {
        free(entry);
        return set_error_text(errorMsg, errorMsgSize, "cannot copy SDF handle cache identity");
    }
    entry->handle = handle;
    pg_atomic_init_u32(&entry->referenced, 1);
    pg_atomic_init_u32(&entry->users, 0);
    SdfHandleBucket* bucket = &g_sdfHandles[handle_bucket(cmkId, cipher)];
    pthread_rwlock_wrlock(&bucket->lock);
    entry->next = bucket->head;
    bucket->head = entry;
    ++g_sdfHandleCount;
    pthread_rwlock_unlock(&bucket->lock);
    return true;
}

static SdfHandleEntry* find_cached_handle(const uint8 cmkId[CMK_ID_LEN_PAGE], const char* cipher)
{
    SdfHandleBucket* bucket = &g_sdfHandles[handle_bucket(cmkId, cipher)];
    pthread_rwlock_rdlock(&bucket->lock);
    for (SdfHandleEntry* entry = bucket->head; entry != NULL; entry = entry->next) {
        if (memcmp(entry->cmkId, cmkId, CMK_ID_LEN_PAGE) == 0 && strcmp(entry->envelope, cipher) == 0) {
            /* Pin before dropping the bucket lock: unlink/reclamation cannot race this lookup. */
            (void)pg_atomic_fetch_add_u32(&entry->users, 1);
            if (pg_atomic_read_u32(&entry->referenced) == 0) {
                pg_atomic_write_u32(&entry->referenced, 1);
            }
            pthread_rwlock_unlock(&bucket->lock);
            return entry;
        }
    }
    pthread_rwlock_unlock(&bucket->lock);
    return NULL;
}

static void ReleaseHandle(SdfHandleEntry* entry)
{
    /* After this decrement a collector may free entry; never dereference it again. */
    uint32 previous = pg_atomic_fetch_sub_u32(&entry->users, 1);
    if (previous == 1 && pg_atomic_read_u32(&g_sdfRetiredPending) != 0 &&
        pthread_mutex_trylock(&g_sdfColdMutex) == 0) {
        CollectRetiredHandles();
        pthread_mutex_unlock(&g_sdfColdMutex);
    }
}

static bool compute_envelope_mac(SdfSessionState* state, SdfHandle macHandle,
    const uint8 cmkId[CMK_ID_LEN_PAGE], const unsigned char* envelope, const SdfEnvelopeHeader* header,
    unsigned char mac[SDF_HMAC_LEN], char* errorMsg, Size errorMsgSize)
{
    unsigned char input[sizeof(SDF_MAC_DOMAIN) + CMK_ID_LEN_PAGE + DEK_CIPHER_AES128_LEN_PAGE] = {};
    Size prefixLen = header->total_len - header->macLen;
    if (!CopyBytes(input, sizeof(input), SDF_MAC_DOMAIN, sizeof(SDF_MAC_DOMAIN), errorMsg, errorMsgSize)) {
        OPENSSL_cleanse(input, sizeof(input));
        return false;
    }
    if (!CopyBytes(input + sizeof(SDF_MAC_DOMAIN), sizeof(input) - sizeof(SDF_MAC_DOMAIN),
        cmkId, CMK_ID_LEN_PAGE, errorMsg, errorMsgSize)) {
        OPENSSL_cleanse(input, sizeof(input));
        return false;
    }
    if (!CopyBytes(input + sizeof(SDF_MAC_DOMAIN) + CMK_ID_LEN_PAGE,
        sizeof(input) - sizeof(SDF_MAC_DOMAIN) - CMK_ID_LEN_PAGE, envelope, prefixLen, errorMsg, errorMsgSize)) {
        OPENSSL_cleanse(input, sizeof(input));
        return false;
    }
    unsigned int inputLen = (unsigned int)(sizeof(SDF_MAC_DOMAIN) + CMK_ID_LEN_PAGE + prefixLen);
    unsigned int macLen = SDF_HMAC_LEN;
    if (!sdf_call(state, g_sdfApi.hmac_init(state->session, macHandle, SDF_SM3_HMAC),
        "SDF_HMACInit", errorMsg, errorMsgSize) ||
        !sdf_call(state, g_sdfApi.hmac_update(state->session, input, inputLen),
            "SDF_HMACUpdate", errorMsg, errorMsgSize) ||
        !sdf_call(state, g_sdfApi.hmac_final(state->session, mac, &macLen),
            "SDF_HMACFinal", errorMsg, errorMsgSize)) {
        OPENSSL_cleanse(input, sizeof(input));
        return false;
    }
    OPENSSL_cleanse(input, sizeof(input));
    if (macLen != header->macLen) {
        return set_error_text(errorMsg, errorMsgSize, "SDF returned an invalid envelope MAC length");
    }
    return true;
}

static bool generate_wrapped_key(SdfSessionState* state, unsigned int keyIndex, unsigned char* wrapped,
    unsigned int* wrappedLen, SdfHandle* handle, const char* name, char* errorMsg, Size errorMsgSize)
{
    *wrappedLen = SDF_WRAPPED_KEY_MAX;
    *handle = NULL;
    if (!sdf_call(state, g_sdfApi.generate_key_with_kek(state->session, SDF_DEK_LEN * CHAR_BIT,
        SDF_SM4_CBC, keyIndex, wrapped, wrappedLen, handle), name, errorMsg, errorMsgSize)) {
        return false;
    }
    if (*handle == NULL || *wrappedLen == 0 || *wrappedLen > SDF_WRAPPED_KEY_MAX) {
        pg_atomic_write_u32(&state->reset_required, 1);
        return set_error_text(errorMsg, errorMsgSize, "SDF returned an invalid wrapped key or null handle");
    }
    return true;
}

static bool import_wrapped_key(SdfSessionState* state, unsigned int keyIndex, const unsigned char* wrapped,
    unsigned int wrappedLen, SdfHandle* handle, char* errorMsg, Size errorMsgSize)
{
    *handle = NULL;
    if (!sdf_call(state, g_sdfApi.import_key_with_kek(state->session, SDF_SM4_CBC, keyIndex,
        (unsigned char*)wrapped, wrappedLen, handle), "SDF_ImportKeyWithKEK", errorMsg, errorMsgSize)) {
        return false;
    }
    if (*handle == NULL) {
        pg_atomic_write_u32(&state->reset_required, 1);
        return set_error_text(errorMsg, errorMsgSize, "SDF_ImportKeyWithKEK returned a null handle");
    }
    return true;
}

/* Caller retains all handles and performs cleanup on both success and failure. */
static bool BuildEnvelope(SdfSessionState* state, const uint8* cmkId,
    const unsigned char* wrappedDek, unsigned int dekLen, const unsigned char* wrappedMac,
    unsigned int macKeyLen, SdfHandle macHandle, unsigned char* envelope, unsigned char* mac,
    char* errorMsg, Size errorMsgSize)
{
    SdfEnvelopeHeader header = {};
    header.total_len = SDF_ENVELOPE_HEADER_LEN + dekLen + macKeyLen + SDF_HMAC_LEN;
    header.version = SDF_ENVELOPE_VERSION;
    header.dek_key_len = (uint8)dekLen;
    header.macKeyLen = (uint8)macKeyLen;
    header.macLen = SDF_HMAC_LEN;
    Size offset = sizeof(header);
    return CopyBytes(envelope, DEK_CIPHER_AES128_LEN_PAGE, &header, sizeof(header), errorMsg, errorMsgSize) &&
        CopyBytes(envelope + offset, DEK_CIPHER_AES128_LEN_PAGE - offset,
            wrappedDek, dekLen, errorMsg, errorMsgSize) &&
        CopyBytes(envelope + offset + dekLen, DEK_CIPHER_AES128_LEN_PAGE - offset - dekLen,
            wrappedMac, macKeyLen, errorMsg, errorMsgSize) &&
        compute_envelope_mac(state, macHandle, cmkId, envelope, &header, mac, errorMsg, errorMsgSize) &&
        CopyBytes(envelope + header.total_len - SDF_HMAC_LEN,
            DEK_CIPHER_AES128_LEN_PAGE - header.total_len + SDF_HMAC_LEN,
            mac, SDF_HMAC_LEN, errorMsg, errorMsgSize);
}
static bool generate_envelope(SdfSessionState* state, const uint8 cmkId[CMK_ID_LEN_PAGE],
    char cipher[DEK_CIPHER_AES128_LEN_PAGE * SDF_HEX_CHARS_PER_BYTE + 1], SdfHandle* dekHandle,
    char* errorMsg, Size errorMsgSize)
{
    unsigned char envelope[DEK_CIPHER_AES128_LEN_PAGE] = {};
    unsigned char wrappedDek[SDF_WRAPPED_KEY_MAX] = {};
    unsigned char wrappedMac[SDF_WRAPPED_KEY_MAX] = {};
    unsigned char mac[SDF_HMAC_LEN] = {};
    unsigned int dekLen = 0;
    unsigned int macKeyLen = 0;
    SdfHandle macHandle = NULL;
    *dekHandle = NULL;
    bool ok = generate_wrapped_key(state, g_sdfFile.keyIndex, wrappedDek, &dekLen,
        dekHandle, "SDF_GenerateKeyWithKEK(DEK)", errorMsg, errorMsgSize) &&
        generate_wrapped_key(state, g_sdfFile.keyIndex, wrappedMac, &macKeyLen,
            &macHandle, "SDF_GenerateKeyWithKEK(envelope MAC key)", errorMsg, errorMsgSize);
    Size total = SDF_ENVELOPE_HEADER_LEN + dekLen + macKeyLen + SDF_HMAC_LEN;
    if (ok && total > sizeof(envelope)) {
        ok = set_error_text(errorMsg, errorMsgSize,
            "SDF wrapped DEK envelope exceeds the existing 140-byte page format");
    }
    if (ok) {
        ok = BuildEnvelope(state, cmkId, wrappedDek, dekLen, wrappedMac, macKeyLen,
            macHandle, envelope, mac, errorMsg, errorMsgSize);
        if (ok) {
            bytes_to_hex(envelope, sizeof(envelope), cipher);
        }
    }
    if (macHandle != NULL && g_sdfApi.destroy_key(state->session, macHandle) != SDF_SUCCESS) {
        pg_atomic_write_u32(&state->reset_required, 1);
        if (ok) {
            ok = set_error_text(errorMsg, errorMsgSize, "SDF_DestroyKey failed for envelope MAC key");
        }
    }
    if (!ok && *dekHandle != NULL) {
        if (g_sdfApi.destroy_key(state->session, *dekHandle) != SDF_SUCCESS) {
            pg_atomic_write_u32(&state->reset_required, 1);
        }
        *dekHandle = NULL;
    }
    OPENSSL_cleanse(envelope, sizeof(envelope));
    OPENSSL_cleanse(wrappedDek, sizeof(wrappedDek));
    OPENSSL_cleanse(wrappedMac, sizeof(wrappedMac));
    OPENSSL_cleanse(mac, sizeof(mac));
    return ok;
}

static bool import_envelope(SdfSessionState* state, const uint8 cmkId[CMK_ID_LEN_PAGE],
    const unsigned char envelope[DEK_CIPHER_AES128_LEN_PAGE],
    const SdfEnvelopeHeader* header, SdfHandle* dekHandle, char* errorMsg, Size errorMsgSize)
{
    Size offset = sizeof(*header);
    const unsigned char* wrappedDek = envelope + offset;
    offset += header->dek_key_len;
    const unsigned char* wrappedMac = envelope + offset;
    SdfHandle macHandle = NULL;
    *dekHandle = NULL;
    if (!import_wrapped_key(state, g_sdfFile.keyIndex, wrappedMac, header->macKeyLen,
        &macHandle, errorMsg, errorMsgSize)) {
        return false;
    }
    unsigned char mac[SDF_HMAC_LEN] = {};
    bool ok = compute_envelope_mac(state, macHandle, cmkId, envelope, header, mac,
        errorMsg, errorMsgSize);
    if (ok && !constant_time_equal(mac, envelope + header->total_len - header->macLen, header->macLen)) {
        ok = set_error_text(errorMsg, errorMsgSize, "SDF DEK envelope MAC verification failed");
    }
    OPENSSL_cleanse(mac, sizeof(mac));
    int destroyRet = g_sdfApi.destroy_key(state->session, macHandle);
    if (destroyRet != SDF_SUCCESS) {
        pg_atomic_write_u32(&state->reset_required, 1);
    }
    if (!ok) {
        return false;
    }
    if (destroyRet != SDF_SUCCESS) {
        pg_atomic_write_u32(&state->reset_required, 1);
        return set_error_text(errorMsg, errorMsgSize, "SDF_DestroyKey failed for envelope MAC key");
    }
    if (!import_wrapped_key(state, g_sdfFile.keyIndex, wrappedDek, header->dek_key_len,
        dekHandle, errorMsg, errorMsgSize)) {
        return false;
    }
    return true;
}

/* Session read lock held. Exactly one cold importer handles concurrent misses. */
static SdfHandleEntry* acquire_dek_handle(SdfSessionState* state, const char* cmkId, const char* cipher,
    char* errorMsg, Size errorMsgSize)
{
    if (cipher == NULL || strlen(cipher) != DEK_CIPHER_AES128_LEN_PAGE * SDF_HEX_CHARS_PER_BYTE) {
        set_error_text(errorMsg, errorMsgSize, "invalid SDF DEK envelope length");
        return NULL;
    }
    uint8 id[CMK_ID_LEN_PAGE];
    if (!valid_cmk(cmkId, id, errorMsg, errorMsgSize)) {
        return NULL;
    }
    SdfHandleEntry* entry = find_cached_handle(id, cipher);
    if (entry != NULL) {
        return entry;
    }
    pthread_mutex_lock(&g_sdfColdMutex);
    CollectRetiredHandles();
    entry = find_cached_handle(id, cipher); /* A preceding importer may have published it. */
    if (entry == NULL && !pg_atomic_read_u32(&state->reset_required)) {
        unsigned char envelope[DEK_CIPHER_AES128_LEN_PAGE] = {};
        SdfEnvelopeHeader header = {};
        SdfHandle handle = NULL;
        bool ok = decode_envelope(cipher, envelope, &header, errorMsg, errorMsgSize) &&
            reserve_handle_slot(errorMsg, errorMsgSize) &&
            import_envelope(state, id, envelope, &header, &handle, errorMsg, errorMsgSize);
        if (ok) {
            ok = cache_handle(id, cipher, handle, errorMsg, errorMsgSize);
            if (!ok && g_sdfApi.destroy_key(state->session, handle) != SDF_SUCCESS) {
                pg_atomic_write_u32(&state->reset_required, 1);
            }
        }
        if (ok) {
            entry = find_cached_handle(id, cipher);
        }
        OPENSSL_cleanse(envelope, sizeof(envelope));
    }
    if (entry == NULL && errorMsg != NULL && errorMsgSize > 0 && errorMsg[0] == '\0') {
        set_error_text(errorMsg, errorMsgSize, "SDF shared session requires recovery");
    }
    CollectRetiredHandles();
    pthread_mutex_unlock(&g_sdfColdMutex);
    return entry;
}

template <Size Length>
static bool TestSdfCipher(SdfSessionState* state, SdfHandle imported,
    unsigned char (&input)[Length], unsigned char (&encrypted)[Length],
    unsigned char (&decrypted)[Length], unsigned char (&iv)[SDF_IV_LEN],
    char* errorMsg, Size errorMsgSize)
{
    bool ok = true;
    if (ok) {
        unsigned int outLen = sizeof(encrypted);
        ok = sdf_call(state, g_sdfApi.encrypt(state->session, imported, SDF_SM4_CTR, iv,
            input, sizeof(input), encrypted, &outLen), "SDF_Encrypt(SM4-CTR self-test)",
            errorMsg, errorMsgSize) && outLen == sizeof(input);
        if (!ok && errorMsg[0] == '\0') {
            set_error_text(errorMsg, errorMsgSize, "SDF SM4-CTR encryption self-test returned an invalid length");
        }
    }
    if (ok) {
        OPENSSL_cleanse(iv, sizeof(iv));
        unsigned int outLen = sizeof(decrypted);
        ok = sdf_call(state, g_sdfApi.decrypt(state->session, imported, SDF_SM4_CTR, iv,
            encrypted, sizeof(encrypted), decrypted, &outLen), "SDF_Decrypt(SM4-CTR self-test)",
            errorMsg, errorMsgSize) && outLen == sizeof(input) &&
            constant_time_equal(input, decrypted, sizeof(input));
        if (!ok && errorMsg[0] == '\0') {
            set_error_text(errorMsg, errorMsgSize, "SDF SM4-CTR self-test returned incorrect plaintext");
        }
    }
    return ok;
}

static bool DestroySelfTestHandle(SdfSessionState* state, SdfHandle& handle, const char* message,
    char* errorMsg, Size errorMsgSize)
{
    if (handle == NULL) {
        return true;
    }
    int result = g_sdfApi.destroy_key(state->session, handle);
    handle = NULL;
    if (result != SDF_SUCCESS) {
        pg_atomic_write_u32(&state->reset_required, 1);
        return set_error_text(errorMsg, errorMsgSize, message);
    }
    return true;
}
static bool run_sdf_self_test(char* errorMsg, Size errorMsgSize)
{
    SdfConfig config = {};
    if (!copy_sdf_config(&config, errorMsg, errorMsgSize)) {
        return false;
    }
    SdfSessionState* state = &g_sdfShared;
    bool ok = open_session(state, &config, errorMsg, errorMsgSize);
    uint8 id[CMK_ID_LEN_PAGE] = {};
    for (Size i = 0; i < sizeof(config.keyIndex); ++i) {
        id[CMK_ID_LEN_PAGE - 1 - i] = (uint8)(config.keyIndex >> (i * CHAR_BIT));
    }
    char cipher[DEK_CIPHER_AES128_LEN_PAGE * SDF_HEX_CHARS_PER_BYTE + 1] = {};
    SdfHandle generated = NULL;
    SdfHandle imported = NULL;
    unsigned char iv[SDF_IV_LEN] = {};
    unsigned char input[31] = {};
    for (Size i = 0; i < sizeof(input); ++i) {
        input[i] = (unsigned char)i;
    }
    unsigned char encrypted[sizeof(input)] = {};
    unsigned char decrypted[sizeof(input)] = {};
    SdfEnvelopeHeader header = {};
    unsigned char envelope[DEK_CIPHER_AES128_LEN_PAGE] = {};
    if (ok) {
        ok = generate_envelope(state, id, cipher, &generated, errorMsg, errorMsgSize);
    }
    if (ok) {
        ok = DestroySelfTestHandle(state, generated, "SDF_DestroyKey failed during self-test",
            errorMsg, errorMsgSize) &&
            decode_envelope(cipher, envelope, &header, errorMsg, errorMsgSize) &&
            import_envelope(state, id, envelope, &header, &imported, errorMsg, errorMsgSize);
    }
    if (ok) {
        ok = TestSdfCipher(state, imported, input, encrypted, decrypted, iv, errorMsg, errorMsgSize);
    }
    if (!DestroySelfTestHandle(state, imported, "SDF_DestroyKey failed for self-test DEK",
        errorMsg, errorMsgSize)) {
        ok = false;
    }
    config.Clear();
    OPENSSL_cleanse(envelope, sizeof(envelope));
    OPENSSL_cleanse(input, sizeof(input));
    OPENSSL_cleanse(encrypted, sizeof(encrypted));
    OPENSSL_cleanse(decrypted, sizeof(decrypted));
    return ok;
}

/* Session write lock held (or startup failed before publishing any callers). */
static void CloseSdfDevice()
{
    if (g_sdfShared.session != NULL || g_sdfValidation.session != NULL) {
        return; /* Failed session close still owns the device and library. */
    }
    if (g_sdfDevice != NULL) {
        int ret = g_sdfApi.close_device(g_sdfDevice);
        if (ret != SDF_SUCCESS) {
            (void)fprintf(stderr, "WARNING: SDF_CloseDevice failed with SDF error 0x%08x; "
                "device/library retained until process exit\n", (unsigned int)ret);
            return;
        }
        g_sdfDevice = NULL;
    }
    if (g_sdfApi.library != NULL) {
        (void)dlclose(g_sdfApi.library);
        g_sdfApi = {};
    }
}

/* Validate changed credentials without disturbing the live shared session/cache. */
static bool ValidateSdfCredentials(const SdfConfig* config, char* errorMsg, Size errorMsgSize)
{
    if (g_sdfValidation.session != NULL &&
        !close_session(&g_sdfValidation, errorMsg, errorMsgSize)) {
        return false;
    }
    if (!open_session(&g_sdfValidation, config, errorMsg, errorMsgSize)) {
        return false;
    }
    return close_session(&g_sdfValidation, errorMsg, errorMsgSize);
}

void SdfConfig::Clear()
{
    (void)memset_s(this, sizeof(*this), 0, sizeof(*this));
}

static void FreeSdfConfig(SdfConfig* config)
{
    if (config != NULL) {
        config->Clear();
        free(config);
    }
}

static bool publish_sdf_config(SdfConfig* config, uint32 expectedGeneration, SdfConfig** oldConfig,
    char* errorMsg, Size errorMsgSize)
{
    pthread_mutex_lock(&g_sdfMutex);
    if (g_sdfStopping) {
        pthread_mutex_unlock(&g_sdfMutex);
        return set_error_text(errorMsg, errorMsgSize, "SDF TDE provider is shutting down");
    }
    SdfConfig* old = g_sdfConfig;
    uint32 currentGeneration = old == NULL ? 0 : old->generation;
    if (currentGeneration != expectedGeneration) {
        pthread_mutex_unlock(&g_sdfMutex);
        return set_error_text(errorMsg, errorMsgSize,
            "SDF TDE credentials changed concurrently; retry the reload");
    }
    config->generation = old == NULL ? 1 : old->generation + 1;
    if (config->generation == 0) {
        config->generation = 1;
    }
    g_sdfConfig = config;
    if (g_sdfRunning) {
        pg_atomic_write_u32(&g_sdfGeneration, config->generation);
    }
    *oldConfig = old;
    pthread_mutex_unlock(&g_sdfMutex);
    return true;
}

bool SdfTdeReload(char* errorMsg, Size errorMsgSize)
{
    SdfConfig* config = (SdfConfig*)calloc(1, sizeof(SdfConfig));
    if (config == NULL) {
        return set_error_text(errorMsg, errorMsgSize, "out of memory loading SDF configuration");
    }
    config->keyIndex = g_sdfFile.keyIndex;
    if (!config->LoadPassword(errorMsg, errorMsgSize)) {
        FreeSdfConfig(config);
        return false;
    }
    pthread_mutex_lock(&g_sdfMutex);
    if (g_sdfStopping) {
        pthread_mutex_unlock(&g_sdfMutex);
        FreeSdfConfig(config);
        return set_error_text(errorMsg, errorMsgSize, "SDF TDE provider is shutting down");
    }
    SdfConfig* old = g_sdfConfig;
    if (old != NULL && config->passwordLen == old->passwordLen &&
        constant_time_equal(config->password, old->password, config->passwordLen)) {
        pthread_mutex_unlock(&g_sdfMutex);
        FreeSdfConfig(config);
        return true;
    }
    uint32 oldGeneration = old == NULL ? 0 : old->generation;
    bool needsValidation = old != NULL;
    if (needsValidation && !g_sdfRunning) {
        pthread_mutex_unlock(&g_sdfMutex);
        FreeSdfConfig(config);
        return set_error_text(errorMsg, errorMsgSize, "SDF TDE provider is not running");
    }
    pthread_mutex_unlock(&g_sdfMutex);

    if (needsValidation && !ValidateSdfCredentials(config, errorMsg, errorMsgSize)) {
        FreeSdfConfig(config);
        return false;
    }

    SdfConfig* oldConfig = NULL;
    if (!publish_sdf_config(config, oldGeneration, &oldConfig, errorMsg, errorMsgSize)) {
        FreeSdfConfig(config);
        return false;
    }
    FreeSdfConfig(oldConfig);
    return true;
}

static void sdf_shutdown(int code, Datum arg)
{
    pthread_mutex_lock(&g_sdfMutex);
    g_sdfStopping = true;
    g_sdfRunning = false;
    pg_atomic_write_u32(&g_sdfGeneration, 0);
    SdfConfig* config = g_sdfConfig;
    g_sdfConfig = NULL;
    pthread_mutex_unlock(&g_sdfMutex);
    FreeSdfConfig(config);
    pthread_rwlock_wrlock(&g_sdfSessionGate);
    ClearSharedHandles();
    char error[256] = {0};
    if (!close_session(&g_sdfShared, error, sizeof(error))) {
        (void)fprintf(stderr, "WARNING: %s; shared SDF session/device retained until process exit\n", error);
    }
    char validationError[256] = {0};
    if (!close_session(&g_sdfValidation, validationError, sizeof(validationError))) {
        (void)fprintf(stderr, "WARNING: %s; validation SDF session/device retained until process exit\n",
            validationError);
    }
    CloseSdfDevice();
    pthread_rwlock_unlock(&g_sdfSessionGate);
}

bool SdfTdeInitialize(char* errorMsg, Size errorMsgSize)
{
    pg_atomic_init_u32(&g_sdfGeneration, 0);
    if (errorMsg != NULL && errorMsgSize > 0) {
        errorMsg[0] = '\0';
    }
    if (!load_sdf_file_config(errorMsg, errorMsgSize) || !load_sdf_api(errorMsg, errorMsgSize)) {
        return false;
    }
    int ret = g_sdfApi.open_device(&g_sdfDevice);
    if (ret != SDF_SUCCESS || g_sdfDevice == NULL) {
        if (ret != SDF_SUCCESS) {
            format_sdf_error(errorMsg, errorMsgSize,
                "SDF_OpenDevice failed with SDF error 0x%08x", (unsigned int)ret);
        } else {
            set_error_text(errorMsg, errorMsgSize, "SDF_OpenDevice returned a null device");
        }
        CloseSdfDevice();
        return false;
    }
    if (!SdfTdeReload(errorMsg, errorMsgSize)) {
        sdf_shutdown(0, 0);
        return false;
    }
    pthread_mutex_lock(&g_sdfMutex);
    g_sdfStopping = false;
    g_sdfRunning = true;
    pg_atomic_write_u32(&g_sdfGeneration, g_sdfConfig->generation);
    pthread_mutex_unlock(&g_sdfMutex);
    if (!run_sdf_self_test(errorMsg, errorMsgSize)) {
        sdf_shutdown(0, 0);
        return false;
    }
    if (begin_shared_session(errorMsg, errorMsgSize) == NULL) {
        sdf_shutdown(0, 0);
        return false;
    }
    EndSharedSession();
    on_proc_exit(sdf_shutdown, 0);
    return true;
}

bool SdfCreateTdeDek(const char* cmkId, char** dekCipher, char* errorMsg, Size errorMsgSize)
{
    if (errorMsg != NULL && errorMsgSize > 0) {
        errorMsg[0] = '\0';
    }
    if (dekCipher == NULL) {
        return set_error_text(errorMsg, errorMsgSize, "missing SDF DEK envelope output");
    }
    *dekCipher = NULL;
    uint8 id[CMK_ID_LEN_PAGE];
    if (!valid_cmk(cmkId, id, errorMsg, errorMsgSize)) {
        return false;
    }
    SdfSessionState* state = begin_shared_session(errorMsg, errorMsgSize);
    if (state == NULL) {
        return false;
    }
    char cipher[DEK_CIPHER_AES128_LEN_PAGE * SDF_HEX_CHARS_PER_BYTE + 1] = {};
    SdfHandle handle = NULL;
    pthread_mutex_lock(&g_sdfColdMutex);
    bool ok = !pg_atomic_read_u32(&state->reset_required) &&
        reserve_handle_slot(errorMsg, errorMsgSize) &&
        generate_envelope(state, id, cipher, &handle, errorMsg, errorMsgSize);
    if (ok) {
        ok = cache_handle(id, cipher, handle, errorMsg, errorMsgSize);
        if (!ok && g_sdfApi.destroy_key(state->session, handle) != SDF_SUCCESS) {
            pg_atomic_write_u32(&state->reset_required, 1);
        }
    }
    if (!ok && errorMsg != NULL && errorMsgSize > 0 && errorMsg[0] == '\0') {
        set_error_text(errorMsg, errorMsgSize, "SDF shared session requires recovery");
    }
    CollectRetiredHandles();
    pthread_mutex_unlock(&g_sdfColdMutex);
    EndSharedSession();
    if (ok) {
        *dekCipher = pstrdup(cipher); /* No PostgreSQL allocation while holding pthread locks. */
    }
    return ok;
}

bool SdfGetTdeDek(const char* cmkId, const char* dekCipher, char* errorMsg, Size errorMsgSize)
{
    if (errorMsg != NULL && errorMsgSize > 0) {
        errorMsg[0] = '\0';
    }
    SdfSessionState* state = begin_shared_session(errorMsg, errorMsgSize);
    if (state == NULL) {
        return false;
    }
    SdfHandleEntry* entry = acquire_dek_handle(state, cmkId, dekCipher, errorMsg, errorMsgSize);
    bool found = entry != NULL;
    if (found) {
        ReleaseHandle(entry);
    }
    EndSharedSession();
    return found;
}

/* The caller pins the shared session and key until this synchronous operation returns. */
static bool CryptWithHandle(SdfSessionState* state, SdfHandle handle, SdfScratch* scratch,
    const uint8* iv, const char* input, char* output, Size length, bool encrypt,
    char* errorMsg, Size errorMsgSize)
{
    bool ok = true;
    if (length > 0) {
        unsigned char localIv[SDF_IV_LEN];
        if (!CopyBytes(localIv, sizeof(localIv), iv, sizeof(localIv), errorMsg, errorMsgSize)) {
            return false;
        }
        unsigned int outputLen = (unsigned int)length;
        int ret = encrypt ? g_sdfApi.encrypt(state->session, handle, SDF_SM4_CTR, localIv,
            (unsigned char*)input, (unsigned int)length, scratch->data, &outputLen) :
            g_sdfApi.decrypt(state->session, handle, SDF_SM4_CTR, localIv,
                (unsigned char*)input, (unsigned int)length, scratch->data, &outputLen);
        ok = sdf_call(state, ret, encrypt ? "SDF_Encrypt(SM4-CTR page)" : "SDF_Decrypt(SM4-CTR page)",
            errorMsg, errorMsgSize);
        if (ok && outputLen != length) {
            pg_atomic_write_u32(&state->reset_required, 1);
            ok = set_error_text(errorMsg, errorMsgSize, "SDF returned an invalid page cipher length");
        }
        if (ok) {
            ok = CopyBytes(output, length, scratch->data, length, errorMsg, errorMsgSize);
        }
        OPENSSL_cleanse(scratch->data, length);
        OPENSSL_cleanse(localIv, sizeof(localIv));
    }
    return ok;
}

bool SdfTdeCryptPage(const char* cmkId, const char* dekCipher, const uint8* iv,
    const char* input, char* output, Size length, bool encrypt, char* errorMsg, Size errorMsgSize)
{
    if (errorMsg != NULL && errorMsgSize > 0) {
        errorMsg[0] = '\0';
    }
    if ((length > 0 && (iv == NULL || input == NULL || output == NULL)) || length > UINT_MAX) {
        return set_error_text(errorMsg, errorMsgSize, "invalid SDF page cipher arguments or length");
    }
    SdfScratch* scratch = get_sdf_scratch(errorMsg, errorMsgSize);
    if (scratch == NULL) {
        return false;
    }
    if (scratch->capacity < length) {
        unsigned char* data = (unsigned char*)malloc(length);
        if (data == NULL) {
            return set_error_text(errorMsg, errorMsgSize, "out of memory allocating SDF cipher scratch buffer");
        }
        if (scratch->data != NULL) {
            OPENSSL_cleanse(scratch->data, scratch->capacity);
            free(scratch->data);
        }
        scratch->data = data;
        scratch->capacity = length;
    }
    SdfSessionState* state = begin_shared_session(errorMsg, errorMsgSize);
    if (state == NULL) {
        return false;
    }
    SdfHandleEntry* entry = acquire_dek_handle(state, cmkId, dekCipher, errorMsg, errorMsgSize);
    if (entry == NULL) {
        EndSharedSession();
        return false;
    }
    bool ok = CryptWithHandle(state, entry->handle, scratch, iv, input, output, length,
        encrypt, errorMsg, errorMsgSize);
    ReleaseHandle(entry);
    EndSharedSession();
    return ok;
}
