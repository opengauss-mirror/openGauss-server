#include "common_cipher.h"
#include "securec.h"
#include "securec_check.h"
#include "port.h"

#define MAX_PROVIDER_NAME_LEN 128
#define MAX_ERRMSG_LEN 256

typedef enum {
    MODULE_AES_128_CBC = 0,
    MODULE_AES_128_CTR,
    MODULE_AES_128_GCM,
    MODULE_AES_256_CBC,
    MODULE_AES_256_CTR,
    MODULE_AES_256_GCM,
    MODULE_SM4_CBC,
    MODULE_SM4_CTR,
    MODULE_HMAC_SHA256,
    MODULE_HMAC_SM3,
    MODULE_DETERMINISTIC_KEY,
    MODULE_ALGO_MAX = 1024
} ModuleSymmKeyAlgo;

typedef enum {
    MODULE_SHA256 = 0,
    MODULE_SM3,
    MODULE_DIGEST_MAX = 1024
} ModuleDigestAlgo;

typedef enum {
    KEY_TYPE_INVALID,
    KEY_TYPE_PLAINTEXT,
    KEY_TYPE_CIPHERTEXT,
    KEY_TYPE_NAMEORIDX,
    KEY_TYPE_MAX
} KeyType;

typedef struct {
    char provider_name[MAX_PROVIDER_NAME_LEN];
    KeyType key_type;
    int supported_symm[MODULE_ALGO_MAX]; // 不支持算法填入0或者支持算法填入1
    int supported_digest[MODULE_DIGEST_MAX]; // 不支持算法填入0或者支持算法填入1
} SupportedFeature;
 

typedef int (*crypto_module_init_type)(char *load_info, SupportedFeature *supported_feature);
typedef int (*crypto_module_sess_init_type)(char *key_info, void **sess);
typedef void (*crypto_module_sess_exit_type)(void *sess);
typedef int (*crypto_create_symm_key_type)(void *sess, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t *key_id_size);
typedef int (*crypto_ctx_init_type)(void *sess, void **ctx, ModuleSymmKeyAlgo algo, int enc, unsigned char *key_id, size_t key_id_size);
typedef int (*crypto_result_size_type)(void *ctx, int enc, size_t data_size);
typedef void (*crypto_ctx_clean_type)(void *ctx);
typedef int (*crypto_digest_type)(void *sess, ModuleDigestAlgo algo, unsigned char * data, size_t data_size,unsigned char *result, size_t *result_size);
typedef int (*crypto_hmac_init_type)(void *sess, void **ctx, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t key_id_size);
typedef void (*crypto_hmac_clean_type)(void *ctx);
typedef int (*crypto_hmac_type)(void *ctx, unsigned char * data, size_t data_size, unsigned char *result, size_t *result_size);
typedef int (*crypto_gen_random_type)(void *sess, char *buffer, size_t size);
typedef int (*crypto_deterministic_enc_dec_type)(void *sess, int enc, unsigned char *data, unsigned char *key_id,	 size_t key_id_size, size_t data_size, unsigned char *result, size_t *result_size);
typedef int (*crypto_get_errmsg_type)(void *sess, char *errmsg);


static void *libhandle = NULL;

static crypto_module_init_type crypto_module_init_use = NULL;
static crypto_module_sess_init_type crypto_module_sess_init_use = NULL;
static crypto_module_sess_exit_type crypto_module_sess_exit_use = NULL;
static crypto_create_symm_key_type crypto_create_symm_key_use = NULL;
static crypto_ctx_init_type crypto_ctx_init_use = NULL;
static crypto_result_size_type crypto_result_size_use = NULL;
static crypto_ctx_clean_type crypto_ctx_clean_use = NULL;
crypto_encrypt_decrypt_type crypto_encrypt_decrypt_use = NULL;
static crypto_digest_type crypto_digest_use = NULL;
static crypto_hmac_init_type crypto_hmac_init_use = NULL;
static crypto_hmac_clean_type crypto_hmac_clean_use = NULL;
static crypto_hmac_type crypto_hmac_use = NULL;
static crypto_gen_random_type crypto_gen_random_use = NULL;
static crypto_deterministic_enc_dec_type crypto_deterministic_enc_dec_use = NULL;
static crypto_get_errmsg_type crypto_get_errmsg_use = NULL;

bool load_crypto_module_lib()
{
    errno_t rc = 0;
    char libpath[1024] = {0};
    char* gaussHome = gs_getenv_r("GAUSSHOME");

    rc = snprintf_s(libpath, sizeof(libpath), sizeof(libpath) - 1, "%s/lib/postgresql/common_cipher.so",gaussHome);
    securec_check_ss_c(rc, "", "");

    libhandle = dlopen(libpath, RTLD_LAZY);
    if (libhandle == NULL) {
        return false;
    }

    crypto_module_init_use = (crypto_module_init_type)dlsym(libhandle, "crypto_module_init");
    crypto_module_sess_init_use = (crypto_module_sess_init_type)dlsym(libhandle, "crypto_module_sess_init");
    crypto_module_sess_exit_use = (crypto_module_sess_exit_type)dlsym(libhandle, "crypto_module_sess_exit");
    crypto_create_symm_key_use = (crypto_create_symm_key_type)dlsym(libhandle, "crypto_create_symm_key");
    crypto_ctx_init_use = (crypto_ctx_init_type)dlsym(libhandle, "crypto_ctx_init");
    crypto_result_size_use = (crypto_result_size_type)dlsym(libhandle, "crypto_result_size");
    crypto_ctx_clean_use = (crypto_ctx_clean_type)dlsym(libhandle, "crypto_ctx_clean");
    crypto_encrypt_decrypt_use = (crypto_encrypt_decrypt_type)dlsym(libhandle, "crypto_encrypt_decrypt");
    crypto_digest_use = (crypto_digest_type)dlsym(libhandle, "crypto_digest");
    crypto_hmac_init_use = (crypto_hmac_init_type)dlsym(libhandle, "crypto_hmac_init");
    crypto_hmac_clean_use = (crypto_hmac_clean_type)dlsym(libhandle, "crypto_hmac_clean");
    crypto_hmac_use = (crypto_hmac_type)dlsym(libhandle, "crypto_hmac");
    crypto_gen_random_use = (crypto_gen_random_type)dlsym(libhandle, "crypto_gen_random");
    crypto_deterministic_enc_dec_use = (crypto_deterministic_enc_dec_type)dlsym(libhandle, "crypto_deterministic_enc_dec");
    crypto_get_errmsg_use = (crypto_get_errmsg_type)dlsym(libhandle, "crypto_get_errmsg");

    if (crypto_module_init_use == NULL
        || crypto_module_sess_init_use == NULL
        || crypto_module_sess_exit_use == NULL
        || crypto_create_symm_key_use == NULL
        || crypto_ctx_init_use == NULL
        || crypto_result_size_use == NULL
        || crypto_ctx_clean_use == NULL
        || crypto_encrypt_decrypt_use == NULL
        || crypto_digest_use == NULL
        || crypto_hmac_init_use == NULL
        || crypto_hmac_clean_use == NULL
        || crypto_hmac_use == NULL
        || crypto_gen_random_use == NULL
        || crypto_deterministic_enc_dec_use == NULL
        || crypto_get_errmsg_use == NULL) {
        dlclose(libhandle);
        return false;
    }

    return true;
}

void unload_crypto_module(int code, void* args)
{
    if (libhandle) {
        dlclose(libhandle);
        libhandle = NULL;
    }
}

static int transform_type(char* type)
{
    if (strcmp(type, "AES128_CBC") == 0) {
        return MODULE_AES_128_CBC;
    } else if (strcmp(type, "AES128_CTR") == 0) {
        return MODULE_AES_128_CTR;
    } else if (strcmp(type, "AES128_GCM") == 0) {
        return MODULE_AES_128_GCM;
    } else if (strcmp(type, "AES256_CBC") == 0) {
        return MODULE_AES_256_CBC;
    } else if (strcmp(type, "AES256_CTR") == 0) {
        return MODULE_AES_256_CTR;
    } else if (strcmp(type, "AES256_GCM") == 0) {
        return MODULE_AES_256_GCM;
    } else if (strcmp(type, "SM4_CBC") == 0) {
        return MODULE_SM4_CBC;
    } else if (strcmp(type, "SM4_CTR") == 0) {
        return MODULE_SM4_CTR;
    }

    return -1;

}

void initCryptoModule(DecryptInfo* pDecryptInfo)
{
    int ret = 1;
    SupportedFeature supportedfeature;
    int modulType = 0;

    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_module_init_use(pDecryptInfo->crypto_modlue_params, &supportedfeature);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }

    modulType = transform_type(pDecryptInfo->crypto_type);
    if (modulType < 0 || supportedfeature.supported_symm[modulType] == 0) {
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }

}

void initCryptoSession(DecryptInfo* pDecryptInfo)
{
    int ret = 1;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_module_sess_init_use(NULL, &(pDecryptInfo->moduleSessionCtx));
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }

}

void releaseCryptoSession(int code, void* args)
{
    if (libhandle && ((DecryptInfo*)args)->moduleSessionCtx) {
        crypto_module_sess_exit_use(((DecryptInfo*)args)->moduleSessionCtx);
        ((DecryptInfo*)args)->moduleSessionCtx = NULL;
    }
}

void initCryptoKeyCtx(DecryptInfo* pDecryptInfo)
{
    int ret = 1;
    int enc = 0;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_ctx_init_use(pDecryptInfo->moduleSessionCtx, &(pDecryptInfo->moduleKeyCtx), (ModuleSymmKeyAlgo)transform_type(pDecryptInfo->crypto_type), enc, pDecryptInfo->Key, pDecryptInfo->keyLen);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        crypto_module_sess_exit_use(pDecryptInfo->moduleSessionCtx);
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }
}

void releaseCryptoCtx(int code, void* args)
{
    if (libhandle && ((DecryptInfo*)args)->moduleKeyCtx) {
        crypto_ctx_clean_use(((DecryptInfo*)args)->moduleKeyCtx);
        ((DecryptInfo*)args)->moduleKeyCtx = NULL;
    }
}

void symmEncDec(DecryptInfo* pDecryptInfo, bool isEnc, char* indata, int inlen, char* outdata, int* outlen)
{
    int ret = 1;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_encrypt_decrypt_use(pDecryptInfo->moduleKeyCtx, isEnc, (unsigned char*)indata, inlen, pDecryptInfo->rand, 16, (unsigned char*)outdata, (size_t*)outlen, NULL);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        releaseCryptoCtx(0, pDecryptInfo);
        releaseCryptoSession(0, pDecryptInfo);
        unload_crypto_module(0, NULL);
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }
}

void CryptoModuleParamsCheck(DecryptInfo* pDecryptInfo, const char* params, const char* module_encrypt_mode, const char* module_encrypt_key, const char* module_encrypt_salt)
{
    errno_t rc = 0;

    if (!load_crypto_module_lib()) {
        fprintf(stderr, ("load crypto module lib failed\n"));
        exit(1);
    }

    rc = memcpy_s((GS_UCHAR*)pDecryptInfo->crypto_modlue_params, CRYPTO_MODULE_PARAMS_MAX_LEN, params, strlen(params));
    securec_check_c(rc, "\0", "\0");

    if (module_encrypt_mode == NULL) {
        fprintf(stderr, ("encrypt_mode cannot be NULL\n"));
        exit(1);
    } else {
        rc = memcpy_s((GS_UCHAR*)pDecryptInfo->crypto_type, CRYPTO_MODULE_ENC_TYPE_MAX_LEN, module_encrypt_mode, strlen(module_encrypt_mode));
        securec_check_c(rc, "\0", "\0");
    }

    if (module_encrypt_salt == NULL || strlen(module_encrypt_salt) != 16) {
        fprintf(stderr, ("salt is needed and must be 16 bytes\n"));
        exit(1);
    } else {
        rc = memcpy_s((GS_UCHAR*)pDecryptInfo->rand, RANDOM_LEN + 1, module_encrypt_salt, strlen(module_encrypt_salt));
        securec_check_c(rc, "\0", "\0");

        pDecryptInfo->randget = true;
    }

    initCryptoModule(pDecryptInfo);
    initCryptoSession(pDecryptInfo);

    if (module_encrypt_key) {
        char *tmpkey = NULL;
        unsigned int tmpkeylen = 0;

        tmpkey = SEC_decodeBase64(module_encrypt_key, &tmpkeylen);
        if (tmpkey == NULL || tmpkeylen > KEY_MAX_LEN) {
            if (tmpkey) {
                OPENSSL_free(tmpkey);
            }
            fprintf(stderr, ("invalid key\n"));
            exit(1);	
        } else {
            rc = memcpy_s((GS_UCHAR*)pDecryptInfo->Key, KEY_MAX_LEN, tmpkey, tmpkeylen);
            securec_check_c(rc, "\0", "\0");
            pDecryptInfo->keyLen = tmpkeylen;
        }
    } else {
        fprintf(stderr, ("invalid key\n"));
        exit(1);
    }

    initCryptoKeyCtx(pDecryptInfo);

    pDecryptInfo->encryptInclude = true;
    pDecryptInfo->clientSymmCryptoFunc = crypto_encrypt_decrypt_use;
}
