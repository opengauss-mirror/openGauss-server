#include "common_cipher.h"
#include "securec.h"
#include "securec_check.h"
#include "port.h"
#include "libpq/pqcomm.h"

#define MAX_PROVIDER_NAME_LEN 128
#define MAX_ERRMSG_LEN 256


typedef int (*crypto_module_init_type)(char *load_info, SupportedFeature *supported_feature);
typedef int (*crypto_module_sess_init_type)(char *key_info, void **sess);
typedef void (*crypto_module_sess_exit_type)(void *sess);
typedef int (*crypto_result_size_type)(void *ctx, int enc, size_t data_size);
typedef void (*crypto_ctx_clean_type)(void *ctx);
typedef int (*crypto_digest_type)(void *sess, ModuleDigestAlgo algo, unsigned char * data, size_t data_size,unsigned char *result, size_t *result_size);
typedef void (*crypto_hmac_clean_type)(void *ctx);
typedef int (*crypto_gen_random_type)(void *sess, char *buffer, size_t size);
typedef int (*crypto_deterministic_enc_dec_type)(void *sess, int enc, unsigned char *data, unsigned char *key_id,	 size_t key_id_size, size_t data_size, unsigned char *result, size_t *result_size);


static void *libhandle = NULL;

static crypto_module_init_type crypto_module_init_use = NULL;
static crypto_module_sess_init_type crypto_module_sess_init_use = NULL;
static crypto_module_sess_exit_type crypto_module_sess_exit_use = NULL;
crypto_create_symm_key_type crypto_create_symm_key_use = NULL;
crypto_ctx_init_type crypto_ctx_init_use = NULL;
static crypto_result_size_type crypto_result_size_use = NULL;
static crypto_ctx_clean_type crypto_ctx_clean_use = NULL;
crypto_encrypt_decrypt_type crypto_encrypt_decrypt_use = NULL;
static crypto_digest_type crypto_digest_use = NULL;
crypto_hmac_init_type crypto_hmac_init_use = NULL;
static crypto_hmac_clean_type crypto_hmac_clean_use = NULL;
crypto_hmac_type crypto_hmac_use = NULL;
static crypto_gen_random_type crypto_gen_random_use = NULL;
static crypto_deterministic_enc_dec_type crypto_deterministic_enc_dec_use = NULL;
crypto_get_errmsg_type crypto_get_errmsg_use = NULL;

bool load_crypto_module_lib()
{
    errno_t rc = 0;
    char libpath[1024] = {0};
    char* gaussHome = gs_getenv_r("GAUSSHOME");
    if(check_client_env(gaussHome) == NULL) {
        fprintf(stderr, "crypto module get GAUSSHOME failed.");
        exit(1);
    }

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

void unload_crypto_module()
{
    if (libhandle) {
        dlclose(libhandle);
        libhandle = NULL;
    }
}

int transform_type(const char* type)
{
    if (strcmp(type, "AES128_CBC") == 0 || strcmp(type, "AES128_CBC_HMAC_SHA256") == 0) {
        return MODULE_AES_128_CBC;
    } else if (strcmp(type, "AES128_CTR") == 0 || strcmp(type, "AES128_CTR_HMAC_SHA256") == 0) {
        return MODULE_AES_128_CTR;
    } else if (strcmp(type, "AES128_GCM") == 0 || strcmp(type, "AES128_GCM_HMAC_SHA256") == 0) {
        return MODULE_AES_128_GCM;
    } else if (strcmp(type, "AES256_CBC") == 0 || strcmp(type, "AES256_CBC_HMAC_SHA256") == 0) {
        return MODULE_AES_256_CBC;
    } else if (strcmp(type, "AES256_CTR") == 0 || strcmp(type, "AES256_CTR_HMAC_SHA256") == 0) {
        return MODULE_AES_256_CTR;
    } else if (strcmp(type, "AES256_GCM") == 0 || strcmp(type, "AES256_GCM_HMAC_SHA256") == 0) {
        return MODULE_AES_256_GCM;
    } else if (strcmp(type, "SM4_CBC") == 0 || strcmp(type, "SM4_CBC_HMAC_SM3") == 0) {
        return MODULE_SM4_CBC;
    } else if (strcmp(type, "SM4_CTR") == 0 || strcmp(type, "SM4_CTR_HMAC_SM3") == 0) {
        return MODULE_SM4_CTR;
    }

    return -1;

}

int getHmacType(ModuleSymmKeyAlgo algo)
{
    if (algo >= MODULE_AES_128_CBC && algo <= MODULE_AES_256_GCM) {
        return MODULE_HMAC_SHA256;
    } else if (algo == MODULE_SM4_CBC || algo == MODULE_SM4_CTR) {
        return MODULE_HMAC_SM3;
    }

    return MODULE_ALGO_MAX;
}

int transform_hmac_type(const char* type)
{
    if (strcmp(type, "AES128_CBC_HMAC_SHA256") == 0
        || strcmp(type, "AES128_CTR_HMAC_SHA256") == 0
        || strcmp(type, "AES128_GCM_HMAC_SHA256") == 0
        || strcmp(type, "AES256_CBC_HMAC_SHA256") == 0
        || strcmp(type, "AES256_CTR_HMAC_SHA256") == 0
        || strcmp(type, "AES256_GCM_HMAC_SHA256") == 0) {
        return MODULE_HMAC_SHA256;
    } else if (strcmp(type, "SM4_CBC_HMAC_SM3") == 0
        || strcmp(type, "SM4_CTR_HMAC_SM3") == 0) {
        return MODULE_HMAC_SM3;
    }

    return MODULE_ALGO_MAX;
}

void initCryptoModule(char* crypto_module_params, const char* encrypt_mode, int* key_type)
{
    int ret = 1;
    SupportedFeature supportedfeature;
    int modulType = 0;

    char errmsg[MAX_ERRMSG_LEN] = {0};

    if (NULL == encrypt_mode) {
        fprintf(stderr, "encrypt mode cannot be NULL.");
        exit(1);
    }
    ret = crypto_module_init_use(crypto_module_params, &supportedfeature);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }

    modulType = transform_type(encrypt_mode);
    if (modulType < 0 || supportedfeature.supported_symm[modulType] == 0) {
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }

    *key_type = supportedfeature.key_type;
}

void initCryptoSession(void** crypto_module_session)
{
    int ret = 1;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_module_sess_init_use(NULL, crypto_module_session);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        fprintf(stderr, ("%s\n"), errmsg);
        exit(1);
    }

}

void releaseCryptoSession(void* crypto_module_session)
{
    if (libhandle && crypto_module_session) {
        crypto_module_sess_exit_use(crypto_module_session);
        crypto_module_session = NULL;
    }
}

void releaseCryptoCtx(void* crypto_module_keyctx)
{
    if (libhandle && crypto_module_keyctx) {
        crypto_ctx_clean_use(crypto_module_keyctx);
        crypto_module_keyctx = NULL;
    }
}

void releaseHmacCtx(void* crypto_hmac_keyctx)
{
    if (libhandle && crypto_hmac_keyctx) {
        crypto_hmac_clean_use(crypto_hmac_keyctx);
        crypto_hmac_keyctx = NULL;
    }
}

void clearCrypto(void* crypto_module_session, void* crypto_module_keyctx, void* crypto_hmac_keyctx)
{
    releaseHmacCtx(crypto_hmac_keyctx);
    releaseCryptoCtx(crypto_module_keyctx);
    releaseCryptoSession(crypto_module_session);
    unload_crypto_module();
}

void CryptoModuleParamsCheck(bool gen_key, char* params, const char* module_encrypt_mode, const char* module_encrypt_key, const char* module_encrypt_salt, int* key_type)
{
    if (!load_crypto_module_lib()) {
        fprintf(stderr, ("load crypto module lib failed\n"));
        exit(1);
    }

    if (module_encrypt_mode == NULL || params == NULL) {
        fprintf(stderr, ("encrypt mode and crypto module params cannot be NULL\n"));
        exit(1);
    } else {
        initCryptoModule(params, module_encrypt_mode, key_type);
    }
    
    if (gen_key && NULL != module_encrypt_key) {
        fprintf(stderr, ("--gen-key cannot be used with --with-key at the same time."));
    }

    if (module_encrypt_salt == NULL || strlen(module_encrypt_salt) != MAX_IV_LEN) {
        fprintf(stderr, ("invalid salt, salt is needed and must be 16 bytes\n"));
        exit(1);	
    }

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
        }
    }
}