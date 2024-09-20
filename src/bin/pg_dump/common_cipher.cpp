#include "pg_backup_cipher.h"
#include "port.h"
#include "libpq/pqcomm.h"

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
crypto_hmac_type crypto_hmac_use = NULL;
static crypto_gen_random_type crypto_gen_random_use = NULL;
static crypto_deterministic_enc_dec_type crypto_deterministic_enc_dec_use = NULL;
static crypto_get_errmsg_type crypto_get_errmsg_use = NULL;

bool load_crypto_module_lib()
{
    errno_t rc = 0;
    char libpath[1024] = {0};
    char* gaussHome = gs_getenv_r("GAUSSHOME");
    if (check_client_env(gaussHome)){
        rc = snprintf_s(libpath, sizeof(libpath), sizeof(libpath) - 1, "%s/lib/postgresql/common_cipher.so",gaussHome);
        securec_check_ss_c(rc, "", "");
    } else {
        exit_horribly(NULL, "$GAUSSHOME set error or net set\n");
    }

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

static void transform_type(char* type, ModuleSymmKeyAlgo* symmtype, ModuleSymmKeyAlgo* hmactype)
{
    *symmtype = MODULE_ALGO_MAX;
    *hmactype = MODULE_ALGO_MAX;

    if (strcmp(type, "AES128_CBC") == 0) {
        *symmtype = MODULE_AES_128_CBC;
    } else if (strcmp(type, "AES128_CTR") == 0) {
        *symmtype = MODULE_AES_128_CTR;
    } else if (strcmp(type, "AES128_GCM") == 0) {
        *symmtype = MODULE_AES_128_GCM;
    } else if (strcmp(type, "AES256_CBC") == 0) {
        *symmtype = MODULE_AES_256_CBC;
    } else if (strcmp(type, "AES256_CTR") == 0) {
        *symmtype = MODULE_AES_256_CTR;
    } else if (strcmp(type, "AES256_GCM") == 0) {
        *symmtype = MODULE_AES_256_GCM;
    } else if (strcmp(type, "SM4_CBC") == 0) {
        *symmtype = MODULE_SM4_CBC;
    } else if (strcmp(type, "SM4_CTR") == 0) {
        *symmtype = MODULE_SM4_CTR;
    }else if (strcmp(type, "AES128_CBC_HMAC_SHA256") == 0) {
        *symmtype = MODULE_AES_128_CBC;
        *hmactype = MODULE_HMAC_SHA256;
    } else if (strcmp(type, "AES128_CTR_HMAC_SHA256") == 0) {
        *symmtype = MODULE_AES_128_CTR;
        *hmactype = MODULE_HMAC_SHA256;
    } else if (strcmp(type, "AES128_GCM_HMAC_SHA256") == 0) {
        *symmtype = MODULE_AES_128_GCM;
        *hmactype = MODULE_HMAC_SHA256;
    } else if (strcmp(type, "AES256_CBC_HMAC_SHA256") == 0) {
        *symmtype = MODULE_AES_256_CBC;
        *hmactype = MODULE_HMAC_SHA256;
    } else if (strcmp(type, "AES256_CTR_HMAC_SHA256") == 0) {
        *symmtype = MODULE_AES_256_CTR;
        *hmactype = MODULE_HMAC_SHA256;
    } else if (strcmp(type, "AES256_GCM_HMAC_SHA256") == 0) {
        *symmtype = MODULE_AES_256_GCM;
        *hmactype = MODULE_HMAC_SHA256;
    } else if (strcmp(type, "SM4_CBC_HMAC_SM3") == 0) {
        *symmtype = MODULE_SM4_CBC;
        *hmactype = MODULE_HMAC_SM3;
    } else if (strcmp(type, "SM4_CTR_HMAC_SM3") == 0) {
        *symmtype = MODULE_SM4_CTR;
        *hmactype = MODULE_HMAC_SM3;
    }

    if (*symmtype == MODULE_ALGO_MAX) {
        exit_horribly(NULL, ("error algocrypto type\n"));
    }

}

void initCryptoModule(ArchiveHandle* AH)
{
    int ret = 1;
    SupportedFeature supportedfeature;
    Archive* fort = (Archive*)AH;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ModuleSymmKeyAlgo symmtype;
    ModuleSymmKeyAlgo hmactype;

    transform_type(fort->crypto_type, &symmtype, &hmactype);

    ret = crypto_module_init_use(fort->crypto_module_params, &supportedfeature);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        exit_horribly(NULL, "%s\n", errmsg);
    }

    if (symmtype < 0 || supportedfeature.supported_symm[symmtype] == 0) {
        exit_horribly(NULL, "unsupported this mode:%s\n", fort->crypto_type);
    }

    fort->key_type = supportedfeature.key_type;

}

void initCryptoSession(ArchiveHandle* AH)
{
    int ret = 1;
    Archive* fort = (Archive*)AH;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_module_sess_init_use(NULL, &(fort->cryptoModuleCtx.moduleSession));
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        exit_horribly(NULL, "%s\n", errmsg);
    }

}

void releaseCryptoSession(int code, void* args)
{
    if (libhandle && ((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.moduleSession) {
        crypto_module_sess_exit_use(((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.moduleSession);
        ((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.moduleSession = NULL;
    }
}

void initCryptoKeyCtx(ArchiveHandle* AH)
{
    int ret = 1;
    int enc = (AH->mode == archModeWrite) ? 1 : 0;
    Archive* fort = (Archive*)AH;
    char errmsg[MAX_ERRMSG_LEN] = {0};
    ModuleSymmKeyAlgo symmtype;
    ModuleSymmKeyAlgo hmactype;

    transform_type(AH->publicArc.crypto_type, &symmtype, &hmactype);

    ret = crypto_ctx_init_use(fort->cryptoModuleCtx.moduleSession, &(fort->cryptoModuleCtx.key_ctx), symmtype, enc, fort->Key, fort->keylen);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        crypto_module_sess_exit_use(fort->cryptoModuleCtx.moduleSession);
        exit_horribly(NULL, "%s\n", errmsg);
    }
}

void releaseCryptoCtx(int code, void* args)
{
    if (libhandle && ((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.key_ctx) {
        crypto_ctx_clean_use(((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.key_ctx);
        ((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.key_ctx = NULL;
    }
}

void symmGenerateKey(ArchiveHandle* AH)
{
    int ret = 1;
    char errmsg[MAX_ERRMSG_LEN] = {0};
    ModuleSymmKeyAlgo symmtype;
    ModuleSymmKeyAlgo hmactype;

    transform_type(AH->publicArc.crypto_type, &symmtype, &hmactype);

    ret = crypto_create_symm_key_use(AH->publicArc.cryptoModuleCtx.moduleSession, symmtype, AH->publicArc.Key, (size_t*)&(AH->publicArc.keylen));
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        releaseCryptoSession(0, AH);
        unload_crypto_module(0, NULL);
        exit_horribly(NULL, "%s\n", errmsg);
    }
}

void symmEncDec(ArchiveHandle* AH, bool isEnc, char* indata, int inlen, char* outdata, int* outlen)
{
    int ret = 1;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_encrypt_decrypt_use(AH->publicArc.cryptoModuleCtx.key_ctx, isEnc, (unsigned char*)indata, inlen, AH->publicArc.rand, 16, (unsigned char*)outdata, (size_t*)outlen, NULL);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        releaseHmacCtx(0, AH);
        releaseCryptoCtx(0, AH);
        releaseCryptoSession(0, AH);
        unload_crypto_module(0, NULL);
        exit_horribly(NULL, "%s\n", errmsg);
    }
}

void initHmacCtx(ArchiveHandle* AH)
{
    int ret = 1;
    Archive* fort = (Archive*)AH;
    char errmsg[MAX_ERRMSG_LEN] = {0};
    ModuleSymmKeyAlgo symmtype;
    ModuleSymmKeyAlgo hmactype;

    transform_type(fort->crypto_type, &symmtype, &hmactype);

    /*不需要计算hmac*/
    if (hmactype == MODULE_ALGO_MAX) {
        fort->cryptoModuleCtx.hmac_ctx = NULL;
        return;
    }

    ret = crypto_hmac_init_use(fort->cryptoModuleCtx.moduleSession, &(fort->cryptoModuleCtx.hmac_ctx), hmactype, fort->Key, fort->keylen);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        crypto_module_sess_exit_use(fort->cryptoModuleCtx.moduleSession);
        exit_horribly(NULL, "%s\n", errmsg);
    }

}

void releaseHmacCtx(int code, void* args)
{
    if (libhandle && ((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.hmac_ctx) {
        crypto_hmac_clean_use(((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.hmac_ctx);
        ((ArchiveHandle*)args)->publicArc.cryptoModuleCtx.hmac_ctx = NULL;
    }
}

void cryptoHmac(ArchiveHandle* AH, char* indata, int inlen, char* outdata, int* outlen)
{
    int ret = 1;
    char errmsg[MAX_ERRMSG_LEN] = {0};

    ret = crypto_hmac_use(AH->publicArc.cryptoModuleCtx.hmac_ctx, (unsigned char*)indata, inlen, (unsigned char*)outdata, (size_t*)outlen);
    if (ret != 1) {
        crypto_get_errmsg_use(NULL, errmsg);
        releaseHmacCtx(0, AH);
        releaseCryptoCtx(0, AH);
        releaseCryptoSession(0, AH);
        unload_crypto_module(0, NULL);
        exit_horribly(NULL, "%s\n", errmsg);
    }
}

void CryptoModuleParamsCheck(ArchiveHandle* AH, const char* params, const char* module_encrypt_mode, const char* module_encrypt_key, const char* module_encrypt_salt, bool is_gen_key)
{
    errno_t rc = 0;
    Archive *fout = (Archive*)AH;

    if (!load_crypto_module_lib()) {
        exit_horribly(NULL, "load crypto module lib failed\n");
    }

    rc = memset_s(fout->crypto_module_params, CRYPTO_MODULE_PARAMS_MAX_LEN, 0x0, CRYPTO_MODULE_PARAMS_MAX_LEN);
    securec_check_c(rc, "\0", "\0");

    rc = memcpy_s((GS_UCHAR*)fout->crypto_module_params, CRYPTO_MODULE_PARAMS_MAX_LEN, params, strlen(params));
    securec_check_c(rc, "\0", "\0");

    if (module_encrypt_mode == NULL) {
        exit_horribly(NULL, "encrypt_mode cannot be NULL\n");
    } else {
        rc = memset_s(fout->crypto_type, CRYPTO_MODULE_ENC_TYPE_MAX_LEN, 0x0, CRYPTO_MODULE_ENC_TYPE_MAX_LEN);
        securec_check_c(rc, "\0", "\0");

        rc = memcpy_s((GS_UCHAR*)fout->crypto_type, CRYPTO_MODULE_ENC_TYPE_MAX_LEN, module_encrypt_mode, strlen(module_encrypt_mode));
        securec_check_c(rc, "\0", "\0");
    }

    if (module_encrypt_salt == NULL || strlen(module_encrypt_salt) != 16) {
        exit_horribly(NULL, "salt is needed and must be 16 bytes\n");
    } else {
        rc = memset_s(fout->rand, RANDOM_LEN, 0x0, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");

        rc = memcpy_s((GS_UCHAR*)fout->rand, RANDOM_LEN + 1, module_encrypt_salt, strlen(module_encrypt_salt));
        securec_check_c(rc, "\0", "\0");
    }

    initCryptoModule(AH);
    initCryptoSession(AH);

    if (module_encrypt_key && is_gen_key) {
        exit_horribly(NULL, "can not use with-key and gen-key together\n");
    } else if (module_encrypt_key) {
        char *tmpkey = NULL;
        unsigned int tmpkeylen = 0;

        tmpkey = SEC_decodeBase64(module_encrypt_key, &tmpkeylen);
        if (tmpkey == NULL || tmpkeylen > KEY_MAX_LEN) {
            if (tmpkey) {
                OPENSSL_free(tmpkey);
            }
            exit_horribly(NULL, "invalid key\n");
        } else {
            rc = memset_s(fout->Key, KEY_MAX_LEN, 0x0, KEY_MAX_LEN);
            securec_check_c(rc, "\0", "\0");

            rc = memcpy_s((GS_UCHAR*)fout->Key, KEY_MAX_LEN, tmpkey, tmpkeylen);
            securec_check_c(rc, "\0", "\0");
            fout->keylen = tmpkeylen;
            OPENSSL_free(tmpkey);
        }
    } else if (is_gen_key){
        if (fout->key_type == KEY_TYPE_PLAINTEXT) {
            exit_horribly(NULL, "forbid to generate plaint key\n");
        } else {
            char *encodedley = NULL;
            symmGenerateKey((ArchiveHandle*)fout);
            encodedley = SEC_encodeBase64((char*)fout->Key, fout->keylen);
            write_msg(NULL, "generate key success:%s\n", encodedley);
            OPENSSL_free(encodedley);
        }
        

    }

    initCryptoKeyCtx((ArchiveHandle*)fout);
    initHmacCtx((ArchiveHandle*)fout);

    fout->encryptfile = true;

}
