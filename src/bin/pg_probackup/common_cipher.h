#ifndef COMMON_CIPHER_H
#define COMMON_CIPHER_H
#include <pthread.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include "utils/aes.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_CRYPTO_MODULE_LEN 8192
#define CRYPTO_BLOCK_SIZE 16
#define MAX_ENCRYPT_LEN (MAX_CRYPTO_MODULE_LEN - CRYPTO_BLOCK_SIZE) /*加密算法补pad模式为强补，最多可以补16字节，所以最大加密长度少16字节，则密文最长8192、保证读取时可以整块密文读入*/

#define MAX_PROVIDER_NAME_LEN 128
#define MAX_ERRMSG_LEN 256
#define MAX_IV_LEN 16
#define MAX_HMAC_LEN 32

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

typedef int (*crypto_encrypt_decrypt_type)(void *ctx, int enc, unsigned char *data, size_t data_size, unsigned char *iv, size_t iv_size, unsigned char *result, size_t *result_size, unsigned char *tag);
typedef int (*crypto_create_symm_key_type)(void *sess, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t *key_id_size);
typedef int (*crypto_get_errmsg_type)(void *sess, char *errmsg);
typedef int (*crypto_ctx_init_type)(void *sess, void **ctx, ModuleSymmKeyAlgo algo, int enc, unsigned char *key_id, size_t key_id_size);
typedef int (*crypto_hmac_init_type)(void *sess, void **ctx, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t key_id_size);
typedef int (*crypto_hmac_type)(void *ctx, unsigned char * data, size_t data_size, unsigned char *result, size_t *result_size);

extern crypto_create_symm_key_type crypto_create_symm_key_use;
extern crypto_encrypt_decrypt_type crypto_encrypt_decrypt_use;
extern crypto_get_errmsg_type crypto_get_errmsg_use;
extern crypto_ctx_init_type crypto_ctx_init_use;
extern crypto_hmac_init_type crypto_hmac_init_use;
extern crypto_hmac_type crypto_hmac_use;

extern int transform_type(const char* type);
extern int getHmacType(ModuleSymmKeyAlgo algo);
extern int transform_hmac_type(const char* type);
extern bool load_crypto_module_lib();
extern void unload_crypto_module();
extern void initCryptoModule(char* crypto_module_params, const char* encrypt_mode, int* key_type);
extern void initCryptoSession(void** crypto_module_session);
extern void releaseCryptoSession(void* crypto_module_session);
extern void releaseCryptoCtx(void* crypto_module_keyctx);
extern void clearCrypto(void* crypto_module_session, void* crypto_module_keyctx, void* crypto_hmac_keyctx);
extern void CryptoModuleParamsCheck(bool gen_key, char* params, const char* module_encrypt_mode, const char* module_encrypt_key, const char* module_encrypt_salt, int* key_type);

#ifdef __cplusplus
}
#endif

#endif /*COMMON_CIPHER_H*/