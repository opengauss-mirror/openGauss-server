#ifndef COMMON_CIPHER_H
#define COMMON_CIPHER_H
#include <pthread.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include "pg_backup_archiver.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_CRYPTO_CACHE_LEN 8192
#define CRYPTO_BLOCK_SIZE 16
#define CRYPTO_HMAC_SIZE 32
/*加密算法补pad模式为强补，最多可以补16字节，所以写缓存少16字节，又因为要带上hmac，需要再少32字节，这样能保证密文最长8192、读取时可以整块密文读入*/
#define MAX_WRITE_CACHE_LEN (MAX_CRYPTO_CACHE_LEN - CRYPTO_BLOCK_SIZE - CRYPTO_HMAC_SIZE)

typedef int (*crypto_encrypt_decrypt_type)(void *ctx, int enc, unsigned char *data, size_t data_size, unsigned char *iv, size_t iv_size, unsigned char *result, size_t *result_size, unsigned char *tag);
typedef int (*crypto_hmac_type)(void *ctx, unsigned char * data, size_t data_size, unsigned char *result, size_t *result_size);

extern crypto_encrypt_decrypt_type crypto_encrypt_decrypt_use;
extern crypto_hmac_type crypto_hmac_use;

extern bool load_crypto_module_lib();
extern void unload_crypto_module(int code, void* args);
extern void initCryptoModule(ArchiveHandle* AH);
extern void initCryptoSession(ArchiveHandle* AH);
extern void releaseCryptoSession(int code, void* args);
extern void initCryptoKeyCtx(ArchiveHandle* AH);
extern void releaseCryptoCtx(int code, void* args);
extern void symmEncDec(ArchiveHandle* AH, bool isEnc, char* indata, int inlen, char* outdata, int* outlen);
extern void symmGenerateKey(ArchiveHandle* AH);
extern void initHmacCtx(ArchiveHandle* AH);
extern void releaseHmacCtx(int code, void* args);
extern void cryptoHmac(ArchiveHandle* AH, char* indata, int inlen, char* outdata, int* outlen);
extern void CryptoModuleParamsCheck(ArchiveHandle* AH, const char* params, const char* module_encrypt_mode, const char* module_encrypt_key, const char* module_encrypt_salt, bool is_gen_key);

#ifdef __cplusplus
}
#endif

#endif /*COMMON_CIPHER_H*/
