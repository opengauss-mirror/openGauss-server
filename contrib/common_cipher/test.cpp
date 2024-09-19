#include<pthread.h>
#include<dlfcn.h>
#include<stdlib.h>
#include<stdio.h>
#include<string.h>

#define MAX_PROVIDER_NAME_LEN 128
#define MAX_ERRMSG_LEN 256

static pthread_rwlock_t drivermutex;

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
typedef int (*crypto_encrypt_decrypt_type)(void *ctx, int enc, unsigned char *data, size_t data_size, unsigned char *iv, size_t iv_size, unsigned char *result, size_t *result_size, unsigned char *tag);
typedef int (*crypto_digest_type)(void *sess, ModuleDigestAlgo algo, unsigned char * data, size_t data_size,unsigned char *result, size_t *result_size);
typedef int (*crypto_hmac_init_type)(void *sess, void **ctx, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t key_id_size);
typedef void (*crypto_hmac_clean_type)(void *ctx);
typedef int (*crypto_hmac_type)(void *ctx, unsigned char * data, size_t data_size, unsigned char *result, size_t *result_size);
typedef int (*crypto_gen_random_type)(void *sess, char *buffer, size_t size);
typedef int (*crypto_deterministic_enc_dec_type)(void *sess, int enc, unsigned char *data, unsigned char *key_id,    size_t key_id_size, size_t data_size, unsigned char *result, size_t *result_size);
typedef int (*crypto_get_errmsg_type)(void *sess, char *errmsg);


void *libhandle = NULL;
crypto_module_init_type crypto_module_init_use = NULL;
crypto_module_sess_init_type crypto_module_sess_init_use = NULL;
crypto_module_sess_exit_type crypto_module_sess_exit_use = NULL;
crypto_create_symm_key_type crypto_create_symm_key_use = NULL;
crypto_ctx_init_type crypto_ctx_init_use = NULL;
crypto_result_size_type crypto_result_size_use = NULL;
crypto_ctx_clean_type crypto_ctx_clean_use = NULL;
crypto_encrypt_decrypt_type crypto_encrypt_decrypt_use = NULL;
crypto_digest_type crypto_digest_use = NULL;
crypto_hmac_init_type crypto_hmac_init_use = NULL;
crypto_hmac_clean_type crypto_hmac_clean_use = NULL;
crypto_hmac_type crypto_hmac_use = NULL;
crypto_gen_random_type crypto_gen_random_use = NULL;
crypto_deterministic_enc_dec_type crypto_deterministic_enc_dec_use = NULL;
crypto_get_errmsg_type crypto_get_errmsg_use = NULL;

static void load_lib()
{
	libhandle = dlopen("/home//vastbase/contrib/common_cipher/libcommoncipher.so", RTLD_LAZY);
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
	
}

static void* one_thread_func(void *data)
{
	int ret = 1;
	int i = 0;
	char options[] = {"MODULE_TYPE=JNTAKMS,MODULE_LIB_PATH=/home//vastbase/contrib/common_cipher/libTassSDF4GHVSM.so,MODULE_CONFIG_FILE_PATH=/home//vastbase/contrib/common_cipher/"};
	SupportedFeature supportedfeature;
	char errmsg[MAX_ERRMSG_LEN] = {0};
	void *session = NULL;
	unsigned char key[32] = {0};
	long unsigned  int keylen = 0;
	void *keyctx = NULL;
	unsigned char srcdata[] = {"12345678"};
	unsigned char plaint[32] = {0};
	long unsigned int plaintlen = 32;
	unsigned char encdata[32] = {0};
	long unsigned int enclen = 32;
	unsigned char encdata2[32] = {0};
	long unsigned int enclen2 = 32;
	unsigned char hashdata[32] = {0};
	long unsigned int hashlen = 0;
	unsigned char hmacdata[32] = {0};
	long unsigned int hmaclen = 0;
	long unsigned int needlen = 16;
	char random[32] = {0};
	unsigned char iv[] = {"1234567812345678"};

	ret = crypto_module_init_use(options, &supportedfeature);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_module_init error,errmsg:%s\n", errmsg);
		return NULL;
	} else {
		printf("crypto_module_init success\n");
		printf("provider_name = %s,key_type = %d\n",supportedfeature.provider_name,supportedfeature.key_type);
		for (i = 0; i < MODULE_ALGO_MAX;i++) {
			if (supportedfeature.supported_symm[i] == 1)
				printf("supported_symm[%d]\n",i);
		}
		for (i = 0; i < MODULE_DIGEST_MAX;i++) {
			if (supportedfeature.supported_digest[i] == 1)
				printf("supported_digest[%d]\n",i);
		}
	}

	ret = crypto_module_sess_init_use(NULL, &session);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_module_sess_init error,errmsg:%s\n", errmsg);
		return NULL;
	} else {
		printf("crypto_module_sess_init success\n");
	}

	ret = crypto_create_symm_key_use(session, MODULE_SM4_CBC, key, &keylen);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_create_symm_key_use error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_create_symm_key_use success\n");
		printf("key = %s\n",key);
	}

	ret = crypto_ctx_init_use(session, &keyctx, MODULE_SM4_CBC, 1, key, keylen);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_ctx_init_use error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_ctx_init_use success\n");
	}

	ret = crypto_encrypt_decrypt_use(keyctx, 1, srcdata, 8, iv, 16, encdata, &enclen, NULL);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_encrypt_decrypt_use enc error,errmsg:%s\n", errmsg);
		crypto_ctx_clean_use(keyctx);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_encrypt_decrypt_use enc success\n");
	}

	ret = crypto_encrypt_decrypt_use(keyctx, 0, encdata, enclen, iv, 16, plaint, &plaintlen, NULL);
	if (ret != 1 || strcmp((char*)plaint, (char*)srcdata))
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_encrypt_decrypt_use dec error,errmsg:%s\n", errmsg);
		crypto_ctx_clean_use(keyctx);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_encrypt_decrypt_use dec success\n");
	}

	printf("enc nedd len:%d\n",crypto_result_size_use(keyctx, 1, needlen));

	printf("dec nedd len:%d\n",crypto_result_size_use(keyctx, 0, needlen));

	crypto_ctx_clean_use(keyctx);

	ret = crypto_digest_use(session, MODULE_SM3, plaint, plaintlen, hashdata, &hashlen);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_digest_use  error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return  NULL;
	} else {
		printf("crypto_digest_use success\n");
		printf("hashlen = %ld\n", hashlen);
	}

	ret = crypto_hmac_init_use(session, &keyctx, MODULE_HMAC_SM3, key, keylen);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_hmac_init_use  error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_hmac_init_use success\n");
	}

	ret = crypto_hmac_use(keyctx, plaint, plaintlen, hmacdata, &hmaclen);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_hmac_use  error,errmsg:%s\n", errmsg);
		crypto_hmac_clean_use(keyctx);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_hmac_use success\n");
		printf("hmaclen = %ld\n", hmaclen);
	}

	crypto_hmac_clean_use(keyctx);

	ret = crypto_gen_random_use(session, random, 31);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_gen_random_use  error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_gen_random_use success\n");
	}

	ret = crypto_deterministic_enc_dec_use(session, 1, plaint, key, keylen, plaintlen, encdata, &enclen);
	if (ret != 1)
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_deterministic_enc_dec_use1 enc  error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_deterministic_enc_dec_use1 enc success\n");
	}

	ret = crypto_deterministic_enc_dec_use(session, 1, plaint, key, keylen, plaintlen, encdata2, &enclen2);
	if (ret != 1 || strcmp((char*)encdata, (char*)encdata2))
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_deterministic_enc_dec_use2 enc  error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_deterministic_enc_dec_use2 enc success\n");
	}

	ret = crypto_deterministic_enc_dec_use(session, 0, encdata2, key, keylen, enclen2, plaint, &plaintlen);
	if (ret != 1 || strcmp((char*)plaint, (char*)srcdata))
	{
		crypto_get_errmsg_use(NULL, errmsg);
		printf("crypto_deterministic_enc_dec_use2 dec  error,errmsg:%s\n", errmsg);
		crypto_module_sess_exit_use(session);
		return NULL;
	} else {
		printf("crypto_deterministic_enc_dec_use2 dec success\n");
	}

	crypto_get_errmsg_use(NULL, errmsg);
	printf("crypto_get_errmsg_use errmsg:%s\n", errmsg);

	crypto_module_sess_exit_use(session);

	return NULL;
}

int main()
{
	pthread_t t1, t2, t3, t4;

	load_lib();

	pthread_create(&t1,0,one_thread_func,NULL);
	pthread_create(&t2,0,one_thread_func,NULL);
	pthread_create(&t3,0,one_thread_func,NULL);
	pthread_create(&t4,0,one_thread_func,NULL);
	pthread_join(t1,NULL);
	pthread_join(t2,NULL);
	pthread_join(t3,NULL);
	pthread_join(t4,NULL);
	
    return 0;
}
