#ifndef COMMON_INTERNAL_INTERFACES_H
#define COMMON_INTERNAL_INTERFACES_H

#include "common_algo.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ECCref_MAX_BITS 512
#define ECCref_MAX_LEN ((ECCref_MAX_BITS+7) / 8)

/* ECC key data public key structure definition */
typedef struct ECCrefPublicKey_st
{
    unsigned int bits;
    unsigned char x[ECCref_MAX_LEN];
    unsigned char y[ECCref_MAX_LEN];
} ECCrefPublicKey;

typedef int (*StandardOpenDevice_type)(void**);
typedef int (*StandardCloseDevice_type)(void*);
typedef int (*StandardOpenSession_type)(void*, void**);
typedef int (*StandardCloseSession_type)(void*);
typedef int (*StandardGenerateRandom_type)(void*, unsigned int, unsigned char*);
typedef int (*StandardHashInit_type)(void*, unsigned int, ECCrefPublicKey*, unsigned char*, unsigned int);
typedef int (*StandardHashUpdate_type)(void*, unsigned char*, unsigned int);
typedef int (*StandardHashFinal_type)(void*, unsigned char*, unsigned int*);
typedef int (*StandardEncrypt_type)(void*, void*, unsigned int, unsigned char*, unsigned char*, unsigned int, unsigned char*, unsigned int*);
typedef int (*StandardDecrypt_type)(void*, void*, unsigned int, unsigned char*, unsigned char*, unsigned int, unsigned char*, unsigned int*);
typedef int (*StandardDestroyKey_type)(void*, void*);


/*SDF标准接口*/
typedef struct {
    StandardOpenDevice_type InternalOpenDevice;             /*打开设备*/
    StandardCloseDevice_type InternalCloseDevice;           /*关闭设备*/
    StandardOpenSession_type InternalOpenSession;           /*打开会话*/
    StandardCloseSession_type InternalCloseSession;         /*关闭会话*/
    StandardGenerateRandom_type InternalGenerateRandom;     /*生成随机数*/
    StandardHashInit_type InternalHashInit;                 /*哈希初始化*/
    StandardHashUpdate_type InternalHashUpdate;             /*哈希update*/
    StandardHashFinal_type InternalHashFinal;               /*哈希结束*/
    StandardEncrypt_type InternalEncrypt;                   /*使用密钥句柄加密*/
    StandardDecrypt_type InternalDecrypt;                   /*使用密钥句柄解密*/
    StandardDestroyKey_type InternalDestroyKey;             /*销毁密钥句柄*/
}ModuleStandardInterfaces;


typedef int (*GDACHMAC_type)(void*, unsigned char*, unsigned int, unsigned int, unsigned char*, unsigned int, unsigned char*, unsigned int*);
typedef int (*GDACGenerateKEK_type)(void*, unsigned int, unsigned int);
typedef int (*GDACEncryptWithIndex_type)(void*, unsigned char*, unsigned int, unsigned int, unsigned char*, unsigned int, unsigned char*, unsigned int, unsigned char*, unsigned int*, unsigned char*, unsigned int, unsigned char*);
typedef int (*GDACDecryptWithIndex_type)(void*, unsigned char*, unsigned int, unsigned int, unsigned char*, unsigned int, unsigned char*, unsigned int, unsigned char*, unsigned int*, unsigned char*, unsigned int, unsigned char*);
typedef int (*GDACExportKEK_type)(void*, unsigned int, void*, unsigned int*);
typedef int (*GDACGetkeyState_type)(void*, unsigned int, unsigned int, unsigned int*);

/*光电安辰扩展接口*/
typedef struct {
    GDACHMAC_type GDACHMAC;                                     /*hmac*/
    GDACGenerateKEK_type GDACGenerateKEK;                       /*生成指定索引密钥*/
    GDACEncryptWithIndex_type GDACEncryptWithIndex;             /*使用索引密钥加密*/
    GDACDecryptWithIndex_type GDACDecryptWithIndex;             /*使用索引密钥解密*/
    GDACExportKEK_type GDACExportKEK;                           /*导出KEK,不对外提供,内做索引转密钥使用*/
    GDACGetkeyState_type GDACGetkeyState;                       /*获取密钥状态*/
}ModuleGdacInterfaces;


typedef int (*SWXAOpenDeviceWithPathAndName_type)(unsigned char*, void **);
typedef int (*SWXAHMAC_type)(void *, unsigned char *, unsigned int, unsigned int, unsigned char *, unsigned int, unsigned char *,unsigned int*);
typedef int (*SWXAGenerateKeyCipher_type)(void*, unsigned int, unsigned char*, unsigned int*);
typedef int (*SWXAEncKeyEncrypt_type)(void*, unsigned char*, unsigned int, unsigned int, unsigned char*, unsigned char*, unsigned int, unsigned char*, unsigned int*);
typedef int (*SWXAEncKeyDecrypt_type)(void*, unsigned char*, unsigned int, unsigned int, unsigned char*, unsigned char*, unsigned int, unsigned char*, unsigned int*);

/*三未信安扩展接口*/
typedef struct {
    SWXAOpenDeviceWithPathAndName_type SWXAOpenDeviceWithPathAndName;   /*指定配置文件路径和名称打开设备*/
    SWXAHMAC_type SWXAHMAC;                                             /*hmac*/
    SWXAGenerateKeyCipher_type SWXAGenerateKeyCipher;                   /*生成密钥密文*/
    SWXAEncKeyEncrypt_type SWXAEncKeyEncrypt;                           /*使用密钥密文加密*/
    SWXAEncKeyDecrypt_type SWXAEncKeyDecrypt;                           /*使用密钥密文解密*/
}ModuleSwxaInterfaces;


typedef int (*JNTACalculateHmac_type)(void *, TA_HMAC_ALG, unsigned int, const unsigned char*, unsigned int, unsigned char*, unsigned int, unsigned char*, unsigned int*);
typedef int (*JNTAGenerateSymmKeyWithLMK_type)(void *, TA_SYMM_ALG, unsigned char*, unsigned int*, unsigned char*,unsigned int*);
typedef int (*JNTAImportKeyCipherByLMK_type)(void*, unsigned int, int, int, TA_SYMM_ALG, int, const unsigned char*, unsigned int, const unsigned char*, unsigned int,
            const unsigned char*, unsigned int, const unsigned char symmKcv[8], unsigned int);


/*江南天安扩展接口*/
typedef struct {
    JNTACalculateHmac_type JNTACalculateHmac;                       /*hmac*/
    JNTAGenerateSymmKeyWithLMK_type JNTAGenerateSymmKeyWithLMK;     /*生成LMK加密的密钥密文*/
    JNTAImportKeyCipherByLMK_type JNTAImportKeyCipherByLMK;         /*导入LMK加密的密钥密文到指定的索引位置*/
}ModuleJntaInterfaces;


/*硬件提供的所有接口*/
typedef struct {
    ModuleType type;
    ModuleStandardInterfaces *standardInterfaces;
    union {
        ModuleGdacInterfaces *gdacInterfaces;
        ModuleSwxaInterfaces *swxaInterfaces;
        ModuleJntaInterfaces *jntaInterfaces;
    }extendInterfaces;
}ModuleInterfaces;

extern int load_module_driver(ModuleParams moduleparams);
extern int unload_module_driver();
extern int internal_open_device(char* cfg_path);
extern int internal_close_device();
extern int internal_open_session(void **sess);
extern int internal_close_session(void *sess);
extern ModuleType get_current_module_type();
extern int internal_generate_symm_key(void* sess, ModuleSymmKeyAlgo algo, unsigned char* key, unsigned int* keylen);
extern int internal_symm_encrypt(void *keyctx, unsigned char *indata, unsigned int inlen, unsigned char *iv, unsigned int ivlen, unsigned char *outdata, unsigned int *outlen, unsigned char *tag);
extern int internal_symm_decrypt(void *keyctx, unsigned char *indata, unsigned int inlen, unsigned char *iv, unsigned int ivlen, unsigned char *outdata, unsigned int *outlen, unsigned char *tag);
extern int internal_digest(void *sess, ModuleDigestAlgo algo, unsigned char * indata, unsigned int inlen, unsigned char *outdata, unsigned int *outlen);
extern int internal_hmac(void *ctx, unsigned char * data, unsigned int data_size, unsigned char *result, long unsigned int *result_size);
extern int internal_generate_random(void *sess, char *buffer, long unsigned int size);

#ifdef __cplusplus
}
#endif

#endif /* COMMON_INTERNAL_INTERFACES_H */
