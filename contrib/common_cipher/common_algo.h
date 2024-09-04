#ifndef COMMON_ALGO_H
#define COMMON_ALGO_H

#include "common_cipher.h"
#include "common_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

/*密钥索引最大值1024，最小值1*/

#define INTERNAL_MIN_INDEX_NUM      1
#define INTERNAL_MAX_INDEX_NUM      1024

/*光电安辰相关宏定义*/

/*光电安辰卡内密钥类型*/
#define GDAC_KEK_TYPE               9

/*光电安辰卡内密钥状态*/
#define GDAC_KEY_EXIST              0x100000
#define GDAC_KEY_NOT_EXIST          0


/*对称算法模式*/
#define GDAC_MODE_CBC               0x00000002

/*对称算法类型*/
#define GDAC_SM4                    0x00000400
#define GDAC_AES128                 0x00004000
#define GDAC_AES256                 0x00008000

/*对称算法及模式*/
#define GDAC_SM4_CBC                (GDAC_SM4|GDAC_MODE_CBC)
#define GDAC_AES128_CBC             (GDAC_AES128|GDAC_MODE_CBC)
#define GDAC_AES256_CBC             (GDAC_AES256|GDAC_MODE_CBC)

/*摘要算法*/
#define GDAC_SM3                    0x00000001
#define GDAC_SHA256                 0x00000004

/*HMAC算法*/
#define GDAC_HMAC_SM3               0x00100001
#define GDAC_HMAC_SHA256            0x00100004

/*三未信安相关宏定义*/

/*对称算法及模式*/
#define SWXA_SMS4_CBC               0x00000402
#define SWXA_SMS4_CTR               0x00000420

#define SWXA_AES_CBC                0x00002002
#define SWXA_AES_CTR                0x00002020

/*摘要算法，三未的hmac指定算法和摘要算法相同即可*/
#define SWXA_SM3                    0x00000001
#define SWXA_SHA256                 0x00000004

/*江南天安相关*/

typedef enum {
    TA_HMAC_SHA256 = 6,
    TA_HMAC_SM3 = 20,
}TA_HMAC_ALG;

typedef enum {
    TA_AES128 = 3,
    TA_AES256 = 5,
    TA_SM4 = 7,
}TA_SYMM_ALG;

typedef enum {
    TA_CBC = 1,
}TA_SYMM_MODE;

typedef enum {
    TA_SM3 = 1,
    TA_SHA256 = 4,
}TA_HASH_ALG;


#define TA_SM4_CBC              0X00000402
#define TA_AES_CBC              0x80000202

/*内部通用宏定义*/
#define INTERNAL_DO_ENC 1
#define INTERNAL_DO_DEC 0

#define INTERNAL_MAX_KEY_LEN 32

#define INTERNAL_BLOCK_LEN 16

#define INTERNAL_IV_LEN 16

#define INTERNAL_HMAC_LEN 32

#define INTERNAL_MSG_BLOCK_LEN 8192

#define INTERNAL_KEY_128_BITS 128
#define INTERNAL_KEY_256_BITS 256


typedef struct {
    void *session;                              /*和硬件建立的会话*/
    unsigned int algotype;                      /*算法类型*/
    unsigned int algomode;                      /*算法模式，江南天安使用；光电安辰和三未信安algotype已包含模式*/
    int enc;                                    /*0表示解密，1表示加密*/
    unsigned int keysize;                       /*key的长度*/
    unsigned char key[INTERNAL_MAX_KEY_LEN];    /*存储密钥id/密钥密文值/密钥明文值，AES256密钥长度32*/
}InternalKeyCtx;


extern int get_supported_feature(ModuleType type, SupportedFeature *supported_feature);
extern int get_real_symm_algo_type(ModuleType moduletype, ModuleSymmKeyAlgo symmalgotype, unsigned int* realtype, unsigned int* realmode);
extern int get_real_digest_algo_type(ModuleType moduletype, ModuleDigestAlgo type, unsigned int* realtype);
extern int get_key_len_by_algo_type(ModuleSymmKeyAlgo type);
extern void transform_jnta_algo_type(unsigned int type, unsigned int mode, unsigned int *standardtype);

#ifdef __cplusplus
}
#endif

#endif /* COMMON_ALGO_H */
