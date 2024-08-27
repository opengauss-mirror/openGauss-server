#ifndef COMMON_CIPHER_H
#define COMMON_CIPHER_H
#include <pthread.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

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


/** 初始化密码模块 
 *
 * @param[in]    
 *    load_info            密码模块相关信息（硬件设备口令，硬件设备、硬件库路径等），通过kv方式传入
 *
 * @param[out]   
 *    supported_feature    返回当前密码模块支持的加密方式，参考上述结构体
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 * 示例：
 * 用户设置GUC参数crypto_module_info = 'enable_crypto_module=on,module_third_msg=aaa'
 * 传入接口时load_info = 'aaa'
 */
int crypto_module_init(char *load_info, SupportedFeature *supported_feature);

/** 会话中连接密码模块 
 *
 * @param[in]    
 *    key_info        密码相关信息（用户口令等信息），通过kv方式传入
 *
 * @param[out]   
 *    sess            会话信息
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 * 示例：
 * 用户设置GUC参数tde_key_info = 'keyType=third_kms, keyThirdMsg =bbb'
 * 传入接口时key_info = 'bbb'
 */
int crypto_module_sess_init(char *key_info, void **sess);

/** 会话中断开连接密码模块 
 *
 * @param[in]    
 *    sess            会话信息
 *
 * @param[out]   
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
void crypto_module_sess_exit(void *sess);

/** 创建密钥 
 *
 * @param[in]    
 *    sess            会话信息
 *    algo            密钥使用场景的算法
 *
 * @param[out]   
 *    key_id          返回生成密钥/密钥ID/密钥密文
 *    key_id_size     返回生成内容长度 
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_create_symm_key(void *sess, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t *key_id_size);

/** 密钥上下文初始化，后续进行加解密可直接使用上下文 
 *
 * @param[in]    
 *    sess            会话信息
 *    algo            加密算法
 *    enc             加密1、解密0
 *    key_id          密码信息
 *    key_id_size     密码信息长度
 * @param[out]   
 *    ctx             返回使用密钥信息  
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_ctx_init(void *sess, void **ctx, ModuleSymmKeyAlgo algo, int enc, unsigned char *key_id, size_t key_id_size);

/** 获取数据加解密后的数据长度
 *
 * @param[in]    
 *    ctx             加解密上下文信息
 *    enc             加密1、解密0
 *    data_size       返回加解密结果长度  
 * @return    返回数据长度
 *
 */
int crypto_result_size(void *ctx, int enc, size_t data_size);

/** 密钥上下文清理 
 *
 * @param[in]    
 *    ctx             加解密上下文信息
 * @param[out]   
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
void crypto_ctx_clean(void *ctx);

/** 执行加解密 
 *
 * @param[in]    
 *    ctx             加解密上下文信息
 *    enc             加密1、解密0
 *    data            原数据信息
 *    data_size       原数据长度
 *    iv              iv信息
 *    iv_size         iv信息长度
 *    tag             GCM模式的校验值
 * @param[out]   
 *    result          返回结果信息  
 *    result_size     返回结果信息长度 
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_encrypt_decrypt(void *ctx, int enc, unsigned char *data, size_t data_size, unsigned char *iv, size_t iv_size, unsigned char *result, size_t *result_size, unsigned char *tag);

/** 计算摘要 
 *
 * @param[in]    
 *    sess            会话信息
 *    algo            摘要算法
 *    data            原数据信息
 *    data_size       原数据长度
 * @param[out]   
 *    result          返回结果信息  
 *    result_size     返回结果信息长度 
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_digest(void *sess, ModuleDigestAlgo algo, unsigned char * data, size_t data_size,unsigned char *result, size_t *result_size);

/** hmac初始化 
 *
 * @param[in]    
 *    sess            会话信息
 *    algo            摘要算法
 *    key_id          密码信息
 *    key_id_size     密码信息长度
 * @param[out]   
 *    ctx             返回密钥上下文 
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_hmac_init(void *sess, void **ctx, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t key_id_size);

/** hmac清理 
 *
 * @param[in]    
 *    ctx             密钥上下文信息
 *    
 * @param[out]   
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
void crypto_hmac_clean(void *ctx);

/** 执行hmac计算 
 *
 * @param[in]    
 *    ctx             密钥上下文信息
 *    data            原数据信息
 *    data_size       原数据长度
 * @param[out]   
 *    result          返回结果信息  
 *    result_size     返回结果信息长度 
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_hmac(void *ctx, unsigned char * data, size_t data_size, unsigned char *result, size_t *result_size);

/** 生成随机数
 *
 * @param[in]    
 *    sess            会话信息
 *    size            申请的随机信息长度 
 * 
 * @param[out]   
 *    buffer          返回随机信息  
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_gen_random(void *sess, char *buffer, size_t size);

/** 执行确定性加解密 
 *
 * @param[in]    
 *    sess            会话信息
 *    enc             加密1、解密0
 *    data            原数据信息
 *    data_size       原数据长度
 *    key_id          密钥信息
 *    key_id_size     密钥信息长度
 * @param[out]   
 *    result          返回结果信息  
 *    result_size     返回结果信息长度 
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_deterministic_enc_dec(void *sess, int enc, unsigned char *data, unsigned char *key_id,    size_t key_id_size, size_t data_size, unsigned char *result, size_t *result_size);

/** 获取报错信息 
 *
 * @param[in]    
 *    sess            会话信息
 * @param[out]   
 *    errmsg          返回结果信息,最长256字节
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
int crypto_get_errmsg(void *sess, char *errmsg);

#ifdef __cplusplus
}
#endif

#endif /*COMMON_CIPHER_H*/