#include "common_cipher.h"
#include "common_err.h"
#include "common_utils.h"
#include "common_internal_interfaces.h"

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
int crypto_module_init(char *load_info, SupportedFeature *supported_feature)
{
    int ret = CRYPT_MOD_OK;

    ModuleParams moduleparams;

    memset(&moduleparams, 0x0, sizeof(ModuleParams));

    /* 1.解析load_info获取加载密码模块的参数*/
    ret = parse_module_params(load_info, &moduleparams);
    if (ret != CRYPT_MOD_OK) {
        goto err;
    }

    /* 2.根据密码模块类型填充supported_feature*/
    ret = get_supported_feature(moduleparams.moduletype, supported_feature);
    if (ret != CRYPT_MOD_OK) {

        goto err;
    }

    /* 3.dlopen打开密码模块驱动库，并加载接口函数*/
    ret = load_module_driver(moduleparams);
    if (ret != CRYPT_MOD_OK) {
        goto err;
    }

    /* 4.打开设备*/
    ret = internal_open_device(moduleparams.cfgfilepath);
    if (ret != CRYPT_MOD_OK) {
        goto err;
    }

    return CRYPT_MOD_OK;
err:
    /*可能已经打开/加载，尝试释放*/
    (void)internal_close_device();
    (void)unload_module_driver();
    set_thread_errno(ret);
    return ret;

}

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
int crypto_module_sess_init(char *key_info, void **sess)
{
    int ret = CRYPT_MOD_OK;

    ret = internal_open_session(sess);
    if (ret != CRYPT_MOD_OK) {
        set_thread_errno(ret);
    }

    return ret;
}

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
void crypto_module_sess_exit(void *sess)
{
    int ret = CRYPT_MOD_OK;

    ret = internal_close_session(sess);
    if (ret != CRYPT_MOD_OK) {
        set_thread_errno(ret);
    }

}

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
int crypto_create_symm_key(void *sess, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t *key_id_size)
{
    int ret = CRYPT_MOD_OK;

    ret = internal_generate_symm_key(sess, algo, key_id, (unsigned int*)key_id_size);
    if (ret != CRYPT_MOD_OK) {
        set_thread_errno(ret);
    }

    return ret;
}

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
int crypto_ctx_init(void *sess, void **ctx, ModuleSymmKeyAlgo algo, int enc, unsigned char *key_id, size_t key_id_size)
{
    InternalKeyCtx *keyctx = NULL;
    int ret = CRYPT_MOD_OK;

    if (sess == NULL) {
        return CRYPTO_MOD_NOT_OPENSESSION_ERR;
    }

    if (enc != INTERNAL_DO_DEC && enc != INTERNAL_DO_ENC) {
        return CRYPTO_MOD_INVALID_CRYPTO_TYPE_ERR;
    }

    if (key_id == NULL || key_id[0] == '\0' || key_id_size <= 0 || key_id_size > INTERNAL_MAX_KEY_LEN) {
        return CRYPTO_MOD_INVALID_KEY_ERR;
    }

    keyctx = (InternalKeyCtx*)malloc(sizeof(InternalKeyCtx));

    keyctx->session = sess;

    ret = get_real_symm_algo_type(get_current_module_type(), algo, &(keyctx->algotype), &(keyctx->algomode));
    if (ret != CRYPT_MOD_OK) {
        free(keyctx);
        return ret;
    }

    keyctx->enc = enc;

    memset(keyctx->key, 0x0, sizeof(keyctx->key));
    memcpy(keyctx->key, key_id, key_id_size);
    keyctx->keysize = key_id_size;

    *ctx = keyctx;

    return CRYPT_MOD_OK;
}

/** 获取数据加解密后的数据长度
 *
 * @param[in]    
 *    ctx             加解密上下文信息
 *    enc             加密1、解密0
 *    data_size       返回加解密结果长度  
 * @return    返回数据长度
 *
 */
int crypto_result_size(void *ctx, int enc, size_t data_size)
{
    if (ctx == NULL) {
        return CRYPTO_MOD_INVALID_KEY_CTX_ERR;
    }

    if (enc == INTERNAL_DO_DEC) {
        return data_size;
    } else if (enc == INTERNAL_DO_ENC) {
        return (data_size + INTERNAL_BLOCK_LEN);
    } else
        return CRYPTO_MOD_INVALID_CRYPTO_TYPE_ERR;
}

/** 密钥上下文清理 
 *
 * @param[in]    
 *    ctx             加解密上下文信息
 * @param[out]   
 *    
 * @return    成功返回CRYPT_MOD_OK，失败返回错误码
 *
 */
void crypto_ctx_clean(void *ctx)
{
    if (ctx) {
        free(ctx);
        ctx = NULL;
    }
}

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
int crypto_encrypt_decrypt(void *ctx, int enc, unsigned char *data, size_t data_size, unsigned char *iv, size_t iv_size, unsigned char *result, size_t *result_size, unsigned char *tag)
{
    int ret = CRYPT_MOD_OK;

    if ((enc == INTERNAL_DO_ENC && *result_size < (data_size + INTERNAL_BLOCK_LEN)) 
        || (enc == INTERNAL_DO_DEC && *result_size < data_size)) {
        set_thread_errno(CRYPTO_MOD_NOT_ENOUGH_SPACE_ERR);
        return CRYPTO_MOD_NOT_ENOUGH_SPACE_ERR;
    }

    if (enc == INTERNAL_DO_ENC) {
        ret = internal_symm_encrypt(ctx, data, data_size, iv, iv_size, result, (unsigned int*)result_size, tag);
    } else if (enc == INTERNAL_DO_DEC) {
        ret = internal_symm_decrypt(ctx, data, data_size, iv, iv_size, result, (unsigned int*)result_size, tag);
    }

    if (ret != CRYPT_MOD_OK) {
        set_thread_errno(ret);
    }

    return ret;

}

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
int crypto_digest(void *sess, ModuleDigestAlgo algo, unsigned char * data, size_t data_size,unsigned char *result, size_t *result_size)
{
    int ret = CRYPT_MOD_OK;

    ret = internal_digest(sess, algo, data, data_size, result, (unsigned int*)result_size);
    if (ret != CRYPT_MOD_OK) {
        set_thread_errno(ret);
    }

    return ret;
}

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
int crypto_hmac_init(void *sess, void **ctx, ModuleSymmKeyAlgo algo, unsigned char *key_id, size_t key_id_size)
{
    InternalKeyCtx *keyctx = NULL;
    int ret = CRYPT_MOD_OK;

    if (sess == NULL) {
        return CRYPTO_MOD_NOT_OPENSESSION_ERR;
    }

    if (key_id == NULL || key_id[0] == '\0' || key_id_size <= 0 || key_id_size > INTERNAL_MAX_KEY_LEN) {
        return CRYPTO_MOD_INVALID_KEY_ERR;
    }

    keyctx = (InternalKeyCtx*)malloc(sizeof(InternalKeyCtx));


    keyctx->session = sess;

    ret = get_real_symm_algo_type(get_current_module_type(), algo, &(keyctx->algotype), &(keyctx->algomode));
    if (ret != CRYPT_MOD_OK) {
        free(keyctx);
        return ret;
    }

    memcpy(keyctx->key, key_id, key_id_size);
    keyctx->keysize = key_id_size;

    *ctx = keyctx;

    return ret;
}

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
void crypto_hmac_clean(void *ctx)
{
    if (ctx) {
        free(ctx);
        ctx = NULL;
    }
}

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
int crypto_hmac(void *ctx, unsigned char * data, size_t data_size, unsigned char *result, size_t *result_size)
{
    int ret = CRYPT_MOD_OK;

    ret = internal_hmac(ctx, data, data_size, result, result_size);
    if (ret != CRYPT_MOD_OK)
        set_thread_errno(ret);

    return ret;
}

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
int crypto_gen_random(void *sess, char *buffer, size_t size)
{
    int ret = CRYPT_MOD_OK;

    ret = internal_generate_random(sess, buffer, size);
    if (ret != CRYPT_MOD_OK)
        set_thread_errno(ret);

    return ret;

}

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
int crypto_deterministic_enc_dec(void *sess, int enc, unsigned char *data, unsigned char *key_id,    size_t key_id_size, size_t data_size, unsigned char *result, size_t *result_size)
{
    int ret = CRYPT_MOD_OK;
    void *tmpkeyctx = NULL;
    unsigned char tmpiv[INTERNAL_IV_LEN + 1] = {0};

    ret = crypto_ctx_init(sess, &tmpkeyctx, MODULE_SM4_CBC, enc, key_id, key_id_size);
    if (ret != CRYPT_MOD_OK) {
        set_thread_errno(ret);
        return ret;
    }

    /*加密：对明文计算hmac作为iv，并将hmac放到密文串头；
     *解密：从密文头获取iv和hmac，并对解密出的明文计算hmac进行校验*/

    if (enc == INTERNAL_DO_ENC) {
        void *tmphmacctx = NULL;
        unsigned char tmphmac[INTERNAL_HMAC_LEN] = {0};
        long unsigned int hmaclen = 0;

        /*计算iv */
        ret = crypto_hmac_init(sess, &tmphmacctx, MODULE_HMAC_SM3, key_id, key_id_size);
        if (ret != CRYPT_MOD_OK) {
            set_thread_errno(ret);
            crypto_ctx_clean(tmpkeyctx);
            return ret;
        }

        ret = crypto_hmac(tmphmacctx, data, data_size, tmphmac, &hmaclen);
        if (ret != CRYPT_MOD_OK) {
            set_thread_errno(ret);
            crypto_ctx_clean(tmphmacctx);
            crypto_ctx_clean(tmpkeyctx);
            return ret;
        }

        memcpy(tmpiv, tmphmac, INTERNAL_IV_LEN);

        /*把hmac放到密文头 */
        memcpy(result, tmphmac, INTERNAL_HMAC_LEN);

        crypto_hmac_clean(tmphmacctx);


        ret = internal_symm_encrypt(tmpkeyctx, data, data_size, tmpiv, INTERNAL_IV_LEN, result + INTERNAL_HMAC_LEN, (unsigned int*)result_size, NULL);
        if (ret != CRYPT_MOD_OK) {
            set_thread_errno(ret);
        }

        crypto_ctx_clean(tmpkeyctx);

        *result_size += INTERNAL_HMAC_LEN;

        return ret;

    } else if (enc == INTERNAL_DO_DEC){
        void *tmphmacctx = NULL;
        unsigned char tmphmac[INTERNAL_HMAC_LEN] = {0};
        long unsigned int hmaclen = 0;
        unsigned char verifyhmac[INTERNAL_HMAC_LEN] = {0};
        /*获取iv */
        memcpy(tmpiv, data, INTERNAL_IV_LEN);

        /*获取hmac*/
        memcpy(verifyhmac, data, INTERNAL_HMAC_LEN);

        ret = internal_symm_decrypt(tmpkeyctx, data + INTERNAL_HMAC_LEN, data_size - INTERNAL_HMAC_LEN, tmpiv, INTERNAL_IV_LEN, result, (unsigned int*)result_size, NULL);
        if (ret != CRYPT_MOD_OK) {
            set_thread_errno(ret);
            crypto_ctx_clean(tmpkeyctx);
            return  ret;
        } 

        crypto_ctx_clean(tmpkeyctx);

        /*计算明文hmac */
        ret = crypto_hmac_init(sess, &tmphmacctx, MODULE_HMAC_SM3, key_id, key_id_size);
        if (ret != CRYPT_MOD_OK) {
            set_thread_errno(ret);
            return ret;
        }

        ret = crypto_hmac(tmphmacctx, result, *result_size, tmphmac, &hmaclen);
        if (ret != CRYPT_MOD_OK) {
            set_thread_errno(ret);
            crypto_ctx_clean(tmphmacctx);
            return ret;
        }

         crypto_hmac_clean(tmphmacctx);

        /*校验明文hmac值 */
        if (strncmp((char*)tmphmac, (char*)verifyhmac, INTERNAL_HMAC_LEN)) {
            set_thread_errno(CRYPTO_MOD_DETERMINISTIC_DEC_VERIFY_ERR);
            return CRYPTO_MOD_DETERMINISTIC_DEC_VERIFY_ERR;
        }

        return ret;
    }

    return ret;

}

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
int crypto_get_errmsg(void *sess, char *errmsg)
{
    strcpy(errmsg, common_get_errmsg());

    return CRYPT_MOD_OK;
}


