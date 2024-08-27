#include "common_algo.h"
#include "common_cipher.h"
#include "common_err.h"
#include "common_utils.h"

#define SUPPORTED 1
#define UNSUPPORTED 0


static void set_gdac_supported_feature(SupportedFeature *supported_feature)
{
    memcpy(supported_feature->provider_name, MODULE_GDAC_CARD_STR, strlen(MODULE_GDAC_CARD_STR));

    /*光电安辰提供了扩展的生成内部KEK接口，可以支持索引*/
    supported_feature->key_type = KEY_TYPE_NAMEORIDX;

    supported_feature->supported_symm[MODULE_AES_128_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_AES_256_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_SM4_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_HMAC_SHA256] = SUPPORTED;
    supported_feature->supported_symm[MODULE_HMAC_SM3] = SUPPORTED;
    supported_feature->supported_symm[MODULE_DETERMINISTIC_KEY] = SUPPORTED;

    supported_feature->supported_digest[MODULE_SHA256] = SUPPORTED;
    supported_feature->supported_digest[MODULE_SM3] = SUPPORTED;
}

static void set_swxa_supported_feature(SupportedFeature *supported_feature)
{
    memcpy(supported_feature->provider_name, MODULE_SWXA_KMS_STR, strlen(MODULE_SWXA_KMS_STR));

    /*三未信安提供了生成密钥密文的接口，可以支持密钥密文。*/
    supported_feature->key_type = KEY_TYPE_CIPHERTEXT;

    supported_feature->supported_symm[MODULE_AES_128_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_AES_256_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_SM4_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_AES_128_CTR] = SUPPORTED;
    supported_feature->supported_symm[MODULE_AES_256_CTR] = SUPPORTED;
    supported_feature->supported_symm[MODULE_SM4_CTR] = SUPPORTED;
    supported_feature->supported_symm[MODULE_HMAC_SHA256] = SUPPORTED;
    supported_feature->supported_symm[MODULE_HMAC_SM3] = SUPPORTED;
    supported_feature->supported_symm[MODULE_DETERMINISTIC_KEY] = SUPPORTED;

    supported_feature->supported_digest[MODULE_SHA256] = SUPPORTED;
    supported_feature->supported_digest[MODULE_SM3] = SUPPORTED;
}


static void set_jnta_supported_feature(SupportedFeature *supported_feature)
{
    memcpy(supported_feature->provider_name, MODULE_JNTA_KMS_STR, strlen(MODULE_JNTA_KMS_STR));

    /*江南天安提供了生成密钥密文和导入密钥密文到指定索引的接口，可以支持密钥索引。*/
    supported_feature->key_type = KEY_TYPE_NAMEORIDX;

    supported_feature->supported_symm[MODULE_AES_128_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_AES_256_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_SM4_CBC] = SUPPORTED;
    supported_feature->supported_symm[MODULE_HMAC_SHA256] = SUPPORTED;
    supported_feature->supported_symm[MODULE_HMAC_SM3] = SUPPORTED;
    supported_feature->supported_symm[MODULE_DETERMINISTIC_KEY] = SUPPORTED;

    supported_feature->supported_digest[MODULE_SHA256] = SUPPORTED;
    supported_feature->supported_digest[MODULE_SM3] = SUPPORTED;
}

int get_supported_feature(ModuleType type, SupportedFeature *supported_feature)
{
    if (supported_feature == NULL) {
        return CRYPTO_MOD_PARAM_INVALID_ERR;
    }

    memset(supported_feature->provider_name, 0x0, MAX_PROVIDER_NAME_LEN);
    memset(supported_feature->supported_symm, UNSUPPORTED, sizeof(supported_feature->supported_symm));
    memset(supported_feature->supported_digest, UNSUPPORTED, sizeof(supported_feature->supported_digest));
    supported_feature->key_type = KEY_TYPE_INVALID;

    switch (type) {
        case MODULE_GDAC_CARD_TYPE:
            set_gdac_supported_feature(supported_feature);
            break;
        case MODULE_JNTA_KMS_TYPE:
            set_jnta_supported_feature(supported_feature);
            break;
        case MODULE_SWXA_KMS_TYPE:
            set_swxa_supported_feature(supported_feature);
            break;
        default:
            return CRYPTO_MOD_TYPE_INVALID_ERR;
    }

    return CRYPT_MOD_OK;
}

static int get_gdac_symm_algo_type(ModuleSymmKeyAlgo symmalgotype, unsigned int* realtype)
{
    switch (symmalgotype) {
        case MODULE_AES_128_CBC:
            *realtype = GDAC_AES128_CBC;
            break;
        case MODULE_AES_256_CBC:
            *realtype = GDAC_AES256_CBC;
            break;
        case MODULE_SM4_CBC:
            *realtype = GDAC_SM4_CBC;
            break;
        case MODULE_HMAC_SHA256:
            *realtype = GDAC_HMAC_SHA256;
            break;
        case MODULE_HMAC_SM3:
            *realtype = GDAC_HMAC_SM3;
            break;
        default:
            return CRYPTO_MOD_UNSUPPORTED_SYMM_TYPE_ERR;
    }

    return CRYPT_MOD_OK;
}

static int get_swxa_symm_algo_type(ModuleSymmKeyAlgo symmalgotype, unsigned int* realtype)
{
    switch (symmalgotype) {
        case MODULE_AES_128_CBC:
        case MODULE_AES_256_CBC:
            *realtype = SWXA_AES_CBC;
            break;
        case MODULE_AES_128_CTR:
        case MODULE_AES_256_CTR:
            *realtype = SWXA_AES_CTR;
            break;
        case MODULE_SM4_CBC:
            *realtype = SWXA_SMS4_CBC;
            break;
        case MODULE_SM4_CTR:
            *realtype = SWXA_SMS4_CTR;
            break;
        case MODULE_HMAC_SHA256:
            *realtype = SWXA_SHA256;
            break;
        case MODULE_HMAC_SM3:
            *realtype = SWXA_SM3;
            break;
        default:
            return CRYPTO_MOD_UNSUPPORTED_SYMM_TYPE_ERR;
    }

    return CRYPT_MOD_OK;
}

static int get_jnta_symm_algo_type(ModuleSymmKeyAlgo symmalgotype, unsigned int* realtype, unsigned int* realmode)
{
    switch (symmalgotype) {
        case MODULE_AES_128_CBC:
            *realtype = TA_AES128;
            *realmode = TA_CBC;
            break;
        case MODULE_AES_256_CBC:
            *realtype = TA_AES256;
            *realmode = TA_CBC;
            break;
        case MODULE_SM4_CBC:
            *realtype = TA_SM4;
            *realmode = TA_CBC;
            break;
        case MODULE_HMAC_SHA256:
            *realtype = TA_HMAC_SHA256;
            break;
        case MODULE_HMAC_SM3:
            *realtype = TA_HMAC_SM3;
            break;
        default:
            return CRYPTO_MOD_UNSUPPORTED_SYMM_TYPE_ERR;
    }

    return CRYPT_MOD_OK;
}

int get_real_symm_algo_type(ModuleType moduletype, ModuleSymmKeyAlgo symmalgotype, unsigned int* realtype, unsigned int* realmode)
{
    /*严格加解密，不属于硬件密码模块内部算法类型，在库中直接自行处理*/
    if (symmalgotype == MODULE_DETERMINISTIC_KEY) {
        *realtype = MODULE_DETERMINISTIC_KEY;

        return CRYPT_MOD_OK;
    }

    switch (moduletype) {
        case MODULE_GDAC_CARD_TYPE:
            return get_gdac_symm_algo_type(symmalgotype, realtype);
        case MODULE_JNTA_KMS_TYPE:
            return get_jnta_symm_algo_type(symmalgotype, realtype, realmode);
        case MODULE_SWXA_KMS_TYPE:
            return get_swxa_symm_algo_type(symmalgotype, realtype);
        default:
            return CRYPTO_MOD_TYPE_INVALID_ERR;
    }

    return CRYPT_MOD_OK;
}

void transform_jnta_algo_type(unsigned int type, unsigned int mode, unsigned int *standardtype)
{
    switch (type) {
        case TA_AES128:
        case TA_AES256:
            if (mode == TA_CBC) {
                *standardtype = TA_AES_CBC;
            }
            break;
        case TA_SM4:
            if (mode == TA_CBC) {
                *standardtype = TA_SM4_CBC;
            }
            break;
        default:
            break;
    }
}

static int get_gdac_digest_algo_type(ModuleDigestAlgo type, unsigned int* realtype)
{
    switch (type) {
        case MODULE_SHA256:
            *realtype = GDAC_SHA256;
            break;
        case MODULE_SM3:
            *realtype = GDAC_SM3;
            break;
        default:
            return CRYPTO_MOD_UNSUPPORTED_DIGEST_TYPE_ERR;
    }

    return CRYPT_MOD_OK;
}

static int get_swxa_digest_algo_type(ModuleDigestAlgo type, unsigned int* realtype)
{
    switch (type) {
        case MODULE_SHA256:
            *realtype = SWXA_SHA256;
            break;
        case MODULE_SM3:
            *realtype = SWXA_SM3;
            break;
        default:
            return CRYPTO_MOD_UNSUPPORTED_DIGEST_TYPE_ERR;
    }

    return CRYPT_MOD_OK;
}

static int get_jnta_digest_algo_type(ModuleDigestAlgo type, unsigned int* realtype)
{
    switch (type) {
        case MODULE_SHA256:
            *realtype = TA_SHA256;
            break;
        case MODULE_SM3:
            *realtype = TA_SM3;
            break;
        default:
            return CRYPTO_MOD_UNSUPPORTED_DIGEST_TYPE_ERR;
    }

    return CRYPT_MOD_OK;
}

int get_real_digest_algo_type(ModuleType moduletype, ModuleDigestAlgo type, unsigned int* realtype)
{
    switch (moduletype) {
        case MODULE_GDAC_CARD_TYPE:
            return get_gdac_digest_algo_type(type, realtype);
        case MODULE_JNTA_KMS_TYPE:
            return get_jnta_digest_algo_type(type, realtype);
        case MODULE_SWXA_KMS_TYPE:
            return get_swxa_digest_algo_type(type, realtype);
        default:
            return CRYPTO_MOD_TYPE_INVALID_ERR;
    }

    return CRYPT_MOD_OK;
}

int get_key_len_by_algo_type(ModuleSymmKeyAlgo type)
{
    switch (type) {
        case MODULE_AES_128_CBC:
        case MODULE_AES_128_CTR:
        case MODULE_SM4_CBC:
        case MODULE_SM4_CTR:
            return INTERNAL_KEY_128_BITS; 
        case MODULE_AES_256_CBC:
        case MODULE_AES_256_CTR:
            return INTERNAL_KEY_256_BITS;
        default:
            return INTERNAL_KEY_128_BITS;
    }

}
