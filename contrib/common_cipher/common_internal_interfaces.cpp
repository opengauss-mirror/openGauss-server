#include <pthread.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "common_internal_interfaces.h"
#include "common_err.h"
#include "common_utils.h"

#define INTERNAL_SHARED_LOCK 0
#define INTERNAL_EXCLUSIVE_LOCK 1

#define SET_JNTA_CFG_PATH(path) (setenv("TASS_GHVSM_CFG_PATH", path, 1))

#define INTERNAL_RETURN(ret)    \
{                               \
    if (ret == INTERNAL_OK)     \
        return CRYPT_MOD_OK;    \
    else                        \
        return ret;             \
}while(0)

/*dlopen句柄*/
static void *driverhandle = NULL;

/*密码模块接口*/
static ModuleInterfaces *moduleinterfaces = NULL;

/*定义一个锁，对driverhandle和moduleinterfaces做读写保护*/
static pthread_rwlock_t drivermutex;

/*设备句柄*/
static void *devicehandle = NULL;

/*定义一个锁，对devicehandle做读写保护*/
static pthread_rwlock_t devicehandlemutex;

/*记录当前空闲的最小索引号，在第一次生成密钥时获取并设置。这个需要保证密钥都是从该接口库生成*/
static int currnetindex = 0;

/*定义一个锁，保证同一时刻只有一个线程在生成密钥*/
static pthread_mutex_t genkeymutex;


static int internal_padding(char *indata, int inlen, char *outdata, int *outlen);
static int internal_unpadding(char *indata, int *inlen);

static void internal_lock(int loketype, pthread_rwlock_t *lock)
{
    if (loketype == INTERNAL_SHARED_LOCK) {
        pthread_rwlock_rdlock(lock);
    } else if (loketype == INTERNAL_EXCLUSIVE_LOCK){
        pthread_rwlock_wrlock(lock);
    } else {
        /*do nothing*/
    }
}

static void internal_unlock(pthread_rwlock_t *lock)
{
    pthread_rwlock_unlock(lock);
}

ModuleType get_current_module_type()
{
    internal_lock(INTERNAL_SHARED_LOCK, &drivermutex);

    if (moduleinterfaces) {
        internal_unlock(&drivermutex);
        return moduleinterfaces->type;
    }

    internal_unlock(&drivermutex);

    return MODULE_INVALID_TYPE;
}

static int load_standard_interfaces(ModuleStandardInterfaces *standardinterfaces)
{
    standardinterfaces->InternalOpenDevice = (StandardOpenDevice_type)dlsym(driverhandle, "SDF_OpenDevice");
    standardinterfaces->InternalCloseDevice = (StandardCloseDevice_type)dlsym(driverhandle, "SDF_CloseDevice");
    standardinterfaces->InternalOpenSession = (StandardOpenSession_type)dlsym(driverhandle, "SDF_OpenSession");
    standardinterfaces->InternalCloseSession = (StandardCloseSession_type)dlsym(driverhandle, "SDF_CloseSession");
    standardinterfaces->InternalGenerateRandom = (StandardGenerateRandom_type)dlsym(driverhandle, "SDF_GenerateRandom");
    standardinterfaces->InternalEncrypt = (StandardEncrypt_type)dlsym(driverhandle, "SDF_Encrypt");
    standardinterfaces->InternalDecrypt = (StandardDecrypt_type)dlsym(driverhandle, "SDF_Decrypt");
    standardinterfaces->InternalDestroyKey = (StandardDestroyKey_type)dlsym(driverhandle, "SDF_DestroyKey");
    standardinterfaces->InternalHashInit = (StandardHashInit_type)dlsym(driverhandle, "SDF_HashInit");
    standardinterfaces->InternalHashUpdate = (StandardHashUpdate_type)dlsym(driverhandle, "SDF_HashUpdate");
    standardinterfaces->InternalHashFinal = (StandardHashFinal_type)dlsym(driverhandle, "SDF_HashFinal");

    if (standardinterfaces->InternalOpenDevice == NULL
        || standardinterfaces->InternalCloseDevice == NULL
        || standardinterfaces->InternalOpenSession == NULL
        || standardinterfaces->InternalCloseSession == NULL
        || standardinterfaces->InternalGenerateRandom == NULL
        || standardinterfaces->InternalEncrypt == NULL
        || standardinterfaces->InternalDecrypt == NULL
        || standardinterfaces->InternalDestroyKey == NULL
        || standardinterfaces->InternalHashInit == NULL
        || standardinterfaces->InternalHashUpdate == NULL
        || standardinterfaces->InternalHashFinal == NULL) {

        return CRYPTO_MOD_DLSYM_ERR;
    }

    return CRYPT_MOD_OK;
}

static int load_gdac_interfaces(ModuleGdacInterfaces* gdacinterfaces)
{
    gdacinterfaces->GDACGenerateKEK = (GDACGenerateKEK_type)dlsym(driverhandle, "SDFE_GenerateKEK");
    gdacinterfaces->GDACHMAC = (GDACHMAC_type)dlsym(driverhandle, "SDFE_Hmac");
    gdacinterfaces->GDACEncryptWithIndex = (GDACEncryptWithIndex_type)dlsym(driverhandle, "SDFE_Encrypt");
    gdacinterfaces->GDACDecryptWithIndex = (GDACDecryptWithIndex_type)dlsym(driverhandle, "SDFE_Decrypt");
    gdacinterfaces->GDACExportKEK = (GDACExportKEK_type)dlsym(driverhandle, "SDFE_ExportKEK");
    gdacinterfaces->GDACGetkeyState = (GDACGetkeyState_type)dlsym(driverhandle, "SDFE_GetkeyState");

    if (gdacinterfaces->GDACGenerateKEK == NULL
        || gdacinterfaces->GDACHMAC == NULL
        || gdacinterfaces->GDACEncryptWithIndex == NULL
        || gdacinterfaces->GDACDecryptWithIndex == NULL
        || gdacinterfaces->GDACExportKEK == NULL
        || gdacinterfaces->GDACGetkeyState == NULL) {

        return CRYPTO_MOD_DLSYM_ERR;
    }

    return CRYPT_MOD_OK;
}

static int load_jnta_interfaces(ModuleJntaInterfaces* jntainterfaces)
{
    jntainterfaces->JNTACalculateHmac = (JNTACalculateHmac_type)dlsym(driverhandle, "Tass_CalculateHmac");
    jntainterfaces->JNTAGenerateSymmKeyWithLMK = (JNTAGenerateSymmKeyWithLMK_type)dlsym(driverhandle, "Tass_GenerateSymmKeyWithLMK");
    jntainterfaces->JNTAImportKeyCipherByLMK = (JNTAImportKeyCipherByLMK_type)dlsym(driverhandle, "Tass_ImportKeyCipherByLMK");

    if (jntainterfaces->JNTACalculateHmac == NULL
        || jntainterfaces->JNTAGenerateSymmKeyWithLMK == NULL
        || jntainterfaces->JNTAImportKeyCipherByLMK == NULL) {

        return CRYPTO_MOD_DLSYM_ERR;
    }

    return CRYPT_MOD_OK;
}

static int load_swxa_interfaces(ModuleSwxaInterfaces* swxainterfaces)
{
    swxainterfaces->SWXAGenerateKeyCipher = (SWXAGenerateKeyCipher_type)dlsym(driverhandle, "SDF_GenerateKey_Cipher");
    swxainterfaces->SWXAEncKeyEncrypt = (SWXAEncKeyEncrypt_type)dlsym(driverhandle, "SDF_EncKeyEncrypt");
    swxainterfaces->SWXAEncKeyDecrypt = (SWXAEncKeyDecrypt_type)dlsym(driverhandle, "SDF_EncKeyDecrypt");
    swxainterfaces->SWXAHMAC = (SWXAHMAC_type)dlsym(driverhandle, "SDF_EncKeyHMAC");
    swxainterfaces->SWXAOpenDeviceWithPathAndName = (SWXAOpenDeviceWithPathAndName_type)dlsym(driverhandle, "SDF_OpenDeviceWithPathAndName");

    if (swxainterfaces->SWXAGenerateKeyCipher == NULL
        || swxainterfaces->SWXAEncKeyEncrypt == NULL
        || swxainterfaces->SWXAEncKeyDecrypt == NULL
        || swxainterfaces->SWXAHMAC == NULL
        || swxainterfaces->SWXAOpenDeviceWithPathAndName == NULL) {

        return CRYPTO_MOD_DLSYM_ERR;
    }

    return CRYPT_MOD_OK;
}

/*加载驱动库*/
int load_module_driver(ModuleParams moduleparams)
{

    int ret = CRYPT_MOD_OK;

    /*加写锁*/
    internal_lock(INTERNAL_EXCLUSIVE_LOCK, &drivermutex);

    /*检查是否已经加载,如果已经加载过，则直接返回*/
    if (driverhandle && moduleinterfaces) {
        internal_unlock(&drivermutex);
        return CRYPT_MOD_OK;
    }

    /*dlopen动态库*/
    driverhandle = dlopen(moduleparams.libpath, RTLD_LAZY);
    if (driverhandle == NULL) {
        internal_unlock(&drivermutex);
        return CRYPTO_MOD_DLOPEN_ERR;
    }

    /*为接口分配空间*/
    moduleinterfaces = (ModuleInterfaces*)malloc(sizeof(ModuleInterfaces));

    moduleinterfaces->type = moduleparams.moduletype;

    moduleinterfaces->standardInterfaces = (ModuleStandardInterfaces*)malloc(sizeof(ModuleStandardInterfaces));

    /*加载标准接口*/
    ret = load_standard_interfaces(moduleinterfaces->standardInterfaces);
    if (ret != CRYPT_MOD_OK) {
        free(moduleinterfaces->standardInterfaces);
        free(moduleinterfaces);
        internal_unlock(&drivermutex);
        return ret;
    }

    /*加载扩展接口*/
    switch (moduleparams.moduletype)
    {
    case MODULE_GDAC_CARD_TYPE:
        moduleinterfaces->extendInterfaces.gdacInterfaces = (ModuleGdacInterfaces*)malloc(sizeof(ModuleGdacInterfaces));
        ret = load_gdac_interfaces(moduleinterfaces->extendInterfaces.gdacInterfaces);
        break;
    case MODULE_JNTA_KMS_TYPE:
        moduleinterfaces->extendInterfaces.jntaInterfaces = (ModuleJntaInterfaces*)malloc(sizeof(ModuleJntaInterfaces));
        ret = load_jnta_interfaces(moduleinterfaces->extendInterfaces.jntaInterfaces);
        break;
    case MODULE_SWXA_KMS_TYPE:
        moduleinterfaces->extendInterfaces.swxaInterfaces = (ModuleSwxaInterfaces*)malloc(sizeof(ModuleSwxaInterfaces));
        ret = load_swxa_interfaces(moduleinterfaces->extendInterfaces.swxaInterfaces);
        break;
    default:
        free(moduleinterfaces->standardInterfaces);
        free(moduleinterfaces);
        internal_unlock(&drivermutex);
        return CRYPTO_MOD_TYPE_INVALID_ERR;
    }

    if (ret != CRYPT_MOD_OK) {
        if (moduleinterfaces->extendInterfaces.gdacInterfaces)
            free(moduleinterfaces->extendInterfaces.gdacInterfaces);
        if (moduleinterfaces->extendInterfaces.jntaInterfaces)
            free(moduleinterfaces->extendInterfaces.jntaInterfaces);
        if (moduleinterfaces->extendInterfaces.swxaInterfaces)
            free(moduleinterfaces->extendInterfaces.swxaInterfaces);
        free(moduleinterfaces->standardInterfaces);
        free(moduleinterfaces);
        moduleinterfaces = NULL;
        internal_unlock(&drivermutex);
        return ret;
    }

    internal_unlock(&drivermutex);
    return CRYPT_MOD_OK;

}

int unload_module_driver()
{
    /*加写锁*/
    internal_lock(INTERNAL_EXCLUSIVE_LOCK, &drivermutex);
    if (driverhandle == NULL || moduleinterfaces) {

        internal_unlock(&drivermutex);
        return CRYPTO_MOD_UNLOAD_ERR;
    }

    /*释放接口空间*/

    if (moduleinterfaces->extendInterfaces.gdacInterfaces) {
        free(moduleinterfaces->extendInterfaces.gdacInterfaces);
        moduleinterfaces->extendInterfaces.gdacInterfaces = NULL;
    }

    if (moduleinterfaces->extendInterfaces.jntaInterfaces) {
        free(moduleinterfaces->extendInterfaces.jntaInterfaces);
        moduleinterfaces->extendInterfaces.jntaInterfaces = NULL;
    }

    if (moduleinterfaces->extendInterfaces.swxaInterfaces) {
        free(moduleinterfaces->extendInterfaces.swxaInterfaces);
        moduleinterfaces->extendInterfaces.swxaInterfaces = NULL;
    }

    free(moduleinterfaces->standardInterfaces);
    moduleinterfaces->standardInterfaces = NULL;

    free(moduleinterfaces);
    moduleinterfaces = NULL;

    dlclose(driverhandle);
    driverhandle = NULL;

    internal_unlock(&drivermutex);

    return CRYPT_MOD_OK;

}

int internal_open_device(char* cfg_path)
{
    /*这里调用密码硬件的接口，需要注意硬件接口返回0表示成功。*/
    int ret = INTERNAL_OK;

    /*判断是否已加载驱动并加载驱动接口*/
    internal_lock(INTERNAL_SHARED_LOCK, &drivermutex);

    if (driverhandle == NULL || moduleinterfaces == NULL) {
        internal_unlock(&drivermutex);
        return CRYPTO_MOD_NOT_LOADED_ERR;
    }

    /*大胆一些，在这里释放掉锁*/
    internal_unlock(&drivermutex);

    /*对设备句柄加写锁*/
    internal_lock(INTERNAL_EXCLUSIVE_LOCK, &devicehandlemutex);

    /*已经打开过，直接返回*/
    if (devicehandle != NULL) {
        internal_unlock(&devicehandlemutex);
        return CRYPT_MOD_OK;
    }

    switch (moduleinterfaces->type)
    {
    case MODULE_GDAC_CARD_TYPE:
        ret = moduleinterfaces->standardInterfaces->InternalOpenDevice(&devicehandle);
        break;
    case MODULE_SWXA_KMS_TYPE:
        ret = moduleinterfaces->extendInterfaces.swxaInterfaces->SWXAOpenDeviceWithPathAndName((unsigned char*)cfg_path, &devicehandle);
        break;
    case MODULE_JNTA_KMS_TYPE:
        /*江南天安从环境变量获取配置文件路径，这里设置一下*/
        SET_JNTA_CFG_PATH(cfg_path);
        ret = moduleinterfaces->standardInterfaces->InternalOpenDevice(&devicehandle);
        break;
    default:
        internal_unlock(&devicehandlemutex);
        return CRYPTO_MOD_TYPE_INVALID_ERR;
    }

    internal_unlock(&devicehandlemutex);

    INTERNAL_RETURN(ret);

}

int internal_close_device()
{
    int ret = INTERNAL_OK;

    internal_lock(INTERNAL_EXCLUSIVE_LOCK, &devicehandlemutex);

    if (devicehandle != NULL) {
        ret = moduleinterfaces->standardInterfaces->InternalCloseDevice(devicehandle);
        devicehandle = NULL;
    }

    internal_unlock(&devicehandlemutex);

    INTERNAL_RETURN(ret);
}

int internal_open_session(void **sess)
{
    int ret = INTERNAL_OK;

    internal_lock(INTERNAL_SHARED_LOCK, &devicehandlemutex);

    if (devicehandle == NULL) {
        internal_unlock(&devicehandlemutex);
        return CRYPTO_MOD_NOT_OPENDEVICE_ERR;
    }

    ret = moduleinterfaces->standardInterfaces->InternalOpenSession(devicehandle, sess);

    internal_unlock(&devicehandlemutex);

    INTERNAL_RETURN(ret);
}

int internal_close_session(void *sess)
{
    int ret = INTERNAL_OK;

    if (sess == NULL) {
        return CRYPTO_MOD_NOT_OPENSESSION_ERR;
    }

    internal_lock(INTERNAL_SHARED_LOCK, &devicehandlemutex);

    if (devicehandle == NULL) {
        internal_unlock(&devicehandlemutex);
        return CRYPTO_MOD_NOT_OPENDEVICE_ERR;
    }

    ret = moduleinterfaces->standardInterfaces->InternalCloseSession(sess);

    internal_unlock(&devicehandlemutex);

    INTERNAL_RETURN(ret);
}

static bool use_key_index()
{
    switch (moduleinterfaces->type) {
        case MODULE_GDAC_CARD_TYPE:
        case MODULE_JNTA_KMS_TYPE:
            return true;
        case MODULE_SWXA_KMS_TYPE:
            return false;
        default:
            return false;
    }
}

static int init_gdac_key_index(void* sess)
{
    int keystatus = 0;
    int i =0;
    int ret = INTERNAL_OK;

    /*需要获取当前存在密钥的最大索引值*/
    for (i = INTERNAL_MIN_INDEX_NUM; i <= INTERNAL_MAX_INDEX_NUM; i++) {
        ret = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACGetkeyState(sess, GDAC_KEK_TYPE, (unsigned int)i, (unsigned int*)&keystatus);
        if (ret != INTERNAL_OK) {
            return ret;
        }
        if (keystatus == GDAC_KEY_NOT_EXIST) {
            currnetindex = i;
            break;
        }
    }

    if (i == (INTERNAL_MAX_INDEX_NUM + 1)) {
        return INTERNAL_NOBUFFER;
    }

    return INTERNAL_OK;
}

static int init_jnta_key_index(void* sess)
{
    int ret = INTERNAL_OK;
    int i = 0;
    InternalKeyCtx *keyctx = NULL;

    unsigned char tmpdata[] = {"12345678"};
    int tmplen = 8;

    unsigned char tmpiv[] = {"1234567812345678"};
    int tmpivlen = 16;

    unsigned char tmpout[32] = {0};
    unsigned int tmpoutlen = 0;	
    keyctx = (InternalKeyCtx*)malloc(sizeof(InternalKeyCtx));

    keyctx->session = sess;

    keyctx->enc = INTERNAL_DO_ENC;

    keyctx->algotype = TA_SM4;
    keyctx->algomode = TA_CBC;

    /*需要获取当前存在密钥的最大索引值*/
    for (i = INTERNAL_MIN_INDEX_NUM; i <= INTERNAL_MAX_INDEX_NUM; i++) {
        sprintf((char *)(keyctx->key), "%d",i);
        keyctx->keysize = strlen((char*)(keyctx->key));
        /*没有获取密钥状态接口，通过做加密判断错误发为密钥不存在*/
        ret = internal_symm_encrypt(keyctx, tmpdata, tmplen, tmpiv, tmpivlen, tmpout, &tmpoutlen, NULL);
        if (ret == INTERNAL_KEYNOTEXIST) {
            currnetindex = i;
            break;
        }
        memset(keyctx->key, 0x0, INTERNAL_MAX_KEY_LEN);
    }

    free(keyctx);

    if (i == (INTERNAL_MAX_INDEX_NUM + 1)) {
        return INTERNAL_NOBUFFER;
    }

    return INTERNAL_OK;

}
static int init_current_key_index(void* sess)
{
    int ret = INTERNAL_OK;

    switch (moduleinterfaces->type) {
        case MODULE_GDAC_CARD_TYPE:
            ret = init_gdac_key_index(sess);
            break;
        case MODULE_JNTA_KMS_TYPE: 
            ret = init_jnta_key_index(sess);
            break;	
        case MODULE_SWXA_KMS_TYPE:
        default:
            break;
    }

    return ret;

}


int internal_generate_symm_key(void* sess, ModuleSymmKeyAlgo algo, unsigned char* key, unsigned int* keylen)
{
    int ret = INTERNAL_OK;
    bool useindex = use_key_index();

    if (sess == NULL) {
        return CRYPTO_MOD_NOT_OPENSESSION_ERR;
    }

    if (useindex) {
        pthread_mutex_lock(&genkeymutex);
        if (currnetindex == 0) {
            ret = init_current_key_index(sess);
            if (ret != INTERNAL_OK) {
                pthread_mutex_unlock(&genkeymutex);
                return ret;
            }
        } else if (currnetindex == (INTERNAL_MAX_INDEX_NUM + 1)) {
            pthread_mutex_unlock(&genkeymutex);
            return INTERNAL_NOBUFFER;
        }
    }

    switch(moduleinterfaces->type) {
        case MODULE_GDAC_CARD_TYPE:
            ret = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACGenerateKEK(sess, get_key_len_by_algo_type(algo), currnetindex);
            break;
        case MODULE_JNTA_KMS_TYPE: {
                unsigned int realalgo;
                unsigned int realmode;
                unsigned char tmpcipher[INTERNAL_MAX_KEY_LEN + 1] = {0};
                unsigned int tmpcipherlen = 0;
                unsigned char kcv[8] = {0};
                unsigned int kcvlen = 0;

                ret = get_real_symm_algo_type(moduleinterfaces->type, algo, &realalgo, &realmode);
                if (ret != CRYPT_MOD_OK)
                    break;
                ret = moduleinterfaces->extendInterfaces.jntaInterfaces->JNTAGenerateSymmKeyWithLMK(sess, (TA_SYMM_ALG)realalgo, tmpcipher, &tmpcipherlen, kcv, &kcvlen);
                if (ret != INTERNAL_OK)
                    break;
                ret = moduleinterfaces->extendInterfaces.jntaInterfaces->JNTAImportKeyCipherByLMK(sess, currnetindex, 0, 0, (TA_SYMM_ALG)realalgo, 0, NULL, 0, NULL, 0, tmpcipher, tmpcipherlen, kcv, 0);
                if (ret != INTERNAL_OK)
                    break;
                break;
            }
        case MODULE_SWXA_KMS_TYPE:
            ret = moduleinterfaces->extendInterfaces.swxaInterfaces->SWXAGenerateKeyCipher(sess, get_key_len_by_algo_type(algo), key, keylen);
            break;
        default:
            ret = CRYPTO_MOD_TYPE_INVALID_ERR;
            break;
    }

    if (useindex) {
        if (ret != CRYPT_MOD_OK && ret != INTERNAL_OK) {
            pthread_mutex_unlock(&genkeymutex);
            return ret;
        } else {
            sprintf((char*)key, "%d",currnetindex);
            *keylen = strlen((char*)key);
            currnetindex++;
            pthread_mutex_unlock(&genkeymutex);
            return CRYPT_MOD_OK;
        }
    }

    if (ret != CRYPT_MOD_OK && ret != INTERNAL_OK) {
        return ret;
    } else {
        return CRYPT_MOD_OK;
    }

}

int internal_symm_encrypt(void *keyctx, unsigned char *indata, unsigned int inlen, unsigned char *iv, unsigned int ivlen, unsigned char *outdata, unsigned int *outlen, unsigned char *tag)
{
    int ret = INTERNAL_OK;

    InternalKeyCtx *tmpctx = (InternalKeyCtx *)keyctx;
    unsigned char *paddingdata = NULL;
    int paddinglen = 0;
    unsigned char tmpiv[INTERNAL_IV_LEN + 1] = {0};

    memcpy(tmpiv, iv, ivlen);
    paddinglen = inlen + INTERNAL_BLOCK_LEN + 1;
    paddingdata = (unsigned char *)malloc(paddinglen);

    memset(paddingdata, 0x0, paddinglen);
    internal_padding((char*)indata, inlen, (char*)paddingdata, &paddinglen);

    switch (moduleinterfaces->type) {
        case MODULE_GDAC_CARD_TYPE: {
                unsigned char tmpkey[INTERNAL_MAX_KEY_LEN] = {0};
                unsigned int keylen = 0;
                ret = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACExportKEK(tmpctx->session, atoi((char*)tmpctx->key), tmpkey, &keylen);
                if (ret != INTERNAL_OK)
                    return ret;
                ret = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACEncryptWithIndex(tmpctx->session, tmpkey, keylen, tmpctx->algotype, tmpiv, ivlen, paddingdata, paddinglen, outdata, outlen, NULL, 0, NULL);
                break;
            }
        case MODULE_JNTA_KMS_TYPE: {
                int tmpindex = atoi((char*)(tmpctx->key));
                unsigned int standardtype = 0;
                transform_jnta_algo_type(tmpctx->algotype, tmpctx->algomode, &standardtype);
                ret = moduleinterfaces->standardInterfaces->InternalEncrypt(tmpctx->session, (void*)&tmpindex, standardtype, tmpiv, paddingdata, paddinglen, outdata, outlen);
                break;
            }
        case MODULE_SWXA_KMS_TYPE:
            ret = moduleinterfaces->extendInterfaces.swxaInterfaces->SWXAEncKeyEncrypt(tmpctx->session, tmpctx->key, tmpctx->keysize, tmpctx->algotype, tmpiv, paddingdata, paddinglen, outdata, outlen);
            break;
        default:
            ret = CRYPTO_MOD_TYPE_INVALID_ERR;
            break;;
    }

    free(paddingdata);

    INTERNAL_RETURN(ret);

}

int internal_symm_decrypt(void *keyctx, unsigned char *indata, unsigned int inlen, unsigned char *iv, unsigned int ivlen, unsigned char *outdata, unsigned int *outlen, unsigned char *tag)
{
    int ret = INTERNAL_OK;

    InternalKeyCtx *tmpctx = (InternalKeyCtx *)keyctx;
    unsigned char tmpiv[INTERNAL_IV_LEN + 1] = {0};

    memcpy(tmpiv, iv, ivlen);

    switch (moduleinterfaces->type) {
        case MODULE_GDAC_CARD_TYPE: {
                unsigned char tmpkey[INTERNAL_MAX_KEY_LEN] = {0};
                unsigned int keylen = 0;
                ret = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACExportKEK(tmpctx->session, atoi((char*)tmpctx->key), tmpkey, &keylen);
                if (ret != INTERNAL_OK)
                    return ret;
                ret = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACDecryptWithIndex(tmpctx->session, tmpkey, keylen, tmpctx->algotype, tmpiv, ivlen, indata, inlen, outdata, outlen, NULL, 0, NULL);
                break;
            }
        case MODULE_JNTA_KMS_TYPE: {
                int index = atoi((char *)(tmpctx->key));
                unsigned int standardtype = 0;
                transform_jnta_algo_type(tmpctx->algotype, tmpctx->algomode, &standardtype);
                ret = moduleinterfaces->standardInterfaces->InternalDecrypt(tmpctx->session, (void*)&index, standardtype, tmpiv, indata, inlen, outdata, outlen);
                break;
            }
        case MODULE_SWXA_KMS_TYPE:
                ret = moduleinterfaces->extendInterfaces.swxaInterfaces->SWXAEncKeyDecrypt(tmpctx->session, tmpctx->key, tmpctx->keysize, tmpctx->algotype, tmpiv, indata, inlen, outdata, outlen);
                break;
        default:
                ret = CRYPTO_MOD_TYPE_INVALID_ERR;
                break;
    }

    if (ret == INTERNAL_OK) {
        return internal_unpadding((char*)outdata, (int*)outlen);
    }

    INTERNAL_RETURN(ret);

}

int internal_digest(void *sess, ModuleDigestAlgo algo, unsigned char * indata, unsigned int inlen, unsigned char *outdata, unsigned int *outlen)
{
    int ret = CRYPT_MOD_OK;
    unsigned int realtype = 0;
    int position = 0;
    int updatelen = 0;

    ret = get_real_digest_algo_type(moduleinterfaces->type, algo, &realtype);
    if (ret != CRYPT_MOD_OK) {
        return ret;
    }

    ret = moduleinterfaces->standardInterfaces->InternalHashInit(sess, realtype, NULL, NULL, 0);
    if (ret != INTERNAL_OK)
        return ret;

    while (inlen) {
        if (inlen >= INTERNAL_MSG_BLOCK_LEN) {
            updatelen = INTERNAL_MSG_BLOCK_LEN;
            inlen -= INTERNAL_MSG_BLOCK_LEN;
        } else {
            updatelen = inlen;
            inlen = 0;
        }

        ret = moduleinterfaces->standardInterfaces->InternalHashUpdate(sess, indata + position, updatelen);
        if (ret != INTERNAL_OK)
            return ret;
        position += updatelen;
    }

    ret = moduleinterfaces->standardInterfaces->InternalHashFinal(sess, outdata, outlen);

    INTERNAL_RETURN(ret);
}

/*如果传入的data为NULL，并且data_size为0，则对key自身做hmac*/
int internal_hmac(void *ctx, unsigned char * data, unsigned int data_size, unsigned char *result, size_t *result_size)
{
    int ret = INTERNAL_OK;

    InternalKeyCtx *tmpctx = (InternalKeyCtx *)ctx;

    switch (moduleinterfaces->type) {
        case MODULE_GDAC_CARD_TYPE:{
            unsigned char tmpkey[INTERNAL_MAX_KEY_LEN] = {0};
            unsigned int keylen = 0;
            ret = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACExportKEK(tmpctx->session, atoi((char*)tmpctx->key), tmpkey, &keylen);
            if (ret != INTERNAL_OK)
                return ret;
            ret  = moduleinterfaces->extendInterfaces.gdacInterfaces->GDACHMAC(tmpctx->session, tmpkey, keylen, tmpctx->algotype, 
                (data != NULL) ? data : tmpkey, (data_size != 0) ? data_size : keylen, result, (unsigned int*)result_size);
            memset(tmpkey,0x0,INTERNAL_MAX_KEY_LEN);
            break;
        }
        case MODULE_JNTA_KMS_TYPE:{
            ret = moduleinterfaces->extendInterfaces.jntaInterfaces->JNTACalculateHmac(tmpctx->session, (TA_HMAC_ALG)tmpctx->algotype, atoi((char*)tmpctx->key),
                NULL, 0, (data != NULL) ? data : tmpctx->key, (data_size != 0) ? data_size : tmpctx->keysize, result, (unsigned int*)result_size);
            break;
        }
        case MODULE_SWXA_KMS_TYPE:{
            ret =  moduleinterfaces->extendInterfaces.swxaInterfaces->SWXAHMAC(tmpctx->session, tmpctx->key, tmpctx->keysize, tmpctx->algotype,
                (data != NULL) ? data : tmpctx->key, (data_size != 0) ? data_size : tmpctx->keysize, result, (unsigned int*)result_size);
            break;
        }
        default:
            ret = CRYPTO_MOD_TYPE_INVALID_ERR;
            break;
    }

    INTERNAL_RETURN(ret);

}

int internal_generate_random(void *sess, char *buffer, size_t size)
{
    int ret = INTERNAL_OK;

    ret = moduleinterfaces->standardInterfaces->InternalGenerateRandom(sess, size, (unsigned char*)buffer);

    INTERNAL_RETURN(ret);
}


/*第一位补0x80,后面补0x00,强补，即如果inlen正好是16的整数倍，则补一个0x80和15个0x00，如果只缺一，则只补一个0x80*/
static int internal_padding(char *indata, int inlen, char *outdata, int *outlen)
{
    int i = 0;
    int firstpad = 0x80;
    int secondpad = 0x00;

    int paddlen = 0;

    paddlen = INTERNAL_BLOCK_LEN - inlen%INTERNAL_BLOCK_LEN;

    memcpy(outdata, indata, inlen);


    for (i = 0; i < paddlen; i++) {
        if (i == 0)
            outdata[inlen + i] = firstpad;
        else
            outdata[inlen + i] = secondpad;
    }

    *outlen = inlen + paddlen;

    return CRYPT_MOD_OK;
}

static int internal_unpadding(char *indata, int *inlen)
{
    int firstpad = 0x80;
    int secondpad = 0x00;
    int tmplen = 0;

    tmplen = *inlen - 1;

    while (*(indata + tmplen) == secondpad) {
        tmplen--;
    }

    if (tmplen >= ((*inlen) - INTERNAL_BLOCK_LEN) && *(unsigned char*)(indata + tmplen) == firstpad) {
        *inlen = tmplen;
    } else {
        return CRYPTO_MOD_UNPADDING_ERR;
    }

    indata[tmplen] = '\0';

    return CRYPT_MOD_OK;
}


