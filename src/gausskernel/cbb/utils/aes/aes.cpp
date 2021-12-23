/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 * File Name	: aes.cpp
 * Brief		: Database Security: AES encryption algorithm
 * Description	: Concrete realization of the AES encryption algorithm
 * History	        : 2012-8
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/aes/aes.cpp
 * 
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/aes.h"
#include "executor/instrument.h"
#include "cipher.h"

#include "openssl/rand.h"
#include "openssl/evp.h"
#include "openssl/hmac.h"
#include "openssl/obj_mac.h"

#define MAX_INT_NUM 2147483647

#ifdef ENABLE_UT
#define static
#endif

static bool decryptFromFile(FILE* source, DecryptInfo* pDecryptInfo);

/*
 * Target		:Generate random string as aes vector for encryption.
 * Input		:Pointer of empty aes vector.
 * Output		:successed: true, failed: false.
 */
bool init_aes_vector_random(GS_UCHAR* aes_vector, size_t vector_len)
{
    errno_t errorno = EOK;
    int retval = 0;
    GS_UCHAR random_vector[RANDOM_LEN] = {0};

    retval = RAND_priv_bytes(random_vector, RANDOM_LEN);
    if (retval != 1) {
        errorno = memset_s(random_vector, RANDOM_LEN, '\0', RANDOM_LEN);
        securec_check_c(errorno, "", "");
        (void)fprintf(stderr, _("generate random aes vector failed,errcode:%d\n"), retval);
        return false;
    }

    errorno = memcpy_s(aes_vector, vector_len, random_vector, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(random_vector, RANDOM_LEN, '\0', RANDOM_LEN);
    securec_check_c(errorno, "", "");
    return true;
}

/* inputstrlen must include the terminating '\0' character */
bool writeFileAfterEncryption(
    FILE* pf, char* inputstr, int inputstrlen, int writeBufflen, unsigned char Key[], unsigned char* randvalue)
{
    void* writeBuff = NULL;
    int64 writeBuffLen;
    char encryptleninfo[RANDOM_LEN] = {0};
    GS_UINT32 cipherlen = 0;
    GS_UCHAR* outputstr = NULL;
    GS_UINT32 outputlen;
    bool encryptstatus = false;
    errno_t errorno = EOK;

    if ((inputstr == NULL) || inputstrlen <= 0) {
        return false;
    }

    if (pf == NULL) {
        pf = stdout;
    }

    /*
     * cipher text len max is plain text len + RANDOM_LEN(aes128)
     * writeBufflen equals to  ciphertextlen + RANDOM_LEN(rand_vector) + RANDOM_LEN(encrypt_salt).
     * so writeBufflen equals to inputstrlen(palin text len) + 48.
     */
    writeBuffLen = (int64)inputstrlen + RANDOM_LEN * 3;
    if (writeBuffLen >= MAX_INT_NUM) {
        printf("invalid value of inputstrlen!\n");
        return false;
    }
    writeBuff = malloc(writeBuffLen);
    if (NULL == writeBuff) {
        printf("writeBuff memery alloc failed!\n");
        return false;
    }

    outputlen = (size_t)inputstrlen + RANDOM_LEN * 2;
    outputstr = (GS_UCHAR*)malloc(outputlen);
    if (NULL == outputstr) {
        free(writeBuff);
        writeBuff = NULL;
        printf("outputstr memery alloc failed!\n");
        return false;
    }

    errorno = memset_s(writeBuff, writeBuffLen, '\0', writeBuffLen);
    securec_check_c(errorno, "\0", "\0");
    errorno = memset_s(outputstr, outputlen, '\0', outputlen);
    securec_check_c(errorno, "\0", "\0");

    /* put the rand in file first */
    if (0 == strncmp((char*)randvalue, inputstr, RANDOM_LEN)) {
        errorno = memcpy_s((char*)writeBuff, writeBuffLen, inputstr, RANDOM_LEN);
        securec_check_c(errorno, "\0", "\0");
        if (fwrite(writeBuff, (size_t)RANDOM_LEN, 1, pf) != 1) {
            printf("write the randvalue failed.\n");
            free(writeBuff);
            writeBuff = NULL;
            free(outputstr);
            outputstr = NULL;
            return false;
        }
        (void)fflush(pf);
        free(writeBuff);
        writeBuff = NULL;
        free(outputstr);
        outputstr = NULL;
        return true;
    }

    /* the real encrypt operation */
    encryptstatus = aes128Encrypt((GS_UCHAR*)inputstr,
        (GS_UINT32)inputstrlen,
        Key,
        (GS_UINT32)strlen((const char*)Key),
        randvalue,
        outputstr,
        &cipherlen);
    if (!encryptstatus) {
        free(writeBuff);
        writeBuff = NULL;
        free(outputstr);
        outputstr = NULL;
        return false;
    }

    errorno = sprintf_s(encryptleninfo, sizeof(encryptleninfo), "%u", cipherlen);
    securec_check_ss_c(errorno, "\0", "\0");
    errorno = memcpy_s((void*)((char*)writeBuff), writeBuffLen, encryptleninfo, RANDOM_LEN);
    securec_check_c(errorno, "\0", "\0");
    /* the ciphertext contains the real cipher and salt vector used for encrypt  */
    errorno = memcpy_s((void*)((char*)writeBuff + RANDOM_LEN), writeBuffLen - RANDOM_LEN, outputstr, cipherlen);
    securec_check_c(errorno, "\0", "\0");

    /* write the cipherlen info and cipher text into encrypt file. */
    if (fwrite(writeBuff, (unsigned long)(cipherlen + RANDOM_LEN), 1, pf) != 1) {
        printf("write encrypt file failed.\n");
        free(writeBuff);
        free(outputstr);
        writeBuff = NULL;
        outputstr = NULL;
        return false;
    }
    (void)fflush(pf);
    errorno = memset_s(writeBuff, writeBuffLen, '\0', writeBuffLen);
    securec_check_c(errorno, "\0", "\0");

    free(writeBuff);
    free(outputstr);
    writeBuff = NULL;
    outputstr = NULL;

    return true;
}

/* Initalization of DecryptInfo sturcture for decryption */
void initDecryptInfo(DecryptInfo* pDecryptInfo)
{
    errno_t errorno = EOK;
    pDecryptInfo->decryptBuff = NULL;
    errorno = memset_s(pDecryptInfo->currLine, MAX_DECRYPT_BUFF_LEN, '\0', MAX_DECRYPT_BUFF_LEN);
    securec_check_c(errorno, "\0", "\0");
    errorno = memset_s(pDecryptInfo->Key, KEY_MAX_LEN, '\0', KEY_MAX_LEN);
    securec_check_c(errorno, "\0", "\0");
    pDecryptInfo->isCurrLineProcess = false;
    pDecryptInfo->encryptInclude = false;
    pDecryptInfo->randget = false;
    errorno = memset_s(pDecryptInfo->rand, RANDOM_LEN + 1, '\0', RANDOM_LEN + 1);
    securec_check_c(errorno, "\0", "\0");
}
static bool decryptFromFile(FILE* source, DecryptInfo* pDecryptInfo)
{
    int nread = 0;
    GS_UINT32 plainlen = 0;
    GS_UINT64 cipherlen = 0;
    GS_UCHAR cipherleninfo[RANDOM_LEN + 1] = {0};
    GS_UCHAR* ciphertext = NULL;
    GS_UCHAR* outputstr = NULL;
    bool decryptstatus = false;
    errno_t errorno = EOK;

    if (!feof(source) && (false == pDecryptInfo->isCurrLineProcess)) {
        nread = (int)fread((void*)cipherleninfo, 1, RANDOM_LEN, source);
        if (ferror(source)) {
            printf("could not read from input file, errorno: %d\n", ferror(source));
            return false;
        }
        if (!nread) {
            return false;
        }

        /* get the rand value from encryptfile first */
        if (!pDecryptInfo->randget) {
            errorno = memcpy_s(pDecryptInfo->rand, RANDOM_LEN + 1, cipherleninfo, RANDOM_LEN);
            securec_check_c(errorno, "\0", "\0");
            pDecryptInfo->randget = true;
            /* get the cipherinfo for decrypt */
            nread = (int)fread((void*)cipherleninfo, 1, RANDOM_LEN, source);
            if (ferror(source)) {
                printf("could not read from input file, errorno: %d\n", ferror(source));
                return false;
            }
            if (!nread) {
                return false;
            }
        }

        char *tmp = NULL;
        cipherlen = strtoul((char *)cipherleninfo, &tmp, 10);
        if (*tmp != '\0' || cipherlen > PG_UINT32_MAX || cipherlen == 0) {
#ifndef ENABLE_UT
            printf("Invalid cipherlen(%s), maybe the content is corrupt.\n", cipherleninfo);
#endif
            return false;
        }
        ciphertext = (GS_UCHAR*)malloc((size_t)cipherlen);
        if (NULL == ciphertext) {
            printf("memory alloc failed!\n");
            return false;
        }
        outputstr = (GS_UCHAR*)malloc((size_t)cipherlen);
        if (NULL == outputstr) {
            printf("memory alloc failed!\n");
            free(ciphertext);
            ciphertext = NULL;
            return false;
        }

        errorno = memset_s(ciphertext, cipherlen, '\0', cipherlen);
        securec_check_c(errorno, "\0", "\0");
        errorno = memset_s(outputstr, cipherlen, '\0', cipherlen);
        securec_check_c(errorno, "\0", "\0");

        /* read ciphertext from encrypt file. */
        nread = (int)fread((void*)ciphertext, 1, cipherlen, source);
        if (nread) {
            /* the real decrypt operation */
            decryptstatus = aes128Decrypt(ciphertext,
                (GS_UINT32)cipherlen,
                (GS_UCHAR*)pDecryptInfo->Key,
                (GS_UINT32)strlen((const char*)pDecryptInfo->Key),
                pDecryptInfo->rand,
                outputstr,
                &plainlen);
        }

        if (!nread || !decryptstatus) {
            errorno = memset_s(ciphertext, cipherlen, '\0', cipherlen);
            securec_check_c(errorno, "", "");
            free(ciphertext);
            ciphertext = NULL;
            errorno = memset_s(outputstr, cipherlen, '\0', cipherlen);
            securec_check_c(errorno, "", "");
            free(outputstr);
            outputstr = NULL;
            return false;
        }

        errorno = memset_s(pDecryptInfo->currLine, MAX_DECRYPT_BUFF_LEN, '\0', MAX_DECRYPT_BUFF_LEN);
        securec_check_c(errorno, "\0", "\0");

        pDecryptInfo->decryptBuff = outputstr;
        pDecryptInfo->isCurrLineProcess = true;
        errorno = memset_s(ciphertext, cipherlen, '\0', cipherlen);
        securec_check_c(errorno, "", "");
        free(ciphertext);
        ciphertext = NULL;
    } else if (feof(source) && (false == pDecryptInfo->isCurrLineProcess)) {
        return false;
    }

    return true;
}

char* getLineFromAesEncryptFile(FILE* source, DecryptInfo* pDecryptInfo)
{
    if (false == decryptFromFile(source, pDecryptInfo)) {
        return NULL;
    }

    pDecryptInfo->isCurrLineProcess = false;
    return (char*)pDecryptInfo->decryptBuff;
}

/* make sure the key is must be letters or numbers */
bool check_key(const char* key, int NUM)
{
    int i = 0;
    for (i = 0; i < NUM; i++) {
        if ((key[i] >= '0' && key[i] <= '9') || (key[i] >= 'a' && key[i] <= 'z') || (key[i] >= 'A' && key[i] <= 'Z')) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

/*
 * Target		:Encrypt functions for security.
 * Description	:Encrypt with standard aes128 algorthm using openssl functions.
 * Notes		:The Key and InitVector used here must be the same as it was used in decrypt.
 * Input		:PlainText	plain text need to be encrypted
 *				Key			user assigned key
 *				RandSalt	To generate derive_key for encrypt.
 * Output		:CipherText	cipher text after encrypted
 *				CipherLen	ciphertext length  needed to be saved for decrypt
 */
bool aes128Encrypt(GS_UCHAR* PlainText, GS_UINT32 PlainLen, GS_UCHAR* Key, GS_UINT32 keylen, GS_UCHAR* RandSalt,
    GS_UCHAR* CipherText, GS_UINT32* CipherLen)
{
    GS_UCHAR deriver_key[RANDOM_LEN] = {0};
    GS_UCHAR aes_vector[RANDOM_LEN] = {0};
    GS_UINT32 retval = 0;
    errno_t errorno = EOK;

    if (PlainText == NULL) {
        (void)fprintf(stderr, _("invalid plain text, please check it!\n"));
        return false;
    }

    /* use PKCS5_deriveKey to dump the key for encryption */
    retval = PKCS5_PBKDF2_HMAC(
        (char*)Key, keylen, RandSalt, RANDOM_LEN, ITERATE_TIMES, (EVP_MD*)EVP_sha256(), RANDOM_LEN, deriver_key);
    if (!retval) {
        (void)fprintf(stderr, _("generate the derived key failed, errcode:%u\n"), retval);
        errorno = memset_s(deriver_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    /* get random aes vector for encryption */
    if (init_aes_vector_random(aes_vector, RANDOM_LEN) == false) {
        errorno = memset_s(deriver_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    /* use deriverkey to encrypt the plain key that user specified */
    retval = CRYPT_encrypt(
        NID_aes_128_cbc, deriver_key, RANDOM_LEN, aes_vector, RANDOM_LEN, PlainText, PlainLen, CipherText, CipherLen);

    if (retval != 0) {
        errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(deriver_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        (void)fprintf(stderr, _("encrypt plain text to cipher text failed, errcode:%u\n"), retval);
        return false;
    } 

    /* copy vector salt(aes_vector) to the cipher for decrypt */
    errorno = memcpy_s(CipherText + (*CipherLen), RANDOM_LEN, aes_vector, RANDOM_LEN);
    securec_check_c(errorno, "\0", "\0");
    *CipherLen = *CipherLen + RANDOM_LEN;
    
    /* clean the decrypt_key and aes_vector for security */
    errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(deriver_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    return true;
}

/*
 * Target		:Decrypt functions for security.
 * Description	:Decrypt with standard aes128 algorthm using openssl functions.
 * Notes		:The Key and InitVector here must be the same as it  was used in encrypt.
 * Input		:CipherText	cipher text need to be decrypted
 * 				CipherLen	ciphertext length
 * 				Key			user assigned key
 * 				InitVector	IV for decrypt. General get from file or ciphertext
 * Output		:PlainText	plain text after decrypted
 *				PlainLen		plaintext length
 */
bool aes128Decrypt(GS_UCHAR* CipherText, GS_UINT32 CipherLen, GS_UCHAR* Key, GS_UINT32 keylen, GS_UCHAR* RandSalt,
    GS_UCHAR* PlainText, GS_UINT32* PlainLen)
{
    GS_UCHAR decrypt_key[RANDOM_LEN] = {0};
    GS_UCHAR aes_vector[RANDOM_LEN] = {0};
    GS_UINT32 retval = 0;
    errno_t errorno = EOK;

    if (CipherText == NULL || CipherLen <= RANDOM_LEN) {
        (void)fprintf(stderr, _("invalid cipher text, please check it!\n"));
        return false;
    }

    /* get the decrypt_key value */
    retval = PKCS5_PBKDF2_HMAC(
        (char*)Key, keylen, RandSalt, RANDOM_LEN, ITERATE_TIMES, (EVP_MD*)EVP_sha256(), RANDOM_LEN, decrypt_key);
    if (!retval) {
        /* clean the decrypt_key for security */
        errorno = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        (void)fprintf(stderr, _("generate the derived key failed, errcode:%u\n"), retval);
        return false;
    }

    /* get the aes vector for from stored ciphertext */
    GS_UINT32 cipherpartlen = CipherLen - RANDOM_LEN;
    errorno = memcpy_s(aes_vector, RANDOM_LEN, CipherText + cipherpartlen, RANDOM_LEN);
    securec_check_c(errorno, "", "");

    /* decrypt the cipher */
    retval = CRYPT_decrypt(NID_aes_128_cbc,
        decrypt_key,
        RANDOM_LEN,
        aes_vector,
        RANDOM_LEN,
        CipherText,
        cipherpartlen,
        PlainText,
        PlainLen);

    /* clean the decrypt_key and aes_vector for security */
    errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");

    if (retval != 0) {
        /* try original version decrypt function */
        errorno = memset_s(PlainText, (*PlainLen), '\0', (*PlainLen));
        securec_check_c(errorno, "", "");
        errorno = memset_s(CipherText, CipherLen, '\0', CipherLen);
        securec_check_c(errorno, "", "");

        (void)fprintf(stderr, _("decrypt cipher text to plain text failed.\n"));
        return false;
    }

    return true;
}

/* Save several used derive_keys, random_salt and user_key in one thread. */
THR_LOCAL bool encryption_function_call = false;
THR_LOCAL GS_UCHAR derive_vector_saved[RANDOM_LEN] = {0};
THR_LOCAL GS_UCHAR mac_vector_saved[RANDOM_LEN] = {0};
THR_LOCAL GS_UCHAR input_saved[RANDOM_LEN] = {0};

/*
 * Target		:Encrypt functions for security.
 * Description	:Encrypt with standard aes128 algorthm using openssl functions.
 * Notes		:The Key and RandSalt used here must be the same as it was used in decrypt.
 * Input		:PlainText	plain text need to be encrypted
 *			 Key			user assigned key
 *			 RandSalt		To generate derive_key for encrypt.
 * Output		:CipherText	cipher text after encrypted
 *			 CipherLen	ciphertext length  needed to be saved for decrypt
 * Revision	:Using random initial vector and one fixed salt in one thread
 * 			  in order to avoid generating deriveKey everytime.
 */
bool aes128EncryptSpeed(GS_UCHAR* PlainText, GS_UINT32 PlainLen, GS_UCHAR* Key, GS_UCHAR* RandSalt,
    GS_UCHAR* CipherText, GS_UINT32* CipherLen)
{
    GS_UCHAR derive_key[RANDOM_LEN] = {0};
    GS_UCHAR mac_key[RANDOM_LEN] = {0};
    GS_UCHAR aes_vector[RANDOM_LEN] = {0};
    GS_UCHAR mac_text[MAC_LEN] = {0};
    GS_UINT32 mac_length = 0;
    GS_UINT32 keylen = 0;
    GS_UINT32 retval = 0;
    errno_t errorno = EOK;

    /*
     * Thread-local variables including random_salt, derive_key and key
     * will be saved during the thread.
     */
    static THR_LOCAL GS_UCHAR random_salt_saved[RANDOM_LEN] = {0};

    /* The key input by user in SQL */
    GS_UCHAR user_key[RANDOM_LEN] = {0};
    keylen = strlen((const char*)Key);
    if (keylen > RANDOM_LEN || keylen == 0) {
        (void)fprintf(stderr, _("Key is missing!\n"));
        errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    if (NULL == PlainText) {
        (void)fprintf(stderr, _("Invalid plain text, please check it!\n"));
        errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    /* get random aes vector for encryption */
    if (false == init_aes_vector_random(aes_vector, RANDOM_LEN)) {
        (void)fprintf(stderr, _("generate IV vector failed\n"));
        /* clean up */
        errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    /* Copy the contents of the key to the user_key */
    errorno = memcpy_s(user_key, RANDOM_LEN, Key, keylen);
    securec_check_c(errorno, "\0", "\0");

    if (encryption_function_call == false) {
        encryption_function_call = true;
    }

    /* Use saved deriveKey if random_salt and key haven't changed.
     * Otherwise, generate a new derivekey.
     */
    if (0 == memcmp(RandSalt, random_salt_saved, RANDOM_LEN)) {
        retval = 1;
        /* unmask saved user_key and derive_key */
        for (GS_UINT32 i = 0; i < RANDOM_LEN; ++i) {
            if (user_key[i] == ((char)input_saved[i] ^ (char)random_salt_saved[i])) {
                derive_key[i] = ((char)derive_vector_saved[i] ^ (char)random_salt_saved[i]);
                mac_key[i] = ((char)mac_vector_saved[i] ^ (char)random_salt_saved[i]);
            } else {
                retval = 0;
            }
        }
    }

    if (!retval) {
        /* Otherwise generate a new deriveKey. */
        /* use PKCS5_deriveKey to dump the key for encryption */
        retval = PKCS5_PBKDF2_HMAC(
            (char*)Key, keylen, RandSalt, RANDOM_LEN, ITERATE_TIMES, (EVP_MD*)EVP_sha256(), RANDOM_LEN, derive_key);
        if (!retval) {
            (void)fprintf(stderr, _("generate the derived key failed,errcode:%u\n"), retval);
            /* clean up */
            errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(derive_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            return false;
        }

        /* generate the mac key for hmac */
        retval = PKCS5_PBKDF2_HMAC((char*)user_key,
            RANDOM_LEN,
            RandSalt,
            RANDOM_LEN,
            MAC_ITERATE_TIMES,
            (EVP_MD*)EVP_sha256(),
            RANDOM_LEN,
            mac_key);
        if (!retval) {
            (void)fprintf(stderr, _("generate the mac key failed,errcode:%u\n"), retval);
            /* clean up */
            errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(derive_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            return false;
        }

        /* save random_salt */
        errorno = memcpy_s(random_salt_saved, RANDOM_LEN, RandSalt, RANDOM_LEN);
        securec_check_c(errorno, "\0", "\0");

        /* save user_key, derive_key and mac_key using random_salt as mask */
        for (GS_UINT32 i = 0; i < RANDOM_LEN; ++i) {
            input_saved[i] = ((char)user_key[i] ^ (char)random_salt_saved[i]);
            derive_vector_saved[i] = ((char)derive_key[i] ^ (char)random_salt_saved[i]);
            mac_vector_saved[i] = ((char)mac_key[i] ^ (char)random_salt_saved[i]);
        }
    }

    /* use deriverkey to encrypt the plaintext */
    retval = CRYPT_encrypt(
        NID_aes_128_cbc, derive_key, RANDOM_LEN, aes_vector, RANDOM_LEN, PlainText, PlainLen, CipherText, CipherLen);
    if (retval != 0) {
        (void)fprintf(stderr, _("encrypt plain text to cipher text failed,errcode:%u\n"), retval);
        /* clean up */
        errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(derive_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    /* copy aes vector to the cipher */
    errorno = memcpy_s(CipherText + (*CipherLen), RANDOM_LEN, aes_vector, RANDOM_LEN);
    securec_check_c(errorno, "\0", "\0");
    *CipherLen = *CipherLen + RANDOM_LEN;

    /* use mac_key to generate the mactext base on cipher and IV */
    retval = CRYPT_hmac(NID_hmac_sha1, mac_key, RANDOM_LEN, CipherText, *CipherLen, mac_text, &mac_length);
    if (retval != 0 || mac_length != MAC_LEN) {
        (void)fprintf(stderr, _("generate mac text based on plain text failed,errcode:%u\n"), retval);
        /* clean up */
        errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(derive_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    /* copy mac vector to the cipher */
    errorno = memcpy_s(CipherText + (*CipherLen), MAC_LEN, mac_text, MAC_LEN);
    securec_check_c(errorno, "\0", "\0");
    *CipherLen = *CipherLen + MAC_LEN;

    /* clean the user_key, mac_key, derive_key and aes_vector for security */
    errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(derive_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");

    return true;
}

/* Save several used derive_keys, random_salt and user_key in one thread. */
THR_LOCAL bool decryption_function_call = false;
THR_LOCAL GS_UCHAR derive_vector_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN] = {0};
THR_LOCAL GS_UCHAR mac_vector_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN] = {0};
THR_LOCAL GS_UCHAR random_salt_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN] = {0};
THR_LOCAL GS_UCHAR user_input_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN] = {0};

/* 
 * check input parameters of aes128DecryptSpeed function.
 */
static bool CheckAes128DecryptInput(const GS_UCHAR* cipherText, GS_UINT32 cipherLen, GS_UINT32 keyLen)
{
    if (keyLen > RANDOM_LEN) {
        (void)fprintf(stderr, _("For new decryption function, key must be shorter than 16 bytes!\n"));
        return false;
    }

    if (cipherText == NULL) {
        (void)fprintf(stderr, _("invalid cipher text,please check it!\n"));
        return false;
    }

    if (cipherLen < (2 * RANDOM_LEN + MAC_LEN)) {
        (void)fprintf(stderr, _("Cipertext is too short to decrypt\n"));
        return false;
    }
    return true;
}

/*
 * we save 48(= NUMBER_OF_SAVED_DERIVEKEYS) used decrypt keys and mac keys in thread global variables,
 * if (randsalt, userKey ^ randsalt) match any stored (salt, key) pair, return drived decrypt_key and mac_key,
 * and adjust usage frequency array.
 */

static GS_UINT32 FindDriveKey(const GS_UCHAR* randSalt, GS_UINT32* usageFrequency, const GS_UCHAR* userKey, 
    GS_UCHAR* decryptKey, GS_UCHAR* macKey)
{
    GS_UINT32 derivekeyFound = 0;
    /* 
     * Search for matched derive_vector_used in the order of usageFrequency.
     * Start with usageFrequency[0] by first checking random_salt_used[usageFrequency[0]].
     */
    for (GS_UINT32 i = 0; i < NUMBER_OF_SAVED_DERIVEKEYS && !derivekeyFound; ++i) {
        if (0 == memcmp(random_salt_used[usageFrequency[i]], randSalt, RANDOM_LEN)) {
            derivekeyFound = 1;
            /* unmask saved userKey and derive_key */
            for (GS_UINT32 j = 0; j < RANDOM_LEN; ++j) {
                GS_UCHAR mask = (char)random_salt_used[usageFrequency[i]][j];
                if (userKey[j] == ((char)user_input_used[usageFrequency[i]][j] ^ (char)mask)) {
                    decryptKey[j] = ((char)derive_vector_used[usageFrequency[i]][j] ^ (char)mask);
                    macKey[j] = ((char)mac_vector_used[usageFrequency[i]][j] ^ (char)mask);
                } else {
                    derivekeyFound = 0;
                }
            }
            /* 
             * Once a derive_vector_used is used again, its usage_frency will be promoted.
             * Of coure, for i=0 member, there is no place to promote.
             */
            if (i > 0 && i < NUMBER_OF_SAVED_DERIVEKEYS / 2 && derivekeyFound) {
                /*
                 * For i from 1 to NUMBER_OF_SAVED_DERIVEKEYS/2 - 1,
                 * the usage_frency of found derive_key will be promoted to i-1 and
                 * the previos i-1 member will be relegated to i.
                 */
                GS_UINT32 temp = usageFrequency[i - 1];
                usageFrequency[i - 1] = usageFrequency[i];
                usageFrequency[i] = temp;
            } else if (i >= NUMBER_OF_SAVED_DERIVEKEYS / 2 && derivekeyFound) {
                /*
                 * For i from NUMBER_OF_SAVED_DERIVEKEYS/2 to NUMBER_OF_SAVED_DERIVEKEYS-1,
                 * the usage_frency of found derive_key will be promoted to NUMBER_OF_SAVED_DERIVEKEYS/2 - 1
                 * as it is used more than one time.
                 * And the previos NUMBER_OF_SAVED_DERIVEKEYS/2 - 1 member will be relegated to i,
                 * which means this derivekey may be replaced by a newborn derivekey.
                 */
                GS_UINT32 temp = usageFrequency[NUMBER_OF_SAVED_DERIVEKEYS / 2 - 1];
                usageFrequency[NUMBER_OF_SAVED_DERIVEKEYS / 2 - 1] = usageFrequency[i];
                usageFrequency[i] = temp;
            }
        }
    }
    return derivekeyFound;
}

/*
 * Target		:Decrypt functions for security.
 * Description	:Decrypt with standard aes128 algorthm using openssl functions.
 * Notes		:The Key and InitVector here must be the same as it  was used in encrypt.
 * Input		:CipherText	cipher text need to be decrypted
 * 			 CipherLen	ciphertext length
 * 			 Key			user assigned key
 * 			 RandSalt		To generate derive_key for decrypt. General get from file or ciphertext
 * Output		:PlainText	plain text after decrypted
 *               	 PlainLen		plaintext length
 * Revision	: Save several derive_keys instead of generating deriveKey everytime.
 */
bool aes128DecryptSpeed(GS_UCHAR* CipherText, GS_UINT32 CipherLen, GS_UCHAR* Key, GS_UCHAR* RandSalt,
    GS_UCHAR* PlainText, GS_UINT32* PlainLen)
{
    GS_UCHAR decrypt_key[RANDOM_LEN] = {0};
    GS_UCHAR mac_key[RANDOM_LEN] = {0};
    GS_UCHAR aes_vector[RANDOM_LEN] = {0};
    GS_UCHAR mac_text[MAC_LEN] = {0};
    GS_UCHAR mac_text_saved[MAC_LEN] = {0};
    GS_UCHAR user_key[RANDOM_LEN] = {0};
    GS_UINT32 keylen = 0;
    GS_UINT32 mac_length = 0;
    GS_UINT32 retval = 0;
    GS_UINT32 DERIVEKEY_FOUND = 0;
    errno_t errorno = EOK;
    const GS_UINT64 decryption_count_max = 14000000;
    static THR_LOCAL GS_UINT64 decryption_count = 0;

    keylen = strlen((const char*)Key);

    if (!CheckAes128DecryptInput(CipherText, CipherLen, keylen)) {
        return false;
    }

    decryption_function_call = true;

    /* split the ciphertext to cipherpart, aes vector and mac vector for decrypt
     * read the aes vector for decrypt from ciphertext
     */
    GS_UINT32 cipherpartlen = CipherLen - RANDOM_LEN - MAC_LEN;
    errorno = memcpy_s(aes_vector, RANDOM_LEN, CipherText + cipherpartlen, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memcpy_s(mac_text_saved, MAC_LEN, CipherText + cipherpartlen + RANDOM_LEN, MAC_LEN);
    securec_check_c(errorno, "", "");

    /* The usage_frequency is used to decided which random_salt to start comparing with
     * The usage_frequency is based on the recent using times of derive_key
     */
    static THR_LOCAL GS_UINT32 usage_frequency[NUMBER_OF_SAVED_DERIVEKEYS] = {0};

    /*
     * The insert_position is used to seperate two different region for usage_frequency
     * From 0 to NUMBER_OF_SAVED_DERIVEKEYS/2 -1 , there are derive_keys used many times.
     * From NUMBER_OF_SAVED_DERIVEKEYS/2 to NUMBER_OF_SAVED_DERIVEKEYS -1,
     * these derive_keys were used only one time.
     * Therefore, the newborn derive_key will be saved in insert_position.
     */
    static THR_LOCAL GS_UINT32 insert_position = NUMBER_OF_SAVED_DERIVEKEYS / 2;

    /* Initialize usage_frequency */
    if (usage_frequency[0] == 0 && usage_frequency[NUMBER_OF_SAVED_DERIVEKEYS - 1] == 0) {
        for (GS_UINT32 i = 0; i < NUMBER_OF_SAVED_DERIVEKEYS; ++i)
            usage_frequency[i] = i;
    }

    /* Copy the contents of the key to the user_key */
    errorno = memcpy_s(user_key, RANDOM_LEN, Key, keylen);
    securec_check_c(errorno, "\0", "\0");

    /* Search for matched derive_vector_used in the order of usage_frequency.
     * Start with usage_frequency[0] by first checking random_salt_used[usage_frequency[0]].
     */
    DERIVEKEY_FOUND = FindDriveKey(RandSalt, usage_frequency, user_key, decrypt_key, mac_key);

    /* Generate deriveKey if find no derive_vector_saved  to use. */
    if (!DERIVEKEY_FOUND) {
        retval = PKCS5_PBKDF2_HMAC(
            (char*)Key, keylen, RandSalt, RANDOM_LEN, ITERATE_TIMES, (EVP_MD*)EVP_sha256(), RANDOM_LEN, decrypt_key);
        if (!retval) {
            /* clean the decrypt_key and aes_vector for security */
            errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            (void)fprintf(stderr, _("generate the derived key failed,errcode:%u\n"), retval);
            return false;
        }
        /* generate the mac key */
        retval = PKCS5_PBKDF2_HMAC((char*)user_key,
            RANDOM_LEN,
            RandSalt,
            RANDOM_LEN,
            MAC_ITERATE_TIMES,
            (EVP_MD*)EVP_sha256(),
            RANDOM_LEN,
            mac_key);

        if (!retval) {
            /* clean the decrypt_key and mac_key for security */
            errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            (void)fprintf(stderr, _("generate the mac key failed,errcode:%u\n"), retval);
            return false;
        }

        /* save random_salt. */
        errorno = memcpy_s(random_salt_used[usage_frequency[insert_position]], RANDOM_LEN, RandSalt, RANDOM_LEN);
        securec_check_c(errorno, "\0", "\0");

        /* save user_key, derive_key and mac_key using random_salt as mask. */
        for (GS_UINT32 j = 0; j < RANDOM_LEN; ++j) {
            GS_UCHAR mask = random_salt_used[usage_frequency[insert_position]][j];
            user_input_used[usage_frequency[insert_position]][j] = ((char)user_key[j] ^ (char)mask);
            derive_vector_used[usage_frequency[insert_position]][j] = ((char)decrypt_key[j] ^ (char)mask);
            mac_vector_used[usage_frequency[insert_position]][j] = ((char)mac_key[j] ^ (char)mask);
        }

        /* move insert_posion from  NUMBER_OF_SAVED_DERIVEKEYS/2 to NUMBER_OF_SAVED_DERIVEKEYS-1. */
        insert_position = (insert_position + 1) % (NUMBER_OF_SAVED_DERIVEKEYS / 2) + NUMBER_OF_SAVED_DERIVEKEYS / 2;
    }

    /* decrypt the cipher using cipherpart and cipherpartlen instead of ciphertext and cipherlen */
    retval = CRYPT_decrypt(NID_aes_128_cbc,
        decrypt_key,
        RANDOM_LEN,
        aes_vector,
        RANDOM_LEN,
        CipherText,
        cipherpartlen,
        PlainText,
        PlainLen);

    /* clean the decrypt_key and aes_vector for security */
    errorno = memset_s(aes_vector, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");

    if (retval != 0) {
        (void)fprintf(stderr, _("decrypt cipher text to plain text failed,errcode:%u\n"), retval);
        errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(errorno, "", "");
        return false;
    }

    /*
     * In order to confirm the intergrity of the decrypted text,
     * compare its mac text with the saved mac text.
     */
    retval =
        CRYPT_hmac(NID_hmac_sha1, mac_key, RANDOM_LEN, CipherText, cipherpartlen + RANDOM_LEN, mac_text, &mac_length);

    errorno = memset_s(user_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(mac_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(errorno, "", "");

    if (retval != 0 || mac_length != MAC_LEN) {
        (void)fprintf(stderr, _("Mac generation failed,errcode:%u\n"), retval);
        return false;
    }

    if (0 != memcmp(mac_text, mac_text_saved, MAC_LEN)) {
        (void)fprintf(stderr, _("Mac does not match"));
        return false;
    }

    /*
     * The thread_local variables are not cleaned when decryption fail because
     * the new version decrypt function is designed to decrypt the data encrypted by the old version function.
     * So, if there are some old-encrypted data mixed in the new-encrypted data,
     * it is possible that we still want to use these keys despite this decryption fail.
     * To improve the security, the keys are masked and will be cleaned when the thread ends or
     * a plan is finished or reach 30 minutes.
     * Here, we count the decrytion times to estimate the using time of derive keys.
     */
    decryption_count++;
    if (decryption_count > decryption_count_max) {
        for (GS_UINT32 i = 0; i < NUMBER_OF_SAVED_DERIVEKEYS; ++i) {
            errorno = memset_s(derive_vector_used[i], RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(user_input_used[i], RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            errorno = memset_s(mac_vector_used[i], RANDOM_LEN, 0, RANDOM_LEN);
            securec_check_c(errorno, "", "");
            usage_frequency[i] = i;
        }
        decryption_count = 0;
    }

    return true;
}
