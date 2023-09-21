
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "openssl/rand.h"
#include "openssl/evp.h"
#include "openssl/err.h"
#include "keymgr/enc_adpt/vsdf.h"

#define VSDF_MAX_KEY_NUM 50
#define VSDF_MAX_SESS_NUM 200

typedef struct {
    int id;
    unsigned char *key;
    int keylen;
} VirKeyHdr;

typedef struct {
    int id;
    bool use;

    /* encrypt */
    VirKeyHdr keyarr[VSDF_MAX_KEY_NUM]; /* used for import key */
    int keycnt;

    /* hash */
    unsigned char *hdata;
    unsigned int hdatalen;
} VirSess;

typedef struct {
    int devid;
    int sessid;
} VirSessHdl;

void sess_init(VirSess *sess)
{
    if (sess == NULL) {
        return;
    }

    sess->id = 0;
    sess->use = false;
    sess->keycnt = 0;
    sess->hdata = NULL;
    sess->hdatalen = 0;
    for (int j = 0; j < VSDF_MAX_KEY_NUM; j++) {
        VirKeyHdr *khdr = &sess->keyarr[j];
        khdr->id = j;
        khdr->key = NULL;
        khdr->keylen = 0;
    }
}

typedef struct {
    const char *name;

    VirSess sessarr[VSDF_MAX_SESS_NUM];
    int sesscnt;
} VirDev;

int vsdf_device_open(void **dev)
{
    *dev = (void *)malloc(sizeof(VirDev));
    if (*dev == NULL) {
        return -1;
    }

    VirDev *vd = (VirDev *)(*dev);
    vd->name = "virtual sdf device";
    vd->sesscnt = 0;

    for (int i = 0; i < VSDF_MAX_SESS_NUM; i++) {
        VirSess *sess = &vd->sessarr[i];
        sess_init(sess);
        sess->id = i;
    }

    return 0;
}
int vsdf_session_open(void *dev, void **sess)
{
    VirDev *vd = (VirDev *)dev;

    if (dev == NULL) {
        return -1;
    }

    if (vd->sesscnt >= VSDF_MAX_SESS_NUM) {
        return -1;
    }

    vd->sessarr[vd->sesscnt].use = true;
    *sess =  &vd->sessarr[vd->sesscnt];
    vd->sesscnt++;

    return 0;
}

int vsdf_generate_random(void *sess, unsigned int len, unsigned char *buf)
{
    int ret;

    ret = RAND_priv_bytes(buf, len);
    if (ret != 1) {
        return -1;
    }

    return 0;
}

int vsdf_import_key(void *sess, unsigned char *key, unsigned int keylen, void **keyhdr)
{
    VirSess *vs = (VirSess *)sess;

    if (vs->keycnt >= VSDF_MAX_KEY_NUM) {
        return -1;
    }

    VirKeyHdr *vkhdr = &vs->keyarr[vs->keycnt];
    vkhdr->key = (unsigned char *)malloc(keylen);
    memcpy(vkhdr->key, key, keylen);
    vkhdr->keylen = keylen;

    *keyhdr = &vs->keyarr[vs->keycnt];
    vs->keycnt++;

    return 0;
}

int vsdf_encrypt(void *sess, void *heyhdr, unsigned int algo, unsigned char *iv, unsigned char *plain,
    unsigned int plainlen, unsigned char *cipher, unsigned int *cipherlen)
{
    int ret = -1;
    EVP_CIPHER_CTX *ctx = NULL;

    do {
        VirKeyHdr *vkhdr = (VirKeyHdr *)heyhdr;
        if (vkhdr->keylen == 0) {
            break;
        }

        ctx = EVP_CIPHER_CTX_new();
        if (ctx == NULL) {
            break;
        }

        if (EVP_EncryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, vkhdr->key, iv) == 0) {
            break;
        }

        if (EVP_CIPHER_CTX_set_padding(ctx, 1) == 0) {
            break;
        }

        int outlen;
        if (EVP_EncryptUpdate(ctx, cipher, &outlen, plain, plainlen) == 0) {
            break;
        }

        int padlen;
        if (EVP_EncryptFinal_ex(ctx, cipher + outlen, &padlen) == 0) {
            break;
        }
        *cipherlen = outlen + padlen;

        ret = 0;
    } while (0);

    if (ctx != NULL) {
        EVP_CIPHER_CTX_free(ctx);
    }

    return ret;
}

int vsdf_decrypt(void *sess, void *heyhdr, unsigned int algo, unsigned char *iv, unsigned char *cipher,
    unsigned int cipherlen, unsigned char *plain, unsigned int *plainlen)
{
    int ret = -1;
    EVP_CIPHER_CTX *ctx = NULL;

    do {
        VirKeyHdr *vkhdr = (VirKeyHdr *)heyhdr;
        if (vkhdr->keylen == 0) {
            break;
        }

        ctx = EVP_CIPHER_CTX_new();
        if (ctx == NULL) {
            break;
        }

        if (EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, vkhdr->key, iv) == 0) {
            break;
        }

        if (EVP_CIPHER_CTX_set_padding(ctx, 1) == 0) {
            break;
        }

        int outlen;
        if (EVP_DecryptUpdate(ctx, plain, &outlen, cipher, cipherlen) == 0) {
            break;
        }

        int padlen;
        if (EVP_DecryptFinal_ex(ctx, plain + outlen, &padlen) == 0) {
            break;
        }
        *plainlen = outlen + padlen;

        ret = 0;
    } while (0);

    if (ctx != NULL) {
        EVP_CIPHER_CTX_free(ctx);
    }

    return ret;
}

int vsdf_hash_init(void *sess, unsigned int algo, void *puckey, unsigned char *pucid, unsigned int pucidlen)
{
    return 0;
}

int vsdf_hash_update(void *sess, unsigned char *data, unsigned int datalen)
{
    VirSess *vs = (VirSess *)sess;

    vs->hdata = (unsigned char *)malloc(datalen);
    if (vs->hdata == NULL) {
        return -1;
    }

    memcpy(vs->hdata, data, datalen);

    vs->hdatalen = datalen;
    
    return 0;
}

#define SM3_LEN 32

int vsdf_hash_final(void *sess, unsigned char *hash, unsigned int *hashlen)
{
    EVP_MD_CTX *ctx;
    VirSess *vs = (VirSess *)sess;
    int ret;

    do {
        ctx = EVP_MD_CTX_new();
        if (ctx == NULL) {
            break;
        }
        if (!EVP_DigestInit_ex(ctx, EVP_sm3(), NULL)) {
            break;
        }
        if (!EVP_DigestUpdate(ctx, vs->hdata, vs->hdatalen)) {
            break;
        }
        if (!EVP_DigestFinal_ex(ctx, hash, hashlen)) {
            break;
        }
        ret = (*hashlen == SM3_LEN) ? 0 : -1;
    } while (0);

    if (ctx != NULL) {
        EVP_MD_CTX_free(ctx);
    }

    return ret;
}
