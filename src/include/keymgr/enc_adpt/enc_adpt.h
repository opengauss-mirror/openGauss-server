
#ifndef HWCRYPT_H_
#define HWCRYPT_H_

#include <stdlib.h>
#include "keymgr/comm/security_error.h"
#include "keymgr/comm/security_utils.h"
#include <pthread.h>

typedef enum {
    HEM_UNKNOWN = 0,
    HEA_SM4_CBC,
    HEA_SM4_MAC
} HwEncAlgo;

typedef struct {
    unsigned int algo;
    KmUnStr plain; /* key plain */
    KmUnStr iv;
    KmUnStr cipher;
    void *keyhdr;
} EmKeyData;

void keydata_init(EmKeyData *kd);

typedef enum {
    EMT_INVALID = 0,
    EMT_VIR = 1,
    EMT_JNTA = 2
} EmType;

typedef struct {
    KmErr *err;

    EmType type;
    void *device;
    void *sess;

    pthread_mutex_t lock;
} EncAdpt;

void hwem_prt(const char *type, const void *buf, size_t bufsz);

EncAdpt *enc_adpt_new(KmErr *err, EmType type);
void enc_adpt_free(EncAdpt *ea);
EmType enc_adpt_init_with_env();

void enc_adpt_random(EncAdpt *ea, size_t len, unsigned char *buf);

void *enc_adpt_import_key(EncAdpt *ea, KmUnStr key);
void enc_adpt_encrypt(EncAdpt *ea, HwEncAlgo algo, KmUnStr plain, KmUnStr iv, EmKeyData *key, KmUnStr *cipher);
void enc_adpt_decrypt(EncAdpt *ea, HwEncAlgo algo, KmUnStr cipher, KmUnStr iv, EmKeyData *key, KmUnStr *plain);

#endif