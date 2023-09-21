#include "keymgr/enc_adpt/enc_adpt.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef USE_TASSL
#include "sdf/SDF.h"
#include "sdf/TassAPI4GHVSM.h"
#endif
#include "keymgr/enc_adpt/vsdf.h"

void keydata_init(EmKeyData *kd)
{
    kd->algo = 0;
    kd->plain.val = NULL;
    kd->plain.len = 0;
    kd->cipher.val = NULL;
    kd->cipher.len = 0;
    kd->iv.val = NULL;
    kd->iv.len = 0;
    kd->keyhdr = NULL;
}

void hwem_prt(const char *type, const void *buf, size_t bufsz)
{
    printf("%s: %lu (", type, bufsz);
    char *sbuf = (char *)buf;
    for (size_t i = 0; i < bufsz; i++) {
        printf("%c", sbuf[i]);
    }
    printf(")\n");
}

EncAdpt *enc_adpt_new(KmErr *err, EmType type)
{
#define HWEM_ERR_BUF_SZ 2048

    EncAdpt *ea;
    int ret = -1;

    if (err == NULL) {
        return NULL;
    }

    ea = (EncAdpt *)calloc(1, sizeof(EncAdpt));
    if (ea == NULL) {
        km_err_msg(err, "falied to malloc memory");
        return NULL;
    }

    do {
        ea->type = type;
        ea->err = err;

        ret = pthread_mutex_init(&ea->lock, NULL);
        if (ret != 0) {
            km_err_msg(err, "falied to init mutex");
            break;
        }

        if (type == EMT_JNTA) {
#ifdef USE_TASSL
            ret = SDF_OpenDevice(&ea->device);
            if (ret != 0) {
                km_err_msg(err, "falied to open device: %d", ret);
                break;
            }

            ret = SDF_OpenSession(ea->device, &ea->sess);
            if (ret != 0) {
                km_err_msg(err, "falied to open session: %d", ret);
                break;
            }
#endif
        } else if (type == EMT_VIR) {
            ret = vsdf_device_open(&ea->device);
            if (ret != 0) {
                km_err_msg(err, "falied to open device: %d", ret);
                break;
            }

            ret = vsdf_session_open(ea->device, &ea->sess);
            if (ret != 0) {
                km_err_msg(err, "falied to open session: %d", ret);
                break;
            }
        }
    } while (0);

    if (ret != 0) {
        enc_adpt_free(ea);
        return NULL;
    }

    return ea;
}

EmType enc_adpt_init_with_env()
{
    char *env;
    EmType emtype = EMT_INVALID;

    env = km_env_get("ENC_INFO");
    if (env == NULL) {
        return emtype;
    }

    KvScan *scan;
    int kvcnt;
    scan = kv_scan_init(env);
    for (;;) {
        kvcnt = kv_scan_exec(scan);
        if (kvcnt >= 0) {
            if (strcasecmp(km_str_strip(scan->key), "type") == 0) {
                if (strcasecmp(km_str_strip(scan->value), "jnta") == 0) { /* jnta */
                    emtype= EMT_JNTA;
                    printf("NOTICE: use jnta hsm\n");
                } else if (strcasecmp(km_str_strip(scan->value), "virtual") == 0) {
                    emtype = EMT_VIR;
                    printf("NOTICE: use virtual hsm\n");
                }
            }
        }

        if (kvcnt <= 0) {
            break;
        }
    }

    return emtype;
}

void enc_adpt_free(EncAdpt *ea)
{
    if (ea == NULL) {
        return;
    }

    (void)pthread_mutex_destroy(&ea->lock);

    if (ea->type == EMT_JNTA) {
#ifdef USE_TASSL
        if (ea->sess != NULL) {
            SDF_CloseSession(ea->sess);
        }

        if (ea->device != NULL) {
            SDF_CloseDevice(ea->device);
        }
#endif
    } else if (ea->type == EMT_VIR) {
        /* nothing to do */
    }

    free(ea);
}

static bool enc_adpt_lock(EncAdpt *ea)
{
    if (pthread_mutex_lock(&ea->lock) != 0) {
        km_err_msg(ea->err, "hardware lock error");
        return false;
    }

    return true;
}

#define hwem_unlock(ea) (void)pthread_mutex_unlock(&(ea)->lock)

void enc_adpt_random(EncAdpt *ea, size_t len, unsigned char *buf)
{
    int ret = -1;
    if (ea->type == EMT_JNTA) {
#ifdef USE_TASSL
        ret = SDF_GenerateRandom(ea->sess, len, buf);
        if (ret != 0) {
            km_err_msg(ea->err, "random key error: %d", ret);
        }
#endif
    } else if (ea->type == EMT_VIR) {
        ret = vsdf_generate_random(ea->sess, len, buf);
        if (ret != 0) {
            km_err_msg(ea->err, "random key error: %d", ret);
        }
    }
}

void *enc_adpt_import_key(EncAdpt *ea, KmUnStr key)
{
    void *keyhdr = NULL;
    int ret = -1;

    if (ea->type == EMT_JNTA) {
#ifdef USE_TASSL
        ret = SDF_ImportKey(ea->sess, key.val, key.len, &keyhdr);
        if (ret != 0) {
            km_err_msg(ea->err, "import key error: %d", ret);
        }
#endif
    } else if (ea->type == EMT_VIR) {
        ret = vsdf_import_key(ea->sess, key.val, key.len, &keyhdr);
        if (ret != 0) {
            km_err_msg(ea->err, "import key error: %d", ret);
        }
    }

    return keyhdr;
}

unsigned int sdf_get_algo(HwEncAlgo algo)
{
#ifdef USE_TASSL
    switch (algo) {

        case HEA_SM4_CBC:
            return SGD_SM4_CBC;
        case HEA_SM4_MAC:
            return SGD_SM4_MAC;
        default:
            return 0;
    }
#endif

    return 0;
}

void enc_adpt_encrypt(EncAdpt *ea, HwEncAlgo algo, KmUnStr plain, KmUnStr iv, EmKeyData *key, KmUnStr *cipher)
{
    int ret;

    if (enc_adpt_lock(ea)) {
        do {
            if (ea->type == EMT_JNTA) {
#ifdef USE_TASSL
                ret = SDF_Encrypt(ea->sess, key->keyhdr, sdf_get_algo(algo), iv.val, plain.val, plain.len, cipher->val,
                    (unsigned int *)&cipher->len);
                if (ret != 0) {
                    km_err_msg(ea->err, "encrypt error: %d", ret);
                    break;
                }
#endif
            } else if (ea->type == EMT_VIR) {
                ret = vsdf_encrypt(ea->sess, key->keyhdr, sdf_get_algo(algo), iv.val, plain.val, plain.len, cipher->val,
                    (unsigned int *)&cipher->len);
                if (ret != 0) {
                    km_err_msg(ea->err, "encrypt error: %d", ret);
                    break;
                }
            }
        } while (0);

        hwem_unlock(ea);
    }
}

void enc_adpt_decrypt(EncAdpt *ea, HwEncAlgo algo, KmUnStr cipher, KmUnStr iv, EmKeyData *key, KmUnStr *plain)
{
    int ret;

    if (enc_adpt_lock(ea)) {
        do {
            if (ea->type == EMT_JNTA) {
#ifdef USE_TASSL
                ret = SDF_Decrypt(ea->sess, key->keyhdr, sdf_get_algo(algo), iv.val, cipher.val, cipher.len, plain->val,
                    (unsigned int *)&plain->len);
                if (ret != 0) {
                    km_err_msg(ea->err, "decrypt error: %d", ret);
                    break;
                }
#endif
            } else if (ea->type == EMT_VIR) {
                ret = vsdf_decrypt(ea->sess, key->keyhdr, sdf_get_algo(algo), iv.val, cipher.val, cipher.len, plain->val,
                    (unsigned int *)&plain->len);
                if (ret != 0) {
                    km_err_msg(ea->err, "decrypt error: %d", ret);
                    break;
                }
            }
        } while (0);

        hwem_unlock(ea);
    }
}