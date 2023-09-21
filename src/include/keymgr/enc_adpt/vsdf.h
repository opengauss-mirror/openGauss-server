#ifndef VSDF_H_
#define VSDF_H_

int vsdf_device_open(void **dev);
int vsdf_session_open(void *dev, void **sess);

int vsdf_generate_random(void *sess, unsigned int len, unsigned char *buf);

int vsdf_import_key(void *sess, unsigned char *key, unsigned int keylen, void **keyhdr);
int vsdf_encrypt(void *sess, void *heyhdr, unsigned int algo, unsigned char *iv, unsigned char *plain,
    unsigned int plainlen, unsigned char *cipher, unsigned int *cipherlen);
int vsdf_decrypt(void *sess, void *heyhdr, unsigned int algo, unsigned char *iv, unsigned char *cipher,
    unsigned int cipherlen, unsigned char *plain, unsigned int *plainlen);

int vsdf_hash_init(void *sess, unsigned int algo, void *puckey, unsigned char *pucid, unsigned int pucidlen);
int vsdf_hash_update(void *sess, unsigned char *data, unsigned int datalen);
int vsdf_hash_final(void *sess, unsigned char *hash, unsigned int *hashlen);

#endif