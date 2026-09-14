/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 *
 * backup_encrypt.h: streaming encryption of backup files
 *
 * IDENTIFICATION
 *     src/bin/pg_probackup/backup_encrypt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKUP_ENCRYPT_H
#define BACKUP_ENCRYPT_H

#include <stdio.h>
#include <sys/types.h>
#include "c.h"

/* on-disk container constants */
#define GSPB_ENC_MAGIC              "GSPBENC1"
#define GSPB_ENC_MAGIC_LEN          8
#define GSPB_ENC_HDR_LEN            64
#define GSPB_ENC_TAG_LEN            16
#define GSPB_ENC_NONCE_LEN          12
#define GSPB_ENC_FORMAT_VERSION     1

/* AES-128 to stay aligned with the gs_dump facing "AES128" option */
#define GSPB_ENC_KEY_LEN            16
#define GSPB_ENC_DEK_LEN            32
#define GSPB_ENC_SALT_LEN           16
#define GSPB_ENC_MAC_LEN            32

#define GSPB_ENC_ALG_AES128_GCM     1

/* truncated MAC stored in backup.keyinfo to detect a wrong passphrase early */
#define GSPB_ENC_KEKCHECK_LEN       8

/*
 * Field layout of the 64 byte container header. The first 32 bytes are also
 * fed into the AAD of every chunk, so they must stay contiguous.
 */
#define GSPB_HDR_OFF_VERSION        8
#define GSPB_HDR_OFF_ALG            10
#define GSPB_HDR_OFF_CHUNK_SIZE     12
#define GSPB_HDR_OFF_FILE_NONCE     16
#define GSPB_HDR_OFF_FILE_ID        24
#define GSPB_HDR_OFF_PLAIN_SIZE     32
#define GSPB_HDR_OFF_CHUNK_COUNT    40
#define GSPB_HDR_OFF_MAC            48
#define GSPB_HDR_AAD_PREFIX_LEN     32   /* header bytes covered by the AAD */
#define GSPB_HDR_MAC_INPUT_LEN      48   /* header bytes covered by the header MAC */
#define GSPB_ENC_FILE_NONCE_LEN     8
#define GSPB_ENC_FILE_ID_LEN        8

/* AAD = header prefix || chunk index (4B) || plaintext length (4B) */
#define GSPB_ENC_AAD_LEN            (GSPB_HDR_AAD_PREFIX_LEN + 8)
#define GSPB_ENC_AAD_OFF_CHUNK      GSPB_HDR_AAD_PREFIX_LEN
#define GSPB_ENC_AAD_OFF_PLAIN_LEN  (GSPB_HDR_AAD_PREFIX_LEN + 4)
#define GSPB_ENC_NONCE_OFF_CHUNK    8

#define GSPB_ENC_MIN_CHUNK          (64 * 1024)
#define GSPB_ENC_MAX_CHUNK          (16 * 1024 * 1024)
#define GSPB_ENC_DEFAULT_CHUNK      (1024 * 1024)

#define GSPB_KDF_DEFAULT_ITERATIONS 600000

/* upper bound for keyinfo lines and for passphrase/keyfile material */
#define GSPB_ENC_TEXT_BUF_LEN       1024
#define GSPB_ENC_DECIMAL_BASE       10
#define GSPB_ENC_BITS_PER_BYTE      8
#define GSPB_ENC_SIZE_KB            1024
/* HKDF info strings are short labels; bound the expand buffer */
#define GSPB_ENC_HKDF_INFO_MAX      64
/* backup.keyinfo and backup.control.mac are a few hundred bytes at most */
#define GSPB_ENC_METADATA_MAX_SIZE  (GSPB_ENC_SIZE_KB * GSPB_ENC_SIZE_KB)
/* a backup directory never sits this deep below the instance root */
#define GSPB_ENC_MAX_PARENT_DEPTH   16

/* base64 text fields of backup.keyinfo */
#define GSPB_ENC_B64_FIELD_LEN      256
#define GSPB_ENC_B64_SHORT_LEN      64

#define GSPB_KEYINFO_FILE           "backup.keyinfo"
#define GSPB_CONTROL_MAC_FILE       "backup.control.mac"
#define GSPB_PASSPHRASE_ENV         "GS_PROBACKUP_PASSPHRASE"

/* command line state, defined in pg_probackup.cpp */
extern bool  g_encryptEnabled;
extern char *g_encryptAlgorithmStr;
extern char *g_encryptKeySourceStr;
extern char *g_encryptKeyArg;
extern char *g_encryptKeyFile;
extern char *g_encryptChunkSizeStr;
extern char *g_newEncryptKeyArg;
extern char *g_newEncryptKeyFile;

/* option handling */
extern void EncryptScrubArgv(int argc, char **argv);
extern void EncryptValidateOptions(const char *commandName);

/* per-backup key material lifecycle */
extern void EncryptSetupBackup(const char *backupRoot);
extern void EncryptForgetBackup(const char *backupRoot);
extern void EncryptBackupRenamed(const char *oldRoot, const char *newRoot);
extern bool EncryptDirIsEncrypted(const char *backupRoot);
extern bool EncryptPathIsEncrypted(const char *path);
extern void EncryptCleanup(void);
extern void EncryptRefreshControlMac(const char *backupRoot);
extern void EncryptVerifyControlMac(const char *backupRoot);
extern void EncryptPrepareMetadataUpdate(const char *backupRoot);
extern void EncryptRekeyBackup(const char *backupRoot);

/*
 * Transparent stream layer. Files that do not belong to an encrypted backup
 * are opened as plain streams, so callers need no conditional logic.
 * Streams are closed with plain fclose(): the container flushes its tail
 * chunk from the stdio close callback.
 */
extern FILE *EncFopen(const char *path, const char *mode);
extern FILE *EncFopenStaged(const char *path);
extern bool  EncSealStagedFile(FILE *stage, const char *path);
extern bool  EncStreamIsEncrypted(FILE *fp);
extern int   EncFsyncStream(FILE *fp);

/* whole-file helpers for paths that do not go through stdio */
extern bool  EncFileIsContainer(const char *path);
extern bool  EncEncryptFileInplace(const char *path);
extern bool  EncReadAt(const char *path, void *buf, size_t len, off_t offset);
extern char *EncSlurpFile(const char *path, size_t *filesize, bool safe);
extern void  EncCloseCachedReader(void);
extern int64 enc_plain_size(const char *path, int64 diskSize);
extern int64 enc_expected_disk_size(const char *path, int64 plainSize);

#endif /* BACKUP_ENCRYPT_H */
