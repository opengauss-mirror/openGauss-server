/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 */
#ifndef SEC_SDF_TDE_PROVIDER_H
#define SEC_SDF_TDE_PROVIDER_H

#include "postgres.h"

/* Encode the startup-configured KEK index into the existing CMK representation. */
extern bool SdfTdeGetCmkId(char* cmkId, Size capacity);

/* Startup reads JSON; reload only rereads the selected password file. */
extern bool SdfTdeInitialize(char* errorMsg, Size errorMsgSize);
extern bool SdfTdeReload(char* errorMsg, Size errorMsgSize);

/* One resident session and a reference-counted handle cache per DN; no plaintext DEKs. */
extern bool SdfCreateTdeDek(const char* cmkId, char** dekCipher, char* errorMsg, Size errorMsgSize);
extern bool SdfGetTdeDek(const char* cmkId, const char* dekCipher, char* errorMsg, Size errorMsgSize);
extern bool SdfTdeCryptPage(const char* cmkId, const char* dekCipher, const uint8* iv,
    const char* input, char* output, Size length, bool encrypt, char* errorMsg, Size errorMsgSize);

#endif /* SEC_SDF_TDE_PROVIDER_H */
