/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * tde_key_storage.h
 *    TDE key storage is the external interface of key storage
 *
 * IDENTIFICATION
 *    src/include/tde_key_management/tde_key_storage.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_TDE_KEY_STORAGE_H
#define SEC_TDE_KEY_STORAGE_H

#include "tde_key_management/data_common.h"
#include "utils/hsearch.h"

namespace TDE {
class TDEKeyStorage : public BaseObject {
public:
    static TDEKeyStorage& get_instance()
    {
        static TDEKeyStorage instance;
        return instance;
    }

    void init();
    void clear();
    void reset();
    bool empty();

    bool insert_cache(TDECacheEntry* tde_cache_entry);
    char* search_cache(const char* dek_cipher);
    void cache_watch_dog();
    void clean_cache_entry_value(char* dek_plaintext);
    void reset_dek_plaintext(char* dek_plaintext);

private:
    TDEKeyStorage();
    ~TDEKeyStorage();
    static uint32 tde_cache_entry_hash_func(const void* key, Size keysize);
    static int tde_cache_entry_match_func(const void* key1, const void* key2, Size keySize);

private:
    MemoryContext tde_cache_mem;
    HTAB* tde_cache;
    const char* tde_cache_name = "TDE Key Hash Table";
    const int max_bukect = 64;
};

/* TDE buffer rel file node cache management */
class TDEBufferCache : public BaseObject {
public:
    static TDEBufferCache& get_instance()
    {
        static TDEBufferCache instance;
        return instance;
    }

    void init();
    void clear();
    void reset();
    bool empty();

    bool insert_cache(RelFileNode tde_rnode, TdeInfo *tde_info);
    void search_cache(RelFileNode tde_rnode, TdeInfo *tde_info);

private:
    TDEBufferCache();
    ~TDEBufferCache();

private:
    MemoryContext tde_buffer_mem;
    HTAB* tde_buffer_cache;
    const char* tde_buffer_name = "TDE table encryption key hash table";
    const int max_bucket = 64;
};
}
#endif
