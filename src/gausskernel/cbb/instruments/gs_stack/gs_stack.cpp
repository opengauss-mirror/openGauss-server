/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd. All rights reserved.
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
 * ---------------------------------------------------------------------------------------
 *
 * gs_stack.cpp
 *
 * IDENTIFICATION
 *        src/gaussdbkernel/cbb/instruments/gs_stack/gs_stack.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "c.h"
#include "funcapi.h"
#include "pgstat.h"
#include <dlfcn.h>
#include <execinfo.h>
#include "access/hash.h"
#include "utils/elf_parser.h"
#include <cxxabi.h>

#define MAX_LEN_KEY 30
#define GS_STACK_HASHTBL "stack hash table"

const int FINISH_STACK = 2;
const int US_PER_WAIT = 5;
const int MAX_SIZE_GS_STACK_HASH = 100;
const char *const g_gs_stack_signal_file = "gs_stack.cmd";
const char *const g_gs_stack_result_file = "gs_stack.ret";
const char *const g_gs_stack_tmp_file = "gs_stack.ret.tmp";

typedef struct {
    size_t length_name;
    char elfname[MAX_LEN_KEY];
} GsStackKey;

typedef struct {
    GsStackKey key;
    int header_counter;
    int symbol_counter[SYMHDR_NUM];
    bool dyn;
    Elf64_Sym* sym[SYMHDR_NUM];
    char* str_buff[SYMHDR_NUM];
}GsStackEntry;

typedef struct {
    ThreadId tid;
    pid_t lwtid;
}ThreadInfo;

uint32 GsStackHashFunc(const void *key, Size keysize)
{
    const GsStackKey *item = (const GsStackKey *) key;
    uint32 val1 = DatumGetUInt32(hash_any((const unsigned char *)item->elfname, item->length_name));
    return val1;
}

int GsStackMatch(const void* key1, const void* key2, Size keysize)
{
    const GsStackKey* k1 = (const GsStackKey*)key1;
    const GsStackKey* k2 = (const GsStackKey*)key2;

    if (k1 != NULL && k2 != NULL && k1->length_name == k2->length_name &&
        (strncmp(k1->elfname, k2->elfname, k1->length_name) == 0)) {
        return 0;
    }

    return 1;
}

void InitGsStack()
{
    // init memory context
    if (g_instance.stat_cxt.GsStackContext == NULL) {
        g_instance.stat_cxt.GsStackContext = AllocSetContextCreate(g_instance.instance_context,
            "GsStackContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    }

    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");

    ctl.hcxt = g_instance.stat_cxt.GsStackContext;
    ctl.keysize = sizeof(GsStackKey);
    ctl.entrysize = sizeof(GsStackEntry);

    ctl.hash = GsStackHashFunc;
    ctl.match = GsStackMatch;

    g_instance.stat_cxt.GsStackHashTbl = hash_create(GS_STACK_HASHTBL,
    MAX_SIZE_GS_STACK_HASH,
    &ctl,
    HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_NOEXCEPT);
}

uint get_backtrace_refresh_flag(void)
{
    return (uint)g_instance.stat_cxt.backtrace_info.refresh_flag;
}

void get_backtrace(void)
{
    g_instance.stat_cxt.backtrace_info.number_pc =
        backtrace(g_instance.stat_cxt.backtrace_info.backtrace_buff, STACK_PRINT_LIMIT);
    g_instance.stat_cxt.backtrace_info.refresh_flag = FINISH_STACK;
}

void print_stack(SIGNAL_ARGS)
{
    get_backtrace();
}

bool ready_to_start_backtrace(uint old_refresh_flag)
{
    (void)gs_compare_and_swap_u32(&(g_instance.stat_cxt.backtrace_info.refresh_flag), old_refresh_flag, 1);
    if (g_instance.stat_cxt.backtrace_info.refresh_flag == 1) {
        return true;
    }
    return false;
}

void init_backtrace_info(void)
{
    g_instance.stat_cxt.backtrace_info.refresh_flag = 1;
    int ret = memset_s(g_instance.stat_cxt.backtrace_info.backtrace_buff, STACK_PRINT_LIMIT, 0, STACK_PRINT_LIMIT);
    securec_check(ret, "\0", "\0");
    g_instance.stat_cxt.backtrace_info.number_pc = 0;
}

void free_entry(GsStackEntry *entry)
{
    if (entry == NULL) {
        return;
    }

    int i;
    for (i = 0; i < entry->header_counter; i++) {
        if (entry->sym[i] != NULL) {
            pfree(entry->sym[i]);
            entry->sym[i] = NULL;
        }
        if (entry->str_buff[i] != NULL) {
            pfree(entry->str_buff[i]);
            entry->str_buff[i] = NULL;
        }
    }
}

bool load_string_table(GsStackEntry *entry, int fd, const Elf64_Shdr* symhdr, const Elf64_Ehdr ehdr, Elf64_Shdr shdr)
{
    off_t off;
    size_t len_str_buff[SYMHDR_NUM] = {0};

    for (int ti = 0; ti < entry->header_counter; ti++) {
        /* sh_offset gives the byte offset from the beginning of the file to the first byte in the section */
        off = (off_t)(symhdr[ti].sh_offset);
        if (lseek(fd, off, SEEK_SET) != off) {
            return false;
        }
        /* symbol table holds a table of fixed-size entries, sh_entsize gives the size in bytes of each entry */
        entry->symbol_counter[ti] = (int)(symhdr[ti].sh_size / symhdr[ti].sh_entsize);
        /* sh_size gives the section's size in bytes */
        entry->sym[ti] = (Elf64_Sym*)palloc0(symhdr[ti].sh_size);
        for (int si = 0; si < entry->symbol_counter[ti]; si++) {
            if (read(fd, &entry->sym[ti][si], sizeof(Elf64_Sym)) != sizeof(Elf64_Sym)) {
                return false;
            }
        }
        /* sh_link holds a section header table index link, for UNIX system V,
           The section header index of the associated string table */
        off = (off_t)(ehdr.e_shoff + symhdr[ti].sh_link * ehdr.e_shentsize);
        if (lseek(fd, off, SEEK_SET) != off) {
            return false;
        }
        if (read(fd, &shdr, sizeof(Elf64_Shdr)) != sizeof(Elf64_Shdr)) {
            return false;
        }

        off = (off_t)(shdr.sh_offset);
        if (lseek(fd, off, SEEK_SET) != off) {
            return false;
        }
        len_str_buff[ti] = shdr.sh_size + 1;
        entry->str_buff[ti] = (char*)palloc0(len_str_buff[ti]);
        if (read(fd, entry->str_buff[ti], shdr.sh_size) == -1) {
            return false;
        }
    }

    return true;
}

bool load_symbol_to_hashtbl(GsStackEntry *entry, int fd)
{
    const int limit = 1000;
    Elf64_Ehdr ehdr;
    Elf64_Shdr shdr;
    Elf64_Shdr symhdr[SYMHDR_NUM] = {0};
    off_t off;

    if (read(fd, &ehdr, sizeof(Elf64_Ehdr)) != sizeof(Elf64_Ehdr)) {
        return false;
    }
    entry->dyn = (ehdr.e_type == ET_DYN) ? true : false;
    /* e_shnum holds the number of entries in the section header table. */
    if (ehdr.e_shnum > limit) {
        return false;
    }
    entry->header_counter = 0;
    for (unsigned int i = 0; i < ehdr.e_shnum; i++) {
        /* e_shoff holds the section header table's file offset in bytes. */
        /* e_shentsize holds a section header's size in bytes. A section header is
           one entry in the section header table, all entries are the same size */
        off = (off_t)(ehdr.e_shoff + i * ehdr.e_shentsize);
        if (pread(fd, &shdr, sizeof(Elf64_Shdr), off) != sizeof(Elf64_Shdr)) {
            return false;
        }
        /* sh_type categorizes the section's contents and semantics. SHT_SYMTAB hold a symbol table. Currently,
           an object file may have only one section of each type, but this restriction may be relaxed in the future.
           Typically, SHT_SYMTAB provides symbols for link editing, though it may also be used for dynamic linking.
           As a complete symbol table, it may contain many symbols unnecessary for dynamic linking.
           Consequently, an object file may also contain a SHT_DYNSYM section, which holds a minimal set of dynamic
           linking symbols, to save space. */
        if ((shdr.sh_type == SHT_SYMTAB || shdr.sh_type == SHT_DYNSYM) &&
             entry->header_counter < SYMHDR_NUM) {
            if (memcpy_s(&(symhdr[entry->header_counter]), sizeof(Elf64_Shdr), &shdr, sizeof(Elf64_Shdr))) {
                return false;
            }
            entry->header_counter++;
        }
    }

    return load_string_table(entry, fd, symhdr, ehdr, shdr);
}

bool find_symbol_entry(const char *filename, HTAB *gs_stack_hashtbl, GsStackEntry **entry)
{
    Assert(filename);

    GsStackKey key_gs_stack = {0};
    bool ret = true;
    int n_ret;
    bool found = false;

    char* base_name = basename((char*)filename);
    key_gs_stack.length_name = strlen(base_name);
    n_ret = snprintf_s(key_gs_stack.elfname, MAX_LEN_KEY, MAX_LEN_KEY - 1, "%s", base_name);
    if (n_ret < 0) {
        /* if file name longer than keysize, we just choose first keysize byte from filename as key. */
        ereport(LOG, (errmsg("elf name %s is too long.", base_name)));
    }

    *entry = (GsStackEntry *)hash_search(gs_stack_hashtbl, (const void*)(&key_gs_stack),
        HASH_ENTER, &found);
    if (!found) {
        int fd = open(filename, O_RDONLY);
        if (fd != -1) {
            ret = load_symbol_to_hashtbl(*entry, fd);
            (void)close(fd);
        }
        if (fd == -1 || ret != true) {
            if (ret != true) {
                free_entry(*entry);
            }
            if (hash_search(gs_stack_hashtbl,
                (const void*)(&key_gs_stack), HASH_REMOVE, NULL) == NULL) {
                ereport(ERROR, (
                    errcode(ERRCODE_DATA_CORRUPTED), errmsg("gs_stack hash table corrupted")));
            }
            return false;
        }
    }
    return true;
}

void demangle_one_symbol(const char * symbol_name, char* demangled_symbol_name, size_t len)
{
    size_t len_demangle = 0;
    int status;
    errno_t errorno;

    char* demangle_buf = abi::__cxa_demangle(symbol_name, NULL, &len_demangle, &status);
    if (status != 0) {
        ereport(LOG, (errmsg("can not demangle %s. status is %d", symbol_name, status)));
        if (demangle_buf != NULL) {
            free(demangle_buf);
            demangle_buf = NULL;
        }
        errorno = memcpy_s(demangled_symbol_name, len, symbol_name, len);
        securec_check(errorno, "\0", "\0");
    } else {
        ereport(LOG, (errmsg("demangle from  %s to %s len_demangle is %zu", symbol_name, demangle_buf, len_demangle)));
        errorno = memcpy_s(demangled_symbol_name, len, demangle_buf, len_demangle);
        free(demangle_buf);
        securec_check(errorno, "\0", "\0");
    }
}

bool resolve_addr_from_entry(const GsStackEntry *entry, uintptr_t addr, char *buf, size_t len, uint *offset)
{
    for (int ti = 0; ti < entry->header_counter; ti++) {
        if (entry->sym[ti] == NULL) {
            continue;
        }
        for (int si = 0; si < entry->symbol_counter[ti]; si++) {
            if ((ELF64_ST_TYPE(entry->sym[ti][si].st_info) != STT_FUNC)
                || entry->sym[ti][si].st_value > addr
                || (entry->sym[ti][si].st_value + entry->sym[ti][si].st_size) < addr) {
                continue;
            }
            *offset = (uint)(addr - entry->sym[ti][si].st_value);
            demangle_one_symbol(entry->str_buff[ti] + entry->sym[ti][si].st_name, buf, len);
            return true;
        }
    }
    return false;
}

void addr_to_name_bin(void* pc, const Dl_info dlinfo, StringInfoData* call_stack)
{
    bool ret_resolve = false;
    GsStackEntry* symbol_entry = NULL;
    char tmp_buf[1024] = {0};
    uint sym_off;

    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.stat_cxt.GsStackContext);
    uintptr_t dl_off = (uintptr_t)pc - (uintptr_t)dlinfo.dli_fbase;
    if (find_symbol_entry(dlinfo.dli_fname, g_instance.stat_cxt.GsStackHashTbl, &symbol_entry)) {
        uintptr_t addr = symbol_entry->dyn ? dl_off : (uintptr_t)pc;
        ret_resolve = resolve_addr_from_entry(symbol_entry, addr, tmp_buf, sizeof(tmp_buf), &sym_off);
        (void)MemoryContextSwitchTo(oldcontext);
        if (ret_resolve) {
            appendStringInfo(call_stack, "%s + 0x%x\n", tmp_buf, sym_off);
        } else {
            appendStringInfo(call_stack, "%p\n", pc);
        }
    } else {
        (void)MemoryContextSwitchTo(oldcontext);
        appendStringInfo(call_stack, "%p\n", pc);
    }
}

void addr_to_name_reuse(void* pc, StringInfoData* call_stack)
{
    Dl_info dlinfo;
    char tmp_buf[1024] = {0};

    if (dladdr(pc, &dlinfo) &&
            dlinfo.dli_fbase && dlinfo.dli_fname) {
        /*
         * If a function's name does not in the dynamic symbol table,
         * dladdr can't resolve the function's symbol. In this case, we
         * should parse the executable file to find nearest function name.
         */
        uint sym_off;
        if (dlinfo.dli_saddr && dlinfo.dli_sname) {
            if (ENABLE_DMS && (strcmp(dlinfo.dli_sname, "dms_request_page") == 0)) {
                Buffer bufferid = ResourceOwnerGetBuffer(t_thrd.utils_cxt.CurrentResourceOwner);
                if (bufferid > 0) {
                    BufferDesc *buf_desc = GetBufferDescriptor(bufferid - 1);
                    ereport(LOG, (errmsg("dlinfo.dli_sname %s. Current hold buffer: %u/%u/%u/%d %d-%u",
                        dlinfo.dli_sname, buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode,
                        buf_desc->tag.rnode.relNode, buf_desc->tag.rnode.bucketNode, buf_desc->tag.forkNum,
                        buf_desc->tag.blockNum)));
                }
            } else {
                ereport(LOG, (errmsg("dlinfo.dli_sname %s.", dlinfo.dli_sname)));
            }
            demangle_one_symbol(dlinfo.dli_sname, tmp_buf, sizeof(tmp_buf));
            sym_off = (uint)((uintptr_t)pc - (uintptr_t)dlinfo.dli_saddr);
            appendStringInfo(call_stack, "%s + 0x%x\n", tmp_buf, sym_off);
        } else {
            addr_to_name_bin(pc, dlinfo, call_stack);
        }
    } else {
        appendStringInfo(call_stack, "%p\n", pc);
    }
}

bool ready_to_get_backtrace(void)
{
    return (g_instance.stat_cxt.backtrace_info.refresh_flag == FINISH_STACK);
}

void finish_backtrace(void)
{
    g_instance.stat_cxt.backtrace_info.refresh_flag = 0;
}

void get_stack(StringInfoData* call_stack)
{
    int i;
#ifdef ENABLE_MEMORY_CHECK
    /* not show __interceptor_backtrace.part.110<-get_backtrace<-print_stack<-gs_signal_handle
        <-gs_res_signal_handler<-__restore_rt */
    const int number_pc_show = 6;
#else
    /* not show get_backtrace<-print_stack<-gs_signal_handle<-gs_res_signal_handler<-__restore_rt */
    const int number_pc_show = 5;
#endif
    for (i = number_pc_show; i < g_instance.stat_cxt.backtrace_info.number_pc; i++) {
        addr_to_name_reuse(g_instance.stat_cxt.backtrace_info.backtrace_buff[i], call_stack);
    }
}

void get_stack_according_to_tid(ThreadId tid,         StringInfoData* call_stack)
{
    const int MAX_WAIT = 1000;
    int i;

    if (tid == 0) {
        ereport(WARNING,
            (errmodule(MOD_GSSTACK),
             errcode(ERRCODE_INVALID_STATUS),
             (errmsg("can not get backtrace for thread 0."),
              errdetail("0 is not a valid thread id."))));
        appendStringInfo(call_stack, "invalid thread id\n");
        return;
    }
    (void)LWLockAcquire(GsStackLock, LW_EXCLUSIVE);
    PG_TRY();
    {
        init_backtrace_info();

        signal_child(tid, SIGURG, -1);
        for (i = 0; i < MAX_WAIT; i++) {
            if (ready_to_get_backtrace()) {
                break;
            }
            pg_usleep(US_PER_WAIT);
        }
        ereport(LOG, (errmsg("wait %d times.", i)));
        if (i == MAX_WAIT) {
            ereport(WARNING,
                (errmodule(MOD_GSSTACK),
                 errcode(ERRCODE_INVALID_STATUS),
                 (errmsg("can not get backtrace for thread %lu.", tid),
                  errdetail("This thread maybe finished,"
                      "or the signal handler of this thread had not been registed."))));
            appendStringInfo(call_stack, "thread %lu not available\n", tid);
        } else {
            get_stack(call_stack);
        }
        finish_backtrace();
    }
    PG_CATCH();
    {
        LWLockRelease(GsStackLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LWLockRelease(GsStackLock);
}

bool get_thread_info_from_signal_slot(pid_t lwtid, ThreadInfo** thread_info, int* size)
{
    unsigned int loop;
    int real_size = 0;

    (void)pthread_mutex_lock(&(g_instance.signal_base->slots_lock));
    GsSignalSlot* signal_slot = g_instance.signal_base->slots;
    *thread_info = (ThreadInfo*)palloc0(sizeof(ThreadInfo) * g_instance.signal_base->slots_size);
    unsigned int slot_size = g_instance.signal_base->slots_size;
    for (loop = 0; loop < slot_size; loop++) {
        if (lwtid == 0) {
            if (signal_slot->thread_id != 0) {
                (*thread_info)[real_size].tid = signal_slot->thread_id;
                (*thread_info)[real_size].lwtid = signal_slot->lwtid;
                real_size++;
            }
        } else {
            if (signal_slot->lwtid == lwtid) {
                (*thread_info)[0].tid = signal_slot->thread_id;
                (*thread_info)[0].lwtid = signal_slot->lwtid;
                real_size = 1;
                break;
            }
        }
        signal_slot++;
    }
    (void)pthread_mutex_unlock(&(g_instance.signal_base->slots_lock));
    *size = real_size;

    return (real_size >= 1);
}

void get_stack_according_to_lwtid(pid_t lwtid, StringInfoData* call_stack)
{
    int loop = 0;
    ThreadInfo* thread_info = NULL;
    int slot_size = 0;

    bool found = get_thread_info_from_signal_slot(lwtid, &thread_info, &slot_size);
    if (lwtid != 0) {
        if (found && thread_info[0].tid > 0) {
            appendStringInfo(call_stack, "tid<%lu> lwtid<%d>\n", thread_info[0].tid, lwtid);
            get_stack_according_to_tid(thread_info[0].tid, call_stack);
        } else {
            ereport(ERROR,
                (errmodule(MOD_GSSTACK), errcode(ERRCODE_INVALID_STATUS),
                 (errmsg("can not get backtrace for thread %d.", lwtid),
                  errdetail("please check if the thread is alive."))));
        }
    } else {
        for (loop = 0; loop < slot_size; loop++) {
            if (thread_info[loop].tid != 0) {
                appendStringInfo(call_stack,
                    "Thread %d tid<%lu> lwtid<%d>\n", loop, thread_info[loop].tid, thread_info[loop].lwtid);
                get_stack_according_to_tid(thread_info[loop].tid, call_stack);
                appendStringInfo(call_stack, "\n");
            }
        }
    }

    if (thread_info != NULL) {
        pfree(thread_info);
    }
}

void check_and_process_gs_stack()
{
    struct stat stat_buf;

    if (stat(g_gs_stack_signal_file, &stat_buf) != 0) {
        return;
    }

    g_instance.stat_cxt.stack_perf_start = true;
}

void gs_stack_unlink_file(const char* filename)
{
    if (filename == NULL) {
        return;
    }

    int ret = unlink(filename);
    if (ret != 0 && (errno != ENOENT)) {
        ereport(WARNING, (errmsg("unlink %s failed.", filename)));
    }
}

void gs_stack_read_signal_file(pid_t* lwtid, pid_t* ctl_pid)
{
    size_t ret;

    FILE* fd = fopen(g_gs_stack_signal_file, "r");
    if (fd == NULL) {
        gs_stack_unlink_file(g_gs_stack_signal_file);
        ereport(ERROR, (errmsg("open sig file failed.")));
    }

    ret = fread(ctl_pid, sizeof(pid_t), 1, fd);
    if (ret != 1) {
        (void)fclose(fd);
        gs_stack_unlink_file(g_gs_stack_signal_file);
        ereport(ERROR, (errmsg("read ctl pid failed err.")));
    }

    ret = fread(lwtid, sizeof(pid_t), 1, fd);
    if (ret != 1) {
        (void)fclose(fd);
        gs_stack_unlink_file(g_gs_stack_signal_file);
        ereport(ERROR, (errmsg("read ctl lwtid failed err.")));
    }
    (void)fclose(fd);
    gs_stack_unlink_file(g_gs_stack_signal_file);
}

void gs_stack_write_result_file(const char* result, int length, int ctl_pid)
{
    int mode = O_WRONLY | O_CREAT | PG_BINARY;
    int flags = 0600;

    int fd = open(g_gs_stack_tmp_file, mode, flags);
    if (fd < 0) {
        ereport(LOG, (errmsg("open result file failed.")));
        return;
    }

    ssize_t ret = write(fd, &ctl_pid, sizeof(int));
    if (ret == -1) {
        ereport(LOG, (errmsg("write ctl pid to file failed.")));
        (void)close(fd);
        return;
    }

    ret = write(fd, &length, sizeof(int));
    if (ret == -1) {
        ereport(LOG, (errmsg("write stack size to file failed.")));
        (void)close(fd);
        return;
    }

    ret = write(fd, result, length);
    if (ret == -1) {
        ereport(LOG, (errmsg("write stack to file failed.")));
        (void)close(fd);
        return;
    }

    int iret = fsync(fd);
    if (iret != 0) {
        ereport(LOG, (errmsg("sync tmp file failed.")));
        (void)close(fd);
        return;
    }
    (void)close(fd);
    gs_stack_unlink_file(g_gs_stack_result_file);
    iret = rename(g_gs_stack_tmp_file, g_gs_stack_result_file);
    if (iret != 0) {
        ereport(LOG, (errmsg("rename tmp file to stack failed.")));
        return;
    }
}

void get_stack_and_write_result()
{
    pid_t lwtid = 0;
    pid_t ctl_pid = 0;
    StringInfoData result;

    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.stat_cxt.GsStackContext);
    PG_TRY();
    {
        initStringInfo(&result);
        gs_stack_read_signal_file(&lwtid, &ctl_pid);
        get_stack_according_to_lwtid(lwtid, &result);
    }
    PG_CATCH();
    {
        /* Must reset elog.c's state */
        (void)MemoryContextSwitchTo(g_instance.stat_cxt.GsStackContext);
        ErrorData* edata = CopyErrorData();
        FlushErrorState();
        appendStringInfo(&result, "%s", edata->message);
        /* release edata */
        FreeErrorData(edata);
    }
    PG_END_TRY();

    gs_stack_write_result_file(result.data, result.len, ctl_pid);
    FreeStringInfo(&result);
    (void)MemoryContextSwitchTo(oldcontext);
}

void print_all_stack()
{
    StringInfoData result;
    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.stat_cxt.GsStackContext);
    PG_TRY();
    {
        initStringInfo(&result);
        get_stack_according_to_lwtid(0, &result);
    }
    PG_CATCH();
    {
        /* Must reset elog.c's state */
        ErrorData* edata = CopyErrorData();
        FlushErrorState();
        appendStringInfo(&result, "%s", edata->message);
        /* release edata */
        FreeErrorData(edata);
    }
    PG_END_TRY();
    ereport(LOG, (errmsg("Print all thread stack \n%s", result.data)));
    FreeStringInfo(&result);
    (void)MemoryContextSwitchTo(oldcontext);
}

void save_all_stack_to_tuple(PG_FUNCTION_ARGS)
{
    const int attrs = 3;
    int i = 1;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    StringInfoData call_stack;
    initStringInfo(&call_stack);

    TupleDesc tupdesc = CreateTemplateTupleDesc(attrs, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "tid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "lwtid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i, "stack", TEXTOID, -1, 0);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);
    (void)MemoryContextSwitchTo(oldcontext);

    errno_t rc;
    Datum values[attrs];
    bool nulls[attrs];
    ThreadInfo* thread_info = NULL;
    int slot_size = 0;
    (void)get_thread_info_from_signal_slot(0, &thread_info, &slot_size);

    for (i = 0; i <= slot_size; i++) {
        if (thread_info[i].tid > 0) {
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "\0", "\0");
            rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
            securec_check(rc, "\0", "\0");
            values[ARG_0] = Int64GetDatum(thread_info[i].tid);
            values[ARG_1] = Int32GetDatum(thread_info[i].lwtid);
            get_stack_according_to_tid(thread_info[i].tid, &call_stack);
            values[ARG_2] = CStringGetTextDatum(call_stack.data);
            tuplestore_putvalues(rsinfo->setResult, tupdesc, values, nulls);
            resetStringInfo(&call_stack);
        }
    }
    pfree(thread_info);
    FreeStringInfo(&call_stack);
    tuplestore_donestoring(rsinfo->setResult);
}

 /* print stacks of threads */
Datum gs_stack(PG_FUNCTION_ARGS)
{
    if (!superuser() && (!isMonitoradmin(GetUserId())))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or monitor admin to get stack."))));

    ThreadId tid;
    StringInfoData call_stack;
    initStringInfo(&call_stack);

    if (PG_NARGS() == 1) {
        tid =  (ThreadId)DatumGetUInt64(PG_GETARG_DATUM(0));
        get_stack_according_to_tid(tid, &call_stack);
    } else {
        save_all_stack_to_tuple(fcinfo);
        return (Datum)0;
    }

    PG_RETURN_TEXT_P(cstring_to_text(call_stack.data));
}
