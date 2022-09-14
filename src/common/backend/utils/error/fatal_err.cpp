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
 * ---------------------------------------------------------------------------------------
 *
 * fatal_err.cpp
 *
 * IDENTIFICATION
 *        src/common/backend/utils/error/fatal_err.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <execinfo.h>
#include <dlfcn.h>
#include <sys/utsname.h>
#include <fcntl.h>
#include <time.h>
#include <sys/syscall.h>

#include "postgres.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#endif
#include "utils/fatal_err.h"
#include "utils/elf_parser.h"
#include "utils/guc_tables.h"
#include "knl/knl_session.h"
#include "knl/knl_thread.h"

#define handle_sig_error(msg) \
        do { \
            return false; \
        } while (0)

#define SIG_BUFLEN 1024
#define STACK_PRINT_LIMIT 128
#define TIME_STR_LEN 50

struct sig_unit {
    const char *name;
    int num;
};

static const sig_unit sig_table[] = {
    {"SIGSEGV", SIGSEGV},
    {"SIGABRT", SIGABRT},
    {"SIGILL", SIGILL},
    {"SIGBUS", SIGBUS},
    {"SIGTRAP", SIGTRAP},
    {"SIGFPE", SIGFPE},
    {NULL, -1}
};

/*
 * Open gaussdb error output logfile. The file name format is like
 *      $GAUSSLOG/ffic_log/ffic_gaussdb-$time.log
 * Note: First Failure Info Capture(FFIC)
 */
static bool open_gs_err(int *fd)
{
    int res;
    char path[MAXPGPATH];
    const char *dir = getenv("GAUSSLOG");

    /* If $GAUSSLOG does not set or invalid, replace with current directory */
    if (check_backend_env_sigsafe(dir) != ENV_OK) {
        dir = "./";
    }

    res = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/ffic_log", dir);
    if (res == -1) {
        handle_sig_error("fail to concatenate dir name");
    }

    if (mkdir(path, S_IRWXU) == -1 &&
            errno != EEXIST) {
        handle_sig_error("fail to mkdir $GAUSSLOG/ffic_log");
    }
    pg_time_t stamp_time = (pg_time_t)time(NULL);
    char strfbuf[TIME_STR_LEN];
    pg_tm localTime;
    pg_tm *p = NULL;
    if (log_timezone != NULL && (p = pg_localtime_s(&stamp_time, &localTime, log_timezone)) != NULL) {
        pg_strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d_%H%M%S", p);
        res = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/ffic_log/ffic_gaussdb-%s.log",
            dir, strfbuf);
        if (res == -1) {
            handle_sig_error("fail to concatenate file name");
        }
    } else {
        res = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/ffic_log/ffic_gaussdb-%lu.log",
            dir, time(NULL));
        if (res == -1) {
            handle_sig_error("fail to concatenate file name");
        }
    }
    *fd = open(path, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
    if (*fd == -1) {
        handle_sig_error("fail to open log dir");
    }

    return true;
}

/*
 * Get pc and sp value when the fatal error occurs
 */
#ifdef __x86_64__
static uintptr_t get_pc(const ucontext_t *uc)
{
    return (uintptr_t)uc->uc_mcontext.gregs[REG_RIP];
}

static uintptr_t get_sp(const ucontext_t *uc)
{
    return (uintptr_t)uc->uc_mcontext.gregs[REG_RSP];
}
#elif __aarch64__
static uintptr_t get_pc(const ucontext_t *uc)
{
    return (uintptr_t)uc->uc_mcontext.pc;
}

static uintptr_t get_sp(const ucontext_t *uc)
{
    return (uintptr_t)uc->uc_mcontext.sp;
}
#else
#define get_pc(uc) 0
#define get_sp(uc) 0
#endif

/*
 * Print header for an error message logfile
 */
static void print_header(int fd, uintptr_t pc)
{
    output(fd, "A fatal error occurred at pc = %p\n", (void *)pc);
    output(fd, "pid = %d, tid = %d\n\n", getpid(), (pid_t)syscall(SYS_gettid));
    output(fd, "====== GaussDB Version ======\n ");
    output(fd, "%s\n", PG_VERSION_STR);
    output(fd, "\n");
}

/*
 * Print fatal signal info
 */
static void print_siginfo(int fd, int sig, siginfo_t *si)
{
    output(fd, "====== Fatal signal info ====== \n");

    const char *sig_name = NULL;
    const struct sig_unit *su = sig_table;
    while (su->name) {
        if (su->num == sig) {
            sig_name = su->name;
            break;
        }
        su++;
    }

    sig_name = sig_name ? sig_name : "Unidentified";
    output(fd, "si_signo = %d (%s), si_code = %d",
            sig, sig_name, si->si_code);
    if (sig == SIGILL || sig == SIGFPE || sig == SIGSEGV || sig == SIGBUS || sig == SIGTRAP) {
        output(fd, ",si_addr = %p", si->si_addr);
    }

    output(fd, "\n\n");
}

/*
 * Print a frame information
 */
static void print_frame(int fd, void *pc, elf_parser *parser)
{
    Dl_info dlinfo;
    if (dladdr(pc, &dlinfo) &&
            dlinfo.dli_fbase && dlinfo.dli_fname) {
        uintptr_t dl_off = (uintptr_t)pc - (uintptr_t)dlinfo.dli_fbase;
        output(fd, "[%s + 0x%lx]", dlinfo.dli_fname, dl_off);

        /*
         * If a function's name does not in the dynamic symbol table,
         * dladdr can't resolve the function's symbol. In this case, we
         * should parse the executable file to find nearest function name.
         */
        uint sym_off;
        if (dlinfo.dli_saddr && dlinfo.dli_sname) {
            sym_off = (uint)((uintptr_t)pc - (uintptr_t)dlinfo.dli_saddr);
            output(fd, " %s + 0x%x", dlinfo.dli_sname, sym_off);
        } else {
            if (!parser->is_same_file(dlinfo.dli_fname) &&
                    !parser->reset(dlinfo.dli_fname)) {
                output(fd, "%p", pc);
            } else {
                /* If the current file is a shared object file, address to resolve
                 * is a pseudo address, otherwise it is pc.
                 */
                char buf[SIG_BUFLEN];
                uintptr_t addr = parser->isdyn() ? dl_off : (uintptr_t)pc;
                if (parser->resolve_addr(addr, buf, sizeof(buf), &sym_off)) {
                    output(fd, " %s + 0x%x", buf, sym_off);
                } else {
                    output(fd, "%p", pc);
                }
            }
        }
    } else {
        output(fd, "%p", pc);
    }

    output(fd, "\n");
}

/*
 * Print call stack, this function is called in signal handle context
 */
static void print_call_stack(int fd, uintptr_t pc)
{
    int nptrs;
    void *buf[STACK_PRINT_LIMIT];

    output(fd, "======= Call stack ====== \n");
    nptrs = backtrace(buf, STACK_PRINT_LIMIT);

    int i = 0;
    while (i < nptrs && buf[i] != (void *)pc) {
        i++;
    }

    elf_parser parser;
    while (i < nptrs) {
        print_frame(fd, buf[i], &parser);
        i++;
    }

    output(fd, "\n");
    output(fd, "# Two methods for parsing function names are listed here:\n");
    output(fd, "# 1. Using CLI tool such as \"c++filt\"\n");
    output(fd, "#       $ c++filt <function-symbol>\n");
    output(fd, "# 2. Or Using online tool\n");
    output(fd, "\n");
    output(fd, "# Two steps for parsing a line number:\n");
    output(fd, "# 1. objcopy --add-gnu-debuglink=<gaussdb-debuginfo-file> <gaussdb-exec-file>\n");
    output(fd, "# 2. addr2line -e <gaussdb-exec-file> <relative-address>\n");
    output(fd, "\n");
}

/*
 * Print registers info
 */
static void print_registers(int fd, const ucontext_t *uc)
{
    output(fd, "====== Registers ======\n");

#ifdef __x86_64__
    output(fd, "RDI = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RDI]);
    output(fd, "RSI = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RSI]);
    output(fd, "RBP = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RBP]);
    output(fd, "RBX = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RBX]);
    output(fd, "RDX = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RDX]);
    output(fd, "RAX = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RAX]);
    output(fd, "RCX = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RCX]);
    output(fd, "RSP = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RSP]);
    output(fd, "RIP = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_RIP]);
    output(fd, "R8  = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R8]);
    output(fd, "R9  = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R9]);
    output(fd, "R10 = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R10]);
    output(fd, "R11 = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R11]);
    output(fd, "R12 = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R12]);
    output(fd, "R13 = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R13]);
    output(fd, "R14 = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R14]);
    output(fd, "R15 = 0x%016lx\n", (uint64)uc->uc_mcontext.gregs[REG_R15]);
#elif __aarch64__
    const int normal_reg_num = 31;
    for (int r = 0; r < normal_reg_num; r++) {
        output(fd, "X%d = 0x%016lx\n", r, (uint64)uc->uc_mcontext.regs[r]);
    }
    output(fd, "SP = 0x%016lx\n", (uint64)uc->uc_mcontext.sp);
    output(fd, "PC = 0x%016lx\n", (uint64)uc->uc_mcontext.pc);
#endif

    output(fd, "\n");
}

/*
 * Print top of stack data
 */
static void print_top_stack(int fd, uintptr_t sp)
{
    output(fd, "====== Top of stack ======\nsp = 0x016%lx\n", sp);

    uint64 *start = (uint64 *)sp;
    uint64 *end = start + 64;

    while (start < end) {
        output(fd, "%p: 0x%016lx 0x%016lx\n", start, start[0], start[1]);
        start += 2;
    }

    output(fd, "\n");
}

/*
 * Print instructions context around pc
 */
static void print_inst_ctx(int fd, uintptr_t pc)
{
    output(fd, "Instructions: (pc = 0x%016lx)\n", pc);

    uint8 *start = (uint8 *)pc - 32;
    uint8 *end = (uint8 *)pc + 32;

    /* Ensure that the memory area around pc is valid by calling dladdr. While there
     * is special situation that cannot be considered when the pc is in the runtime
     * instruction generation arena. Fortunately this is not involved in GaussDB.
     */
    Dl_info dlinfo;
    if (dladdr((void *)start, &dlinfo) && dladdr((void *)end, &dlinfo)) {
        while (start < end) {
            output(fd, "%p: %02x %02x %02x %02x %02x %02x %02x %02x\n",
                    start, (uint)start[0], (uint)start[1], (uint)start[2], (uint)start[3],
                    (uint)start[4], (uint)start[5], (uint)start[6], (uint)start[7]);
            start += 8;
        }
    }

    output(fd, "\n");
    output(fd, "# You can use disassembly tool to obtain assembly instructions.\n");
    output(fd, "\n");
}

static void print_statement_info(int fd)
{
    output(fd, "====== Statememt info ======\n");
    output(fd, "[statement] unique SQL key - sql id: %lu, cn id: %u, user id: %u\n",
        u_sess->unique_sql_cxt.unique_sql_id,
        u_sess->unique_sql_cxt.unique_sql_cn_id,
        u_sess->unique_sql_cxt.unique_sql_user_id);
    output(fd, "[statement] debug query id: %lu\n", u_sess->debug_query_id);
    output(fd, "\n");
}

/*
 * Print an entire text file
 */
static void print_text_file(int fd, const char *fname)
{
    Assert(fname);
    int memfd = open(fname, O_RDONLY);
    if (memfd == -1) {
        output(fd, "Fail to open %s : %d\n", fname, errno);
        return;
    }

    output(fd, "====== %s ======\n", fname);

    char buf[128];
    int res;
    do {
        res = read(memfd, buf, sizeof(buf) - 1);
        if (res == -1) {
            output(fd, "Fail to read %s : %d\n", fname, errno);
            break;
        }

        buf[res] = '\0';
        output(fd, "%s", buf);

        if (res != (sizeof(buf) - 1)) {
            break;
        }
    } while (true);

    (void)close(memfd);
}

/*
 * Print memory layout
 */
static void print_mem_layout(int fd)
{
    print_text_file(fd, "/proc/self/maps");
    output(fd, "\n");
}

/*
 * Print OS information, including meminfo、uname...
 */
static void print_os_info(int fd)
{
    print_text_file(fd, "/proc/meminfo");
    output(fd, "\n");

    struct utsname name;
    if (!uname(&name)) {
        output(fd, "%s ", name.sysname);
        output(fd, "%s ", name.release);
        output(fd, "%s ", name.version);
        output(fd, "%s\n", name.machine);
    }

    output(fd, "\n");
}

static void print_guc_bool(int fd, const struct config_bool *conf)
{
    if (conf->show_hook) {
        output(fd, "%s\n", "result in show_hook");
    } else {
        const char *val = *conf->variable ? "on" : "off";
        output(fd, "%s\n", val);
    }
}

static uint64 get_guc_memory(uint64 value, int type)
{
    if (type == GUC_UNIT_XBLOCKS) {
        value *= XLOG_BLCKSZ / 1024;
    } else if (type == GUC_UNIT_BLOCKS) {
        value *= BLCKSZ / 1024;
    }

    return value;
}

static uint64 get_guc_time(uint64 value, int unit)
{
    if (unit == GUC_UNIT_S) {
        value *= 1000;
    } else if (unit == GUC_UNIT_MIN) {
        value *= (1000 * 60);
    } else if (unit == GUC_UNIT_HOUR) {
        value *= (1000 * 60 * 60);
    }

    return value;
}

static void print_guc_int(int fd, const struct config_int *conf)
{
    if (conf->show_hook) {
        output(fd, "%s\n", "result in show_hook");
        return;
    }

    uint64 result = (uint64)*conf->variable;
    if (result > 0 && (conf->gen.flags & GUC_UNIT_MEMORY)) {
        result = get_guc_memory(result, conf->gen.flags & GUC_UNIT_MEMORY);
        output(fd, "%luKB\n", result);
    } else if (result > 0 && (conf->gen.flags & GUC_UNIT_TIME)) {
        result = get_guc_time(result, conf->gen.flags & GUC_UNIT_TIME);
        output(fd, "%lums\n", result);
    } else {
        output(fd, "%lu\n", result);
    }
}

static void print_guc_int64(int fd, const struct config_int64 *conf)
{
    if (conf->show_hook) {
        output(fd, "%s\n", "result in show_hook");
    } else {
        output(fd, "%ld\n", *conf->variable);
    }
}

static void print_guc_real(int fd, const struct config_real *conf)
{
    if (conf->show_hook) {
        output(fd, "%s\n", "result in show_hook");
    } else {
        output(fd, "%g\n", *conf->variable);
    }
}

static void print_guc_string(int fd, const struct config_string* conf)
{
    if (conf->show_hook) {
        output(fd, "%s\n", "result in show_hook");
    } else if (*conf->variable && **conf->variable) {
        output(fd, "%s\n", *conf->variable);
    } else {
        output(fd, "\n");
    }
}

static void print_guc_enum(int fd, const struct config_enum *conf)
{
    if (conf->show_hook) {
        output(fd, "%s", "result in show_hook\n");
        return;
    }

    bool is_single = true;
    if (pg_strncasecmp(conf->gen.name, "rewrite_rule", sizeof("rewrite_rule")) == 0) {
        is_single = false;
    }

    int val = *conf->variable;
    const struct config_enum_entry* entry = NULL;
    for (entry = conf->options; entry && entry->name; entry++) {
        if (is_single) {
            if (entry->val == val) {
                output(fd, "%s", entry->name);
                break;
            }
        } else if (entry->val & val) {
            output(fd, "%s ", entry->name);
        }
    }

    output(fd, "\n");
}

/*
 * Print GUC information held by current thread
 */
static void print_guc_info(int fd)
{
    output(fd, "====== GUC info ======\n");

    for (int i = 0; i < u_sess->num_guc_variables; i++) {
        struct config_generic* base = u_sess->guc_variables[i];

        if (base->flags & GUC_NO_SHOW_ALL) {
            continue;
        }

        output(fd, "%s = ", base->name);
        switch (base->vartype) {
            case PGC_BOOL:
                print_guc_bool(fd, (struct config_bool *)base);
                break;
            case PGC_INT:
                print_guc_int(fd, (struct config_int *)base);
                break;
            case PGC_INT64:
                print_guc_int64(fd, (struct config_int64 *)base);
                break;
            case PGC_REAL:
                print_guc_real(fd, (struct config_real *)base);
                break;
            case PGC_STRING:
                print_guc_string(fd, (struct config_string *)base);
                break;
            case PGC_ENUM:
                print_guc_enum(fd, (struct config_enum *)base);
                break;
            default:
                output(fd, "Unknown\n");
                break;
        }
    }

    output(fd, "\n");
}

/*
 * Output formatted string
 */
void output(int fd, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);

    char buf[SIG_BUFLEN];
    int res = vsnprintf_s(buf, sizeof(buf), sizeof(buf) - 1, fmt, ap);
    if (res == -1) {
        va_end(ap);
        return;
    }

    size_t len = strlen(buf);
    (void)write(fd, buf, len);

    va_end(ap);
}

/*
 * Generate error message when a fatal error has been detected
 */
bool gen_err_msg(int sig, siginfo_t *si, ucontext_t *uc)
{
    int fd;

    if (!open_gs_err(&fd)) {
        return false;
    }

    output(fd, "FFIC start time: %ld\n", time(NULL));

    print_header(fd, get_pc(uc));
    print_siginfo(fd, sig, si);
    print_call_stack(fd, get_pc(uc));
    print_statement_info(fd);
    print_registers(fd, uc);
    print_top_stack(fd, get_sp(uc));
    print_inst_ctx(fd, get_pc(uc));
    print_guc_info(fd);
    print_mem_layout(fd);
    print_os_info(fd);

    output(fd, "FFIC end time: %ld\n", time(NULL));

    (void)close(fd);

    return true;
}
