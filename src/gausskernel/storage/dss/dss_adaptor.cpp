/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * dss_adaptor.cpp
 *  DSS Adapter Interface.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dss/dss_adaptor.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef WIN32
#include "dlfcn.h"
#endif

#include "storage/dss/dss_adaptor.h"
#include "storage/dss/fio_dss.h"
#include "utils/elog.h"

dss_device_op_t device_op = {0};

// return DSS_ERROR if error occurs
#define SS_RETURN_IFERR(ret)                             \
    do {                                                 \
        int _status_ = (ret);                            \
        if (_status_ != DSS_SUCCESS) { \
            return _status_;                             \
        }                                                \
    } while (0)

int dss_load_symbol(void *lib_handle, char *symbol, void **sym_lib_handle)
{
#ifndef WIN32
    const char *dlsym_err = NULL;

    *sym_lib_handle = dlsym(lib_handle, symbol);
    dlsym_err = dlerror();
    if (dlsym_err != NULL) {
        return DSS_ERROR;
    }
    return DSS_SUCCESS;
#endif  // !WIN32
}

int dss_open_dl(void **lib_handle, char *symbol)
{
#ifdef WIN32
    return DSS_ERROR;
#else
    *lib_handle = dlopen(symbol, RTLD_LAZY);
    if (*lib_handle == NULL) {
        return DSS_ERROR;
    }
    return DSS_SUCCESS;
#endif
}

void dss_close_dl(void *lib_handle)
{
#ifndef WIN32
    (void)dlclose(lib_handle);
#endif
}

static int dss_get_lib_version()
{
    return device_op.dss_get_version();
}

static int dss_get_my_version()
{
    return DSS_LOCAL_MAJOR_VERSION * DSS_LOCAL_MAJOR_VER_WEIGHT + DSS_LOCAL_MINOR_VERSION * DSS_LOCAL_MINOR_VER_WEIGHT +
           DSS_LOCAL_VERSION;
}

int dss_device_init(const char *conn_path, bool enable_dss)
{
    if (!enable_dss || device_op.inited) {
        return DSS_SUCCESS;
    }
    SS_RETURN_IFERR(dss_open_dl(&device_op.handle, (char *)SS_LIBDSS_NAME));

    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fcreate", (void **)&device_op.dss_create));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fclose", (void **)&device_op.dss_close));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fread", (void **)&device_op.dss_read));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_pread", (void **)&device_op.dss_pread));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fopen", (void **)&device_op.dss_open));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fremove", (void **)&device_op.dss_remove));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fseek", (void **)&device_op.dss_seek));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_ftruncate", (void **)&device_op.dss_truncate));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fwrite", (void **)&device_op.dss_write));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_pwrite", (void **)&device_op.dss_pwrite));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_dmake", (void **)&device_op.dss_create_dir));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_dopen", (void **)&device_op.dss_open_dir));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_dread", (void **)&device_op.dss_read_dir));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_dclose", (void **)&device_op.dss_close_dir));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_dremove", (void **)&device_op.dss_remove_dir));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_frename", (void **)&device_op.dss_rename));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_check_size", (void **)&device_op.dss_check_size));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_align_size", (void **)&device_op.dss_align_size));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fsize_maxwr", (void **)&device_op.dss_fsize));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_get_fname", (void **)&device_op.dss_fname));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_get_error", (void **)&device_op.dss_get_error));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_symlink", (void **)&device_op.dss_link));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_unlink", (void **)&device_op.dss_unlink));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_readlink", (void **)&device_op.dss_read_link));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_stat", (void **)&device_op.dss_stat));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_lstat", (void **)&device_op.dss_lstat));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_fstat", (void **)&device_op.dss_fstat));
    SS_RETURN_IFERR(
        dss_load_symbol(device_op.handle, "dss_set_main_inst", (void **)&device_op.dss_set_main_inst));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_set_svr_path", (void **)&device_op.dss_set_svr_path));
    SS_RETURN_IFERR(
        dss_load_symbol(device_op.handle, "dss_register_log_callback", (void **)&device_op.dss_register_log_callback));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_get_lib_version", (void **)&device_op.dss_get_version));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_get_addr", (void **)&device_op.dss_get_addr));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_compare_size_equal", (void **)&device_op.dss_compare_size));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_aio_prep_pwrite", (void **)&device_op.dss_aio_pwrite));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_aio_prep_pread", (void **)&device_op.dss_aio_pread));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_init_logger", (void **)&device_op.dss_init_logger));
    SS_RETURN_IFERR(dss_load_symbol(device_op.handle, "dss_refresh_logger", (void **)&device_op.dss_refresh_logger));

    int my_version = dss_get_my_version();
    int lib_version = dss_get_lib_version();
    if (my_version != lib_version) {
        return DSS_ERROR;
    }

    if (device_op.handle == NULL) {
        return DSS_ERROR;
    }

    dss_device_register(&device_op, enable_dss);
    if (conn_path != NULL) {
        device_op.dss_set_svr_path(conn_path);
    }

    device_op.inited = true;
    return DSS_SUCCESS;
}

void dss_register_log_callback(dss_log_output cb_log_output)
{
    device_op.dss_register_log_callback(cb_log_output);
}

int dss_call_init_logger(char *log_home, unsigned int log_level, unsigned int log_backup_file_count, unsigned long long log_max_file_size)
{
    return device_op.dss_init_logger(log_home, log_level, log_backup_file_count, log_max_file_size);
}

void dss_call_refresh_logger(char * log_field, unsigned long long *value)
{
    if (device_op.inited) {
        device_op.dss_refresh_logger(log_field, value);
    }
}
