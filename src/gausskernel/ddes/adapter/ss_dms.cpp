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
 * ss_dms.cpp
 *        Dynamic loading of the DMS
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef WIN32
#include "dlfcn.h"
#endif

#include "ddes/dms/ss_dms.h"
#include "utils/elog.h"

ss_dms_func_t g_ss_dms_func = { 0 };

// return DMS_ERROR if error occurs
#define SS_RETURN_IFERR(ret)                            \
    do {                                                \
        int _status_ = (ret);                           \
        if (SECUREC_UNLIKELY(_status_ != DMS_SUCCESS)) { \
            return _status_;                            \
        }                                               \
    } while (0)

int dms_load_symbol(char *symbol, void **sym_lib_handle)
{
#ifndef WIN32
    const char *dlsym_err = NULL;

    *sym_lib_handle = dlsym(g_ss_dms_func.handle, symbol);
    dlsym_err = dlerror();
    if (dlsym_err != NULL) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_OPERATION),
            errmsg("incompatible library \"%s\", load %s failed, %s", SS_LIBDMS_NAME, symbol, dlsym_err)));
        return DMS_ERROR;
    }
#endif // !WIN32
    return DMS_SUCCESS;
}

int dms_open_dl(void **lib_handle, char *symbol)
{
#ifdef WIN32
    return DMS_ERROR;
#else
    *lib_handle = dlopen(symbol, RTLD_LAZY);
    if (*lib_handle == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not load library %s, %s", SS_LIBDMS_NAME, dlerror())));
        return DMS_ERROR;
    }
    return DMS_SUCCESS;
#endif
}

void dms_close_dl(void *lib_handle)
{
#ifndef WIN32
    (void)dlclose(lib_handle);
#endif
}

#define DMS_LOAD_SYMBOL_FUNC(func) dms_load_symbol(#func, (void **)&g_ss_dms_func.func)

int ss_dms_func_init()
{
    if (g_ss_dms_func.inited) {
        return DMS_SUCCESS;
    }
    SS_RETURN_IFERR(dms_open_dl(&g_ss_dms_func.handle, (char *)SS_LIBDMS_NAME));

    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_show_version));
    char version[DMS_VERSION_MAX_LEN] = { 0 };
    g_ss_dms_func.dms_show_version((char *)version);
    ereport(LOG, (errmsg("Dynamically loading the DMS library, version: \n%s", version)));

    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_get_version));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_init));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_get_error));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_uninit));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_request_page));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_broadcast_msg));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_request_opengauss_update_xid));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_request_opengauss_xid_csn));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_request_opengauss_txn_status));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_request_opengauss_txn_snapshot));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_request_opengauss_txn_of_master));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_request_opengauss_page_status));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_register_thread_init));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_release_owner));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_wait_reform));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_get_event));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_buf_res_rebuild_drc_parallel));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_is_recovery_session));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(drc_get_page_master_id));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_register_ssl_decrypt_pwd));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_set_ssl_param));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_get_ssl_param));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_recovery_page_need_skip));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_reform_failed));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_switchover));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_drc_accessible));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_broadcast_opengauss_ddllock));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_reform_last_failed));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_latch_timed_x));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_latch_timed_s));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_unlatch));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_pre_uninit));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_init_logger));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_refresh_logger));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_validate_drc));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_reform_req_opengauss_ondemand_redo_buffer));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_get_mes_max_watting_rooms));
    SS_RETURN_IFERR(DMS_LOAD_SYMBOL_FUNC(dms_send_opengauss_oldest_xmin));
    g_ss_dms_func.inited = true;
    return DMS_SUCCESS;
}

void ss_dms_func_uninit()
{
    if (g_ss_dms_func.handle != NULL) {
        dms_close_dl(g_ss_dms_func.handle);
        g_ss_dms_func.handle = NULL;
        g_ss_dms_func.inited = false;
    }
}

static int dms_get_lib_version()
{
    return g_ss_dms_func.dms_get_version();
}

static int dms_get_my_version()
{
    return DMS_LOCAL_MAJOR_VERSION * DMS_LOCAL_MAJOR_VER_WEIGHT + DMS_LOCAL_MINOR_VERSION * DMS_LOCAL_MINOR_VER_WEIGHT +
           DMS_LOCAL_VERSION;
}

int dms_init(dms_profile_t *dms_profile)
{
    int my_version = dms_get_my_version();
    int lib_version = dms_get_lib_version();

    if (my_version != lib_version) {
        ereport(FATAL, (errmsg("dms library version is not matched, expected = %d, actual = %d",
            my_version, lib_version)));
    }

    int ret = g_ss_dms_func.dms_init(dms_profile);
    if (ret != DMS_SUCCESS) {
        return ret;
    }

    return DMS_SUCCESS;
}

int dms_init_logger(logger_param_t *log_param)
{
    return g_ss_dms_func.dms_init_logger(log_param);
}

void dms_refresh_logger(char *log_field, unsigned long long *value)
{
    if (g_ss_dms_func.inited) {
        g_ss_dms_func.dms_refresh_logger(log_field, value);
    }
}

void dms_uninit(void)
{
    g_ss_dms_func.dms_uninit();
    ss_dms_func_uninit();
}

void dms_get_error(int *errcode, const char **errmsg)
{
    g_ss_dms_func.dms_get_error(errcode, errmsg);
}

int dms_request_page(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, dms_lock_mode_t mode)
{
    return g_ss_dms_func.dms_request_page(dms_ctx, ctrl, mode);
}

int dms_broadcast_msg(dms_context_t *dms_ctx, char *data, unsigned int len, unsigned char handle_recv_msg,
    unsigned int timeout)
{
    return g_ss_dms_func.dms_broadcast_msg(dms_ctx, data, len, handle_recv_msg, timeout);
}

int dms_request_opengauss_update_xid(dms_context_t *dms_ctx, unsigned short t_infomask, unsigned short t_infomask2,
    unsigned long long *uxid)
{
    return g_ss_dms_func.dms_request_opengauss_update_xid(dms_ctx, t_infomask, t_infomask2, uxid);
}
int dms_request_opengauss_xid_csn(dms_context_t *dms_ctx, dms_opengauss_xid_csn_t *dms_txn_info,
    dms_opengauss_csn_result_t *xid_csn_result)
{
    return g_ss_dms_func.dms_request_opengauss_xid_csn(dms_ctx, dms_txn_info, xid_csn_result);
}
int dms_request_opengauss_txn_status(dms_context_t *dms_ctx, unsigned char request, unsigned char *result)
{
    return g_ss_dms_func.dms_request_opengauss_txn_status(dms_ctx, request, result);
}
int dms_request_opengauss_txn_snapshot(dms_context_t *dms_ctx, dms_opengauss_txn_snapshot_t *dms_txn_snapshot)
{
    return g_ss_dms_func.dms_request_opengauss_txn_snapshot(dms_ctx, dms_txn_snapshot);
}

int dms_request_opengauss_txn_of_master(dms_context_t *dms_ctx, dms_opengauss_txn_sw_info_t *dms_txn_swinfo)
{
    return g_ss_dms_func.dms_request_opengauss_txn_of_master(dms_ctx, dms_txn_swinfo);
}

int dms_request_opengauss_page_status(dms_context_t *dms_ctx, unsigned int page, int page_num,
    unsigned long int *page_map, int *bit_count)
{
    return g_ss_dms_func.dms_request_opengauss_page_status(dms_ctx, page, page_num, page_map, bit_count);
}

int dms_broadcast_opengauss_ddllock(dms_context_t *dms_ctx, char *data, unsigned int len, unsigned char handle_recv_msg,
    unsigned int timeout, unsigned char resend_after_reform)
{
    return g_ss_dms_func.dms_broadcast_opengauss_ddllock(dms_ctx, data, len, handle_recv_msg, timeout,
        resend_after_reform);
}

bool dms_latch_timed_x(dms_context_t *dms_ctx, dms_drlatch_t *dlatch, unsigned int wait_ticks)
{
    return (bool)g_ss_dms_func.dms_latch_timed_x(dms_ctx, dlatch, wait_ticks);
}

bool dms_latch_timed_s(dms_context_t *dms_ctx, dms_drlatch_t *dlatch, unsigned int wait_ticks, unsigned char is_force)
{
    return (bool)g_ss_dms_func.dms_latch_timed_s(dms_ctx, dlatch, wait_ticks, is_force);
}

void dms_unlatch(dms_context_t *dms_ctx, dms_drlatch_t *dlatch)
{
    g_ss_dms_func.dms_unlatch(dms_ctx, dlatch);
}

int dms_register_thread_init(dms_thread_init_t thrd_init)
{
    return g_ss_dms_func.dms_register_thread_init(thrd_init);
}

int dms_release_owner(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, unsigned char *released)
{
    return g_ss_dms_func.dms_release_owner(dms_ctx, ctrl, released);
}

int dms_wait_reform(unsigned int *has_offline)
{
    return g_ss_dms_func.dms_wait_reform(has_offline);
}

void dms_get_event(dms_wait_event_t event_type, unsigned long long *event_cnt, unsigned long long *event_time)
{
    return g_ss_dms_func.dms_get_event(event_type, event_cnt, event_time);
}

int dms_buf_res_rebuild_drc_parallel(dms_context_t *dms_ctx, dms_ctrl_info_t *ctrl_info, unsigned char thread_index)
{
    return g_ss_dms_func.dms_buf_res_rebuild_drc_parallel(dms_ctx, ctrl_info, thread_index);
}

int dms_is_recovery_session(unsigned int sid)
{
    return g_ss_dms_func.dms_is_recovery_session(sid);
}

int drc_get_page_master_id(char pageid[DMS_PAGEID_SIZE], unsigned char *master_id)
{
    return g_ss_dms_func.drc_get_page_master_id(pageid, master_id);
}

int dms_register_ssl_decrypt_pwd(dms_decrypt_pwd_t cb_func)
{
    return g_ss_dms_func.dms_register_ssl_decrypt_pwd(cb_func);
}

int dms_set_ssl_param(const char *param_name, const char *param_value)
{
    return g_ss_dms_func.dms_set_ssl_param(param_name, param_value);
}

int dms_get_ssl_param(const char *param_name, char *param_value, unsigned int size)
{
    return g_ss_dms_func.dms_get_ssl_param(param_name, param_value, size);
}

int dms_recovery_page_need_skip(char pageid[DMS_PAGEID_SIZE], unsigned char *skip)
{
    return g_ss_dms_func.dms_recovery_page_need_skip(pageid, skip);
}

int dms_reform_failed(void)
{
    return g_ss_dms_func.dms_reform_failed();
}

int dms_switchover(unsigned int sess_id)
{
    return g_ss_dms_func.dms_switchover(sess_id);
}

int dms_drc_accessible(unsigned char res_type)
{
    return g_ss_dms_func.dms_drc_accessible(res_type);
}

int dms_reform_last_failed(void)
{
    return g_ss_dms_func.dms_reform_last_failed();
}

void dms_pre_uninit(void)
{
    return g_ss_dms_func.dms_pre_uninit();
}

void dms_validate_drc(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, unsigned long long lsn,
    unsigned char is_dirty)
{
    return g_ss_dms_func.dms_validate_drc(dms_ctx, ctrl, lsn, is_dirty);
}

int dms_reform_req_opengauss_ondemand_redo_buffer(dms_context_t *dms_ctx, void *block_key, unsigned int key_len,
    int *redo_status)
{
    return g_ss_dms_func.dms_reform_req_opengauss_ondemand_redo_buffer(dms_ctx, block_key, key_len, redo_status);
}

unsigned int dms_get_mes_max_watting_rooms(void)
{
    return g_ss_dms_func.dms_get_mes_max_watting_rooms();
}

int dms_send_opengauss_oldest_xmin(dms_context_t *dms_ctx, unsigned long long oldest_xmin, unsigned char dest_id)
{
    return g_ss_dms_func.dms_send_opengauss_oldest_xmin(dms_ctx, oldest_xmin, dest_id);
}