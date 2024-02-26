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
 * ss_dms.h
 *        Defines the DMS function pointer.
 *
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_dms.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __SS_DMS_H__
#define __SS_DMS_H__

#include "dms_api.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SS_LIBDMS_NAME "libdms.so"
#define CM_INVALID_ID8 0xff

typedef struct st_ss_dms_func {
    bool inited;
    void *handle;
    int (*dms_get_version)(void);
    void (*dms_show_version)(char *version);
    int (*dms_init)(dms_profile_t *dms_profile);
    void (*dms_get_error)(int *errcode, const char **errmsg);
    void (*dms_uninit)(void);
    int (*dms_request_page)(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, dms_lock_mode_t mode);
    int (*dms_broadcast_msg)(dms_context_t *dms_ctx, char *data, unsigned int len, unsigned char handle_recv_msg,
                             unsigned int timeout);
    int (*dms_request_opengauss_update_xid)(dms_context_t *dms_ctx,
                                     unsigned short t_infomask, unsigned short t_infomask2, unsigned long long *uxid);
    int (*dms_request_opengauss_xid_csn)(dms_context_t *dms_ctx, dms_opengauss_xid_csn_t *dms_txn_info,
                                      dms_opengauss_csn_result_t *xid_csn_result);
    int (*dms_request_opengauss_txn_status)(dms_context_t *dms_ctx, unsigned char request, unsigned char *result);
    int (*dms_request_opengauss_txn_snapshot)(dms_context_t *dms_ctx,
                                           dms_opengauss_txn_snapshot_t *dms_txn_snapshot);
    int (*dms_request_opengauss_txn_of_master)(dms_context_t *dms_ctx,
                                           dms_opengauss_txn_sw_info_t *dms_txn_swinfo);
    int (*dms_request_opengauss_page_status)(dms_context_t *dms_ctx, unsigned int page, int page_num,
                                           unsigned long int *page_map, int *bit_count);
    int (*dms_register_thread_init)(dms_thread_init_t thrd_init);
    int (*dms_register_thread_deinit)(dms_thread_deinit_t thrd_deinit);
    int (*dms_release_owner)(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, unsigned char *released);
    int (*dms_wait_reform)(unsigned int *has_offline);
    void (*dms_get_event)(dms_wait_event_t event_type, unsigned long long *event_cnt, unsigned long long *event_time);
    int (*dms_buf_res_rebuild_drc_parallel)(dms_context_t *dms_ctx, dms_ctrl_info_t *ctrl_info, unsigned char thread_index);
    int (*dms_is_recovery_session)(unsigned int sid);
    int (*drc_get_page_master_id)(char pageid[DMS_PAGEID_SIZE], unsigned char *master_id);
    int (*dms_register_ssl_decrypt_pwd)(dms_decrypt_pwd_t cb_func);
    int (*dms_set_ssl_param)(const char *param_name, const char *param_value);
    int (*dms_get_ssl_param)(const char *param_name, char *param_value, unsigned int size);
    int (*dms_recovery_page_need_skip)(char pageid[DMS_PAGEID_SIZE], unsigned char *skip, unsigned int alloc);
    int (*dms_reform_failed)(void);
    int (*dms_switchover)(unsigned int sess_id);
    int (*dms_drc_accessible)(unsigned char res_type);
    int (*dms_broadcast_opengauss_ddllock)(dms_context_t *dms_ctx, char *data, unsigned int len,
        unsigned char handle_recv_msg, unsigned int timeout, unsigned char resend_after_reform);
    int (*dms_reform_last_failed)(void);
    bool (*dms_latch_timed_x)(dms_context_t *dms_ctx, dms_drlatch_t *dlatch, unsigned int wait_ticks);
    bool (*dms_latch_timed_s)(dms_context_t *dms_ctx, dms_drlatch_t *dlatch, unsigned int wait_ticks,
        unsigned char is_force);
    void (*dms_unlatch)(dms_context_t *dms_ctx, dms_drlatch_t *dlatch);
    void (*dms_pre_uninit)(void);
    int (*dms_init_logger)(logger_param_t *log_param);
    void (*dms_refresh_logger)(char *log_field, unsigned long long *value);
    void (*dms_validate_drc)(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, unsigned long long lsn,
        unsigned char is_dirty);
    int (*dms_reform_req_opengauss_ondemand_redo_buffer)(dms_context_t *dms_ctx, void *block_key, unsigned int key_len,
        int *redo_status);
    unsigned int (*dms_get_mes_max_watting_rooms)(void);
    int (*dms_send_opengauss_oldest_xmin)(dms_context_t *dms_ctx, unsigned long long oldest_xmin, unsigned char dest_id);
    int (*dms_get_drc_info)(int *is_found, dv_drc_buf_info *drc_info);
    int (*dms_info)(char *buf, unsigned int len, dms_info_id_e id);
    void (*dms_get_buf_res)(unsigned long long *row_id, dv_drc_buf_info *drc_info, int type);
    void (*dms_get_cmd_stat)(int index, wait_cmd_stat_result_t *cmd_stat_result);
    int (*dms_req_opengauss_immediate_ckpt)(dms_context_t *dms_ctx, unsigned long long *ckpt_loc);
} ss_dms_func_t;

int ss_dms_func_init();
int dms_init(dms_profile_t *dms_profile);
int dms_init_logger(logger_param_t *log_param);
void dms_refresh_logger(char *log_field, unsigned long long *value);
void dms_get_error(int *errcode, const char **errmsg);
void dms_uninit(void);
int dms_request_page(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, dms_lock_mode_t mode);
int dms_broadcast_msg(dms_context_t *dms_ctx, char *data, unsigned int len, unsigned char handle_recv_msg,
                      unsigned int timeout);
int dms_request_opengauss_update_xid(dms_context_t *dms_ctx,
                                     unsigned short t_infomask, unsigned short t_infomask2, unsigned long long *uxid);
int dms_request_opengauss_xid_csn(dms_context_t *dms_ctx, dms_opengauss_xid_csn_t *dms_txn_info,
                                  dms_opengauss_csn_result_t *xid_csn_result);
int dms_request_opengauss_txn_status(dms_context_t *dms_ctx, unsigned char request, unsigned char *result);
int dms_request_opengauss_txn_snapshot(dms_context_t *dms_ctx,
                                       dms_opengauss_txn_snapshot_t *dms_txn_snapshot);
int dms_request_opengauss_txn_of_master(dms_context_t *dms_ctx,
                                       dms_opengauss_txn_sw_info_t *dms_txn_swinfo);
int dms_request_opengauss_page_status(dms_context_t *dms_ctx, unsigned int page, int page_num,
    unsigned long int *page_map, int *bit_count);
int dms_register_thread_init(dms_thread_init_t thrd_init);
int dms_register_thread_deinit(dms_thread_deinit_t thrd_deinit);
int dms_release_owner(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, unsigned char *released);
int dms_wait_reform(unsigned int *has_offline);
void dms_get_event(dms_wait_event_t event_type, unsigned long long *event_cnt, unsigned long long *event_time);
int dms_buf_res_rebuild_drc_parallel(dms_context_t *dms_ctx, dms_ctrl_info_t *ctrl_info, unsigned char thread_index);
int dms_is_recovery_session(unsigned int sid);
int drc_get_page_master_id(char pageid[DMS_PAGEID_SIZE], unsigned char *master_id);
int dms_register_ssl_decrypt_pwd(dms_decrypt_pwd_t cb_func);
int dms_set_ssl_param(const char *param_name, const char *param_value);
int dms_get_ssl_param(const char *param_name, char *param_value, unsigned int size);
int dms_recovery_page_need_skip(char pageid[DMS_PAGEID_SIZE], unsigned char *skip, unsigned int alloc);
int dms_reform_failed(void);
int dms_switchover(unsigned int sess_id);
int dms_drc_accessible(unsigned char res_type);
int dms_broadcast_opengauss_ddllock(dms_context_t *dms_ctx, char *data, unsigned int len, unsigned char handle_recv_msg,
    unsigned int timeout, unsigned char resend_after_reform);
int dms_reform_last_failed(void);
bool dms_latch_timed_x(dms_context_t *dms_ctx, dms_drlatch_t *dlatch, unsigned int wait_ticks);
bool dms_latch_timed_s(dms_context_t *dms_ctx, dms_drlatch_t *dlatch, unsigned int wait_ticks, unsigned char is_force);
void dms_unlatch(dms_context_t *dms_ctx, dms_drlatch_t *dlatch);
void dms_pre_uninit(void);
void dms_validate_drc(dms_context_t *dms_ctx, dms_buf_ctrl_t *ctrl, unsigned long long lsn, unsigned char is_dirty);
int dms_reform_req_opengauss_ondemand_redo_buffer(dms_context_t *dms_ctx, void *block_key, unsigned int key_len,
                                                  int *redo_status);
unsigned int dms_get_mes_max_watting_rooms(void);
int dms_send_opengauss_oldest_xmin(dms_context_t *dms_ctx, unsigned long long oldest_xmin, unsigned char dest_id);
int dms_req_opengauss_immediate_checkpoint(dms_context_t *dms_ctx, unsigned long long *redo_lsn);
int get_drc_info(int *is_found, dv_drc_buf_info *drc_info);
int dms_info(char *buf, unsigned int len, dms_info_id_e id);
void dms_get_buf_res(unsigned long long *row_id, dv_drc_buf_info *drc_info, int type);
void dms_get_cmd_stat(int index, wait_cmd_stat_result_t *cmd_stat_result);

#ifdef __cplusplus
}
#endif

#endif /* __SS_DMS_H__ */
