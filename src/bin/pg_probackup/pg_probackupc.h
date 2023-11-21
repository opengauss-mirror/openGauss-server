/*-------------------------------------------------------------------------
 *
 * pg_probackup.h: Backup/Recovery manager for PostgreSQL.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROBACKUPC_H
#define PG_PROBACKUPC_H

#define IsPartialCompressXLogFileName(fname)    \
    (strlen(fname) == XLOG_FNAME_LEN + strlen(".gz.partial") && \
     strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&        \
     strcmp((fname) + XLOG_FNAME_LEN, ".gz.partial") == 0)

#define IsTempXLogFileName(fname)    \
    (strlen(fname) == XLOG_FNAME_LEN + strlen(".part") &&    \
     strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&        \
     strcmp((fname) + XLOG_FNAME_LEN, ".part") == 0)

#define IsTempCompressXLogFileName(fname)    \
    (strlen(fname) == XLOG_FNAME_LEN + strlen(".gz.part") && \
     strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&        \
     strcmp((fname) + XLOG_FNAME_LEN, ".gz.part") == 0)

#ifdef ENABLE_LITE_MODE
#define IsDssMode() false
#else
#define IsDssMode() (instance_config.dss.enable_dss == true)
#endif
#define IsSshProtocol() (instance_config.remote.host && strcmp(instance_config.remote.proto, "ssh") == 0)

/* directory options */
extern char       *backup_path;
extern char        backup_instance_path[MAXPGPATH];
extern char        arclog_path[MAXPGPATH];

/* common options */
extern pid_t    my_pid;
extern __thread int my_thread_num;
extern int        num_threads;
extern bool        stream_wal;
extern bool        progress;
#if PG_VERSION_NUM >= 100000
/* In pre-10 'replication_slot' is defined in receivelog.h */
extern char       *replication_slot;
#endif
extern bool     temp_slot;
extern char       *password;
extern int        rw_timeout;

/* backup options */
extern bool        smooth_checkpoint;

/* list of dirs which will not to be backuped
   it will be backuped up in external dirs  */
extern parray *pgdata_nobackup_dir;

/* list of logical replication slots */
extern parray *logical_replslot;

/* remote probackup options */
extern char* remote_agent;

extern bool exclusive_backup;

/* delete options */
extern bool        delete_wal;
extern bool        delete_expired;
extern bool        merge_expired;
extern bool        dry_run;

/* compression options */
extern bool        compress_shortcut;

/* other options */
extern char *instance_name;
extern bool specify_extdir;
extern bool specify_tbsdir;

/* show options */
extern ShowFormat show_format;

extern bool skip_block_validation;
/* current settings */
extern pgBackup current;

/* argv of the process */
extern char** commands_args;

/* in dir.c */
/* exclude directory list for $PGDATA file listing */
extern const char *pgdata_exclude_dir[];

/* in backup.c */
extern int do_backup(time_t start_time, pgSetBackupParams *set_backup_params,
                     bool no_validate, bool no_sync, bool backup_logs, bool backup_replslots);
extern BackupMode parse_backup_mode(const char *value);
extern const char *deparse_backup_mode(BackupMode mode);
extern void process_block_change(ForkNumber forknum, const RelFileNode rnode,
                                 BlockNumber blkno);

/* in restore.c */
extern int do_restore_or_validate(time_t target_backup_id,
                      pgRecoveryTarget *rt,
                      pgRestoreParams *params,
                      bool no_sync);
extern bool satisfy_timeline(const parray *timelines, const pgBackup *backup);
extern bool satisfy_recovery_target(const pgBackup *backup,
                                    const pgRecoveryTarget *rt);
extern pgRecoveryTarget *parseRecoveryTargetOptions(
    const char *target_time, const char *target_xid,
    const char *target_inclusive, TimeLineID target_tli, const char* target_lsn,
    const char *target_stop, const char *target_name,
    const char *target_action);

extern parray *get_backup_filelist(pgBackup *backup, bool strict);
extern parray *read_timeline_history(const char *arclog_path, TimeLineID targetTLI, bool strict);
extern bool tliIsPartOfHistory(const parray *timelines, TimeLineID tli);

/* in merge.c */
extern void do_merge(time_t backup_id);
extern void merge_backups(pgBackup *backup, pgBackup *next_backup);
extern void merge_chain(parray *parent_chain,
                        pgBackup *full_backup, pgBackup *dest_backup);

extern parray *read_database_map(pgBackup *backup);

/* in init.c */
extern int do_init(void);
extern int do_add_instance(InstanceConfig *instance);

/* in configure.c */
extern void do_show_config(void);
extern void do_set_config(bool missing_ok);
extern void init_config(InstanceConfig *config, const char *instance_name);
extern InstanceConfig *readInstanceConfigFile(const char *instance_name);

/* in show.c */
extern int do_show(const char *instance_name, time_t requested_backup_id, bool show_archive);

/* in delete.c */
extern void do_delete(time_t backup_id);
extern void delete_backup_files(pgBackup *backup);
extern void do_retention(void);
extern int do_delete_instance(void);
extern void do_delete_status(InstanceConfig *instance_config, const char *status);

/* in fetch.c */
extern char *slurpFile(const char *fullpath,
                       size_t *filesize,
                       bool safe,
                       fio_location location);
extern char *fetchFile(PGconn *conn, const char *filename, size_t *filesize);

/* in help.c */
extern void help_pg_probackup(void);
extern void help_command(const char *command);

/* in validate.c */
extern void pgBackupValidate(pgBackup* backup, pgRestoreParams *params);
extern int do_validate_all(void);
extern int validate_one_page(Page page, BlockNumber absolute_blkno,
                             XLogRecPtr stop_lsn, PageState *page_st,
                             uint32 checksum_version);

/* return codes for validate_one_page */
/* TODO: use enum */
#define PAGE_IS_VALID (-1)
#define PAGE_IS_NOT_FOUND (-2)
#define PAGE_IS_ZEROED (-3)
#define PAGE_HEADER_IS_INVALID (-4)
#define PAGE_CHECKSUM_MISMATCH (-5)
#define PAGE_LSN_FROM_FUTURE (-6)
#define PAGE_MAYBE_COMPRESSED (-7)

/* in catalog.c */
extern pgBackup *read_backup(const char *root_dir);
extern void write_backup(pgBackup *backup, bool strict);
extern void write_backup_status(pgBackup *backup, BackupStatus status,
                                const char *instance_name, bool strict);
extern void write_backup_data_bytes(pgBackup *backup);
extern bool lock_backup(pgBackup *backup, bool strict);

extern const char *pgBackupGetBackupMode(pgBackup *backup);

extern parray *catalog_get_instance_list(void);
extern parray *catalog_get_backup_list(const char *instance_name, time_t requested_backup_id);
extern void catalog_lock_backup_list(parray *backup_list, int from_idx,
                                     int to_idx, bool strict);
extern pgBackup *catalog_get_last_data_backup(parray *backup_list,
                                              TimeLineID tli,
                                              time_t current_start_time);
extern pgBackup *get_multi_timeline_parent(parray *backup_list, parray *tli_list,
                          TimeLineID current_tli, time_t current_start_time,
                          InstanceConfig *instance);
extern void timelineInfoFree(void *tliInfo);
extern parray *catalog_get_timelines(InstanceConfig *instance);
extern void do_set_backup(const char *instance_name, time_t backup_id,
                            pgSetBackupParams *set_backup_params);
extern void pin_backup(pgBackup    *target_backup,
                            pgSetBackupParams *set_backup_params);
extern void add_note(pgBackup *target_backup, const char *note);
extern void pgBackupWriteControl(FILE *out, pgBackup *backup);
extern void write_backup_filelist(pgBackup *backup, parray *files,
                                  const char *root, parray *external_list, bool sync);

extern void pgBackupGetPath(const pgBackup *backup, char *path, size_t len,
                            const char *subdir);
extern void pgBackupGetPath2(const pgBackup *backup, char *path, size_t len,
                             const char *subdir1, const char *subdir2);
extern void pgBackupGetPathInInstance(const char *instance_name,
                 const pgBackup *backup, char *path, size_t len,
                 const char *subdir1, const char *subdir2);
extern int pgBackupCreateDir(pgBackup *backup);
extern void pgNodeInit(PGNodeInfo *node);
extern void pgBackupInit(pgBackup *backup);
extern void pgBackupFree(void *backup);
extern int pgBackupCompareId(const void *f1, const void *f2);
extern int pgBackupCompareIdDesc(const void *f1, const void *f2);
extern int pgBackupCompareIdEqual(const void *l, const void *r);

extern pgBackup* find_parent_full_backup(pgBackup *current_backup);
extern int scan_parent_chain(pgBackup *current_backup, pgBackup **result_backup);
/* return codes for scan_parent_chain */
#define ChainIsBroken 0
#define ChainIsInvalid 1
#define ChainIsOk 2

extern bool is_parent(time_t parent_backup_time, pgBackup *child_backup, bool inclusive);
extern bool is_prolific(parray *backup_list, pgBackup *target_backup);
extern int get_backup_index_number(parray *backup_list, pgBackup *backup);
extern void append_children(parray *backup_list, pgBackup *target_backup, parray *append_list);
extern bool launch_agent(void);
extern void launch_ssh(char* argv[]);
extern void wait_ssh(void);

#define COMPRESS_ALG_DEFAULT NOT_DEFINED_COMPRESS
#define COMPRESS_LEVEL_DEFAULT 1

extern CompressAlg parse_compress_alg(const char *arg);
extern const char* deparse_compress_alg(int alg);

/* in dir.c */
extern void dir_list_file(parray *files, const char *root, bool exclude,
                          bool follow_symlink, bool add_root, bool backup_logs,
                          bool skip_hidden, int external_dir_num, fio_location location,
                          bool backup_replslots = false);

extern void create_data_directories(parray *dest_files,
                                        const char *data_dir,
                                        const char *backup_dir,
                                        bool extract_tablespaces,
                                        bool incremental,
                                        fio_location location,
                                        bool is_restore);

extern void read_tablespace_map(parray *files, const char *backup_dir);
extern void opt_tablespace_map(ConfigOption *opt, const char *arg);
extern void opt_externaldir_map(ConfigOption *opt, const char *arg);
extern void check_tablespace_mapping(pgBackup *backup, bool incremental, bool *tblspaces_are_empty);
extern void check_external_dir_mapping(pgBackup *backup, bool incremental);
extern char *get_external_remap(char *current_dir);

extern void print_database_map(FILE *out, parray *database_list);
extern void write_database_map(pgBackup *backup, parray *database_list,
                                   parray *backup_file_list);
extern void db_map_entry_free(void *map);

extern void print_file_list(FILE *out, const parray *files, const char *root,
                            const char *external_prefix, parray *external_list);
extern parray *dir_read_file_list(const char *root, const char *external_prefix,
                                  const char *file_txt, fio_location location, pg_crc32 expected_crc);
extern parray *make_external_directory_list(const char *colon_separated_dirs,
                                            bool remap);
extern void free_dir_list(parray *list);
extern void makeExternalDirPathByNum(char *ret_path, const char *pattern_path,
                                     const int dir_num);
extern bool backup_contains_external(const char *dir, parray *dirs_list);

extern int dir_create_dir(const char *path, mode_t mode);
extern bool dir_is_empty(const char *path, fio_location location);

extern bool fileExists(const char *path, fio_location location);
extern off_t pgFileSize(const char *path);

extern pgFile *pgFileNew(const char *path, const char *rel_path,
                         bool follow_symlink, int external_dir_num,
                         fio_location location);
extern pgFile *pgFileInit(const char *rel_path);
extern void pgFileDelete(mode_t mode, const char *full_path);
extern void fio_pgFileDelete(pgFile *file, const char *full_path);

extern void pgFileFree(void *file);

extern pg_crc32 pgFileGetCRC(const char *file_path, bool use_crc32c, bool missing_ok);
extern pg_crc32 pgFileGetCRCgz(const char *file_path, bool missing_ok, bool use_crc32c);

extern int pgFileMapComparePath(const void *f1, const void *f2);
extern int pgFileCompareName(const void *f1, const void *f2);
extern int pgFileCompareRelPathWithExternal(const void *f1, const void *f2);
extern int pgFileCompareRelPathWithExternalDesc(const void *f1, const void *f2);
extern int pgFileCompareLinked(const void *f1, const void *f2);
extern int pgFileCompareSize(const void *f1, const void *f2);
extern int pgCompareOid(const void *f1, const void *f2);

/* in data.c */
extern bool check_data_file(ConnectionArgs *arguments, pgFile *file,
                            const char *from_fullpath, uint32 checksum_version);

extern void backup_data_file(ConnectionArgs* conn_arg, pgFile *file,
                                 const char *from_fullpath, const char *to_fullpath,
                                 XLogRecPtr prev_backup_start_lsn, BackupMode backup_mode,
                                 CompressAlg calg, int clevel, uint32 checksum_version,
                                 HeaderMap *hdr_map, bool missing_ok);
extern void backup_non_data_file(pgFile *file, pgFile *prev_file,
                                 const char *from_fullpath, const char *to_fullpath,
                                 BackupMode backup_mode, time_t parent_backup_time,
                                 bool missing_ok);
extern void backup_non_data_file_internal(const char *from_fullpath,
                                          fio_location from_location,
                                          const char *to_fullpath, pgFile *file,
                                          bool missing_ok);

extern size_t restore_data_file(parray *parent_chain, pgFile *dest_file, FILE *out,
                                const char *to_fullpath, bool use_bitmap, PageState *checksum_map,
                                XLogRecPtr shift_lsn, datapagemap_t *lsn_map, bool use_headers);
extern size_t restore_data_file_internal(FILE *in, FILE *out, pgFile *file, uint32 backup_version,
                                         const char *from_fullpath, const char *to_fullpath, int nblocks,
                                         datapagemap_t *map, PageState *checksum_map, int checksum_version,
                                         datapagemap_t *lsn_map, BackupPageHeader2 *headers);
extern size_t restore_non_data_file(parray *parent_chain, pgBackup *dest_backup, pgFile *dest_file, FILE *out,
                                    const char *to_fullpath, bool already_exists);
extern void restore_non_data_file_internal(FILE *in, FILE *out, pgFile *file,
                                           const char *from_fullpath, const char *to_fullpath);
extern bool create_empty_file(fio_location from_location, const char *to_root,
                              fio_location to_location, pgFile *file);

extern PageState *get_checksum_map(const char *fullpath, uint32 checksum_version,
                                int n_blocks, XLogRecPtr dest_stop_lsn, BlockNumber segmentno);
extern datapagemap_t *get_lsn_map(const char *fullpath, uint32 checksum_version,
                                  int n_blocks, XLogRecPtr shift_lsn, BlockNumber segmentno);
extern pid_t check_postmaster(const char *pgdata);

extern bool validate_file_pages(pgFile *file, const char *fullpath, XLogRecPtr stop_lsn,
                                uint32 checksum_version, uint32 backup_version, HeaderMap *hdr_map);

extern BackupPageHeader2* get_data_file_headers(HeaderMap *hdr_map, pgFile *file, uint32 backup_version, bool strict);
extern void write_page_headers(BackupPageHeader2 *headers, pgFile *file, HeaderMap *hdr_map, bool is_merge);
extern void init_header_map(pgBackup *backup);
extern void cleanup_header_map(HeaderMap *hdr_map);
/* parsexlog.c */
extern bool extractPageMap(const char *archivedir, uint32 wal_seg_size,
                           XLogRecPtr startpoint, TimeLineID start_tli,
                           XLogRecPtr endpoint, TimeLineID end_tli,
                           parray *tli_list);
extern void validate_wal(pgBackup *backup, const char *archivedir,
                         time_t target_time, TransactionId target_xid,
                         XLogRecPtr target_lsn, TimeLineID tli,
                         uint32 seg_size);
extern bool validate_wal_segment(TimeLineID tli, XLogSegNo segno,
                                 const char *prefetch_dir, uint32 wal_seg_size);
extern bool read_recovery_info(const char *archivedir, TimeLineID tli,
                               uint32 seg_size,
                               XLogRecPtr start_lsn, XLogRecPtr stop_lsn,
                               time_t *recovery_time);
extern bool wal_contains_lsn(const char *archivedir, XLogRecPtr target_lsn,
                             TimeLineID target_tli, uint32 seg_size);
extern XLogRecPtr get_prior_record_lsn(const char *archivedir, XLogRecPtr start_lsn,
                                   XLogRecPtr stop_lsn, TimeLineID tli,
                                   bool seek_prev_segment, uint32 seg_size);

extern XLogRecPtr get_first_record_lsn(const char *archivedir, XLogRecPtr start_lsn,
                                       TimeLineID tli, uint32 wal_seg_size, int timeout);
extern XLogRecPtr get_next_record_lsn(const char *archivedir, XLogSegNo    segno, TimeLineID tli,
                                      uint32 wal_seg_size, int timeout, XLogRecPtr target);

/* in util.c */
extern TimeLineID get_current_timeline(PGconn *conn);
extern TimeLineID get_current_timeline_from_control(bool safe);
extern XLogRecPtr get_checkpoint_location(PGconn *conn);
extern uint64 get_system_identifier(const char *pgdata_path);
extern uint64 get_remote_system_identifier(PGconn *conn);
extern uint32 get_data_checksum_version(bool safe);
extern pg_crc32c get_pgcontrol_checksum(const char *fullpath);
extern uint32 get_xlog_seg_size(char *pgdata_path);
extern void get_redo(const char *pgdata_path, RedoParams *redo);
extern void parse_vgname_args(const char* args);
extern bool is_ss_xlog(const char *ss_dir);
extern void ss_createdir(const char *ss_dir, const char *vgdata, const char *vglog);
extern bool ss_create_if_pg_replication(pgFile *dir, const char *vgdata, const char *vglog);
extern bool ss_create_if_doublewrite(pgFile* dir, const char* vgdata, int instance_id);
extern bool ss_create_if_pg_replication(pgFile* dir, const char* vgdata, const char* vglog);
extern char* xstrdup(const char* s);
extern void set_min_recovery_point(pgFile *file, const char *fullpath,
                                   XLogRecPtr stop_backup_lsn);
extern void copy_pgcontrol_file(const char *from_fullpath, fio_location from_location,
                    const char *to_fullpath, fio_location to_location, pgFile *file);

extern void time2iso(char *buf, size_t len, time_t time);
extern const char *status2str(BackupStatus status);
extern BackupStatus str2status(const char *status);
extern const char *dev2str(device_type_t type);
extern device_type_t str2dev(const char *dev);
extern const char *base36enc(long unsigned int value);
extern char *base36enc_dup(long unsigned int value);
extern long unsigned int base36dec(const char *text);
extern uint32 parse_server_version(const char *server_version_str);
extern uint32 parse_program_version(const char *program_version);
extern bool   parse_page(Page page, XLogRecPtr *lsn);
extern int32  do_compress(void* dst, size_t dst_size, void const* src, size_t src_size,
                          CompressAlg alg, int level, const char **errormsg);
extern int32  do_decompress(void* dst, size_t dst_size, void const* src, size_t src_size,
                            CompressAlg alg, const char **errormsg);

extern void pretty_size(int64 size, char *buf, size_t len);
extern void pretty_time_interval(double time, char *buf, size_t len);

extern PGconn *pgdata_basic_setup(const ConnectionOptions conn_opt, PGNodeInfo *nodeInfo);
extern void check_system_identifiers(PGconn *conn, const char *pgdata);
extern void parse_filelist_filenames(parray *files, const char *root);

/* in ptrack.c */
extern void make_pagemap_from_ptrack(parray* files,
                                     PGconn* backup_conn,
                                     XLogRecPtr lsn);
extern XLogRecPtr get_last_ptrack_lsn(PGconn *backup_conn, PGNodeInfo *nodeInfo);
extern parray * pg_ptrack_get_pagemapset(PGconn *backup_conn, XLogRecPtr lsn);

/* open local file to writing */
extern FILE* open_local_file_rw(const char *to_fullpath, char **out_buf, uint32 buf_size);

extern int send_pages(ConnectionArgs* conn_arg, const char *to_fullpath, const char *from_fullpath,
                      pgFile *file, XLogRecPtr prev_backup_start_lsn, CompressAlg calg, int clevel,
                      uint32 checksum_version, bool use_pagemap, BackupPageHeader2 **headers,
                      BackupMode backup_mode);

/* FIO */
extern void fio_delete(mode_t mode, const char *fullpath, fio_location location);
extern int fio_send_pages(const char *to_fullpath, const char *from_fullpath, pgFile *file,
                          XLogRecPtr horizonLsn, int calg, int clevel, uint32 checksum_version,
                          bool use_pagemap, BlockNumber *err_blknum, char **errormsg,
                          BackupPageHeader2 **headers);
/* return codes for fio_send_pages */
extern int fio_send_file_gz(const char *from_fullpath, const char *to_fullpath, FILE* out, char **errormsg);
extern int fio_send_file(const char *from_fullpath, const char *to_fullpath, FILE* out,
                                                        pgFile *file, char **errormsg);

extern void fio_list_dir(parray *files, const char *root, bool exclude, bool follow_symlink,
                         bool add_root, bool backup_logs, bool skip_hidden, int external_dir_num,
                         bool backup_replslots = false);

extern bool pgut_rmtree(const char *path, bool rmtopdir, bool strict);

extern PageState *fio_get_checksum_map(const char *fullpath, uint32 checksum_version, int n_blocks,
                                    XLogRecPtr dest_stop_lsn, BlockNumber segmentno, fio_location location);

extern datapagemap_t *fio_get_lsn_map(const char *fullpath, uint32 checksum_version,
                            int n_blocks, XLogRecPtr horizonLsn, BlockNumber segmentno,
                            fio_location location);
extern pid_t fio_check_postmaster(const char *pgdata, fio_location location);

extern int32 fio_decompress(void* dst, void const* src, size_t size, int compress_alg);

/* return codes for fio_send_pages() and fio_send_file() */
#define SEND_OK       (0)
#define FILE_MISSING (-1)
#define OPEN_FAILED  (-2)
#define READ_FAILED  (-3)
#define WRITE_FAILED (-4)
#define ZLIB_ERROR   (-5)
#define REMOTE_ERROR (-6)
#define PAGE_CORRUPTION (-8)

/* Check if specified location is local for current node */
extern bool fio_is_remote(fio_location location);
extern bool fio_is_remote_simple(fio_location location);

extern void get_header_errormsg(Page page, char **errormsg);
extern void get_checksum_errormsg(Page page, char **errormsg,
                                  BlockNumber absolute_blkno);
extern void unlink_lock_atexit(bool fatal, void *userdata);

extern bool
datapagemap_is_set(datapagemap_t *map, BlockNumber blkno);

extern void
datapagemap_print_debug(datapagemap_t *map);

extern void replace_password(int argc, char** argv, const char* optionName);

void *gs_palloc0(Size size);
char *gs_pstrdup(const char *in);
void *gs_repalloc(void *pointer, Size size);

#endif /* PG_PROBACKUPC_H */
