#ifndef BACKUP_H
#define BACKUP_H

#define BUILD_TAG_START "build_completed.start"
#define BUILD_TAG_DONE "build_completed.done"

extern int standby_recv_timeout;
extern int standby_connect_timeout;
extern int standby_message_timeout;

extern char* conn_str;
extern pid_t process_id;
extern bool no_need_fsync;
extern bool need_copy_upgrade_file;
extern char* basedir;
extern int bgpipe[2];
extern pid_t bgchild;
extern char remotelsn[MAXPGPATH];
extern char remotenodename[MAXPGPATH];

extern char* formatLogTime();
bool backup_main(const char* dir, uint32 term, bool isFromStandby);
bool CopySecureFilesMain(char* dirname, uint32 term);
bool backup_incremental_xlog(char* dir);
void get_xlog_location(char (&xlog_location)[MAXPGPATH]);
bool CreateBuildtagFile(const char* fulltagname);
bool StartLogStreamer(
    char* startpos, uint32 timeline, char* sysidentifier, const char* xloglocation, uint primaryTerm = 0);
bool RenameTblspcDir(char* dataDir);

#endif /* BACKUP_H */
