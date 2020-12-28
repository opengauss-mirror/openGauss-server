#ifndef BACKUP_H
#define BACKUP_H

#define BUILD_TAG_START "build_completed.start"
#define BUILD_TAG_DONE "build_completed.done"

extern int standby_recv_timeout;
extern int standby_connect_timeout;
extern int standby_message_timeout;

extern char* conn_str;
extern pid_t process_id;
extern char* basedir;
extern int bgpipe[2];
extern pid_t bgchild;

extern char* formatLogTime();
void backup_main(char* dir, uint32 term);
void backup_incremental_xlog(char* dir);
void get_xlog_location(char (&xlog_location)[MAXPGPATH]);
bool CreateBuildtagFile(const char* fulltagname);
void StartLogStreamer(
    char* startpos, uint32 timeline, char* sysidentifier, const char* xloglocation, uint primaryTerm = 0);

#endif /* BACKUP_H */
