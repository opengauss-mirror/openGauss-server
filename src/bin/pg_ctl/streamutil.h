#ifndef SRC_BIN_PG_CTL_STREAMUTIL_H
#define SRC_BIN_PG_CTL_STREAMUTIL_H
#include "libpq/libpq-fe.h"

extern const char* progname;

extern char* replication_slot;

/* Connection kept global so we can disconnect easily */
extern PGconn* streamConn;

extern PGconn* dbConn;

extern bool g_is_obsmode;

/* max length of password */
#define MAX_PASSWORD_LENGTH 999
#define MAXPGPATH 1024
extern char* register_username;
extern char* register_password;
extern char* dbport;
extern char* pg_host;
extern int dbgetpassword;
extern char pid_file[MAXPGPATH];

extern char* slotname;
extern char* taskid;
extern char pgxc_node_name[MAXPGPATH];

extern char* xstrdup(const char* s);
extern void* xmalloc0(int size);
extern PGconn* GetConnection(void);
char* inc_dbport(const char* dbport);
bool exe_sql(PGconn* Conn, const char* sqlCommond);
void update_obs_build_status(const char* build_status);

#endif  // SRC_BIN_PG_CTL_STREAMUTIL_H