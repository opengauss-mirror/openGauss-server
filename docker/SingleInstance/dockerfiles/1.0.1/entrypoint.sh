#!/usr/bin/env bash
set -Eeo pipefail 

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)

export GAUSSHOME=/usr/local/opengauss
export PATH=$GAUSSHOME/bin:$PATH
export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH

file_env() {
        local var="$1"
        local fileVar="${var}_FILE"
        local def="${2:-}"
        if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
                echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
                exit 1
        fi
        local val="$def"
        if [ "${!var:-}" ]; then
                val="${!var}"
        elif [ "${!fileVar:-}" ]; then
                val="$(< "${!fileVar}")"
        fi
        export "$var"="$val"
        unset "$fileVar"
}

# check to see if this file is being run or sourced from another script
_is_sourced() {
        [ "${#FUNCNAME[@]}" -ge 2 ] \
                && [ "${FUNCNAME[0]}" = '_is_sourced' ] \
                && [ "${FUNCNAME[1]}" = 'source' ]
}

# used to create initial opengauss directories and if run as root, ensure ownership belong to the omm user
docker_create_db_directories() {
        local user; user="$(id -u)"

        mkdir -p "$PGDATA"
        chmod 700 "$PGDATA"

        # ignore failure since it will be fine when using the image provided directory;
        mkdir -p /var/run/opengauss || :
        chmod 775 /var/run/opengauss || :

        # Create the transaction log directory before initdb is run so the directory is owned by the correct user
        if [ -n "$POSTGRES_INITDB_XLOGDIR" ]; then
                mkdir -p "$POSTGRES_INITDB_XLOGDIR"
                if [ "$user" = '0' ]; then
                        find "$POSTGRES_INITDB_XLOGDIR" \! -user postgres -exec chown postgres '{}' +
                fi
                chmod 700 "$POSTGRES_INITDB_XLOGDIR"
        fi

        # allow the container to be started with `--user`
        if [ "$user" = '0' ]; then
                find "$PGDATA" \! -user omm -exec chown omm '{}' +
                find /var/run/opengauss \! -user omm -exec chown omm '{}' +
        fi
}

# initialize empty PGDATA directory with new database via 'initdb'
# arguments to `initdb` can be passed via POSTGRES_INITDB_ARGS or as arguments to this function
# `initdb` automatically creates the "postgres", "template0", and "template1" dbnames
# this is also where the database user is created, specified by `GS_USER` env
docker_init_database_dir() {
        # "initdb" is particular about the current user existing in "/etc/passwd", so we use "nss_wrapper" to fake that if necessary
        if ! getent passwd "$(id -u)" &> /dev/null && [ -e /usr/lib/libnss_wrapper.so ]; then
                export LD_PRELOAD='/usr/lib/libnss_wrapper.so'
                export NSS_WRAPPER_PASSWD="$(mktemp)"
                export NSS_WRAPPER_GROUP="$(mktemp)"
                echo "postgres:x:$(id -u):$(id -g):PostgreSQL:$PGDATA:/bin/false" > "$NSS_WRAPPER_PASSWD"
                echo "postgres:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
        fi

        if [ -n "$POSTGRES_INITDB_XLOGDIR" ]; then
                set -- --xlogdir "$POSTGRES_INITDB_XLOGDIR" "$@"
        fi

        if [ -n "$GS_NODENAME" ]; then
                eval 'gs_initdb --pwfile=<(echo "$GS_PASSWORD") --nodename=$GS_NODENAME '"$POSTGRES_INITDB_ARGS"' "$@"'
        else
                eval 'gs_initdb --pwfile=<(echo "$GS_PASSWORD") --nodename=gaussdb '"$POSTGRES_INITDB_ARGS"' "$@"'
        fi        
        # unset/cleanup "nss_wrapper" bits
        if [ "${LD_PRELOAD:-}" = '/usr/lib/libnss_wrapper.so' ]; then
                rm -f "$NSS_WRAPPER_PASSWD" "$NSS_WRAPPER_GROUP"
                unset LD_PRELOAD NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
        fi
}

# print large warning if GS_PASSWORD is long
# error if both GS_PASSWORD is empty and GS_HOST_AUTH_METHOD is not 'trust'
# print large warning if GS_HOST_AUTH_METHOD is set to 'trust'
# assumes database is not set up, ie: [ -z "$DATABASE_ALREADY_EXISTS" ]
docker_verify_minimum_env() {
        # check password first so we can output the warning before postgres
        # messes it up
        if [ "${#GS_PASSWORD}" -ge 100 ]; then
                cat >&2 <<-'EOWARN'

                        WARNING: The supplied GS_PASSWORD is 100+ characters.

EOWARN
        fi
        if [ -z "$GS_PASSWORD" ] && [ 'trust' != "$GS_HOST_AUTH_METHOD" ]; then
                # The - option suppresses leading tabs but *not* spaces. :)
                cat >&2 <<-'EOE'
                        Error: Database is uninitialized and superuser password is not specified.
                               You must specify GS_PASSWORD to a non-empty value for the
                               superuser. For example, "-e GS_PASSWORD=password" on "docker run".

                               You may also use "GS_HOST_AUTH_METHOD=trust" to allow all
                               connections without a password. This is *not* recommended.

EOE
                exit 1
        fi
        if [ 'trust' = "$GS_HOST_AUTH_METHOD" ]; then
                cat >&2 <<-'EOWARN'
                        ********************************************************************************
                        WARNING: GS_HOST_AUTH_METHOD has been set to "trust". This will allow
                                 anyone with access to the opengauss port to access your database without
                                 a password, even if GS_PASSWORD is set.
                                 It is not recommended to use GS_HOST_AUTH_METHOD=trust. Replace
                                 it with "-e GS_PASSWORD=password" instead to set a password in
                                 "docker run".
                        ********************************************************************************
EOWARN
        fi
}

# usage: docker_process_init_files [file [file [...]]]
#    ie: docker_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions and permissions
docker_process_init_files() {
        # gsql here for backwards compatiblilty "${gsql[@]}"
        gsql=( docker_process_sql )

        echo
        local f
        for f; do
                case "$f" in
                        *.sh)
                                if [ -x "$f" ]; then
                                        echo "$0: running $f"
                                        "$f"
                                else
                                        echo "$0: sourcing $f"
                                        . "$f"
                                fi
                                ;;
                        *.sql)    echo "$0: running $f"; docker_process_sql -f "$f"; echo ;;
                        *.sql.gz) echo "$0: running $f"; gunzip -c "$f" | docker_process_sql; echo ;;
                        *.sql.xz) echo "$0: running $f"; xzcat "$f" | docker_process_sql; echo ;;
                        *)        echo "$0: ignoring $f" ;;
                esac
                echo
        done
}

# Execute sql script, passed via stdin (or -f flag of pqsl)
# usage: docker_process_sql [gsql-cli-args]
#    ie: docker_process_sql --dbname=mydb <<<'INSERT ...'
#    ie: docker_process_sql -f my-file.sql
#    ie: docker_process_sql <my-file.sql
docker_process_sql() {
        local query_runner=( gsql -v ON_ERROR_STOP=1 --username "$GS_USER" --password "$GS_PASSWORD")
        if [ -n "$GS_DB" ]; then
                query_runner+=( --dbname "$GS_DB" )
        fi

        "${query_runner[@]}" "$@"
}

# create initial database
# uses environment variables for input: GS_DB
docker_setup_db() {
        if [ "$GS_DB" != 'postgres' ]; then
                GS_DB= docker_process_sql --dbname postgres --set db="$GS_DB" --set passwd="$GS_PASSWORD" --set passwd="$GS_PASSWORD" <<-'EOSQL'
                        CREATE DATABASE :"db" ;
                        create user gaussdb with login password :"passwd" ;

EOSQL
                echo
        fi
}

docker_setup_user() {
        if [ -n "$GS_USERNAME" ]; then
                GS_DB= docker_process_sql --dbname postgres --set db="$GS_DB" --set passwd="$GS_PASSWORD" --set user="$GS_USERNAME" <<-'EOSQL'
                        create user :"user" with login password :"passwd" ;
EOSQL
        else           
                echo " default user is gaussdb"
        fi
}


# Loads various settings that are used elsewhere in the script
# This should be called before any other functions
docker_setup_env() {
        export GS_USER=omm
        file_env 'GS_PASSWORD'

        # file_env 'GS_USER' 'omm'
        file_env 'GS_DB' "$GS_USER"
        file_env 'POSTGRES_INITDB_ARGS'
        # default authentication method is md5
        : "${GS_HOST_AUTH_METHOD:=md5}"

        declare -g DATABASE_ALREADY_EXISTS
        # look specifically for OG_VERSION, as it is expected in the DB dir
        if [ -s "$PGDATA/PG_VERSION" ]; then
                DATABASE_ALREADY_EXISTS='true'
        fi
}

# append GS_HOST_AUTH_METHOD to pg_hba.conf for "host" connections
opengauss_setup_hba_conf() {
        {
                echo
                if [ 'trust' = "$GS_HOST_AUTH_METHOD" ]; then
                        echo '# warning trust is enabled for all connections'
                fi
                echo "host all all 0.0.0.0/0 $GS_HOST_AUTH_METHOD"
        } >> "$PGDATA/pg_hba.conf"
}

# append parameter to postgres.conf for connections
opengauss_setup_postgresql_conf() {
        {
                echo
                if [ -n "$GS_PORT" ]; then
                    echo "password_encryption_type = 0"
                    echo "listen_addresses = '*'"
                    echo "port = $GS_PORT"
                else
                    echo '# use default port 5432'
                    echo "password_encryption_type = 0"
                    echo "listen_addresses = '*'"
                fi
        } >> "$PGDATA/postgresql.conf"
}

opengauss_setup_mot_conf() {
         echo "enable_numa = false" >> "$PGDATA/mot.conf"
}

# start socket-only postgresql server for setting up or running scripts
# all arguments will be passed along as arguments to `postgres` (via pg_ctl)
docker_temp_server_start() {
        if [ "$1" = 'gaussdb' ]; then
                shift
        fi

        # internal start of server in order to allow setup using gsql client
        # does not listen on external TCP/IP and waits until start finishes
        set -- "$@" -c listen_addresses='' -p "${PGPORT:-5432}"

        PGUSER="${PGUSER:-$GS_USER}" \
        gs_ctl -D "$PGDATA" \
                -o "$(printf '%q ' "$@")" \
                -w start
}

# stop postgresql server after done setting up user and running scripts
docker_temp_server_stop() {
        PGUSER="${PGUSER:-postgres}" \
        gs_ctl -D "$PGDATA" -m fast -w stop
}

# check arguments for an option that would cause opengauss to stop
# return true if there is one
_opengauss_want_help() {
        local arg
        for arg; do
                case "$arg" in
                        # postgres --help | grep 'then exit'
                        # leaving out -C on purpose since it always fails and is unhelpful:
                        # postgres: could not access the server configuration file "/var/lib/postgresql/data/postgresql.conf": No such file or directory
                        -'?'|--help|--describe-config|-V|--version)
                                return 0
                                ;;
                esac
        done
        return 1
}

_main() {
        # if first arg looks like a flag, assume we want to run postgres server
        if [ "${1:0:1}" = '-' ]; then
                set -- gaussdb "$@"
        fi

        if [ "$1" = 'gaussdb' ] && ! _opengauss_want_help "$@"; then
                docker_setup_env
                # setup data directories and permissions (when run as root)
                docker_create_db_directories
                if [ "$(id -u)" = '0' ]; then
                        # then restart script as postgres user
                        exec gosu omm "$BASH_SOURCE" "$@"
                fi

                # only run initialization on an empty data directory
                if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
                        docker_verify_minimum_env

                        # check dir permissions to reduce likelihood of half-initialized database
                        ls /docker-entrypoint-initdb.d/ > /dev/null

                        docker_init_database_dir
                        opengauss_setup_hba_conf
                        opengauss_setup_postgresql_conf
                        opengauss_setup_mot_conf

                        # PGPASSWORD is required for gsql when authentication is required for 'local' connections via pg_hba.conf and is otherwise harmless
                        # e.g. when '--auth=md5' or '--auth-local=md5' is used in POSTGRES_INITDB_ARGS
                        export PGPASSWORD="${PGPASSWORD:-$GS_PASSWORD}"
                        docker_temp_server_start "$@"

                        docker_setup_db
                        docker_setup_user
                        docker_process_init_files /docker-entrypoint-initdb.d/*

                        docker_temp_server_stop
                        unset PGPASSWORD

                        echo
                        echo 'openGauss  init process complete; ready for start up.'
                        echo
                else
                        echo
                        echo 'openGauss Database directory appears to contain a database; Skipping initialization'
                        echo
                fi
        fi

        exec "$@"
}

if ! _is_sourced; then
        _main "$@"
fi
