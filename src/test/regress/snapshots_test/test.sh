#!/bin/bash
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ---------------------------------------------------------------------------------------
#
# test.sh
#     Test for DB4AI.Snapshot functionality.
#
#
# ---------------------------------------------------------------------------------------

set -eo pipefail # stop on all errors

[[ -n $GAUSSHOME ]] || export GAUSSHOME=$PGXC_INSTALL_DIR
[[ -n $GAUSSHOME ]] || { echo "GAUSSHOME not set." >&2; exit -1; }

SNAPSHOT_DIR="$(readlink -f ${BASH_SOURCE%/*})"
source "$SNAPSHOT_DIR"/snapshots.cfg

TEST_RUN_DIR="$SNAPSHOT_DIR/tests"
DEFAULT_ARGS='-v ON_ERROR_STOP=1 -Xef'
TEST_RUN_ARGS="-v TEST_BREAK_POINT= $DEFAULT_ARGS"
TEST_RUN_OUT='> $CURRENT_LOG 2>&1'

# overwrite cleanup hook
CLEANUP()
{
    if [[ $NEED_CLEANUP ]]; then
        if [[ $LOGIN_ON_ERROR ]]; then
            eval ${GSQL[@]} -Xr
        fi
        eval ${tasks[2]}
        printf "$(RED $CURRENT_BANNER) %0.35s " "$CURRENT_DETAIL $CURRENT_TASK $PAD"
        eval ${GSQL2[@]} && printf $PASS || printf $FAIL
        printf "$(RED '# TESTS FAILED #') DB4AI Snapshots regression test failed\n"
    fi
}

USAGE()
{
    cat <<EOF
Usage: ${BASH_SOURCE##*/} [OPTION]... [PATTERN]

PATTERN
  Execute only test where '*PATTERN*' matches the test name
OPTIONS
  -a,     --all             Run tests across all SQL compatibility modes
  -g,     --debug           Run test, but stop at breakpoints and keep GSQL
                            session open. Cleanup continues on logout.
  -s,     --single-step     Run the test in single-step interactive mode
  -l,     --login_on_error  Login as superuser to the database where the error
                            occurred. Allows to take a look around in a new
                            session. Cleanup continues on logout.
  -r      --reproducible    Generate reproducible output.
  -p      --port            Override TCP port
  -d      --dbname          Override database name
  -h,     --help            Print this help.

EOF
    exit $1
}

for ((i=1; i<=$#; i++)); do
case ${!i} in
    -h|--help)
        USAGE 0
        ;;
    -g|--debug)
        DEFAULT_ARGS='-v ON_ERROR_STOP=1 -Xf'
        TEST_RUN_ARGS='-v ON_ERROR_STOP=1 -v ON_ERROR_ROLLBACK=on -Xf'
        TEST_RUN_OUT='-o $CURRENT_LOG'
        ;;
    -s|--single-step)
        DEFAULT_ARGS='-v ON_ERROR_STOP=1 -Xf'
        TEST_RUN_ARGS='-v ON_ERROR_STOP=1 -v TEST_BREAK_POINT= -sXf'
        TEST_RUN_OUT='-o $CURRENT_LOG'
        ;;
    -l|--login_on_error)
        LOGIN_ON_ERROR=1
        ;;
    -r|--reproducible)
        TEST_REPRODUCIBLE=1
        ;;
    -p|--port)
        ((i++))
        GSQL_CMD_FLAGS="$GSQL_CMD_FLAGS -p ${!i}" 
        ;;
    -d|--dbname)
        ((i++))
        GSQL_CMD_FLAGS="$GSQL_CMD_FLAGS -d ${!i}"
        ;;
    -a|--all)
        export DATABASE_COMPATIBILITY
        if [[ $i != 1 ]]; then
            USAGE 1
        else
            shift
            for DATABASE_COMPATIBILITY in a b c; do
                "$SNAPSHOT_DIR"/test.sh $@
            done
            exit
        fi
        ;;
    *)
        if [[ ${!i} == -* || $i != $# ]]; then
            USAGE 1
        else
            TEST_RUN_PATTERN="${!i}"
        fi
        ;;
esac; done

GSQL=(LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH "$GAUSSHOME"/bin/gsql $GSQL_CMD_FLAGS)
GSQL2=(${GSQL[@]} -v TEST_BREAK_POINT='"\unset ECHO\o\i -\o ${CURRENT_LOG}"'
        -v TEST_CASE="$TEST_RUN_DIR/\$CURRENT_TEST \$CURRENT_ARGS $TEST_RUN_DIR/\$CURRENT_TASK $TEST_RUN_OUT")

tasks=("CURRENT_TASK=SetupTestHarness.sql; CURRENT_MASK=\$CURRENT_TASK; CURRENT_BANNER='## SETTING UP ##';
            CURRENT_DETAIL='harness'; CURRENT_PATTERN=''; CURRENT_ARGS=\$DEFAULT_ARGS;
            CURRENT_LOG=$TEST_RUN_DIR/\${CURRENT_TASK%.sql}.log"
       "CURRENT_TASK=ExecTestHarness.sql; CURRENT_MASK='??_*.sql'; CURRENT_BANNER='# RUNNING TEST #';
            CURRENT_DETAIL='case'; CURRENT_PATTERN=$TEST_RUN_PATTERN; CURRENT_ARGS=\$TEST_RUN_ARGS;
            CURRENT_LOG=\${CURRENT_TEST%.sql}.log"
       "CURRENT_TASK=TeardownTestHarness.sql; CURRENT_MASK=\$CURRENT_TASK; CURRENT_BANNER='### TEARDOWN ###';
            CURRENT_DETAIL='harness'; CURRENT_PATTERN=''; CURRENT_ARGS=\$DEFAULT_ARGS;
            CURRENT_LOG=$TEST_RUN_DIR/\${CURRENT_TASK%.sql}.log")

for task in "${tasks[@]}"; do eval $task
    for CURRENT_TEST in "$TEST_RUN_DIR"/$CURRENT_MASK; do eval $task
        CURRENT_TEST=${CURRENT_TEST##*/}
        [[ ! $CURRENT_PATTERN && "$CURRENT_TEST" == XX_*.sql ]] && continue
        printf "$(YELLOW $CURRENT_BANNER) %0.35s " "$CURRENT_DETAIL: ${CURRENT_TEST//_/ } $PAD"
        if [[ $CURRENT_PATTERN && $CURRENT_TEST != *$CURRENT_PATTERN* ]]; then
            printf $SKIP
        else
            START_TIME="$(date -u +%s%N)"
            eval ${GSQL2[@]}
            if [[ $TEST_REPRODUCIBLE ]]; then
                printf "$PASS"
            else
                printf "[%4s ms] $PASS" "$((($(date +%s%N) - $START_TIME)/1000000))"
            fi
        fi
    done
    NEED_CLEANUP=1
done

printf "$(GREEN '##### DONE #####') DB4AI Snapshots regression test complete\n"
