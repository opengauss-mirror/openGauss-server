#!/usr/bin/env python3
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

"""Check that recovery conflict retries preserve the partition lock tag."""
import getpass
import os
from pathlib import Path
import resource
import shutil
import socket
import subprocess
import sys
import tempfile
import time

POLL_TIMEOUT = 30
COMMAND_TIMEOUT = 300
PORT_OFFSETS = (0, 1, 4, 5, 10, 20)


def reserve_ports():
    """Reserve a node's SQL, replication, heartbeat and communication ports."""
    for _ in range(100):
        sockets = []
        try:
            first = socket.socket()
            sockets.append(first)
            first.bind(('127.0.0.1', 0))
            port = first.getsockname()[1]
            for offset in PORT_OFFSETS[1:]:
                sock = socket.socket()
                sockets.append(sock)
                sock.bind(('127.0.0.1', port + offset))
            return port, sockets
        except (OSError, OverflowError):
            for sock in sockets:
                sock.close()
    raise RuntimeError('could not reserve node ports')


class RecoveryLockTest:
    def __init__(self, bindir, workdir):
        self.bindir = Path(bindir).absolute()
        self.logdir = Path(workdir).absolute() / 'log'
        self.logdir.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix='standby_partition_lock_', dir=str(self.logdir.parent)))
        self.socket_dir = Path(tempfile.mkdtemp(prefix='spl_socket_'))
        self.primary = self.root / 'primary'
        self.standby = self.root / 'standby'
        self.env = os.environ.copy()
        for name in list(self.env):
            if name.startswith('PG'):
                self.env.pop(name)
        self.env.update(PGHOST=str(self.socket_dir), PGUSER=getpass.getuser(), PGDATABASE='postgres',
                        LC_ALL='C', GAUSSHOME=str(self.bindir.parent))
        self.setup_log = (self.root / 'setup.log').open('w')
        self.reader = None
        self.reservations = []
        self.nodes = []

    def command(self, *args, check=True):
        result = subprocess.run([str(self.bindir / args[0])] + list(args[1:]),
                                env=self.env, stdout=self.setup_log, stderr=subprocess.STDOUT,
                                timeout=COMMAND_TIMEOUT)
        if check and result.returncode:
            raise RuntimeError('%s failed with exit code %s' % (args[0], result.returncode))

    def sql(self, port, statement, check=True):
        result = subprocess.run([str(self.bindir / 'gsql'), '-X', '-q', '-A', '-t',
                                 '-p', str(port), '-d', 'postgres', '-v', 'ON_ERROR_STOP=1',
                                 '-c', statement], env=self.env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        if check and result.returncode:
            raise RuntimeError(result.stderr)
        return result

    def wait_for(self, port, statement, expected):
        deadline = time.monotonic() + POLL_TIMEOUT
        while time.monotonic() < deadline:
            result = self.sql(port, statement, check=False)
            if result.returncode == 0 and result.stdout.strip() == expected:
                return
            time.sleep(0.1)
        raise RuntimeError('timed out waiting for: ' + statement)

    def start(self):
        self.command('gs_initdb', '-D', str(self.primary), '--nodename=partition_lock_primary',
                     '-U', getpass.getuser(), '-w', 'StandbyLock@123', '--locale=C', '-E', 'UTF-8')
        # Copy a clean, stopped cluster so both nodes have the same system identifier.
        shutil.copytree(str(self.primary), str(self.standby))
        primary_port, primary_sockets = reserve_ports()
        self.reservations.extend(primary_sockets)
        standby_port, standby_sockets = reserve_ports()
        self.reservations.extend(standby_sockets)
        self.nodes = [(self.primary, primary_port, standby_port, 'primary', primary_sockets),
                      (self.standby, standby_port, primary_port, 'standby', standby_sockets)]
        for data, port, peer, role, sockets in self.nodes:
            with (data / 'postgresql.conf').open('a') as config:
                config.write("""
listen_addresses = '127.0.0.1'
unix_socket_directory = '{socket_dir}'
port = {port}
comm_control_port = {control}
comm_sctp_port = {sctp}
replconninfo1 = 'localhost=127.0.0.1 localport={repl} localheartbeatport={heartbeat} localservice={service} remotehost=127.0.0.1 remoteport={peer_repl} remoteheartbeatport={peer_heartbeat} remoteservice={peer_service}'
pgxc_node_name = 'partition_lock_{role}'
hot_standby = on
hot_standby_feedback = off
max_standby_streaming_delay = 3000
recovery_parallelism = 4
recovery_parse_workers = 1
recovery_redo_workers = 4
synchronous_commit = local
logging_collector = off
""".format(socket_dir=self.socket_dir, port=port, control=port + 10, sctp=port + 20,
           repl=port + 1, heartbeat=port + 5, service=port + 4, peer_repl=peer + 1,
           peer_heartbeat=peer + 5, peer_service=peer + 4, role=role))
            for sock in sockets:
                sock.close()
            self.command('gs_ctl', 'start', '-D', str(data), '-M', role,
                         '-l', str(self.root / (role + '.log')), '-t', '60')
        self.primary_port = primary_port
        self.standby_port = standby_port
        self.wait_for(standby_port, 'SELECT pg_is_in_recovery()', 't')

    def run(self):
        self.start()
        self.sql(self.primary_port, """
CREATE TABLE sar_min(id int, w int, v text) PARTITION BY LIST(w)
(PARTITION sar_m1 VALUES (1), PARTITION sar_m2 VALUES (2));
INSERT INTO sar_min SELECT g, (g % 2) + 1, 'm' FROM generate_series(1,4000) g;
CREATE TABLE replay_marker(id int);
""")
        self.wait_for(self.standby_port, 'SELECT count(*) FROM sar_min', '4000')
        pid_file = self.standby / 'postmaster.pid'
        standby_pid = pid_file.read_text().splitlines()[0]
        with (self.root / 'reader.log').open('w') as reader_log:
            self.reader = subprocess.Popen([str(self.bindir / 'gsql'), '-X', '-q', '-A', '-t',
                                           '-p', str(self.standby_port), '-d', 'postgres',
                                           '-v', 'ON_ERROR_STOP=1', '-c', """
SET application_name = 'standby_partition_lock_reader';
BEGIN;
SELECT count(*) FROM sar_min;
SELECT pg_sleep(60);
COMMIT;
"""], env=self.env, stdout=reader_log, stderr=subprocess.STDOUT)
            # Do not issue VACUUM until the standby reader actually holds a partition lock.
            self.wait_for(self.standby_port, """
SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a ON l.pid = a.pid
WHERE a.application_name = 'standby_partition_lock_reader'
AND l.locktype = 'partition' AND l.mode = 'AccessShareLock' AND l.granted;
""", 't')
            self.sql(self.primary_port, 'VACUUM FULL sar_min PARTITION (sar_m1)')
            self.sql(self.primary_port, 'INSERT INTO replay_marker VALUES (1)')
            # A successful VACUUM on the primary alone does not prove commit replay succeeded.
            self.wait_for(self.standby_port, 'SELECT count(*) FROM replay_marker', '1')
            self.reader.wait(timeout=POLL_TIMEOUT)
        reader_output = (self.root / 'reader.log').read_text()
        if self.reader.returncode == 0 or 'conflict with recovery' not in reader_output:
            raise RuntimeError('the standby reader was not canceled by recovery')
        if pid_file.read_text().splitlines()[0] != standby_pid:
            raise RuntimeError('standby restarted during recovery')
        self.wait_for(self.standby_port, 'SELECT count(*) FROM sar_min', '4000')
        self.wait_for(self.standby_port, """
SELECT count(*) FROM pg_locks WHERE mode = 'AccessExclusiveLock' AND granted
AND database = (SELECT oid FROM pg_database WHERE datname = current_database());
""", '0')
        standby_log = (self.root / 'standby.log').read_text(errors='replace')
        for message in ("you don't own a lock", 'RecoveryLockLists contains entry', 'FailedAssertion'):
            if message in standby_log:
                raise RuntimeError('standby reported: ' + message)

    def cleanup(self):
        if self.reader is not None and self.reader.poll() is None:
            self.reader.terminate()
            self.reader.wait(timeout=10)
        for data, _, _, _, _ in reversed(self.nodes):
            self.command('gs_ctl', 'stop', '-D', str(data), '-m', 'immediate', '-t', '30', check=False)
        for sock in self.reservations:
            sock.close()
        self.setup_log.close()
        with (self.logdir / 'standby_partition_lock.log').open('w') as output:
            for log in sorted(self.root.glob('*.log')):
                output.write('\n%s\n%s' % (log.name, log.read_text(errors='replace')))
        shutil.rmtree(str(self.root))
        shutil.rmtree(str(self.socket_dir))


def main():
    # A failing cassert build should not leave a large core file in the test directory.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    test = RecoveryLockTest(sys.argv[1], sys.argv[2])
    success = False
    try:
        test.run()
        success = True
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        test.setup_log.write('\nTest failed: %s\n' % error)
    finally:
        test.cleanup()
    if success:
        print('standby partition lock recovery: ok')
        return 0
    print('standby partition lock recovery: FAILED (see log/standby_partition_lock.log)')
    return 1


if __name__ == '__main__':
    sys.exit(main())
