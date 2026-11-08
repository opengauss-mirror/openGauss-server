#!/usr/bin/env python3
"""Run native DiskANN prerequisites and cancellation in an explicitly owned instance."""
import argparse
import getpass
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time

import psycopg2


TESTS = (
    "diskann_vector", "union_all_required_columns",
    "union_all_required_columns_safety", "union_all_ann_feature_matrix",
    "ann_orderby_distance_reuse", "ann_orderby_distance_reuse_safety",
    "ann_orderby_distance_reuse_lifecycle", "diskann_filter_baseline",
)


def verify_instance(args):
    with psycopg2.connect(host=args.host, port=args.port, dbname="regression") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_setting('data_directory'), version()")
            data, version = cursor.fetchone()
            if Path(data).resolve() != args.data.resolve():
                raise RuntimeError("connected to a different data directory: " + data)
    pid = int((args.data / "postmaster.pid").read_text().splitlines()[0])
    binary = Path("/proc", str(pid), "exe").resolve()
    expected = (args.install / "bin/gaussdb").resolve()
    if binary != expected:
        raise RuntimeError("running binary differs from the specified install: " + str(binary))
    return {"data": data, "version": version, "pid": pid, "binary": str(binary)}


def cancellation_check(args):
    """Exercise statement cancellation and connection reuse on the ordinary executor."""
    conn = psycopg2.connect(host=args.host, port=args.port, dbname="regression")
    monitor = psycopg2.connect(host=args.host, port=args.port, dbname="regression")
    conn.autocommit = monitor.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute("SELECT pg_backend_pid()")
        activity_pid = cursor.fetchone()[0]
    # openGauss's wire cancellation key is not the full thread ID shown by
    # pg_stat_activity. Monitor the SQL PID, while libpq handles cancellation.
    report = {"scope": "ordinary executor prerequisite; new PRE/IN cancellation is tested later",
              "sql": "SELECT pg_sleep(30)", "pid": activity_pid,
              "libpq_cancel_pid": conn.get_backend_pid()}
    observed = threading.Event()
    errors = []

    def cancel_when_active():
        try:
            deadline = time.monotonic() + 5
            with monitor.cursor() as cursor:
                while time.monotonic() < deadline:
                    cursor.execute("SELECT state, query FROM pg_stat_activity "
                                   "WHERE pid=%s", (report["pid"],))
                    row = cursor.fetchone()
                    if row and row[0] == "active" and "pg_sleep(30)" in row[1]:
                        report["observed_query"] = row
                        conn.cancel()
                        observed.set()
                        return
                    time.sleep(0.02)
            errors.append("statement was not observed active before cancellation")
            conn.cancel()
        except Exception as exc:
            errors.append(str(exc))
            conn.cancel()

    worker = threading.Thread(target=cancel_when_active)
    worker.start()
    try:
        with conn.cursor() as cursor:
            try:
                cursor.execute(report["sql"])
            except psycopg2.Error as exc:
                report["sqlstate"] = exc.pgcode
                report["error"] = exc.pgerror
            else:
                raise AssertionError("statement completed without cancellation")
            worker.join(timeout=6)
            if worker.is_alive() or errors or not observed.is_set():
                raise AssertionError("cancellation monitor failed: " + str(errors))
            if report["sqlstate"] != "57014":
                raise AssertionError("unexpected cancellation SQLSTATE")
            cursor.execute("SELECT 1")
            report["connection_reusable"] = cursor.fetchone() == (1,)
            if not report["connection_reusable"]:
                raise AssertionError("connection did not recover after cancellation")
        report["result"] = "PASS"
        return report
    finally:
        worker.join(timeout=6)
        monitor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.install, args.data, args.output = (p.resolve() for p in (args.install, args.data, args.output))
    if args.data.name != "data" or "diskann-vector-filter" not in args.data.parent.name:
        parser.error("use a dedicated diskann-vector-filter data directory")
    # Native diskann_vector creates/drops test and returns to regression.
    # Never point this runner at an existing user instance.
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"result": "FAIL", "tests": TESTS, "host": args.host, "port": args.port}
    try:
        report["instance"] = verify_instance(args)
        regress = Path(__file__).resolve().parent
        source = regress.parents[2]
        inputs = args.output / "inputs"
        run = args.output / "run"
        for root in (inputs, run):
            for folder in ("sql", "expected", "results", "log"):
                (root / folder).mkdir(parents=True)
        for name in TESTS:
            for folder, suffix in (("sql", "sql"), ("expected", "out")):
                shutil.copy2(regress / folder / (name + "." + suffix), inputs / folder)
            for alternate in (regress / "expected").glob(name + "_[0-9].out"):
                shutil.copy2(alternate, inputs / "expected")
        shutil.copy2(regress / "smartmatch.pl", inputs)
        schedule = args.output / "baseline.schedule"
        schedule.write_text("".join("test: " + name + "\n" for name in TESTS))
        # Per-test gsql uses myinfo.dn_port from -p, not just --port.
        command = [str(regress / "pg_regress"), "--use-existing", "--single_node",
                   "-r", "1", "-c", "0", "-d", "1", "-p", str(args.port),
                   "--top-builddir=" + str(source), "--host=" + args.host,
                   "--port=" + str(args.port), "--user=" + getpass.getuser(),
                   "--psqldir=" + str(args.install / "bin"), "--inputdir=" + str(inputs),
                   "--outputdir=" + str(run), "--dbname=regression", "--schedule=" + str(schedule)]
        env = dict(os.environ)
        env.update(GAUSSHOME=str(args.install), CODE_BASE_SRC=str(source),
                   SSL_LIB_PATH=str(args.install / "lib"),
                   LD_LIBRARY_PATH=str(args.install / "lib") + os.pathsep + env.get("LD_LIBRARY_PATH", ""))
        report["native_command"] = command
        report["native_exit_code"] = subprocess.run(command, cwd=regress, env=env).returncode
        if report["native_exit_code"] != 0:
            return report["native_exit_code"]
        report["cancellation"] = cancellation_check(args)
        report["result"] = "PASS"
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        report["error"] = str(exc)
        raise
    finally:
        (args.output / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
