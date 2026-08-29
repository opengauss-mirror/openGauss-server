#!/bin/sh
set -eu
: "${GAUSSHOME:?GAUSSHOME must point to an openGauss installation}"
: "${TEST_PGDATA:?TEST_PGDATA must point to an isolated running instance}"
TEST_PORT=${TEST_PORT:-5432}
TEST_DB=${TEST_DB:-postgres}
# TEST_ROOT must sit on a filesystem that supports O_DIRECT: the restored
# instance opens its double-write files with O_DIRECT, which tmpfs rejects.
TEST_ROOT=${TEST_ROOT:-${TMPDIR:-/tmp}/gs_probackup_encrypt_$$}
PROBACKUP=${PROBACKUP:-${GAUSSHOME}/bin/gs_probackup}
GSQL=${GSQL:-${GAUSSHOME}/bin/gsql}
GS_CTL=${GS_CTL:-${GAUSSHOME}/bin/gs_ctl}
GS_GUC=${GS_GUC:-${GAUSSHOME}/bin/gs_guc}
CATALOG=${TEST_ROOT}/catalog
RESTORE_DIR=${TEST_ROOT}/restore
KEY_FILE=${TEST_ROOT}/backup.key
OLD_KEY='probackup-encryption-regression-old'
NEW_KEY='probackup-encryption-regression-new'
cleanup() { "${GS_CTL}" stop -D "${RESTORE_DIR}" -m immediate >/dev/null 2>&1 || true; rm -rf "${TEST_ROOT}"; }
trap cleanup EXIT INT TERM
rm -rf "${TEST_ROOT}"; mkdir -p "${TEST_ROOT}"
printf '%s\n' "${OLD_KEY}" > "${KEY_FILE}"; chmod 0600 "${KEY_FILE}"
"${PROBACKUP}" init -B "${CATALOG}"
"${PROBACKUP}" add-instance -B "${CATALOG}" --instance=enc -D "${TEST_PGDATA}"
"${GSQL}" -p "${TEST_PORT}" -d "${TEST_DB}" -v ON_ERROR_STOP=1 <<'SQL'
DROP TABLE IF EXISTS probackup_encrypt_test;
CREATE TABLE probackup_encrypt_test(id integer PRIMARY KEY, value text);
INSERT INTO probackup_encrypt_test SELECT value, md5(value::text) FROM generate_series(1, 10000) AS value;
CHECKPOINT;
SQL
"${PROBACKUP}" backup -B "${CATALOG}" --instance=enc -b full -d "${TEST_DB}" -p "${TEST_PORT}" --stream --encrypt --encrypt-key-source=keyfile --encrypt-key-file="${KEY_FILE}"
FULL_ID=$("${PROBACKUP}" show -B "${CATALOG}" --instance=enc --format=json | python3 -c "import json,sys; print(json.load(sys.stdin)['backup_info'][0]['backups'][0]['id'])")
FULL_ROOT=${CATALOG}/backups/enc/${FULL_ID}
test "$(dd if="${FULL_ROOT}/backup_content.control" bs=8 count=1 2>/dev/null)" = GSPBENC1
test "$(dd if="${FULL_ROOT}/database/global/pg_control" bs=8 count=1 2>/dev/null)" = GSPBENC1
test -s "${FULL_ROOT}/backup.control.mac"
"${GSQL}" -p "${TEST_PORT}" -d "${TEST_DB}" -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO probackup_encrypt_test SELECT value, md5(value::text) FROM generate_series(10001, 20000) AS value;
CHECKPOINT;
SQL
"${PROBACKUP}" backup -B "${CATALOG}" --instance=enc -b ptrack --incremental-type=cumulative -d "${TEST_DB}" -p "${TEST_PORT}" --stream --encrypt --encrypt-key-file="${KEY_FILE}"
INCREMENTAL_ID=$("${PROBACKUP}" show -B "${CATALOG}" --instance=enc --format=json | python3 -c "import json,sys; print(json.load(sys.stdin)['backup_info'][0]['backups'][0]['id'])")
"${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${INCREMENTAL_ID}" --encrypt-key-file="${KEY_FILE}"
"${PROBACKUP}" restore -B "${CATALOG}" --instance=enc -i "${INCREMENTAL_ID}" -D "${RESTORE_DIR}" --encrypt-key-file="${KEY_FILE}"
RESTORE_PORT=$((TEST_PORT + 100))
"${GS_GUC}" set -D "${RESTORE_DIR}" -c "port=${RESTORE_PORT}" >/dev/null
"${GS_CTL}" start -D "${RESTORE_DIR}" -Z single_node -l "${TEST_ROOT}/restore.log"
test "$("${GSQL}" -p "${RESTORE_PORT}" -d "${TEST_DB}" -Atc 'SELECT count(*) FROM probackup_encrypt_test')" = 20000
"${GS_CTL}" stop -D "${RESTORE_DIR}"
DATA_FILE=$(python3 - "${FULL_ROOT}/database" <<'PY'
import os, sys
for root, _, files in os.walk(sys.argv[1]):
    for name in files:
        path = os.path.join(root, name)
        if os.path.getsize(path) > 1024 * 1024:
            print(path); raise SystemExit
raise SystemExit('no data file found')
PY
)
BEFORE=$(stat -c '%i:%Y:%s' "${DATA_FILE}"); BEFORE_HASH=$(sha256sum "${DATA_FILE}")
GS_PROBACKUP_PASSPHRASE=${OLD_KEY} "${PROBACKUP}" rekey -B "${CATALOG}" --instance=enc -i "${FULL_ID}" --new-encrypt-key="${NEW_KEY}"
test "${BEFORE}" = "$(stat -c '%i:%Y:%s' "${DATA_FILE}")"; test "${BEFORE_HASH}" = "$(sha256sum "${DATA_FILE}")"
GS_PROBACKUP_PASSPHRASE=${NEW_KEY} "${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${FULL_ID}"
if GS_PROBACKUP_PASSPHRASE=${OLD_KEY} "${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${FULL_ID}" >/dev/null 2>&1; then echo 'old key unexpectedly worked after rekey' >&2; exit 1; fi

# --- guard: show works without any key material and reports encryption metadata ---
env -u GS_PROBACKUP_PASSPHRASE "${PROBACKUP}" show -B "${CATALOG}" --instance=enc --format=json | \
    python3 -c "import json,sys; b=json.load(sys.stdin)['backup_info'][0]['backups']; assert any(x.get('encrypt-algorithm')=='AES128' for x in b), 'encrypt-algorithm missing in show output'"

# --- guard: a wrong passphrase is rejected before any data is produced ---
if GS_PROBACKUP_PASSPHRASE='definitely-wrong-key' "${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${FULL_ID}" >/dev/null 2>&1; then echo 'wrong key unexpectedly accepted' >&2; exit 1; fi

# --- guard: a flipped ciphertext byte must fail chunk authentication ---
cp "${DATA_FILE}" "${DATA_FILE}.orig"
python3 - "${DATA_FILE}" <<'PY'
import sys
path = sys.argv[1]
with open(path, 'r+b') as f:
    f.seek(64 + 128)          # inside chunk 0 ciphertext, after the 64B header
    b = f.read(1)
    f.seek(64 + 128)
    f.write(bytes([b[0] ^ 0x01]))
PY
if GS_PROBACKUP_PASSPHRASE=${NEW_KEY} "${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${FULL_ID}" >/dev/null 2>&1; then echo 'tampered data file unexpectedly validated' >&2; exit 1; fi
mv "${DATA_FILE}.orig" "${DATA_FILE}"
GS_PROBACKUP_PASSPHRASE=${NEW_KEY} "${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${FULL_ID}"

# --- guard: a tampered backup.control must fail its MAC check ---
CONTROL_FILE=${FULL_ROOT}/backup.control
cp "${CONTROL_FILE}" "${CONTROL_FILE}.orig"
sed -i 's/^status = OK$/status = DONE/' "${CONTROL_FILE}"
if GS_PROBACKUP_PASSPHRASE=${NEW_KEY} "${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${FULL_ID}" >/dev/null 2>&1; then echo 'tampered backup.control unexpectedly accepted' >&2; exit 1; fi
mv "${CONTROL_FILE}.orig" "${CONTROL_FILE}"
GS_PROBACKUP_PASSPHRASE=${NEW_KEY} "${PROBACKUP}" validate -B "${CATALOG}" --instance=enc -i "${FULL_ID}"

# --- guard: option validation rejects unsupported combinations ---
if "${PROBACKUP}" backup -B "${CATALOG}" --instance=enc -b full -d "${TEST_DB}" -p "${TEST_PORT}" --stream --encrypt --encrypt-key-file="${KEY_FILE}" --encrypt-chunk-size=32kB >/dev/null 2>&1; then echo 'undersized chunk unexpectedly accepted' >&2; exit 1; fi
if "${PROBACKUP}" backup -B "${CATALOG}" --instance=enc -b full -d "${TEST_DB}" -p "${TEST_PORT}" --stream --encrypt --encrypt-key-file="${KEY_FILE}" --encrypt-algorithm=AES256 >/dev/null 2>&1; then echo 'unsupported algorithm unexpectedly accepted' >&2; exit 1; fi
if "${PROBACKUP}" backup -B "${CATALOG}" --instance=enc -b full -d "${TEST_DB}" -p "${TEST_PORT}" --stream --encrypt-key-file="${KEY_FILE}" >/dev/null 2>&1; then echo 'key without --encrypt unexpectedly accepted' >&2; exit 1; fi
if "${PROBACKUP}" restore -B "${CATALOG}" --instance=enc -i "${FULL_ID}" -D "${TEST_ROOT}/never" --encrypt >/dev/null 2>&1; then echo '--encrypt on restore unexpectedly accepted' >&2; exit 1; fi

# --- guard: encrypted backups can be deleted without any key material ---
env -u GS_PROBACKUP_PASSPHRASE "${PROBACKUP}" delete -B "${CATALOG}" --instance=enc -i "${INCREMENTAL_ID}"
if env -u GS_PROBACKUP_PASSPHRASE "${PROBACKUP}" show -B "${CATALOG}" --instance=enc --format=json | grep -q "${INCREMENTAL_ID}"; then
    echo 'deleted backup still listed' >&2; exit 1
fi
test ! -d "${CATALOG}/backups/enc/${INCREMENTAL_ID}"

echo 'gs_probackup streaming encryption test passed'
