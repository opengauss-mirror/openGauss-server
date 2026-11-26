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

"""Query SM4 KEK slots and create/import protected backups for openGauss TDE."""

import argparse
import base64
import ctypes
import ctypes.util
import getpass
import hashlib
import json
import os
import pwd
import secrets
import stat
import sys
import tempfile


FORMAT_NAME = "openGauss-tde-sdf-kek"
FORMAT_VERSION = 3
# Values and index encoding from the BoostKit SDF interface: SDF_SM4 << 20 | slot.
SDF_SM4 = 0x00000003
KEY_TYPE_SHIFT = 20
MAX_KEY_INDEX = 1000
SM4_CBC = 0x00000402
SM4_CTR = 0x00000420
MAX_KEY_FILE_SIZE = 1024 * 1024
MAX_MANIFEST_SIZE = 2 * 1024 * 1024
SALT_LENGTH = 16
U8_PTR = ctypes.POINTER(ctypes.c_ubyte)
PASSWORD_MAGIC = b"OGSDFP01"
PASSWORD_NONCE_LEN = 12
PASSWORD_TAG_LEN = 16
PASSWORD_KEY_LEN = 32
PASSWORD_OVERHEAD = len(PASSWORD_MAGIC) + PASSWORD_NONCE_LEN + PASSWORD_TAG_LEN


def crypt_password(content, key, decrypt=False):
    """OpenSSL AES-256-GCM; format: magic || nonce || ciphertext || tag."""
    if len(key) != PASSWORD_KEY_LEN:
        raise TdeSdfError("runtime password key must contain exactly 32 bytes")
    if decrypt:
        if not PASSWORD_OVERHEAD + 8 <= len(content) <= PASSWORD_OVERHEAD + 128 or content[:8] != PASSWORD_MAGIC:
            raise TdeSdfError("invalid encrypted runtime password; plaintext files are not supported")
        nonce, data, tag = bytes(content[8:20]), content[20:-16], bytes(content[-16:])
    else:
        if not 8 <= len(content) <= 128 or any(c in content for c in (0, 10, 13)):
            raise TdeSdfError("SDF KEK password must be 8 to 128 bytes without NUL or newline")
        nonce, data, tag = secrets.token_bytes(PASSWORD_NONCE_LEN), content, None
    try:
        lib = ctypes.CDLL(ctypes.util.find_library("crypto") or "libcrypto.so")
        def bind(name, result, arguments):
            func = getattr(lib, name)
            func.restype, func.argtypes = result, arguments
            return func
        ptr, integer = ctypes.c_void_p, ctypes.c_int
        new = bind("EVP_CIPHER_CTX_new", ptr, [])
        free = bind("EVP_CIPHER_CTX_free", None, [ptr])
        cipher = bind("EVP_aes_256_gcm", ptr, [])
        mode = "Decrypt" if decrypt else "Encrypt"
        init = bind("EVP_" + mode + "Init_ex", integer, [ptr, ptr, ptr, ptr, ptr])
        update = bind("EVP_" + mode + "Update", integer,
                      [ptr, ptr, ctypes.POINTER(integer), ptr, integer])
        final = bind("EVP_" + mode + "Final_ex", integer, [ptr, ptr, ctypes.POINTER(integer)])
        ctrl = bind("EVP_CIPHER_CTX_ctrl", integer, [ptr, integer, integer, ptr])
    except (OSError, AttributeError) as exc:
        raise TdeSdfError("cannot load OpenSSL password encryption functions: {}".format(exc))
    key_buffer = (ctypes.c_ubyte * len(key)).from_buffer_copy(key)
    data_buffer = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
    output = ctypes.create_string_buffer(len(data) + 16)
    count, tail = integer(), integer()
    ctx = new()
    try:
        if not ctx:
            raise TdeSdfError("cannot allocate OpenSSL password cipher context")
        if init(ctx, cipher(), None, key_buffer, nonce) != 1 or \
                update(ctx, None, ctypes.byref(count), PASSWORD_MAGIC, len(PASSWORD_MAGIC)) != 1 or \
                update(ctx, output, ctypes.byref(count), data_buffer, len(data)) != 1:
            raise TdeSdfError("runtime password cipher initialization/update failed")
        if decrypt:
            tag_buffer = ctypes.create_string_buffer(tag)
            if ctrl(ctx, 0x11, PASSWORD_TAG_LEN, tag_buffer) != 1:  # EVP_CTRL_GCM_SET_TAG
                raise TdeSdfError("cannot set runtime password authentication tag")
        if final(ctx, ctypes.byref(output, count.value), ctypes.byref(tail)) != 1:
            raise TdeSdfError("runtime password authentication failed (wrong key or damaged file)")
        result = bytearray(output.raw[:count.value + tail.value])
        if decrypt:
            return result
        tag_buffer = ctypes.create_string_buffer(PASSWORD_TAG_LEN)
        if ctrl(ctx, 0x10, PASSWORD_TAG_LEN, tag_buffer) != 1:  # EVP_CTRL_GCM_GET_TAG
            raise TdeSdfError("cannot obtain runtime password authentication tag")
        return PASSWORD_MAGIC + nonce + bytes(result) + tag_buffer.raw
    finally:
        if ctx:
            free(ctx)
        ctypes.memset(key_buffer, 0, len(key_buffer))
        ctypes.memset(data_buffer, 0, len(data_buffer))
        ctypes.memset(output, 0, len(output))


def save_runtime_password(path, password, database_user):
    if path is None:
        return
    key_path = path + ".key"
    if len(os.fsencode(key_path)) >= 4096:
        raise TdeSdfError("runtime password companion key path must be shorter than 4096 bytes")
    try:
        owner = pwd.getpwnam(database_user).pw_uid
    except KeyError:
        raise TdeSdfError("database OS user does not exist: {}".format(database_user))
    key = plain = None
    try:
        if os.path.lexists(path) or os.path.lexists(key_path):
            if not os.path.lexists(path) or not os.path.lexists(key_path):
                raise TdeSdfError("runtime password credential pair is incomplete; inspect {} and {}".format(
                    path, key_path))
            key = bytearray(read_secure_file(key_path, PASSWORD_KEY_LEN, 0o077, "runtime password key", owner))
            cipher = read_secure_file(path, PASSWORD_OVERHEAD + 128, 0o077, "encrypted runtime password", owner)
            plain = crypt_password(cipher, key, decrypt=True)
            if not secrets.compare_digest(plain, password):
                raise TdeSdfError("runtime password must match the existing encrypted credential")
            return
        key = bytearray(secrets.token_bytes(PASSWORD_KEY_LEN))
        cipher = crypt_password(password, key)
        save_private_file(key_path, key, database_user, "runtime password key")
        # Publish ciphertext last. Never remove a key after uncertain publication:
        # it might already protect a durable ciphertext file.
        try:
            save_private_file(path, cipher, database_user, "encrypted runtime password")
        except BaseException:
            print("Warning: credential publication failed; inspect {} and {} before retrying.".format(
                path, key_path), file=sys.stderr)
            raise
    finally:
        wipe_password(key)
        wipe_password(plain)


class TdeSdfError(Exception):
    pass


class ManifestPublishedError(TdeSdfError):
    pass


class SdfApi:
    def __init__(self, library_path):
        try:
            self.lib = ctypes.CDLL(library_path)
        except OSError as exc:
            raise TdeSdfError("cannot load SDF library {}: {}".format(library_path, exc))

        self.open_device = self._function("SDF_OpenDevice", [ctypes.POINTER(ctypes.c_void_p)])
        self.close_device = self._function("SDF_CloseDevice", [ctypes.c_void_p])
        self.open_session = self._function(
            "ECM_OpenSession", [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)])
        self.close_session = self._function("SDF_CloseSession", [ctypes.c_void_p])
        self.generate_random = self._function(
            "SDF_GenerateRandom", [ctypes.c_void_p, ctypes.c_uint32, U8_PTR])
        self.generate_key = self._function(
            "ECM_GenerateKey", [ctypes.c_void_p, ctypes.c_uint32, U8_PTR, ctypes.c_uint32])
        self.query_key = self._function(
            "ECM_QueryKey", [ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32)])
        self.export_key = self._function(
            "ECM_ExportKey", [ctypes.c_void_p, ctypes.c_uint32, U8_PTR,
                              ctypes.POINTER(ctypes.c_uint32), U8_PTR])
        self.import_key = self._function(
            "ECM_ImportKey", [ctypes.c_void_p, ctypes.c_uint32, U8_PTR,
                              ctypes.c_uint32, U8_PTR])
        self.delete_key = self._function("ECM_DeleteKey", [ctypes.c_void_p, ctypes.c_uint32])
        self.get_kek_access_right = self._function(
            "SDF_GetKEKAccessRight", [ctypes.c_void_p, ctypes.c_uint32, U8_PTR, ctypes.c_uint32])
        self.generate_key_with_kek = self._function(
            "SDF_GenerateKeyWithKEK", [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32,
                                       ctypes.c_uint32, U8_PTR, ctypes.POINTER(ctypes.c_uint32),
                                       ctypes.POINTER(ctypes.c_void_p)])
        self.import_key_with_kek = self._function(
            "SDF_ImportKeyWithKEK", [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32,
                                     U8_PTR, ctypes.c_uint32, ctypes.POINTER(ctypes.c_void_p)])
        cipher_args = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32, U8_PTR,
                       U8_PTR, ctypes.c_uint32, U8_PTR, ctypes.POINTER(ctypes.c_uint32)]
        self.encrypt = self._function("SDF_Encrypt", cipher_args)
        self.decrypt = self._function("SDF_Decrypt", cipher_args)
        self.destroy_key = self._function("SDF_DestroyKey", [ctypes.c_void_p, ctypes.c_void_p])

    def _function(self, name, argtypes):
        try:
            function = getattr(self.lib, name)
        except AttributeError:
            raise TdeSdfError("SDF library does not export {}".format(name))
        function.restype = ctypes.c_int
        function.argtypes = argtypes
        return function

    def open(self):
        device = ctypes.c_void_p()
        self._check(self.open_device(ctypes.byref(device)), "SDF_OpenDevice")
        if not device.value:
            raise TdeSdfError("SDF_OpenDevice returned success without a device handle")
        session = ctypes.c_void_p()
        try:
            self._check(self.open_session(device, ctypes.byref(session)), "ECM_OpenSession")
            if not session.value:
                raise TdeSdfError("ECM_OpenSession returned success without a session handle")
        except Exception:
            self.close_device(device)
            raise
        return device, session

    def close(self, device, session):
        if session:
            self.close_session(session)
        if device:
            self.close_device(device)

    @staticmethod
    def _check(code, operation):
        if code != 0:
            raise TdeSdfError("{} failed with SDF error 0x{:08x}".format(operation, code & 0xffffffff))


def require_root():
    if not hasattr(os, "geteuid") or os.geteuid() != 0:
        raise TdeSdfError("BoostKit ECM key-management operations must run as root")


def key_index_value(key_index):
    if key_index < 1 or key_index > MAX_KEY_INDEX:
        raise TdeSdfError("key index must be between 1 and {}".format(MAX_KEY_INDEX))
    return (SDF_SM4 << KEY_TYPE_SHIFT) | key_index


def key_exists(api, session, encoded_index):
    key_info = ctypes.c_uint32()
    api._check(api.query_key(session, encoded_index, ctypes.byref(key_info)), "ECM_QueryKey")
    return key_info.value == 0


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def create_probe(api, session, key_index):
    """A node must recover the source KEK's session key to pass this probe."""
    handle = ctypes.c_void_p()
    wrapped = (ctypes.c_ubyte * 256)()
    length = ctypes.c_uint32(len(wrapped))
    plain = (ctypes.c_ubyte * 32)()
    iv = (ctypes.c_ubyte * 16)()
    cipher = (ctypes.c_ubyte * 32)()
    try:
        api._check(api.generate_key_with_kek(session, 128, SM4_CBC, key_index, wrapped,
                                            ctypes.byref(length), ctypes.byref(handle)),
                   "SDF_GenerateKeyWithKEK(verification)")
        if not handle.value or not 0 < length.value <= len(wrapped):
            raise TdeSdfError("invalid SDF KEK verification key")
        wrapped_bytes = bytes(wrapped[:length.value])
        api._check(api.generate_random(session, len(plain), plain), "SDF_GenerateRandom(verification)")
        api._check(api.generate_random(session, len(iv), iv), "SDF_GenerateRandom(verification IV)")
        iv_bytes = bytes(iv)
        length = ctypes.c_uint32(len(cipher))
        api._check(api.encrypt(session, handle, SM4_CTR, iv, plain, len(plain), cipher,
                              ctypes.byref(length)), "SDF_Encrypt(verification)")
        if length.value != len(cipher):
            raise TdeSdfError("invalid KEK verification ciphertext length")
        return {"wrapped_key": base64.b64encode(wrapped_bytes).decode("ascii"),
                "iv": base64.b64encode(iv_bytes).decode("ascii"),
                "ciphertext": base64.b64encode(bytes(cipher)).decode("ascii"),
                "plaintext_sha256": hashlib.sha256(bytes(plain)).hexdigest()}
    finally:
        ctypes.memset(plain, 0, ctypes.sizeof(plain))
        if handle.value:
            api._check(api.destroy_key(session, handle), "SDF_DestroyKey(verification)")


def probe_fields(probe):
    try:
        wrapped = base64.b64decode(probe["wrapped_key"], validate=True)
        iv = base64.b64decode(probe["iv"], validate=True)
        cipher = base64.b64decode(probe["ciphertext"], validate=True)
        digest = probe["plaintext_sha256"]
        if not 0 < len(wrapped) <= 256 or len(iv) != 16 or len(cipher) != 32 or \
                not isinstance(digest, str) or len(digest) != 64 or len(bytes.fromhex(digest)) != 32:
            raise ValueError("invalid field size")
        return wrapped, iv, cipher, digest
    except (KeyError, TypeError, ValueError) as exc:
        raise TdeSdfError("invalid KEK verification record: {}".format(exc))


def verify_probe(api, session, key_index, probe):
    wrapped, iv, cipher, digest = probe_fields(probe)
    handle = ctypes.c_void_p()
    plain = (ctypes.c_ubyte * len(cipher))()
    length = ctypes.c_uint32(len(plain))
    try:
        api._check(api.import_key_with_kek(session, SM4_CBC, key_index,
                   (ctypes.c_ubyte * len(wrapped)).from_buffer_copy(wrapped), len(wrapped),
                   ctypes.byref(handle)), "SDF_ImportKeyWithKEK(verification)")
        if not handle.value:
            raise TdeSdfError("SDF verification returned a null key handle")
        api._check(api.decrypt(session, handle, SM4_CTR,
                   (ctypes.c_ubyte * len(iv)).from_buffer_copy(iv),
                   (ctypes.c_ubyte * len(cipher)).from_buffer_copy(cipher), len(cipher), plain,
                   ctypes.byref(length)), "SDF_Decrypt(verification)")
        if length.value != len(plain) or not secrets.compare_digest(hashlib.sha256(bytes(plain)).hexdigest(), digest):
            raise TdeSdfError("local KEK does not match the protected backup verification record")
    finally:
        ctypes.memset(plain, 0, ctypes.sizeof(plain))
        if handle.value:
            api._check(api.destroy_key(session, handle), "SDF_DestroyKey(verification)")


def create_manifest(key_index, salt, backup, probe):
    payload = {
        "format": FORMAT_NAME,
        "version": FORMAT_VERSION,
        "key_type": "SM4",
        "key_index": key_index,
        "salt": salt.decode("ascii"),
        "backup_ciphertext": base64.b64encode(backup).decode("ascii"),
        "verification": probe,
    }
    payload["sha256"] = hashlib.sha256(canonical_json(payload)).hexdigest()
    return json.dumps(payload, sort_keys=True, indent=2).encode("ascii") + b"\n"


def write_manifest_once(path, contents, publication):
    parent = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(parent):
        raise TdeSdfError("backup directory does not exist: {}".format(parent))

    fd, temporary_path = tempfile.mkstemp(prefix=".gs_tde_sdf_", dir=parent)
    published = False
    try:
        with os.fdopen(fd, "wb") as backup_file:
            os.fchmod(backup_file.fileno(), 0o600)
            backup_file.write(contents)
            backup_file.flush()
            os.fsync(backup_file.fileno())
        # Set caller-owned state before link: interruption can occur after the
        # filesystem publishes the name but before Python records success.
        publication["attempted"] = True
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            raise TdeSdfError("backup file already exists; refusing to replace it")
        published = True
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            try:
                os.fsync(directory_fd)
            except OSError as exc:
                raise ManifestPublishedError(
                    "backup was published but its directory could not be synced: {}".format(exc))
        finally:
            os.close(directory_fd)
    except OSError as exc:
        if published:
            raise ManifestPublishedError(
                "backup was published but durability could not be confirmed: {}".format(exc))
        raise
    finally:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            if published:
                raise ManifestPublishedError(
                    "backup was published but temporary-file cleanup failed: {}".format(exc))
            raise


def read_secure_file(path, limit, forbidden_mode, label, owner=None):
    """Read a bounded regular file through the descriptor whose mode was checked."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) & forbidden_mode or \
                (owner is not None and info.st_uid != owner):
            raise TdeSdfError("{} has invalid file type, ownership or permissions".format(label))
        if info.st_size < 0 or info.st_size > limit:
            raise TdeSdfError("{} is too large".format(label))
        chunks = []
        remaining = limit + 1
        while remaining > 0:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        if remaining == 0:
            raise TdeSdfError("{} is too large".format(label))
        return b"".join(chunks)
    finally:
        os.close(fd)


def read_manifest(path):
    try:
        raw = read_secure_file(path, MAX_MANIFEST_SIZE, 0o077, "backup")
    except OSError as exc:
        raise TdeSdfError("cannot read backup file: {}".format(exc))

    try:
        payload = json.loads(raw.decode("ascii"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise TdeSdfError("backup file is not valid JSON: {}".format(exc))

    if not isinstance(payload, dict) or payload.get("format") != FORMAT_NAME or \
            payload.get("version") != FORMAT_VERSION or payload.get("key_type") != "SM4":
        raise TdeSdfError("unsupported SDF KEK backup format")
    try:
        if type(payload["key_index"]) is not int:
            raise ValueError("key index must be an integer")
        key_index = payload["key_index"]
        encoded_index = key_index_value(key_index)
        if not isinstance(payload["salt"], str) or not isinstance(payload["sha256"], str):
            raise ValueError("salt and checksum must be strings")
        salt = payload["salt"].encode("ascii")
        if len(salt) != SALT_LENGTH:
            raise ValueError("wrong salt length")
        backup = base64.b64decode(payload["backup_ciphertext"], validate=True)
        expected_digest = payload["sha256"]
        if len(expected_digest) != 64 or len(bytes.fromhex(expected_digest)) != 32:
            raise ValueError("checksum must contain 64 hexadecimal characters")
    except (KeyError, TypeError, ValueError, UnicodeEncodeError) as exc:
        raise TdeSdfError("invalid SDF KEK backup metadata: {}".format(exc))

    if not backup or len(backup) > MAX_KEY_FILE_SIZE:
        raise TdeSdfError("encrypted key backup size is invalid")
    checksum_payload = dict(payload)
    del checksum_payload["sha256"]
    actual_digest = hashlib.sha256(canonical_json(checksum_payload)).hexdigest()
    if not secrets.compare_digest(expected_digest, actual_digest):
        raise TdeSdfError("backup checksum mismatch")
    probe = payload.get("verification")
    probe_fields(probe)
    return key_index, encoded_index, salt, backup, probe


def wipe_password(password):
    if password is not None:
        for i in range(len(password)):
            password[i] = 0


def prompt_password(confirm=True):
    prompt = "New SDF KEK password: " if confirm else "SDF KEK password: "
    password = bytearray(getpass.getpass(prompt).encode("utf-8"))
    try:
        if not 8 <= len(password) <= 128 or any(c in password for c in (0, 10, 13)):
            raise TdeSdfError("SDF KEK password must be 8 to 128 bytes without NUL or newline")
        if confirm:
            confirmation = bytearray(getpass.getpass("Confirm SDF KEK password: ").encode("utf-8"))
            try:
                if password != confirmation:
                    raise TdeSdfError("passwords do not match")
            finally:
                wipe_password(confirmation)
        return password
    except BaseException:
        wipe_password(password)
        raise


def save_private_file(path, content, database_user, description):
    if path is None:
        return
    parent = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(parent):
        raise TdeSdfError("{} directory does not exist: {}".format(description, parent))
    try:
        user = pwd.getpwnam(database_user) if database_user else None
    except KeyError:
        raise TdeSdfError("database OS user does not exist: {}".format(database_user))
    expected_uid = user.pw_uid if user else os.geteuid()
    if os.path.lexists(path):
        try:
            existing = read_secure_file(path, len(content), 0o077, description, expected_uid)
        except OSError as exc:
            raise TdeSdfError("cannot read {}: {}".format(description, exc))
        if existing != content:
            raise TdeSdfError("{} must match the requested content".format(description))
        return

    fd, temporary_path = tempfile.mkstemp(prefix=".gs_tde_sdf_secret_", dir=parent)
    try:
        with os.fdopen(fd, "wb") as secret_file:
            if user:
                os.fchown(secret_file.fileno(), user.pw_uid, user.pw_gid)
            os.fchmod(secret_file.fileno(), 0o600)
            secret_file.write(content)
            secret_file.flush()
            os.fsync(secret_file.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            raise TdeSdfError("{} already exists; refusing to replace it".format(description))
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass


def save_runtime_config(args, key_index, password):
    save_runtime_password(args.runtime_password_file, password, args.database_user)
    if args.emit_config:
        config = {"kek_index": key_index, "library_path": args.library,
                  "password_file": args.runtime_password_file}
        content = (json.dumps(config, indent=2, sort_keys=True) + "\n").encode("utf-8")
        if len(content) > 4096:
            raise TdeSdfError("SDF configuration exceeds 4096 bytes")
        save_private_file(args.emit_config, content, args.database_user, "SDF configuration file")
        print("Local SDF configuration file={}".format(args.emit_config))


def query_key(args):
    require_root()
    encoded_index = key_index_value(args.key_index)
    api = SdfApi(args.library)
    device, session = api.open()
    try:
        occupied = key_exists(api, session, encoded_index)
        print("Local SM4 KEK slot {}: {}.".format(
            args.key_index, "occupied (key already exists)" if occupied else "empty (available)"))
        if occupied:
            print("create-backup cannot use this slot. To verify an existing KEK against a backup, "
                  "use import-backup; slot occupancy alone does not establish key equality.")
    finally:
        api.close(device, session)


def create_backup(args):
    require_root()
    if os.path.lexists(args.backup_file):
        raise TdeSdfError("backup file already exists; refusing to replace it")

    encoded_index = key_index_value(args.key_index)
    api = SdfApi(args.library)
    device, session = api.open()
    generated = False
    publication = {"attempted": False}
    password = None
    try:
        if key_exists(api, session, encoded_index):
            raise TdeSdfError("SM4 KEK slot {} is occupied; choose an unused slot, or use import-backup "
                              "to verify an existing KEK against its backup. No key was changed".format(
                args.key_index))
        password = prompt_password()
        password_buffer = (ctypes.c_ubyte * len(password)).from_buffer(password)
        api._check(api.generate_key(session, encoded_index, password_buffer, len(password)), "ECM_GenerateKey")
        generated = True
        api._check(api.get_kek_access_right(session, args.key_index, password_buffer, len(password)),
                   "SDF_GetKEKAccessRight")

        random_salt = ctypes.create_string_buffer(12)
        api._check(api.generate_random(session, 12,
                                      ctypes.cast(random_salt, U8_PTR)), "SDF_GenerateRandom")
        salt = base64.urlsafe_b64encode(random_salt.raw[:12])
        salt_buffer = ctypes.create_string_buffer(salt)
        key_buffer = ctypes.create_string_buffer(MAX_KEY_FILE_SIZE)
        key_length = ctypes.c_uint32(MAX_KEY_FILE_SIZE)
        api._check(api.export_key(session, encoded_index, ctypes.cast(key_buffer, U8_PTR),
                                  ctypes.byref(key_length), ctypes.cast(salt_buffer, U8_PTR)),
                   "ECM_ExportKey")
        if key_length.value == 0 or key_length.value > MAX_KEY_FILE_SIZE:
            raise TdeSdfError("ECM_ExportKey returned an invalid backup length")

        probe = create_probe(api, session, args.key_index)
        manifest = create_manifest(args.key_index, salt, key_buffer.raw[:key_length.value], probe)
        write_manifest_once(args.backup_file, manifest, publication)
        generated = False
        save_runtime_config(args, args.key_index, password)
        print("Created protected SDF SM4 KEK backup.")
        print("SDF key index={}".format(args.key_index))
        print("Backup file={}".format(args.backup_file))
        if args.runtime_password_file:
            print("Local runtime password file={}".format(args.runtime_password_file))
        print("Import this backup into the same slot on every eligible node before enabling SDF TDE.")
    finally:
        wipe_password(password)
        if generated and not publication["attempted"]:
            delete_ret = api.delete_key(session, encoded_index)
            if delete_ret != 0:
                print("Warning: KEK creation succeeded but no durable backup was recorded, and deleting "
                      "the KEK failed with SDF error 0x{:08x}; inspect the SDF key slot.".format(
                          delete_ret & 0xffffffff), file=sys.stderr)
        elif generated:
            print("Warning: KEK retained because backup publication was attempted; "
                  "inspect the backup file and SDF key index {} before retrying.".format(args.key_index),
                  file=sys.stderr)
        api.close(device, session)


def import_backup(args):
    require_root()
    key_index, encoded_index, salt, backup, probe = read_manifest(args.backup_file)
    api = SdfApi(args.library)
    device, session = api.open()
    password = None
    imported = False
    try:
        if not key_exists(api, session, encoded_index):
            backup_buffer = ctypes.create_string_buffer(backup)
            salt_buffer = ctypes.create_string_buffer(salt)
            api._check(api.import_key(session, encoded_index, ctypes.cast(backup_buffer, U8_PTR),
                                      len(backup), ctypes.cast(salt_buffer, U8_PTR)), "ECM_ImportKey")
            imported = True
        if not key_exists(api, session, encoded_index):
            raise TdeSdfError("ECM_ImportKey returned success but the KEK is not present afterward")
        password = prompt_password(confirm=False)
        password_buffer = (ctypes.c_ubyte * len(password)).from_buffer(password)
        api._check(api.get_kek_access_right(session, key_index, password_buffer, len(password)),
                   "SDF_GetKEKAccessRight")
        verify_probe(api, session, key_index, probe)
        # The verified KEK is provisioned; an optional password-file failure must not delete it.
        imported = False
        save_runtime_config(args, key_index, password)
        print("Verified local SM4 KEK against the source-node verification record.")
        print("SDF key index={}".format(key_index))
        if args.runtime_password_file:
            print("Local runtime password file={}".format(args.runtime_password_file))
    finally:
        if imported:
            delete_ret = api.delete_key(session, encoded_index)
            if delete_ret != 0:
                print("Warning: import setup failed and removing SDF key index {} also failed with "
                      "0x{:08x}; inspect the key slot.".format(key_index, delete_ret & 0xffffffff),
                      file=sys.stderr)
        wipe_password(password)
        api.close(device, session)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Query SM4 KEK slots or create/import protected backups used by openGauss TDE.")
    parser.add_argument("--library", default=os.environ.get("SDF_LIBRARY", "/usr/lib64/libsdf.so"),
                        help="BoostKit libsdf.so path (default: %(default)s)")
    commands = parser.add_subparsers(dest="command", required=True)

    query = commands.add_parser("query-key", help="check whether a local SM4 KEK slot is occupied")
    query.add_argument("--key-index", type=int, required=True,
                       help="SDF key slot number from 1 through 1000")
    query.set_defaults(handler=query_key, runtime_password_file=None, database_user=None, emit_config=None)

    create = commands.add_parser("create-backup", help="create one SM4 KEK and export its protected backup")
    create.add_argument("--key-index", type=int, required=True,
                        help="unused SDF key slot number (the SM4 type is added automatically)")
    create.add_argument("--backup-file", required=True,
                        help="new protected backup file; distribute it securely to the other nodes")
    create.add_argument("--runtime-password-file",
                        help="save an encrypted runtime password and companion .key (requires --database-user)")
    create.add_argument("--database-user",
                        help="owner of the optional runtime password file")
    create.set_defaults(handler=create_backup)

    imp = commands.add_parser("import-backup", help="import a protected KEK backup into this node's SDF")
    imp.add_argument("--backup-file", required=True,
                     help="protected backup file created by create-backup, imported into its original slot")
    imp.add_argument("--runtime-password-file",
                     help="save an encrypted runtime password and companion .key (requires --database-user)")
    imp.add_argument("--database-user",
                     help="owner of the optional runtime password file")
    imp.set_defaults(handler=import_backup)
    for command in (create, imp):
        command.add_argument("--emit-config", metavar="PATH",
                             help="write a private sdf_info.json (requires --runtime-password-file)")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.runtime_password_file and not args.database_user:
        parser.error("--runtime-password-file requires --database-user")
    if args.database_user and not args.runtime_password_file:
        parser.error("--database-user requires --runtime-password-file")
    if args.emit_config:
        if not args.runtime_password_file:
            parser.error("--emit-config requires --runtime-password-file and --database-user")
        for name, path in (("--library", args.library), ("--runtime-password-file", args.runtime_password_file)):
            if not os.path.isabs(path) or len(os.fsencode(path)) >= 4096:
                parser.error("{} must be an absolute path shorter than 4096 bytes".format(name))
    try:
        args.handler(args)
    except TdeSdfError as exc:
        print("gs_tde_sdf: {}".format(exc), file=sys.stderr)
        return 1
    except OSError as exc:
        print("gs_tde_sdf: {}".format(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
