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

import ctypes
import os

try:
    from ctypes import wintypes
except ImportError:
    raise AssertionError('BUG: Should not call here.')

ntdll = ctypes.WinDLL('ntdll')
kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)

# WIN32API Definitions
PROCESS_VM_READ = 0x0010
PROCESS_QUERY_INFORMATION = 0x0400
SYNCHRONIZE = 0x100000

ERROR_INVALID_HANDLE = 0x0006
ERROR_PARTIAL_COPY = 0x012B


to_pointer = ctypes.POINTER

PULONG = to_pointer(wintypes.ULONG)
ULONG_PTR = wintypes.LPVOID
SIZE_T = ctypes.c_size_t

kernel32.ReadProcessMemory.argtypes = (
    wintypes.HANDLE,
    wintypes.LPCVOID,
    wintypes.LPVOID,
    SIZE_T,
    to_pointer(SIZE_T))

kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)

kernel32.GetCurrentProcess.restype = wintypes.HANDLE
kernel32.GetCurrentProcess.argtypes = ()

kernel32.OpenProcess.restype = wintypes.HANDLE
kernel32.OpenProcess.argtypes = (
    wintypes.DWORD,
    wintypes.BOOL,
    wintypes.DWORD)

kernel32.WaitForSingleObject.restype = wintypes.DWORD
kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)

kernel32.GetExitCodeProcess.restype = wintypes.BOOL
kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, wintypes.LPDWORD)

# NTAPI Definitions
NTSTATUS = wintypes.LONG
PVOID = wintypes.LPVOID
ULONG = wintypes.ULONG
PROCESSINFOCLASS = wintypes.ULONG

ProcessBasicInformation = 0
ProcessDebugPort = 7
ProcessWow64Information = 26
ProcessImageFileName = 27
ProcessBreakOnTermination = 29

STATUS_UNSUCCESSFUL = NTSTATUS(0xC0000001)
STATUS_INFO_LENGTH_MISMATCH = NTSTATUS(0xC0000004).value
STATUS_INVALID_HANDLE = NTSTATUS(0xC0000008).value
STATUS_OBJECT_TYPE_MISMATCH = NTSTATUS(0xC0000024).value

WAIT_TIMEOUT = 0x00000102
STILL_ACTIVE = 259


class UNICODE_STRING(ctypes.Structure):
    _fields_ = (('Length', wintypes.USHORT),
                ('MaximumLength', wintypes.USHORT),
                ('Buffer', wintypes.PWCHAR))  # to_pointer(wintypes.WCHAR)


class LIST_ENTRY(ctypes.Structure):
    pass


RPLIST_ENTRY = to_pointer(LIST_ENTRY)

LIST_ENTRY._fields_ = (('Flink', RPLIST_ENTRY),
                       ('Blink', RPLIST_ENTRY))


class LDR_DATA_TABLE_ENTRY(ctypes.Structure):
    _fields_ = (('Reserved1', PVOID * 2),
                ('InMemoryOrderLinks', LIST_ENTRY),
                ('Reserved2', PVOID * 2),
                ('DllBase', PVOID),
                ('EntryPoint', PVOID),
                ('Reserved3', PVOID),
                ('FullDllName', UNICODE_STRING),
                ('Reserved4', wintypes.BYTE * 8),
                ('Reserved5', PVOID * 3),
                ('CheckSum', PVOID),
                ('TimeDateStamp', wintypes.ULONG))


RPLDR_DATA_TABLE_ENTRY = to_pointer(LDR_DATA_TABLE_ENTRY)


class PEB_LDR_DATA(ctypes.Structure):
    _fields_ = (('Reserved1', wintypes.BYTE * 8),
                ('Reserved2', PVOID * 3),
                ('InMemoryOrderModuleList', LIST_ENTRY))


RPPEB_LDR_DATA = to_pointer(PEB_LDR_DATA)


class RTL_USER_PROCESS_PARAMETERS(ctypes.Structure):
    _fields_ = (('MaximumLength', ULONG),
                ('Length', ULONG),
                ('Flags', ULONG),
                ('DebugFlags', ULONG),
                ('ConsoleHandle', PVOID),
                ('ConsoleFlags', ULONG),
                ('StdInputHandle', PVOID),
                ('StdOutputHandle', PVOID),
                ('StdErrorHandle', PVOID),
                ('CurrentDirectoryPath', UNICODE_STRING),
                ('CurrentDirectoryHandle', PVOID),
                ('DllPath', UNICODE_STRING),
                ('ImagePathName', UNICODE_STRING),
                ('CommandLine', UNICODE_STRING)
                # ...
                )


RPRTL_USER_PROCESS_PARAMETERS = to_pointer(RTL_USER_PROCESS_PARAMETERS)
PPS_POST_PROCESS_INIT_ROUTINE = PVOID


class PEB(ctypes.Structure):
    _fields_ = (('Reserved1', wintypes.BYTE * 2),
                ('BeingDebugged', wintypes.BYTE),
                ('Reserved2', wintypes.BYTE * 1),
                ('Reserved3', PVOID * 2),
                ('Ldr', RPPEB_LDR_DATA),
                ('ProcessParameters', RPRTL_USER_PROCESS_PARAMETERS),
                ('Reserved4', wintypes.BYTE * 104),
                ('Reserved5', PVOID * 52),
                ('PostProcessInitRoutine', PPS_POST_PROCESS_INIT_ROUTINE),
                ('Reserved6', wintypes.BYTE * 128),
                ('Reserved7', PVOID * 1),
                ('SessionId', wintypes.ULONG))


RPPEB = to_pointer(PEB)


class PROCESS_BASIC_INFORMATION(ctypes.Structure):
    _fields_ = (('Reserved1', PVOID),
                ('PebBaseAddress', RPPEB),
                ('Reserved2', PVOID * 2),
                ('UniqueProcessId', ULONG_PTR),
                ('Reserved3', PVOID))


ntdll.NtQueryInformationProcess.restype = NTSTATUS
ntdll.NtQueryInformationProcess.argtypes = (
    wintypes.HANDLE,
    PROCESSINFOCLASS,
    PVOID,
    wintypes.ULONG,
    PULONG)


def _win32_get_user_process_params(handle):
    info = PROCESS_BASIC_INFORMATION()
    status = ntdll.NtQueryInformationProcess(handle,
                                             ProcessBasicInformation,
                                             ctypes.byref(info),
                                             ctypes.sizeof(info),
                                             None)
    if status < 0:
        raise OSError

    peb = PEB()
    address = PVOID.from_buffer(info.PebBaseAddress).value
    kernel32.ReadProcessMemory(handle,
                               address,
                               ctypes.byref(peb),
                               ctypes.sizeof(peb),
                               None)

    params = RTL_USER_PROCESS_PARAMETERS()
    n_read = SIZE_T()
    address = PVOID.from_buffer(peb.ProcessParameters).value
    kernel32.ReadProcessMemory(handle,
                               address,
                               ctypes.byref(params),
                               ctypes.sizeof(params),
                               ctypes.byref(n_read))

    return params


def win32_get_process_cwd(pid):
    """Implement the func with pure WIN32 API.

    Reference:

    - https://stackoverflow.com/questions/14018280/how-to-get-a-process-working-dir-on-windows
    - https://stackoverflow.com/questions/35106511/how-to-access-the-peb-of-another-process-with-python-ctypes

    :param pid: Process ID
    :return: return process's current working directory
    """
    handle = kernel32.OpenProcess(PROCESS_VM_READ |
                                  PROCESS_QUERY_INFORMATION,
                                  False, pid)

    upp = _win32_get_user_process_params(handle)
    path = (wintypes.WCHAR * (upp.CurrentDirectoryPath.Length // 2 + 1))()
    kernel32.ReadProcessMemory(handle,
                               upp.CurrentDirectoryPath.Buffer,
                               ctypes.byref(path),
                               upp.CurrentDirectoryPath.Length,
                               None)
    kernel32.CloseHandle(handle)
    return path.value


def win32_get_process_cmdline(pid):
    handle = kernel32.OpenProcess(PROCESS_VM_READ |
                                  PROCESS_QUERY_INFORMATION,
                                  False, pid)

    params = _win32_get_user_process_params(handle)
    path = (wintypes.WCHAR * (params.CommandLine.Length // 2 + 1))()
    kernel32.ReadProcessMemory(handle,
                               params.CommandLine.Buffer,
                               ctypes.byref(path),
                               params.CommandLine.Length,
                               None)
    kernel32.CloseHandle(handle)
    return path.value


def win32_get_process_path(pid):
    handle = kernel32.OpenProcess(PROCESS_VM_READ |
                                  PROCESS_QUERY_INFORMATION,
                                  False, pid)

    upp = _win32_get_user_process_params(handle)
    path = (wintypes.WCHAR * (upp.ImagePathName.Length // 2 + 1))()
    kernel32.ReadProcessMemory(handle,
                               upp.ImagePathName.Buffer,
                               ctypes.byref(path),
                               upp.ImagePathName.Length,
                               None)
    kernel32.CloseHandle(handle)
    return path.value


def win32_is_process_running(pid):
    if pid in (os.getppid(), os.getpid()):
        return True

    handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION,
                                  False, pid)
    exit_code = wintypes.DWORD()
    kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
    kernel32.CloseHandle(handle)
    return exit_code.value == STILL_ACTIVE

