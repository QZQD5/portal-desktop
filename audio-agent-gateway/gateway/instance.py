"""Exclusive instance lease for a Desktop scene."""

import ctypes
import json
import os
from pathlib import Path


from .store import iso_now, uid


def alive(pid):
    if type(pid) is not int or pid <= 0:
        return False
    if os.name == "nt":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong)]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            return ctypes.get_last_error() == 5
        try:
            code = ctypes.c_ulong()
            return not kernel.GetExitCodeProcess(handle, ctypes.byref(code)) or code.value == 259
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except ProcessLookupError:
        return False


def read_lease(directory):
    try:
        value = json.loads((Path(directory) / "gateway.lock").read_text(encoding="utf-8"))
        if value.get("schema") == "qwaudio.gateway-lock/v1" and isinstance(value.get("instanceId"), str):
            return value
    except (OSError, ValueError, AttributeError):
        pass
    return None


class Lease:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.path = self.directory / "gateway.lock"
        self.record = {
            "schema": "qwaudio.gateway-lock/v1",
            "instanceId": uid("gateway"),
            "pid": os.getpid(),
            "owner": os.getenv("AUDIO_GATEWAY_OWNER", "gateway"),
            "state": "starting",
            "origin": "",
            "startedAt": iso_now(),
            "heartbeatAt": iso_now(),
        }
        self.guard = None

    def acquire(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.guard = (self.directory / "gateway.guard").open("a+b")
        self.guard.seek(0)
        if self.guard.read(1) == b"":
            self.guard.write(b"\0")
            self.guard.flush()
        self.guard.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.guard.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.guard.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            existing = read_lease(self.directory)
            if existing and alive(existing.get("pid")):
                raise RuntimeError("Gateway already running: " + str(existing.get("origin", "")))
            self.write()
            return self
        except Exception:
            self.guard.close()
            self.guard = None
            raise

    def write(self):
        temporary = self.path.with_name("gateway." + self.record["instanceId"] + ".tmp")
        try:
            temporary.write_text(json.dumps(self.record) + "\n", encoding="utf-8")
            temporary.replace(self.path)
        finally:
            temporary.unlink(missing_ok=True)

    def update(self, **fields):
        current = read_lease(self.directory)
        if not self.guard or not current or current["instanceId"] != self.record["instanceId"]:
            return False
        self.record.update(fields, heartbeatAt=iso_now())
        self.write()
        return True

    def close(self):
        current = read_lease(self.directory)
        if self.guard:
            if current and current["instanceId"] == self.record["instanceId"]:
                self.path.unlink(missing_ok=True)
            self.guard.close()
            self.guard = None
