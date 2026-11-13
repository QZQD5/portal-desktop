"""Build the headless Python runtime consumed by Portal Desktop."""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    if sys.platform not in {"win32", "darwin"}:
        raise SystemExit("This build supports Windows and macOS only")
    subprocess.run([
        sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean", "--onedir", "--console",
        "--name", "AudioAgentGateway", "--distpath", str(ROOT / "scripts/dist"),
        "--workpath", str(ROOT / "scripts/build/desktop"), "--specpath", str(ROOT / "scripts/build"),
        "--paths", str(ROOT), "--add-data", str(ROOT / "gateway/resources") + os.pathsep + "gateway/resources",
        "--collect-submodules", "uvicorn", "--exclude-module", "tkinter", "--exclude-module", "pytest",
        "--exclude-module", "playwright", str(ROOT / "scripts/desktop_entry.py"),
    ], cwd=ROOT, check=True)
    print("Headless runtime built; no WebUI, DSH or credentials included.")


if __name__ == "__main__":
    main()
