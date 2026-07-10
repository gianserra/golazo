import os
import tempfile
from pathlib import Path

os.environ.setdefault("PYINSTALLER_CONFIG_DIR", str(Path(tempfile.gettempdir()) / "golazo-pyinstaller"))
import PyInstaller.__main__


ROOT = Path(__file__).resolve().parents[1]
separator = os.pathsep

PyInstaller.__main__.run(
    [
        str(ROOT / "scripts" / "backend_entry.py"),
        "--name=golazo-python-backend",
        "--onefile",
        "--clean",
        "--noconfirm",
        f"--distpath={ROOT / 'dist' / 'backend'}",
        f"--workpath={ROOT / 'build' / 'pyinstaller'}",
        f"--specpath={ROOT / 'build'}",
        f"--add-data={ROOT / 'src' / 'golazo' / 'static-react'}{separator}golazo/static-react",
        f"--add-data={ROOT / 'src' / 'golazo' / 'static'}{separator}golazo/static",
        f"--add-data={ROOT / 'skills' / 'manage-implementation' / 'scripts' / 'implementation_tracker.py'}{separator}golazo",
        "--collect-all=uvicorn",
    ]
)
