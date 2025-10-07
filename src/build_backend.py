import os, sys, subprocess, importlib
from pathlib import Path

"""
Build the Cython backend (cy_wl) in release or profiling mode
"""
def build_cython_backend(profile: bool):
    project_dir = Path(__file__).parent
    setup_py = project_dir / "setup.py"
    if not setup_py.exists():
        raise FileNotFoundError(f"setup.py not found at: {setup_py}")

    pyexe = sys.executable
    missing = []
    for mod in ("setuptools", "wheel", "Cython"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        print(f"[Build] Installing prerequisites: {', '.join(missing)} ...")
        proc = subprocess.run(
            [pyexe, "-m", "pip", "install", *missing],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            print(proc.stdout)
            print(proc.stderr)
            raise RuntimeError("Failed to install build prerequisites.")

    env = os.environ.copy()
    env["CY_PROFILE"] = "1" if profile else "0"
    mode = "PROFILING" if profile else "RELEASE"
    print(f"Building Cython backend (cy_wl) in {mode} mode...")

    proc = subprocess.run(
        [pyexe, "setup.py", "build_ext", "--inplace", "--force"],
        cwd=project_dir,
        capture_output=True,
        text=True,
        env=env,
    )

    importlib.invalidate_caches()
    try:
        import cy_wl  # noqa: F401
    except ImportError as e:
        print(proc.stdout)
        print(proc.stderr)
        raise RuntimeError("Build succeeded but importing 'cy_wl' failed.") from e

    print("Cython backend built successfully.")
