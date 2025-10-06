import os
import time
import sys
import subprocess
import importlib
from pathlib import Path
from graph_io import load_graph_from_rdf
from preprocessing import wl_preprocessing
from coloring import init_wl_backend


def build_cython_backend(profile: bool):
    project_dir = Path(__file__).parent  # Directory containing setup.py
    setup_py = project_dir / "setup.py"
    if not setup_py.exists():
        raise FileNotFoundError(f"setup.py not found at: {setup_py}")

    pyexe = sys.executable  # Use the current interpreter

    # Ensure build prerequisites are available
    missing = []
    for mod in ("setuptools", "wheel", "Cython"):
        try:
            __import__(mod if mod != "Cython" else "Cython")  # Import test
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
            raise RuntimeError("Failed to install build prerequisites (setuptools/wheel/Cython).")

    # Prepare environment for setup.py
    env = os.environ.copy()
    env["CY_PROFILE"] = "1" if profile else "0"  # Tell setup.py which mode to build
    mode = "PROFILING" if profile else "RELEASE"
    print(f"Building Cython backend (cy_wl) in {mode} mode...")
    proc = subprocess.run(
        [pyexe, "setup.py", "build_ext", "--inplace", "--force"],  # --force regenerates .c/.so
        cwd=project_dir,
        capture_output=True,
        text=True,
        env=env,
    )

    # Invalidate import caches and ensure we can import the freshly built extension
    importlib.invalidate_caches()
    try:
        import cy_wl  # noqa: F401
    except ImportError as e:
        # If import fails, print build logs to help diagnose
        print(proc.stdout)
        print(proc.stderr)
        raise RuntimeError("Build succeeded but importing 'cy_wl' failed.") from e
    print("Cython backend built successfully.")


if __name__ == "__main__":
    file_path = "../inputs/Esempio 1 subject_in_uri.rdf"
    k = 2
    incremental = False
    early_stop = False
    parallel = False
    verbose = False
    max_seconds = 86400
    USE_CYTHON = False
    profiling = False
    subject_as_concept = False
    subject_identifier = "subject"

    if early_stop and not incremental:
        raise ValueError("Early stop can only be enabled if incremental=True.")

    if USE_CYTHON:
        build_cython_backend(profile=profiling)
    init_wl_backend(USE_CYTHON, verbose)

    start_loading_time = time.time()
    n, adj, X_V_dict, index_to_node, subject_idx = load_graph_from_rdf(
        file_path, subject_as_concept, subject_identifier
    )
    graph_loading_time = time.time() - start_loading_time

    start_preprocessing_time = time.time()
    necessary_blanks, singletons = wl_preprocessing(
        n,
        adj,
        X_V_dict,
        index_to_node,
        subject_idx,
        k,
        max_seconds,
        incremental,
        early_stop,
        parallel,
        verbose,
    )
    preprocessing_time = time.time() - start_preprocessing_time

    if necessary_blanks is None:
        print("\nError: Preprocessing stopped working.\n")
        print(f"Graph loading time: {graph_loading_time:.3f} seconds")
        print(f"Preprocessing time: {time.time() - start_preprocessing_time:.3f} seconds")
        exit(1)

    input_base_name = os.path.basename(file_path)
    param_suffix = (
        f"k={k}_"
        f"USE_CYTHON={USE_CYTHON}_"
        f"incremental={incremental}_"
        f"early_stop={early_stop}_"
        f"parallel={parallel}_"
        f"subject_as_concept={subject_as_concept}_"
        f"Profiling={profiling}"
    )

    output_dir = "../results"
    os.makedirs(output_dir, exist_ok=True)
    output_file_name = os.path.join(output_dir, f"{input_base_name}_{param_suffix}.txt")
    with open(output_file_name, "w") as f:
        f.write(f"Graph loading time: {graph_loading_time:.3f} seconds\n")
        f.write(f"Preprocessing time: {preprocessing_time:.3f} seconds\n")
        f.write(f"\nNumber of necessary blanks: {len(necessary_blanks)}\n")
        f.write(f"Number of singletons: {len(singletons)}\n")
        f.write("\nFinal necessary blanks:\n")
        f.write("\n".join(necessary_blanks) + "\n")
        f.write("\nSingletons:\n")
        f.write("\n".join(singletons) + "\n")

    if verbose:
        print("\n[Preprocessing] Final results:")
        print(f"Number of necessary blanks: {len(necessary_blanks)}")
        print(f"Number of singletons: {len(singletons)}")
        print("Final necessary blanks:", necessary_blanks if necessary_blanks else set())
        print("Singletons:", singletons if singletons else set())
        print(f"\nTotal preprocessing time: {preprocessing_time:.3f} seconds")
        print(f"Results written to: {output_file_name}")
