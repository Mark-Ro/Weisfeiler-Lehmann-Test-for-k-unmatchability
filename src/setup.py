import os
import sys
from setuptools import setup, Extension
from Cython.Build import cythonize

IS_WINDOWS = sys.platform.startswith("win")

# Read profiling switch from environment (set by the build helper in main.py)
PROFILE = os.environ.get("CY_PROFILE", "0") == "1"

# Platform-specific optimization flags (release defaults)
if IS_WINDOWS:
    extra_compile_args = ["/O2", "/GL", "/Gy", "/Gw", "/DNDEBUG"]  # MSVC optimization flags
    extra_link_args = ["/LTCG"]  # Link-time code generation
else:
    extra_compile_args = [
        "-O3", "-march=native", "-mtune=native",
        "-fno-math-errno", "-fno-trapping-math",
        "-pipe", "-flto", "-DNDEBUG",
    ]
    extra_link_args = ["-flto"]

# If profiling is enabled, we relax to simpler flags and add tracing macros
define_macros = []
compiler_directives = {
    # Base directives; the pyx header already disables checks for performance
    "language_level": 3,
}

if PROFILE:
    # Enable Cython line tracing and profiling
    define_macros = [("CYTHON_TRACE", "1"), ("CYTHON_TRACE_NOGIL", "1")]
    compiler_directives.update({
        "profile": True,
        "linetrace": True,
        "binding": True,
    })
    # Use safe default compile flags; aggressive LTO/march may hinder debuggability
    if IS_WINDOWS:
        extra_compile_args = ["/O2"]
        extra_link_args = []
    else:
        extra_compile_args = ["-O3"]
        extra_link_args = []

ext_modules = [
    Extension(
        "cy_wl",
        ["cy_wl.pyx"],
        language="c",
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        define_macros=define_macros,
    )
]

setup(
    name="cy_wl",
    ext_modules=cythonize(
        ext_modules,
        language_level="3",   # Ensure Python 3 semantics
        force=True,           # Always regenerate .c when building (prevents stale artifacts)
        compiler_directives=compiler_directives,
    ),
)
