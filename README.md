This repository implements an RDF graph anonymization pipeline based on **Weisfeiler–Lehman** coloring.  
It identifies which RDF nodes must remain **blank** (unlabeled) to guarantee *k-anonymity under structural equivalence* as defined by the WL color refinement algorithm.

Both a **pure Python** and a **Cython-accelerated** backend are available.

---

## Features

- **Weisfeiler–Lehman (WL) Coloring** for structural graph refinement  
- **Incremental coloring** after local feature changes  
- **k-WL compliance checking** for anonymization guarantees  
- **Parallel candidate verification** via Joblib  
- **Cython backend** for high-performance execution  
- **RDF graph parsing** with [RDFlib](https://github.com/RDFLib/rdflib)

---

## Project Structure

```
project/
├── coloring.py           # WL refinement algorithms (Python backend)
├── cy_wl.pyx             # Cython-optimized backend (compiled extension)
├── compliance.py         # k-WL compliance checks and color partitioning
├── graph_io.py           # RDF parser → compact graph representation
├── utils.py              # BFS distance computation & feature serialization
├── preprocessing.py      # Main WL preprocessing and anonymization logic
├── parallel.py           # Parallel batch verification of candidate blanks
├── hash.py               # Fast xxh3 64-bit hashing
├── main.py               # Entry point for preprocessing
├── run_tests.py          # Automated regression test suite
├── setup.py              # Build script for the Cython backend
└── inputs/               # Example RDF input files
```

---

## Installation

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/wl-anonymizer.git
cd wl-anonymizer
```

### 2. Create and activate a Python environment
```bash
python -m venv venv
source venv/bin/activate  # (or venv\Scripts\activate on Windows)
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

If you don’t have a `requirements.txt`, install manually:
```bash
pip install rdflib xxhash joblib cython setuptools wheel
```

---

## Running the Main Script

Run the preprocessing pipeline on an RDF example:

```bash
python main.py
```

Inside `main.py`, the configuration section is clearly marked:

```python
###########################################################################
# >>> USER-CONFIGURABLE PARAMETERS <<<                                    #
###########################################################################

# ------------------------
# INPUT / DATA PARAMETERS
# ------------------------
file_path = "../inputs/Esempio 2 subject_in_uri.rdf"  # Path to RDF input

# ------------------------
# ANONYMIZATION REQUIREMENT
# ------------------------
k = 2  # k-WL: every subject must belong to a WL color class of size >= k

# ------------------------
# EXECUTION STRATEGY
# ------------------------
incremental = False  # If True, use incremental WL for each candidate blank
early_stop = False   # If True (and incremental=True), limit propagation
parallel = False     # Enable joblib-based parallel verification

# ------------------------
# BACKEND SELECTION / BUILD
# ------------------------
USE_CYTHON = False  # If True, use the compiled Cython backend
profiling = False   # Build Cython in profiling mode (for debugging)

# ------------------------
# SUBJECT DETECTION (RDF)
# ------------------------
subject_as_concept = False  # If True, subjects are selected by RDF type concept == subject_identifier.
                            # If False, a node is considered a subject only if its URI contains the
                            # substring given in subject_identifier (case-sensitive).
subject_identifier = "subject"  # Concept label (if subject_as_concept) OR URI substring for subjects

# ------------------------
# RUNTIME / LOGGING
# ------------------------
verbose = True
max_seconds = 86400
###########################################################################
# >>> END OF USER-CONFIGURABLE SECTION <<<                                #
###########################################################################
```

The script automatically saves results to `../results/`.


## Running the Test Suite

The repository includes tests for all RDF examples and parameter combinations.

Run all tests:
```bash
python run_tests.py
```
