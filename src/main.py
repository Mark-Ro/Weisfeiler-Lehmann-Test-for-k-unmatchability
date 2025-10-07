import os
import time
from graph_io import load_graph_from_rdf
from preprocessing import wl_preprocessing
from build_backend import build_cython_backend
from coloring import init_wl_backend


if __name__ == "__main__":

    ###########################################################################
    # >>> USER-CONFIGURABLE PARAMETERS <<<                                    #
    # Modify only this section to adapt preprocessing to your dataset or task #
    ###########################################################################

    file_path = "../inputs/Esempio 2 subject_in_uri.rdf"  # Path to the RDF input file

    k = 2  # k-WL: every subject must belong to a WL color class of size >= k

    # EXECUTION STRATEGY
    incremental = False  # If True, use incremental WL when testing each candidate blank
    early_stop = False   # If True (and incremental=True), bound propagation by candidate's distance to any subject
    parallel = False     # If True, verify candidates in parallel using joblib


    # BACKEND SELECTION / BUILD
    USE_CYTHON = False  # If True, use the Cython backend (much faster on large graphs)
    profiling = False   # If True and USE_CYTHON, build Cython in profiling mode (slower but traceable)

    # SUBJECT DETECTION (RDF)
    subject_as_concept = False       # If True, subjects are selected by RDF type concept == subject_identifier. If False, a node is considered a subject only if its URI contains subject_identifier
    subject_identifier = "subject"   # Concept label (if subject_as_concept) OR URI string to detect subjects

    # RUNTIME / LOGGING
    verbose = True     # Print progress and diagnostics to stdout
    max_seconds = 86400  # Global time budget (in seconds) for preprocessing

    ###########################################################################
    # >>> END OF USER-CONFIGURABLE SECTION <<<                                #
    # Do not edit below this line unless you know what you're doing.          #
    ###########################################################################

    if early_stop and not incremental:
        raise ValueError("Early stop can only be enabled if incremental=True.")


    if USE_CYTHON:
        build_cython_backend(profile=profiling)
    init_wl_backend(USE_CYTHON, verbose)


    start_loading_time = time.time()
    n, adj, X_V_dict, index_to_node, subject_idx = load_graph_from_rdf(file_path, subject_as_concept, subject_identifier)
    graph_loading_time = time.time() - start_loading_time


    start_preprocessing_time = time.time()

    necessary_blanks, singletons = wl_preprocessing(n, adj, X_V_dict, index_to_node, subject_idx, k, max_seconds, incremental, early_stop, parallel, verbose)

    preprocessing_time = time.time() - start_preprocessing_time


    # REPORTING / OUTPUT (unchanged behavior)
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
    with open(output_file_name, "w", encoding="utf-8") as f:
        f.write(f"Graph loading time: {graph_loading_time:.3f} seconds\n")
        f.write(f"Preprocessing time: {preprocessing_time:.3f} seconds\n")
        f.write(f"\nNumber of necessary blanks: {len(necessary_blanks)}\n")
        f.write(f"Number of singletons: {len(singletons)}\n")
        f.write("\nFinal necessary blanks:\n")
        f.write("\n".join(necessary_blanks) + "\n" if necessary_blanks else "(none)\n")
        f.write("\nSingletons:\n")
        f.write("\n".join(singletons) + "\n" if singletons else "(none)\n")

    if verbose:
        print("\n[Preprocessing] Final results:")
        print(f"Number of necessary blanks: {len(necessary_blanks)}")
        print(f"Number of singletons: {len(singletons)}")
        print("Final necessary blanks:", necessary_blanks if necessary_blanks else set())
        print("Singletons:", singletons if singletons else set())
        print(f"\nTotal preprocessing time: {preprocessing_time:.3f} seconds")
        print(f"Results written to: {output_file_name}")
