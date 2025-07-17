import os
from graph_io import *
from preprocessing import *

if __name__ == "__main__":

    file_path = "../inputs/Esempio 1 subject_in_uri.rdf"

    k = 2
    incremental = True
    early_stop = False
    verbose = True
    max_seconds = 8600

    subject_as_concept = False
    subject_identifier = "subject"

    if early_stop and not incremental:
        raise ValueError("Early stop can only be enabled if incremental=True.")

    start_loading_time = time.time()

    n, adj, X_V_dict, index_to_node, subject_idx = load_graph_from_rdf(file_path, subject_as_concept, subject_identifier)

    graph_loading_time = time.time() - start_loading_time

    start_preprocessing_time = time.time()

    necessary_blanks, singletons = wl_preprocessing(n, adj, X_V_dict, index_to_node, subject_idx, k, max_seconds, incremental, early_stop, verbose)

    preprocessing_time = time.time() - start_preprocessing_time

    if necessary_blanks is None:
        print("\nError: Preprocessing stopped working.\n")
        print(f"Graph loading time: {graph_loading_time:.3f} seconds")
        print(f"Preprocessing time: {time.time() - start_preprocessing_time:.3f} seconds")
        exit(1)

    output_dir = "../results"
    os.makedirs(output_dir, exist_ok=True)

    output_file_name = os.path.join(output_dir, f"wl_results_{n}_nodes.txt")

    with open(output_file_name, "w") as f:
        f.write(f"Graph loading time: {graph_loading_time:.3f} seconds\n")
        f.write(f"Preprocessing time: {preprocessing_time:.3f} seconds\n")

        if necessary_blanks is not None:
            f.write("\nFinal necessary blanks:\n")
            f.write("\n".join(necessary_blanks) + "\n")
            f.write("\nSingletons:\n")
            f.write("\n".join(singletons) + "\n")

            if verbose:
                print("\n[Preprocessing] Final results:")
                print("Final necessary blanks:", necessary_blanks if necessary_blanks else {})
                print("Singletons:", singletons if singletons else {})
                print(f"\nTotal preprocessing time: {preprocessing_time:.3f} seconds")
        else:
            f.write("\nThe graph is not k-WL-compliant.\n")
            if verbose:
                print("\nThe graph is not k-WL-compliant.")
                print(f"Total preprocessing time: {preprocessing_time:.3f} seconds")