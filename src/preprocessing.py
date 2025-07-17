from coloring import *
from compliance import *
from utils import *

"""
n: number of nodes in the graph (indices go from 0 to n-1)  
adj: list of tuples such that adj[v] = (d, r, neighbor), for each neighbor of v  
X_V_dict: dictionary of nodes → concepts and roles (but not the type 't')  
index_to_node: mapping between numeric indices of nodes and textual IDs  
subject_idx: set of indices of subject nodes  
k: desired anonymity threshold  
max_seconds: maximum timeout  
incremental: if True, incremental WL is used when changing a node  
early_stop: if True, incremental WL propagation is limited to the minimum distance from subjects
"""
def wl_preprocessing(n, adj, X_V_dict, index_to_node, subject_idx, k, max_seconds, incremental, early_stop, verbose=False):

    # Check if subject_idx is empty
    if not subject_idx:
        if verbose:
            print("\nError: No subjects to anonymize in the graph.")
        return None, None

    if verbose:
        print("\n[Preprocessing] Starting WL{} preprocessing...\n".format(" incremental" if incremental else " full"))

    start_time = time.time()

    # Initializes X_V with all nodes as blank
    X_V = [{} for _ in range(n)]
    for i in range(n):
        node_id = index_to_node[i]
        X_V[i]["t"] = 'b' # Type
        X_V[i]["c"] = sorted(X_V_dict[node_id]["c"]) # Concepts
        X_V[i]["r"] = sorted(X_V_dict[node_id]["r"]) # Roles
        update_feature_string(i, X_V)

    color = wl_initial_coloring(n, X_V, start_time, max_seconds) # Initial coloring, based only on X_V

    # Initializes color_counts: dictionary mapping each color to its frequency
    color_counts = defaultdict(int)
    for c in color:
        color_counts[c] += 1

    color = wl_coloring(n, adj, color, color_counts, start_time, max_seconds) # Applies the first coloring that considers neighbors
    color_counts, color_members = build_color_counts_and_members(color) # Retrieves: 1) how many nodes have each color (e.g., 'red' appears 2 times) 2) which nodes have each color (e.g., 'red' → {0, 2})

    # Checks whether each subject has at least k nodes with the same color
    if not check_k_wl_compliance(color, color_counts, subject_idx, k):
        if verbose:
            print("[Preprocessing] No k-WL-compliant anonymization possible.")
        return None, None

    necessary_blanks = set(subject_idx) # Subjects are always necessary blanks
    singletons = {i for i, c in enumerate(color) if color_counts[c] == 1} # Computes singleton nodes (nodes that are pointless to anonymize)

    # If a subject has color shared by exactly k nodes, all those k are necessary blanks
    for i in subject_idx:
        if color_counts[color[i]] == k:
            necessary_blanks.update(color_members[color[i]])

    if verbose:
        print(f"Initially necessary: {[index_to_node[i] for i in necessary_blanks]}")
        print(f"Singletons: {[index_to_node[i] for i in singletons]}")

    distances = compute_distances(n, adj, subject_idx) # Computes distances from subject nodes
    unmarked_blanks = set(range(n)) - necessary_blanks - singletons # Nodes to verify = not necessary blanks and not singletons
    ranked = sorted(unmarked_blanks, key=lambda b: distances[b]) # Nodes to verify are ordered by distance from subjects (for anytime processing)

    if verbose:
        print(f"Blanks to verify: {[index_to_node[i] for i in ranked]}\n")

    for b in ranked:
        if time.time() - start_time > max_seconds:
            if verbose:
                print(f"\nTimeout reached after {max_seconds} seconds. Stopping early.")
            break

        # Blank is removed, setting it as constant
        original_type = X_V[b]['t']
        X_V[b]['t'] = 'c'
        update_feature_string(b, X_V)

        # If incremental flag is active, incremental coloring is applied
        if incremental:
            color_counts_snapshot = color_counts.copy()
            # If early_stop flag is active, incremental WL with early_stop is applied (stop based on distances from subjects)
            if early_stop:
                d = distances[b]
                if d == float('inf'):
                    color_to_check = wl_coloring_incremental(n, adj, X_V, b, color, color_counts_snapshot)
                else:
                    color_to_check = wl_coloring_incremental(n, adj, X_V, b, color, color_counts_snapshot, distance_limit=int(d))
                    if verbose:
                        print(f"  [EarlyStop] Node {index_to_node[b]} has distance {int(d)} from a subject → WL limited.")
            else:
                color_to_check = wl_coloring_incremental(n, adj, X_V, b, color, color_counts_snapshot)
        # If incremental is not active, full coloring is applied
        else:
            color_to_check = color.copy()
            update_feature_string(b, X_V) # Updates the feature_string of node b based on its new type (t = 'c')
            color_b = fast_hash(X_V[b]["feature_string"]) # Computes the new color of b
            old_color = color[b] # Saves the old color of b (before it becomes constant)
            color_to_check[b] = color_b # Assigns the new color to b in the simulated coloring

            color_counts_snapshot = color_counts.copy() # Creates a copy of the color counts, to be updated during simulation
            # If b’s color changed, decrement old color and increment the new one
            if color_b != old_color:
                color_counts_snapshot[old_color] -= 1
                color_counts_snapshot[color_b] += 1

            color_to_check = wl_coloring(n, adj, color_to_check, color_counts_snapshot, start_time, max_seconds) # Now that b is constant, run full WL coloring starting from the modified coloring to see how changes propagate in the graph and what impact they have

        # If k-WL-compliance is violated, b is added to necessary blanks
        if not check_k_wl_compliance(color_to_check, color_counts_snapshot, subject_idx, k):
            necessary_blanks.add(b)
            if verbose:
                print(f"  {index_to_node[b]} is a necessary blank.")
        # Otherwise, b is useless to keep blank
        else:
            if verbose:
                print(f"  {index_to_node[b]} does not need to be blank.")

        # Restores the original type and feature string
        X_V[b]['t'] = original_type
        update_feature_string(b, X_V)

    return {index_to_node[i] for i in necessary_blanks}, {index_to_node[i] for i in singletons}