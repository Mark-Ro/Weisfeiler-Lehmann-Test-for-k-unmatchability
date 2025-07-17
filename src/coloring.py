import time

from compliance import *
from utils import *
from hash import *

"""
n: number of nodes in the graph (indices go from 0 to n-1)  
X_V: list of dictionaries such that X_V[i] = {
    "t": "b" or "c"        # string  
    "c": ["Concept1", ...],        # list of strings  
    "r": ["2,1"],                  # list of strings  
    "feature_string": "..."       # long string built dynamically  
start_time, max_seconds: for timeout control
"""
def wl_initial_coloring(n, X_V, start_time=None, max_seconds=None):
    if start_time and max_seconds and (time.time() - start_time > max_seconds):
        print("\nTimeout during initial coloring.")
        return None
    return [fast_hash(X_V[i]['feature_string']) for i in range(n)] # Color is assigned to each node as the hash of its feature_string



"""
n: number of nodes in the graph (indices go from 0 to n-1)  
adj: list of tuples such that adj[v] = (d, r, neighbor), for each neighbor of v  
color: list of initial colors  
color_counts: dictionary mapping each color to the number of times it has been assigned  
start_time, max_seconds: for timeout

Refines the colors of all nodes, starting from an initial coloring
"""
def wl_coloring(n, adj, color, color_counts, start_time=None, max_seconds=None):

    # Recompute color_counts from scratch based on color
    color_counts.clear()
    for c in color:
        color_counts[c] += 1

    prev_partition = None
    current_partition = partition_from_colors(color)  # Initializes the current partition: sets of nodes sharing the same color

    # Continue refining the coloring until the partition stabilizes
    while prev_partition != current_partition:
        if start_time and max_seconds and (time.time() - start_time > max_seconds):
            print("\nTimeout during WL-coloring.")
            return color

        prev_partition = current_partition

        # Prepares a new list of updated colors
        new_color = []
        new_color_counts = defaultdict(int)

        # For each node v, builds a representative string composed of its current color and the colors of its neighbors, annotated with direction and relation (d|r|color)
        for v in range(n):
            if start_time and max_seconds and (time.time() - start_time > max_seconds):
                print(f"\nTimeout during WL-coloring at node {v}")
                return color

            neighbors = sorted(f"{d}|{r}|{color[n]}" for (d, r, n) in adj[v])
            combined = "|".join([color[v], *neighbors])
            # Hashes this string to obtain a new color for v
            hashed = fast_hash(combined)
            new_color.append(hashed)
            new_color_counts[hashed] += 1

        current_partition = partition_from_colors(new_color)

        # Update state for next iteration
        color = new_color
        color_counts.clear()
        color_counts.update(new_color_counts)

    return color

"""
n: number of nodes in the graph (indices go from 0 to n-1)  
adj: list of tuples such that adj[v] = (d, r, neighbor), for each neighbor of v  
X_V: list of dictionaries such that X_V[i] = {
    "t": "b" or "c"        # string  
    "c": ["Concept1", ...],        # list of strings  
    "r": ["2,1"],                  # list of strings  
    "feature_string": "..."       # long string built dynamically  
changed_idx: index of the node that has changed  
prev_color: list of previous colors of the nodes (from last wl_coloring)

Refines the colors of the graph nodes in response to a localized change, avoiding re-executing the entire WL coloring from scratch
"""
def wl_coloring_incremental(n, adj, X_V, changed_idx, prev_color, color_counts, distance_limit=None):
    color = prev_color.copy() # Copies the previous colors to update them locally
    queue = deque([changed_idx]) # Uses a queue to propagate the change from changed_idx and a set to avoid revisiting
    visited = set()
    dist_from_changed = compute_distances(n, adj, {changed_idx}) if distance_limit is not None else None # If a max distance is set, propagation stops beyond that (early_stop)

    update_feature_string(changed_idx, X_V) # Recomputes the feature string of the modified node
    old_color = color[changed_idx] # Stores the old color of the modified node
    new_color_val = fast_hash(X_V[changed_idx]["feature_string"]) # Applies the hash to get the new color

    # If colors differ, update counts
    if old_color != new_color_val:
        color_counts[old_color] -= 1
        color_counts[new_color_val] += 1
        color[changed_idx] = new_color_val

    while queue:
        v = queue.popleft()
        visited.add(v)

        # If distance_limit is set and node is too far, skip
        if distance_limit is not None and dist_from_changed[v] > distance_limit:
            continue

        # Computes the new color of v
        neighbors = sorted(f"{d}|{r}|{color[n]}" for (d, r, n) in adj[v])
        combined = "|".join([color[v], *neighbors])
        new_c = fast_hash(combined)

        # If color changes, update counts and enqueue neighbors of v (if not visited)
        if new_c != color[v]:
            old_c = color[v]
            color[v] = new_c
            color_counts[old_c] -= 1
            color_counts[new_c] += 1
            for _, _, neighbor in adj[v]:
                if neighbor not in visited:
                    queue.append(neighbor)

    return color