from collections import defaultdict

"""
color = ['red', 'blue', ...]: list of length n, where color[i] is the color assigned to node i

The algorithm must know:
    - how many nodes have each color (e.g., 'red' appears 2 times)
    - which nodes have each color (e.g., 'red' → {0, 2})
"""
def build_color_counts_and_members(color):
    color_counts = defaultdict(int) # e.g.: {'red': 2, 'blue': 1}
    color_members = defaultdict(set) # e.g.: {'red': {0, 2}, 'blue': {1}}

    for idx, c in enumerate(color): # enumerate(color) returns (node_index, color)
        color_counts[c] += 1
        color_members[c].add(idx)
    return color_counts, color_members



"""
color = ['red', 'blue', ...]: list of length n, where color[i] is the color assigned to node i  
color_counts: dictionary mapping each color to the number of times it has been assigned  
subjects_idx: set of indices of the subject nodes  
k: desired anonymity threshold
"""
def check_k_wl_compliance(color, color_counts, subjects_idx, k):
    return all(color_counts[color[i]] >= k for i in subjects_idx) # For each subject node i, gets its current color and checks how often this color appears in the graph. If all subjects have a color with frequency at least k, then all(...) returns True, else False


"""
color = ['red', 'blue', ...]: list of length n, where color[i] is the color assigned to node i
"""
def partition_from_colors(color):
    groups = defaultdict(set) # Creates a dictionary where keys are colors and values are sets of node indices

    # For each node, it inserts it into the set associated with its color
    for idx, col in enumerate(color): # enumerate(color) returns tuples like: (0, 'red'), (1, 'blue'), (2, 'red'), (3, 'green')
        groups[col].add(idx)
    return {frozenset(g) for g in groups.values()}

