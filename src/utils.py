from collections import deque

"""
n: number of nodes in the graph (indices go from 0 to n-1)  
adj: list of tuples such that adj[v] = (d, r, neighbor), for each neighbor of v  
sources: set of indices of the source nodes (those in set D, which are neighbors of subjects)

This function computes the distance array dist such that dist[v] = minimum path length (number of edges) from a source node to v
"""
def compute_distances(n, adj, sources):
    dist = [float('inf')] * n # List of n elements initialized to inf (infinite distance = unreachable node)
    queue = deque() # Queue for BFS (double-ended queue, optimized for insertions/removals at both ends)
    for s in sources: # For each node in sources
        dist[s] = 0 # Sets the distance of source nodes to 0
        queue.append(s) # Adds them to the queue to begin BFS
    while queue:
        v = queue.popleft()
        for _, _, neighbor in adj[v]:
            if dist[neighbor] > dist[v] + 1:
                dist[neighbor] = dist[v] + 1
                queue.append(neighbor)
    return dist


"""
idx: integer index of the node  
X_V: list of dictionaries such that X_V[i] = {
    "t": "b" or "c"        # string
    "c": ["Concept1", ...],        # list of strings
    "r": ["2,1"],                  # list of strings
    "feature_string": "..."       # long string built dynamically
}

Builds the feature string of the node with integer index idx in X_V
"""
def update_feature_string(idx, X_V):
    X_V[idx]["feature_string"] = "|".join([
        X_V[idx]["t"],
        *X_V[idx]["c"], # * expands the list elements for concatenation
        *X_V[idx]["r"]
    ])
