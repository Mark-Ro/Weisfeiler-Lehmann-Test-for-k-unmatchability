from collections import defaultdict
from rdflib import Graph, URIRef

"""
file_path: Path to the RDF/XML file to read
subject_as_concept: If True, subjects are identified among concepts (rdf:type); if False, they are identified in the URI name.
subject_identifier: String to search for in concepts or URIs to identify subjects (e.g., "Patient," "Person").
"""
def load_graph_from_rdf(file_path, subject_as_concept=True, subject_identifier="Subject"):
    g = Graph()  # Initialize an empty graph using rdflib
    g.parse(file_path)  # Parse the RDF file

    V = []  # List of node URIs
    E = []  # List of edges (triples: source, relation, target)
    X_V_dict = {}  # Mapping node URI to its concepts and roles
    subjects_set = set()

    for subj, pred, obj in g:  # Iterate over all subjects, predicates, and objects in the graph
        if isinstance(subj, URIRef):  # Only focus on subjects with rdf:about (nodes)
            uri = str(subj)  # Convert the subject to a string URI

            # Ignore RDF properties (relations) like ex:R, which are not nodes
            if 'R' in uri:  # Explicitly check for any URI that refers to a property/role
                continue  # Skip adding 'R' as a node

            if uri not in X_V_dict:
                V.append(uri)  # Add the URI to the list of nodes if it's not already added
                X_V_dict[uri] = {  # Initialize the dictionary entry for this node
                    "c": set(),  # Set to store concepts (rdf:type)
                    "r": set()  # Set to store roles (relations)
                }

            # Check if the predicate is rdf:type (to extract concepts)
            if pred == URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"):
                concept = str(obj).split('/')[-1]  # Extract concept name from URI
                X_V_dict[uri]["c"].add(concept)  # Add the concept to the node's concepts set
            else:
                # Otherwise, treat the predicate as a role (relation)
                pred_str = str(pred)  # Convert the predicate to a string
                if '}' in pred_str:  # If the predicate contains a namespace, split it
                    relation = pred_str.split('}')[
                        1]  # Extract the relation by splitting on the '}' character (namespace separation)
                else:  # If no '}', use the full predicate string as relation
                    relation = pred_str

                E.append((uri, relation, str(obj)))  # Add the edge (source, relation, target)

    # Identify subject nodes based on the 'subject_identifier'
    for uri, data in X_V_dict.items():
        if subject_as_concept:
            if any(subject_identifier in c for c in data["c"]):  # If subject is identified by concept
                subjects_set.add(uri)  # Add to the subject set
        else:
            if subject_identifier.lower() in uri.lower():  # If subject is identified by URI
                subjects_set.add(uri)  # Add to the subject set

    # Map node URIs to indices for easier handling
    node_to_index = {v: i for i, v in enumerate(V)}
    index_to_node = {i: v for v, i in node_to_index.items()}
    n = len(V)

    # Count in-degrees and out-degrees for each node
    roles_map = defaultdict(lambda: {'o': 0, 'i': 0})
    for s, r, o in E:
        roles_map[s]['o'] += 1
        roles_map[o]['i'] += 1

    # Add role summary (out-degree, in-degree) to each node’s feature dictionary
    for v in V:
        out_deg = roles_map[v]['o']
        in_deg = roles_map[v]['i']
        X_V_dict[v]['r'] = [f"{out_deg},{in_deg}"]

    subject_idx = {node_to_index[s] for s in subjects_set if s in node_to_index}

    # Build adjacency list (directed and labeled edges)
    adj_list = [[] for _ in range(n)]
    for s, r, o in E:
        if s in node_to_index and o in node_to_index:
            s_idx = node_to_index[s]
            o_idx = node_to_index[o]
            adj_list[s_idx].append(('o', r, o_idx))  # Outgoing edge
            adj_list[o_idx].append(('i', r, s_idx))  # Incoming edge

    adj = tuple(tuple(neigh) for neigh in adj_list)  # Convert adjacency list to tuple of tuples for immutability

    return n, adj, X_V_dict, index_to_node, subject_idx