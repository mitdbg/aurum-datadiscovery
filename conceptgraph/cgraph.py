from dataanalysis import dataanalysis as da
import api as API
import config as C

from collections import OrderedDict
import time


now = lambda: int(round(time.time())*1000)

def build_graph_skeleton(columns):
    '''
        Builds basic graph skeleton, where columns
        are connected to other columns in the same table.
        No cross-table connections.
    '''
    graph = OrderedDict()
    for p_colname, _ in columns.items():
        (pf, _) = p_colname
        graph[p_colname] = []
        for colname, _ in columns.items():
            (f, c) = colname
            if pf is f:
                if colname not in graph[p_colname]:
                    graph[p_colname].append(colname)
    return graph

def refine_graph_with_columnsignatures(ncol, tcol, graph):
    '''
        Given the signatures for all the numerical columns
        as well as for the textual columns, refine the graph
        creating additional links whenever necessary
    '''
    st = now()
    for col, sig in ncol.items():
        # Get list of columns similar to this one
        (filename, colname) = col
        cols = API.columns_similar_to(filename, colname, "ks")
        # Create links to the similar cols
        for c in cols:
            graph[col].append(c)
    et = now()
    st = now()
    print("Took: " +str(et-st)+ "ms to refine with num values")
    for col, sig in tcol.items():
        # Get list of columns similar to this one
        (filename, colname) = col
        cols = API.columns_similar_to(filename, colname, "ks")
        # Create links to the similar cols
        for c in cols:
            graph[col].append(c)
    et = now()
    print("Took: " +str(et-st)+ "ms to refine with text values")
    return graph

def give_neighbors_of(concept, graph):
    '''
        Given a concept it returns all its neighbors not
        in the same table
    '''
    (f, c) = concept
    allneighbors = graph[concept]
    neighbors = [] 
    for filename, col in allneighbors:
        if filename is not f:
            neighbors.append((filename, col)) 
    return neighbors

def give_structural_sim_of(concept, cgraph, simrank):
    # get index of concept in cgraph
    nodes = list(cgraph.keys())
    index = nodes.index(concept)
    # get row in simrank with the index
    row = simrank[index]
    # Filter elements by threshold, keep indexes
    sim_idx = []
    for idx in range(len(row)):
        if row[idx] > C.simrank_sim_threshold:
            sim_idx.append(idx)
    # Inverse index with the cgraph
    sim_vec = []
    for idx in sim_idx:
        sim_vec.append(nodes[idx])
    # Return list of results
    return sim_vec
                 
def give_n_concepts_close_to(num, key, graph):
    return graph[key][:num]


if __name__ == "__main__":
    print("TODO")
