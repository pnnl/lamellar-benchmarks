



import networkx as nx
import numpy as np
import pickle
import matplotlib.pyplot as plt



# ==========================
# Save matrix function
# ==========================

import os
import pickle
from scipy.sparse import coo_matrix

def save_sparse_matrix(matrix: coo_matrix, directory: str):
    """
    Saves a sparse matrix in COO (Coordinate List) format to individual pickle files.

    Args:
        matrix (coo_matrix): The sparse matrix to save. Must be in COO format.
        directory (str): The directory where the files will be saved. The function
                        will create the directory if it does not exist.

    Creates:
        row_indices.pkl : List of row indices (floats) of non-zero elements.
        col_indices.pkl : List of column indices (floats) of non-zero elements.
        coefficients.pkl : List of non-zero values (floats) in the matrix.
        num_rows.pkl : Single float representing the number of rows in the matrix.
        num_columns.pkl : Single float representing the number of columns in the matrix.

    """
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Save row indices
    with open(os.path.join(directory, "row_indices.pkl"), "wb") as f:
        pickle.dump(matrix.row.astype(float).tolist(), f)

    # Save column indices
    with open(os.path.join(directory, "col_indices.pkl"), "wb") as f:
        pickle.dump(matrix.col.astype(float).tolist(), f)

    # Save coefficients
    with open(os.path.join(directory, "coefficients.pkl"), "wb") as f:
        pickle.dump(matrix.data.tolist(), f)

    # Save dimensions
    with open(os.path.join(directory, "num_rows.pkl"), "wb") as f:
        pickle.dump( [ float(matrix.shape[0]) ], f)

    with open(os.path.join(directory, "num_columns.pkl"), "wb") as f:
        pickle.dump( [ float(matrix.shape[1]) ], f)



# ==========================
# Generate a small graph and save the adjacency matrix and shortest path lengths
# ==========================


# Create graph
# ------------------------
G = nx.Graph()
G.add_edge(0, 1, weight=1.5)
G.add_edge(0, 2, weight=2.5)
G.add_edge(1, 2, weight=3.5)

# Save to file
# ------------------------

filename_root = "custom_graph_0"

# Convert to sparse adjacency matrix
adjacency_matrix = nx.to_scipy_sparse_array(G, format='coo', weight='weight')

save_sparse_matrix(
    matrix          =   adjacency_matrix, 
    directory       =   filename_root
)

# # Calculate SSSP from vertex 0
all_shortest_path_lengths = nx.single_source_dijkstra_path_length(G, 0)   

# Record as a list
max_vertex = max( G.nodes() )
path_lengths = [ 0 for _ in range(max_vertex + 1)]
for (node, length) in all_shortest_path_lengths.items():
    path_lengths[ node ] = length 

with open( filename_root + "/shortest_path_lengths.pkl", "wb") as f:
    pickle.dump( path_lengths, f)    




# # Save the adjacency matrix data
# with open( filename_root + "_adjacency_matrix.pkl", "wb") as f:
#     pickle.dump(( G.number_of_nodes(), adjacency_matrix.row, adjacency_matrix.col, adjacency_matrix.data), f)

# # Save the adjacency matrix data
# with open( filename_root + "_adjacency_matrix_row_indices.pkl", "wb") as f:
#     pickle.dump( adjacency_matrix.row, f)    


# # Calculate SSSP from vertex 0
# all_shortest_path_lengths = nx.single_source_dijkstra_path_length(G, 0)   

# # Record as a list
# max_vertex = max( G.nodes() )
# path_lengths = [ 0 for _ in range(max_vertex + 1)]
# for (node, length) in all_shortest_path_lengths.items():
#     path_lengths[ node ] = length 

# with open( filename_root + "_shortest_path_lengths.pkl", "wb") as f:
#     pickle.dump( path_lengths, f)   



# ==========================
# Generate geometric graphs and save the adjacency matrix and shortest path lengths
# ==========================



import networkx as nx
import random

def generate_and_save_random_geometric_graph( number_of_nodes = 1 ):

    # Fix the random seed
    random_seed = 42
    random.seed(random_seed)

    # Parameters
    num_vertices = number_of_nodes
    radius = 0.1  # Adjust for desired edge density

    # Generate a geometric graph with fixed seed
    G = nx.random_geometric_graph(num_vertices, radius, seed=random_seed)

    # Ensure the graph is connected
    if not nx.is_connected(G):
        # Extract positions of nodes
        pos = nx.get_node_attributes(G, "pos")
        
        # Get the largest connected component
        largest_cc = max(nx.connected_components(G), key=len)
        largest_cc_subgraph = G.subgraph(largest_cc).copy()

        # Add edges to connect all components
        components = list(nx.connected_components(G))
        for i in range(len(components) - 1):
            # Select a random node from each component
            comp1 = random.choice(list(components[i]))
            comp2 = random.choice(list(components[i + 1]))
            
            # Add an edge between the two components
            G.add_edge(comp1, comp2)

    # Validate connectivity
    assert nx.is_connected(G), "The graph should now be connected."


    # Save to file
    # ------------------------

    filename_root = f"geometric_graph_2d_{number_of_nodes}v"

    # Convert to sparse adjacency matrix
    adjacency_matrix = nx.to_scipy_sparse_array(G, format='coo', weight='weight')

    save_sparse_matrix(
        matrix          =   adjacency_matrix, 
        directory       =   filename_root
    )

    # # Calculate SSSP from vertex 0
    all_shortest_path_lengths = nx.single_source_dijkstra_path_length(G, 0)   

    # Record as a list
    max_vertex = max( G.nodes() )
    path_lengths = [ 0 for _ in range(max_vertex + 1)]
    for (node, length) in all_shortest_path_lengths.items():
        path_lengths[ node ] = length               



    with open( filename_root + "/shortest_path_lengths.pkl", "wb") as f:
        pickle.dump( path_lengths, f)  

    # Save picture to file
    #---------------------------------

    if number_of_nodes < 2000:

        # Draw the graph
        plt.figure(figsize=(8, 6))  # Set the figure size
        nx.draw(G, with_labels=True, node_color='lightblue', edge_color='gray', node_size=500, font_size=10)

        # Save the graph to a file
        plt.savefig(filename_root + "/graph.png", format="png")  # You can use other formats like 'pdf', 'svg', etc.
        plt.close()  # Close the figure to free up resources    



generate_and_save_random_geometric_graph( number_of_nodes = 10 )
generate_and_save_random_geometric_graph( number_of_nodes = 20 )
generate_and_save_random_geometric_graph( number_of_nodes = 100 )
generate_and_save_random_geometric_graph( number_of_nodes = 500 )
generate_and_save_random_geometric_graph( number_of_nodes = 1000 )
generate_and_save_random_geometric_graph( number_of_nodes = 10000 )

