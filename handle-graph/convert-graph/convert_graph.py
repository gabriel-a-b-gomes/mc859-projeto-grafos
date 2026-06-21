import sys
import json
import networkx as nx

def load_graph(nodes_path, edges_path):
    with open(nodes_path, "r", encoding="utf-8") as f:
        nodes = json.load(f)
    with open(edges_path, "r", encoding="utf-8") as f:
        edges = json.load(f)
    return nodes, edges

def build_graph(nodes, edges, directed=False):
    G = nx.DiGraph() if directed else nx.Graph()

    for node in nodes:
        node_id = node.get("id") or node.get("node_id") or node.get("_id")
        attrs = {k: v for k, v in node.items() if k not in ("id", "node_id", "_id") and v is not None}
        G.add_node(str(node_id), **attrs)

    for edge in edges:
        src = edge.get("src")
        tgt = edge.get("dst")
        attrs = {k: v for k, v in edge.items()
                 if k not in ("src", "dst")
                 and v is not None}
        G.add_edge(str(src), str(tgt), **attrs)

    return G

def convert(nodes_path, edges_path, output_prefix, directed=False):
    nodes, edges = load_graph(nodes_path, edges_path)
    G = build_graph(nodes, edges, directed=directed)

    print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

    nx.write_graphml(G, f"{output_prefix}.graphml")
    print(f"Saved: {output_prefix}.graphml")

    nx.write_gexf(G, f"{output_prefix}.gexf")
    print(f"Saved: {output_prefix}.gexf")

if __name__ == "__main__":

    if len(sys.argv) != 3:
      print("Uso: python convert_graph.py [caminho_nodes.json] [caminho_edges.json]")
      sys.exit(1)
      
    NODES_PATH = sys.argv[1]
    EDGES_PATH = sys.argv[2]
  
    convert(
        nodes_path=NODES_PATH,
        edges_path=EDGES_PATH,
        output_prefix="./graph",
        directed=True,  # mude para False se o grafo for não-direcionado
    )