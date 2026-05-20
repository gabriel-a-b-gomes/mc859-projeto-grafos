import sys
import json
import networkx as nx

def build_graph_streaming(nodes_path, edges_path, directed=False):
    G = nx.DiGraph() if directed else nx.Graph()

    try:
        import ijson
        print("Usando ijson para streaming...")

        with open(nodes_path, "rb") as f:
            for node in ijson.items(f, "item"):
                node_id = node.get("id") or node.get("node_id") or node.get("_id")
                attrs = {k: v for k, v in node.items()
                         if k not in ("id", "node_id", "_id") and v is not None}
                G.add_node(str(node_id), **attrs)

        with open(edges_path, "rb") as f:
            for edge in ijson.items(f, "item"):
                src, tgt = str(edge.get("src")), str(edge.get("dst"))
                attrs = {k: v for k, v in edge.items()
                         if k not in ("src", "dst") and v is not None}
                G.add_edge(src, tgt, **attrs)

    except ImportError:
        print("ijson não encontrado, usando json padrão. Instale com: pip install ijson")
        with open(nodes_path, "r", encoding="utf-8") as f:
            nodes = json.load(f)
        with open(edges_path, "r", encoding="utf-8") as f:
            edges = json.load(f)

        for node in nodes:
            node_id = node.get("id") or node.get("node_id") or node.get("_id")
            attrs = {k: v for k, v in node.items()
                     if k not in ("id", "node_id", "_id") and v is not None}
            G.add_node(str(node_id), **attrs)

        for edge in edges:
            src, tgt = str(edge.get("src")), str(edge.get("dst"))
            attrs = {k: v for k, v in edge.items()
                     if k not in ("src", "dst") and v is not None}
            G.add_edge(src, tgt, **attrs)

    return G

def convert(nodes_path, edges_path, output_prefix, directed=False):
    G = build_graph_streaming(nodes_path, edges_path, directed=directed)

    print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

    nx.write_graphml(G, f"{output_prefix}.graphml")
    print(f"Saved: {output_prefix}.graphml")

    nx.write_gexf(G, f"{output_prefix}.gexf")
    print(f"Saved: {output_prefix}.gexf")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python convert_graph_lite.py [caminho_nodes.json] [caminho_edges.json]")
        sys.exit(1)

    convert(
        nodes_path=sys.argv[1],
        edges_path=sys.argv[2],
        output_prefix="./graph",
        directed=True,
    )