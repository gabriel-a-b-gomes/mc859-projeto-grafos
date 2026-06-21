import networkx as nx
import argparse
import json
import time
import matplotlib.pyplot as plt
import infomap
import igraph as ig

def load_graph(nodes_path: str, edges_path: str):
    print("1. Carregando arquivos JSON...")
    with open(nodes_path, encoding="utf-8") as f:
        nodes_data = json.load(f)
    with open(edges_path, encoding="utf-8") as f:
        edges_data = json.load(f)

    # Passo extra: Encontrar o min e max peso para normalizar
    raw_weights = [float(ed.get("weight", 1.0)) for ed in edges_data]
    min_w = min(raw_weights)
    max_w = max(raw_weights)
    
    print(f"-> Pesos originais detectados: Mín={min_w} | Máx={max_w}")

    G_directed = nx.DiGraph()
    G_undirected = nx.Graph()

    for nd in nodes_data:
        G_directed.add_node(nd["id"])
        G_undirected.add_node(nd["id"])

    for ed in edges_data:
        src, dst = ed["src"], ed["dst"]
        raw_weight = float(ed.get("weight", 1.0))
        
        if max_w != min_w:
            weight = ((raw_weight - min_w) / (max_w - min_w)) + 0.01
        else:
            weight = 1.0

        G_directed.add_edge(src, dst, weight=weight)
        
        if G_undirected.has_edge(src, dst):
            G_undirected[src][dst]['weight'] = max(G_undirected[src][dst]['weight'], weight)
        else:
            G_undirected.add_edge(src, dst, weight=weight)

    print(f"-> [Grafo Normalizado] Nós={G_directed.number_of_nodes()} | Arestas={G_directed.number_of_edges()}")
    
    g_ig_directed = ig.Graph.from_networkx(G_directed)
    g_ig_undirected = ig.Graph.from_networkx(G_undirected)
    
    return G_directed, G_undirected, g_ig_directed, g_ig_undirected


def calculate_fast_modularity(g_ig, communities):
    """Calcula a modularidade em milissegundos usando a engine em C++ do igraph"""
    name_to_idx = {v['_nx_name']: v.index for v in g_ig.vs}
    membership = [0] * len(g_ig.vs)
    
    for cluster_idx, cluster in enumerate(communities):
        for node_name in cluster:
            if node_name in name_to_idx:
                membership[name_to_idx[node_name]] = cluster_idx
                
    return g_ig.modularity(membership, weights='weight')


def run_label_propagation(G_undirected, g_ig_undirected):
    print("\nExecuting [1/4] Label Propagation (Baseline)...")
    start = time.time()
    coms_generator = nx.community.label_propagation_communities(G_undirected)
    communities = [list(c) for c in coms_generator]
    exec_time = time.time() - start
    
    modularity = calculate_fast_modularity(g_ig_undirected, communities)
    print(f"-> Concluído em {exec_time:.2f}s | Modularidade: {modularity:.4f}")
    return exec_time, modularity, len(communities)


def run_infomap(G_directed, g_ig_directed):
    print("\nExecuting [2/4] Infomap (Fluxo Direcionado)...")
    print("-" * 60)
    start = time.time()
    
    im = infomap.Infomap(silent=False, two_level=True)
    node_map = {node: idx for idx, node in enumerate(G_directed.nodes())}
    reverse_map = {idx: node for node, idx in node_map.items()}
    
    for src, dst, data in G_directed.edges(data=True):
        im.add_link(node_map[src], node_map[dst], data.get('weight', 1.0))
    
    im.run()
    print("-" * 60)
    
    communities_dict = {}
    for node_id, module_id in im.modules:
        communities_dict.setdefault(module_id, []).append(reverse_map[node_id])
        
    communities = list(communities_dict.values())
    exec_time = time.time() - start
    
    modularity = calculate_fast_modularity(g_ig_directed, communities)
    print(f"-> Concluído em {exec_time:.2f}s | Modularidade: {modularity:.4f}")
    return exec_time, modularity, len(communities)


def run_louvain(g_ig_undirected):
    print("\nExecuting [3/4] Louvain (Otimização via C++)...")
    start = time.time()
    partition = g_ig_undirected.community_multilevel(weights='weight')
    exec_time = time.time() - start
    
    communities = [[g_ig_undirected.vs[idx]['_nx_name'] for idx in cluster] for cluster in partition]
    modularity = partition.modularity
    print(f"-> Concluído em {exec_time:.2f}s | Modularidade: {modularity:.4f}")
    return exec_time, modularity, len(communities)


def run_leiden(g_ig_undirected):
    print("\nExecuting [4/4] Leiden (Otimização via C++)...")
    start = time.time()
    partition = g_ig_undirected.community_leiden(weights='weight', objective_function="modularity")
    exec_time = time.time() - start
    
    communities = [[g_ig_undirected.vs[idx]['_nx_name'] for idx in cluster] for cluster in partition]
    modularity = partition.modularity
    print(f"-> Concluído em {exec_time:.2f}s | Modularidade: {modularity:.4f}")
    return exec_time, modularity, len(communities)


def plot_metrics_dashboard(results):
    """Gera o gráfico comparativo de barras para o relatório do trabalho"""
    algos = list(results.keys())
    times = [results[a]['time'] for a in algos]
    modularities = [results[a]['modularity'] for a in algos]
    counts = [results[a]['count'] for a in algos]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    colors = ['#4A90E2', '#50E3C2', '#F5A623', '#E28490']

    axes[0].bar(algos, times, color=colors, edgecolor='black', alpha=0.8)
    axes[0].set_title('Tempo de Execução (Menor é Melhor)', fontsize=12, fontweight='bold')
    axes[0].set_ylabel('Segundos (s)')
    axes[0].grid(axis='y', linestyle='--', alpha=0.5)

    axes[1].bar(algos, modularities, color=colors, edgecolor='black', alpha=0.8)
    axes[1].set_title('Modularidade (Maior é Melhor)', fontsize=12, fontweight='bold')
    axes[1].set_ylabel('Score de Modularidade')
    axes[1].grid(axis='y', linestyle='--', alpha=0.5)

    axes[2].bar(algos, counts, color=colors, edgecolor='black', alpha=0.8)
    axes[2].set_title('Total de Comunidades Detectadas', fontsize=12, fontweight='bold')
    axes[2].set_ylabel('Quantidade')
    axes[2].grid(axis='y', linestyle='--', alpha=0.5)

    plt.suptitle('Benchmark de Detecção de Comunidades - Grafo Steam (700k arestas)', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    
    plt.savefig('comparacao_metricas.png', dpi=300, bbox_inches='tight')
    print("\n[Gráfico Gerado] 'comparacao_metricas.png' salvo com sucesso!")
    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Análise de comunidades no grafo Steam")
    parser.add_argument("--nodes", default="nodes.json", help="Caminho para nodes.json")
    parser.add_argument("--edges", default="edges.json", help="Caminho para edges.json")
    args = parser.parse_args()

    G_directed, G_undirected, g_ig_directed, g_ig_undirected = load_graph(args.nodes, args.edges)
  
    results = {}
    
    # Execuções
    t, mod, cnt = run_label_propagation(G_undirected, g_ig_undirected)
    results['Label Prop. (Baseline)'] = {'time': t, 'modularity': mod, 'count': cnt}
    
    t, mod, cnt = run_infomap(G_directed, g_ig_directed)
    results['Infomap'] = {'time': t, 'modularity': mod, 'count': cnt}
    
    t, mod, cnt = run_louvain(g_ig_undirected)
    results['Louvain'] = {'time': t, 'modularity': mod, 'count': cnt}
    
    t, mod, cnt = run_leiden(g_ig_undirected)
    results['Leiden'] = {'time': t, 'modularity': mod, 'count': cnt}
    
    # Gerando os gráficos finais
    plot_metrics_dashboard(results)

if __name__ == "__main__":
    main()