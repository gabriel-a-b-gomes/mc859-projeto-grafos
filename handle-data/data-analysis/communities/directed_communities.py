import networkx as nx
import argparse
import json
import time
import matplotlib.pyplot as plt
import infomap
import igraph as ig
import leidenalg
from sklearn.metrics import normalized_mutual_info_score

def load_graph_and_ground_truth(nodes_path: str, edges_path: str):
    print("1. Carregando arquivos JSON e extraindo Gabarito (Ground Truth)...")
    with open(nodes_path, encoding="utf-8") as f:
        nodes_data = json.load(f)
    with open(edges_path, encoding="utf-8") as f:
        edges_data = json.load(f)

    # --- 1. RESOLVENDO PESOS NEGATIVOS ---
    raw_weights = [float(ed.get("weight", 1.0)) for ed in edges_data]
    min_w, max_w = min(raw_weights), max(raw_weights)
    print(f"   -> Pesos originais: Mín={min_w} | Máx={max_w}")

    node_games = {}
    
    G_directed = nx.DiGraph()
    G_undirected = nx.Graph()

    for nd in nodes_data:
        G_directed.add_node(nd["id"])
        G_undirected.add_node(nd["id"])

    for ed in edges_data:
        src, dst = ed["src"], ed["dst"]
        game = ed.get("game", "unknown")
        raw_weight = float(ed.get("weight", 1.0))
        
        if max_w != min_w:
            weight = ((raw_weight - min_w) / (max_w - min_w)) + 0.01
        else:
            weight = 1.0

        # Adiciona arestas
        G_directed.add_edge(src, dst, weight=weight)
        if G_undirected.has_edge(src, dst):
            G_undirected[src][dst]['weight'] = max(G_undirected[src][dst]['weight'], weight)
        else:
            G_undirected.add_edge(src, dst, weight=weight)
            
        if game and game != "unknown":
            node_games.setdefault(src, []).append(game)
            node_games.setdefault(dst, []).append(game)

    print(f"   -> [Grafo Normalizado] Nós={G_directed.number_of_nodes()} | Arestas={G_directed.number_of_edges()}")
    
    ground_truth_dict = {}
    for node, games in node_games.items():
        ground_truth_dict[node] = max(set(games), key=games.count)
        
    print("2. Convertendo para igraph (C++)...")
    g_ig_directed = ig.Graph.from_networkx(G_directed)
    g_ig_undirected = ig.Graph.from_networkx(G_undirected)
    
    return G_directed, G_undirected, g_ig_directed, g_ig_undirected, ground_truth_dict


def load_graph_optimized(nodes_path: str, edges_path: str):
    print("1. Carregando JSON e construindo o grafo diretamente na memória (Otimizado)...")
    
    with open(nodes_path, encoding="utf-8") as f:
        nodes_data = json.load(f)
    with open(edges_path, encoding="utf-8") as f:
        edges_data = json.load(f)

    node_map = {}
    for idx, nd in enumerate(nodes_data):
        node_map[nd["id"]] = idx
        
    min_w = float('inf')
    max_w = float('-inf')
    for ed in edges_data:
        w = float(ed.get("weight", 1.0))
        if w < min_w: min_w = w
        if w > max_w: max_w = w
        
    print(f"   -> Pesos originais: Mín={min_w} | Máx={max_w}")

    edge_list = []
    weights = []
    node_games = {}

    for ed in edges_data:
        src_str, dst_str = ed["src"], ed["dst"]
        
        if src_str not in node_map or dst_str not in node_map:
            continue
            
        src_idx = node_map[src_str]
        dst_idx = node_map[dst_str]
        
        game = ed.get("game", "unknown")
        raw_weight = float(ed.get("weight", 1.0))
        
        if max_w != min_w:
            weight = ((raw_weight - min_w) / (max_w - min_w)) + 0.01
        else:
            weight = 1.0
            
        edge_list.append((src_idx, dst_idx))
        weights.append(weight)
        
        # Gabarito
        if game and game != "unknown":
            node_games.setdefault(src_str, []).append(game)
            node_games.setdefault(dst_str, []).append(game)

    del edges_data
    del nodes_data

    print("2. Instanciando o motor C++ do igraph...")
    g_ig_directed = ig.Graph(n=len(node_map), edges=edge_list, directed=True)
    g_ig_directed.es['weight'] = weights
    
    g_ig_directed.vs['_nx_name'] = list(node_map.keys()) 

    g_ig_undirected = g_ig_directed.as_undirected(mode="collapse", combine_edges={"weight": "max"})

    ground_truth_dict = {}
    for node, games in node_games.items():
        ground_truth_dict[node] = max(set(games), key=games.count)

    print(f"   -> [Grafo Pró] Nós={g_ig_directed.vcount()} | Arestas={g_ig_directed.ecount()}")
    
    return None, None, g_ig_directed, g_ig_undirected, ground_truth_dict


def evaluate_metrics(g_ig, partition_membership, ground_truth_dict, is_directed=True):
    """Calcula Modularidade Direcionada e NMI rapidamente"""
    modularity = g_ig.modularity(partition_membership, weights='weight', directed=is_directed)
    
    true_labels = []
    pred_labels = []
    
    for idx, vertex in enumerate(g_ig.vs):
        node_id = vertex['_nx_name']
        if node_id in ground_truth_dict:
            true_labels.append(ground_truth_dict[node_id])
            pred_labels.append(partition_membership[idx])
            
    nmi_score = 0.0
    if len(true_labels) > 0:
        nmi_score = normalized_mutual_info_score(true_labels, pred_labels)
        
    return modularity, nmi_score


def run_infomap_directed(G_directed, g_ig_directed, ground_truth_dict):
    print("\n[1/4] Executando Infomap (Fluxo Direcionado - MODO RÁPIDO)...")
    print("-" * 60)
    start = time.time()
    
    im = infomap.Infomap("--two-level --directed --num-trials 1")
    
    node_map = {node: idx for idx, node in enumerate(G_directed.nodes())}
    
    print("   -> Inserindo arestas no motor do Infomap...")
    for src, dst, data in G_directed.edges(data=True):
        im.add_link(node_map[src], node_map[dst], data.get('weight', 1.0))
        
    print("   -> Iniciando as caminhadas aleatórias (Acompanhe abaixo):")
    im.run()
    print("-" * 60)
    
    membership = [0] * g_ig_directed.vcount()
    name_to_idx = {v['_nx_name']: v.index for v in g_ig_directed.vs}
    
    num_communities = im.num_top_modules
    
    reverse_node_map = {v: k for k, v in node_map.items()}
    
    for node_id, module_id in im.modules:
        nx_name = reverse_node_map[node_id] 
        membership[name_to_idx[nx_name]] = module_id
        
    exec_time = time.time() - start
    mod, nmi = evaluate_metrics(g_ig_directed, membership, ground_truth_dict, is_directed=True)
    
    print(f"-> Tempo: {exec_time:.2f}s | Mod Direcionada: {mod:.4f} | NMI: {nmi:.4f} | Coms: {num_communities}")
    return exec_time, mod, nmi, num_communities


def run_leiden_cpm_directed(g_ig_directed, ground_truth_dict):
    print("\n[2/4] Executando Leiden com CPM (Direcionado e Imune ao Limite de Resolução)...")
    start = time.time()
    
    partition = leidenalg.find_partition(
        g_ig_directed, 
        leidenalg.CPMVertexPartition, 
        weights='weight',
        resolution_parameter=0.05 
    )
    
    exec_time = time.time() - start
    mod, nmi = evaluate_metrics(g_ig_directed, partition.membership, ground_truth_dict, is_directed=True)
    
    print(f"-> Tempo: {exec_time:.2f}s | Mod Direcionada: {mod:.4f} | NMI: {nmi:.4f} | Coms: {len(partition)}")
    return exec_time, mod, nmi, len(partition)


def run_leiden_directed(g_ig_directed, ground_truth_dict):
    print("\n[3/4] Executando Leiden Direcionado (leidenalg)...")
    start = time.time()
    
    partition = leidenalg.find_partition(
        g_ig_directed, 
        leidenalg.ModularityVertexPartition, 
        weights='weight'
    )
    
    exec_time = time.time() - start
    mod, nmi = evaluate_metrics(g_ig_directed, partition.membership, ground_truth_dict, is_directed=True)
    
    print(f"-> Tempo: {exec_time:.2f}s | Mod Direcionada: {mod:.4f} | NMI: {nmi:.4f} | Coms: {len(partition)}")
    return exec_time, mod, nmi, len(partition)


def run_louvain_baseline(g_ig_undirected, ground_truth_dict):
    print("\n[4/4] Executando Louvain Clássico (Baseline Não-Direcionado)...")
    start = time.time()
    
    partition = g_ig_undirected.community_multilevel(weights='weight')
    
    exec_time = time.time() - start
    mod, nmi = evaluate_metrics(g_ig_undirected, partition.membership, ground_truth_dict, is_directed=False)
    
    print(f"-> Tempo: {exec_time:.2f}s | Modularidade: {mod:.4f} | NMI: {nmi:.4f} | Coms: {len(partition)}")
    return exec_time, mod, nmi, len(partition)


def run_spinglass_directed(g_ig_directed, ground_truth_dict):
    print("\n[Novo] Executando Spinglass (Direcionado Termodinâmico)...")
    start = time.time()

    components = g_ig_directed.components(mode='weak')
    giant_component = components.giant()
    
    print(f"   -> Rodando no Componente Gigante ({giant_component.vcount()} de {g_ig_directed.vcount()} nós)")
    
    partition = giant_component.community_spinglass(weights='weight', spins=50)
    
    exec_time = time.time() - start
    
    mod, nmi = evaluate_metrics(giant_component, partition.membership, ground_truth_dict, is_directed=True)
    
    print(f"-> Tempo: {exec_time:.2f}s | Mod Direcionada: {mod:.4f} | NMI: {nmi:.4f} | Coms: {len(partition)}")
    return exec_time, mod, nmi, len(partition)


def plot_advanced_dashboard(results):
    """Gera um painel com 4 gráficos focado nas novas métricas"""
    algos = list(results.keys())
    times = [results[a]['time'] for a in algos]
    mods = [results[a]['modularity'] for a in algos]
    nmis = [results[a]['nmi'] for a in algos]
    counts = [results[a]['count'] for a in algos]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    colors = ['#5DADE2', '#48C9B0', '#F4D03F', '#EB984E']

    axes[0, 0].bar(algos, mods, color=colors, edgecolor='black')
    axes[0, 0].set_title('Modularidade (Qualidade Topológica)')
    axes[0, 0].set_ylabel('Score (Maior é Melhor)')

    axes[0, 1].bar(algos, nmis, color=colors, edgecolor='black')
    axes[0, 1].set_title('NMI (Fidelidade ao Jogo do Usuário)')
    axes[0, 1].set_ylabel('Score NMI (0 a 1)')

    axes[1, 0].bar(algos, times, color=colors, edgecolor='black')
    axes[1, 0].set_title('Tempo de Execução')
    axes[1, 0].set_ylabel('Segundos (s)')

    axes[1, 1].bar(algos, counts, color=colors, edgecolor='black')
    axes[1, 1].set_title('Granularidade')
    axes[1, 1].set_ylabel('Qtd Comunidades')

    plt.tight_layout()
    plt.savefig('dashboard_direcionado.png', dpi=300)
    print("\n[Gráfico Salvo] 'dashboard_direcionado.png' gerado com sucesso!")
    

def extract_network_statistics(g_ig_directed, g_ig_undirected):
    print("\n" + "="*60)
    print(" 📊 EXTRAÇÃO DE ESTATÍSTICAS TOPOLÓGICAS PARA RELATÓRIO")
    print("="*60)

    density = g_ig_directed.density()
    print(f"-> Densidade: {density:.6f}")

    assortativity = g_ig_directed.assortativity_degree(directed=True)
    print(f"-> Assortatividade (Grau): {assortativity:.4f}")

    transitivity = g_ig_undirected.transitivity_undirected()
    print(f"-> Transitividade (Clustering): {transitivity:.4f}")

    print("\n-> Top 5 Nós por PageRank (Influência):")
    pr_scores = g_ig_directed.pagerank(directed=True, weights='weight')
    
    node_names = [v['_nx_name'] for v in g_ig_directed.vs]
    pr_dict = dict(zip(node_names, pr_scores))
    
    top_5_pr = sorted(pr_dict.items(), key=lambda x: x[1], reverse=True)[:5]
    for idx, (node, score) in enumerate(top_5_pr, 1):
        print(f"   {idx}. Nó: {node} | Score: {score:.6f}")
        
    print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Análise de comunidades no grafo Steam")
    parser.add_argument("--nodes", default="nodes.json", help="Caminho para nodes.json")
    parser.add_argument("--edges", default="edges.json", help="Caminho para edges.json")
    args = parser.parse_args()

    _, _, g_ig_dir, g_ig_undir, ground_truth = load_graph_optimized(args.nodes, args.edges)
    
    extract_network_statistics(g_ig_dir, g_ig_undir)
    
    results = {}
    
    t, m, nmi, c = run_infomap_directed(_, g_ig_dir, ground_truth)
    results['Infomap'] = {'time': t, 'modularity': m, 'nmi': nmi, 'count': c}
    
    t, m, nmi, c = run_leiden_cpm_directed(g_ig_dir, ground_truth)
    results['Leiden CPM'] = {'time': t, 'modularity': m, 'nmi': nmi, 'count': c}
    
    t, m, nmi, c = run_leiden_directed(g_ig_dir, ground_truth)
    results['Leiden Dir.'] = {'time': t, 'modularity': m, 'nmi': nmi, 'count': c}
    
    t, m, nmi, c = run_louvain_baseline(g_ig_undir, ground_truth)
    results['Louvain (Baseline)'] = {'time': t, 'modularity': m, 'nmi': nmi, 'count': c}
    
    plot_advanced_dashboard(results)

if __name__ == "__main__":
    main()