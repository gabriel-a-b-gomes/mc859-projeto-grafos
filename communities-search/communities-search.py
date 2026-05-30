import networkx as nx
import random
import numpy as np
from networkx.algorithms import community
import time

# ==========================================
# 1. MODELO DE DIFUSÃO PONDERADO
# ==========================================
def simulate_weighted_icm_target(G, seeds, target_attr, target_val, mc=100):
    target_nodes = set([n for n, attr in G.nodes(data=True) if attr.get(target_attr) == target_val])
    if not target_nodes:
        return 0.0
    
    total_infected_in_target = 0
    for _ in range(mc):
        new_infected = list(seeds)
        infected = set(seeds)
        
        while new_infected:
            next_infected = []
            for node in new_infected:
                for neighbor in G.successors(node):
                    if neighbor not in infected:
                        p_uv = G[node][neighbor]['weight']
                        if random.random() < p_uv:
                            infected.add(neighbor)
                            next_infected.append(neighbor)
            new_infected = next_infected
            
        infected_target = infected.intersection(target_nodes)
        total_infected_in_target += len(infected_target)
        
    return (total_infected_in_target / mc) / len(target_nodes) * 100

# ==========================================
# 2. ALGORITMOS AJUSTADOS PARA REDES ESPARSAS
# ==========================================

def baseline_high_out_strength(G, target_attr, target_val, k):
    target_nodes = [n for n, attr in G.nodes(data=True) if attr.get(target_attr) == target_val]
    sorted_nodes = sorted(target_nodes, key=lambda n: G.out_degree(n, weight='weight'), reverse=True)
    return sorted_nodes[:k]

def celf_weighted_targeted(G, target_attr, target_val, k, mc=20):
    target_nodes = [n for n, attr in G.nodes(data=True) if attr.get(target_attr) == target_val]
    if not target_nodes: return []
    
    marg_gains = []
    for node in target_nodes:
        spread = simulate_weighted_icm_target(G, [node], target_attr, target_val, mc)
        marg_gains.append([node, spread])
        
    marg_gains.sort(key=lambda x: x[1], reverse=True)
    
    seeds = [marg_gains[0][0]]
    spread = marg_gains[0][1]
    marg_gains.pop(0)
    
    while len(seeds) < k and marg_gains:
        matched = False
        while not matched:
            current_node, old_gain = marg_gains[0]
            current_spread = simulate_weighted_icm_target(G, seeds + [current_node], target_attr, target_val, mc)
            new_gain = current_spread - spread
            
            marg_gains[0][1] = new_gain
            marg_gains.sort(key=lambda x: x[1], reverse=True)
            
            if marg_gains[0][0] == current_node:
                seeds.append(current_node)
                spread = current_spread
                marg_gains.pop(0)
                matched = True
    return seeds

def ris_weighted_targeted(G, target_attr, target_val, k, num_sets=600):
    target_nodes = [n for n, attr in G.nodes(data=True) if attr.get(target_attr) == target_val]
    if not target_nodes: return []
    
    rr_sets = []
    for _ in range(num_sets):
        v = random.choice(target_nodes)
        rr_set = {v}
        queue = [v]
        while queue:
            curr = queue.pop(0)
            for predecessor in G.predecessors(curr):
                if predecessor not in rr_set:
                    p_vu = G[predecessor][curr]['weight']
                    if random.random() < p_vu:
                        rr_set.add(predecessor)
                        queue.append(predecessor)
        rr_sets.append(rr_set.intersection(target_nodes))
        
    seeds = []
    for _ in range(k):
        counts = {}
        for rr in rr_sets:
            for node in rr:
                if node not in seeds:
                    counts[node] = counts.get(node, 0) + 1
        if not counts:
            break
        best_node = max(counts, key=counts.get)
        seeds.append(best_node)
        rr_sets = [rr for rr in rr_sets if best_node not in rr]
        
    return seeds

def cga_weighted_targeted(G, target_attr, target_val, k):
    target_nodes = [n for n, attr in G.nodes(data=True) if attr.get(target_attr) == target_val]
    subG = G.subgraph(target_nodes)
    undirected_subG = subG.to_undirected()
    
    # Em redes esparsas, Louvain encontrará pequenas "bolhas" isoladas dentro do grupo alvo
    communities_list = list(community.louvain_communities(undirected_subG, weight='weight', seed=42))
    
    comm_nodes = []
    for comm in communities_list:
        sorted_comm = sorted(list(comm), key=lambda n: G.out_degree(n, weight='weight'), reverse=True)
        comm_nodes.append(sorted_comm)
        
    seeds = []
    idx = 0
    while len(seeds) < k:
        added = False
        for comm in comm_nodes:
            if idx < len(comm):
                node = comm[idx]
                if node not in seeds:
                    seeds.append(node)
                    added = True
            if len(seeds) == k:
                break
        if not added: 
            break
        idx += 1
    return seeds

# ==========================================
# 3. CONFIGURAÇÃO DE ALTA COMPLEXIDADE E ESPARSIDADE
# ==========================================
if __name__ == "__main__":
    random.seed(42)
    np.random.seed(42)
    
    # 5 grupos de 100 usuários cada = 500 usuários na simulação
    # Grupos: 0=RPG (Alvo), 1=FPS, 2=Estratégia, 3=Simulação, 4=Indie
    sizes = [100, 100, 100, 100, 100]
    
    # MATRIZ DE PROBABILIDADE EXTREMAMENTE ESPARSA
    # Interno de cada grupo: 2.5% de chance de conexão (antes era 25%+)
    # Entre grupos diferentes: 0.05% de chance (interação raríssima)
    p_interno = 0.025
    p_externo = 0.0005
    
    probs = np.full((5, 5), p_externo)
    np.fill_diagonal(probs, p_interno)
    
    print("Construindo rede Steam complexa de 500 nós (Severamente Esparsa)...")
    G = nx.stochastic_block_model(sizes, probs.tolist(), seed=42, directed=True)
    
    # Mapeando os rótulos de interesses
    labels = {0: 'RPG', 1: 'FPS', 2: 'Estratégia', 3: 'Simulação', 4: 'Indie'}
    for node in G.nodes():
        group_idx = node // 100
        G.nodes[node]['categoria'] = labels[group_idx]
        
    # Injetando pesos baseados na proximidade PLN e Temporal
    for u, v in G.edges():
        cat_u = G.nodes[u]['categoria']
        cat_v = G.nodes[v]['categoria']
        
        if cat_u == cat_v:
            # Discussão interna do gênero (pesos moderados a altos)
            G[u][v]['weight'] = random.uniform(0.15, 0.55)
        else:
            # Ruído/Cruzamento casual entre gêneros (pesos muito fracos)
            G[u][v]['weight'] = random.uniform(0.01, 0.08)
            
    # Calculando a densidade real do grafo gerado para validação
    real_density = nx.density(G)
    avg_deg = np.mean([d for n, d in G.out_degree()])
    print(f"-> Densidade do Grafo: {real_density*100:.3f}% | Média de conexões por usuário: {avg_deg:.2f}")
    
    TARGET_ATTR = 'categoria'
    TARGET_VAL = 'RPG'
    MAX_K = 6
    X_TARGET_PERCENT = 25.0  # Em uma rede esparsa e segmentada, 25% já é um desafio crítico
    
    print(f"--- Iniciando Busca de Sementes (Alvo: {TARGET_VAL} | Meta: {X_TARGET_PERCENT}%) ---")
    
    algoritmos = {
        "Baseline (High-Out-Strength)": lambda k: baseline_high_out_strength(G, TARGET_ATTR, TARGET_VAL, k),
        "CELF (Lazy Greedy Ponderado)": lambda k: celf_weighted_targeted(G, TARGET_ATTR, TARGET_VAL, k),
        "RIS (Amostragem Reversa Ponderada)": lambda k: ris_weighted_targeted(G, TARGET_ATTR, TARGET_VAL, k),
        "CGA (Comunidades Locais)": lambda k: cga_weighted_targeted(G, TARGET_ATTR, TARGET_VAL, k)
    }
    
    for nome, alg_func in algoritmos.items():
        print(f"\n> Avaliando: {nome}")
        start_time = time.time()
        meta_atingida = False
        
        for k in range(1, MAX_K + 1):
            seeds = alg_func(k)
            # Simulação robusta final com 150 iterações de Monte Carlo
            porcentagem_atingida = simulate_weighted_icm_target(G, seeds, TARGET_ATTR, TARGET_VAL, mc=150)
            
            print(f"  k = {k} -> Sementes Utilizadas: {seeds[:3]}... -> Contágio: {porcentagem_atingida:.2f}%")
            
            if porcentagem_atingida >= X_TARGET_PERCENT and not meta_atingida:
                print(f"  [SUCESSO] Meta de {X_TARGET_PERCENT}% atingida com k = {k} sementes!")
                meta_atingida = True
                
        if not meta_atingida:
            print(f"  [FALHA] Não foi possível atingir a meta com até {MAX_K} sementes neste cenário esparso.")
        print(f"  Tempo: {time.time() - start_time:.2f}s")