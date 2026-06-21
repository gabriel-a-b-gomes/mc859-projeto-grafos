import networkx as nx
import random
import numpy as np
from networkx.algorithms import community
import time

# ==========================================
# 1. MODELO DE DIFUSÃO (Weighted ICM Localizado)
# ==========================================
def simulate_weighted_icm_target(G, seeds, target_attr, target_val, mc=150):
    """Simula o contágio ponderado e retorna o percentual de infectados no grupo alvo."""
    target_nodes = set([n for n, attr in G.nodes(data=True) if attr.get(target_attr) == target_val])
    if not target_nodes or not seeds:
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
# 2. ALGORITMOS DE SELEÇÃO DE SEMENTES
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
# 3. NOVO AVALIADOR: BUSCA PELO CONJUNTO MÍNIMO
# ==========================================
def avaliar_metas_contagio(G, alg_func, target_attr, target_val, metas, max_k=15, mc=150):
    """
    Testa progressivamente subconjuntos do conjunto de sementes máximo gerado
    pelo algoritmo para encontrar o MENOR k que atinge cada meta de contágio.
    """
    # Gera o conjunto máximo permitido de uma vez para eficiência computacional
    max_seeds = alg_func(G, target_attr, target_val, max_k)
    
    resultados = {meta: {"k": None, "spread": 0.0} for meta in metas}
    
    # Avalia progressivamente adicionando uma semente por vez
    for k in range(1, len(max_seeds) + 1):
        sementes_atuais = max_seeds[:k]
        spread_atual = simulate_weighted_icm_target(G, sementes_atuais, target_attr, target_val, mc)
        
        # Registra o k mínimo para cada meta que acabou de ser batida
        for meta in metas:
            if resultados[meta]["k"] is None and spread_atual >= meta:
                resultados[meta]["k"] = k
                resultados[meta]["spread"] = spread_atual
                
        # Se todas as metas já foram batidas, podemos parar a busca
        if all(res["k"] is not None for res in resultados.values()):
            break
            
    return resultados

# ==========================================
# 4. EXECUÇÃO DO CENÁRIO STEAM (ESPARSO)
# ==========================================
if __name__ == "__main__":
    random.seed(42)
    np.random.seed(42)
    
    qtyuser = 250
    
    # Grafo Esparso Steam: 5 Grupos de 250 usuários
    sizes = [qtyuser, qtyuser, qtyuser, qtyuser, qtyuser]
    p_interno = 0.035  # Densidade baixa nas bolhas
    p_externo = 0.001  # Interações inter-grupo raríssimas
    
    probs = np.full((5, 5), p_externo)
    np.fill_diagonal(probs, p_interno)
    
    print(f"Construindo rede Steam Direcionada, Ponderada e Esparsa ({qtyuser} nós)...")
    G = nx.stochastic_block_model(sizes, probs.tolist(), seed=42, directed=True)
    
    labels = {0: 'RPG', 1: 'FPS', 2: 'Estratégia', 3: 'Simulacao', 4: 'Indie'}
    for node in G.nodes():
        G.nodes[node]['categoria'] = labels[node // qtyuser]
        
    for u, v in G.edges():
        if G.nodes[u]['categoria'] == G.nodes[v]['categoria']:
            G[u][v]['weight'] = random.uniform(0.20, 0.60) # Alto peso interno PLN/Tempo
        else:
            G[u][v]['weight'] = random.uniform(0.01, 0.10) # Ruído externo
            
    TARGET_ATTR = 'categoria'
    TARGET_VAL = ['RPG', 'FPS', 'Estratégia', 'Simulacao', 'Indie']
    MAX_K_PERMITIDO = 15
    METAS_X = [10.0, 25.0, 40.0, 60.0] # Testaremos encontrar conjuntos para 10%, 25% e 40%
    
    algoritmos = {
        "Baseline (Out-Strength)": baseline_high_out_strength,
        "CGA (Comunidades Locais)": cga_weighted_targeted,
        "RIS (Amostragem Reversa)": ris_weighted_targeted,
        "CELF (Lazy Greedy)": celf_weighted_targeted
    }
    
    for target in TARGET_VAL:
      print(f"\n--- Iniciando Avaliação de Conjunto Semente Mínimo (Alvo: {target}) ---")
      print(f"Limitação Orçamentária Máxima testada: k={MAX_K_PERMITIDO}")
      
      tabela_resultados = {nome: {} for nome in algoritmos.keys()}
      
      for nome, alg_func in algoritmos.items():
          print(f"  Avaliando {nome}...")
          start_time = time.time()
          
          # Roda o avaliador que retorna o menor k para as metas solicitadas
          resultados = avaliar_metas_contagio(G, alg_func, TARGET_ATTR, target, METAS_X, MAX_K_PERMITIDO, mc=150)
          tabela_resultados[nome] = resultados
          
          print(f"  [Concluído em {time.time() - start_time:.1f}s]")

      # --- IMPRESSÃO DO RELATÓRIO FINAL ---
      print("\n" + "="*70)
      print("RELATÓRIO: TAMANHO MÍNIMO DO CONJUNTO SEMENTE (k)")
      print("="*70)
      
      # Cabeçalho da tabela
      header = f"{'Algoritmo':<30} |"
      for meta in METAS_X:
          header += f" Meta {int(meta)}% |"
      print(header)
      print("-" * 70)
      
      # Linhas de resultado
      for nome in algoritmos.keys():
          row = f"{nome:<30} |"
          for meta in METAS_X:
              k_val = tabela_resultados[nome][meta]["k"]
              spread = tabela_resultados[nome][meta]["spread"]
              if k_val is not None:
                  row += f" k = {k_val:<6} |"
              else:
                  row += f" FALHOU   |"
          print(row)
      print("="*70)