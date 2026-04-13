import requests
import networkx as nx
import matplotlib.pyplot as plt
import networkx as nx
import requests
import csv
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("STEAM_API_KEY")
BASE = "https://api.steampowered.com"

def get_friends(steam_id):
    url = f"{BASE}/ISteamUser/GetFriendList/v1/"
    params = {
        "key": API_KEY,
        "steamid": steam_id,
        "relationship": "friend"
    }
    r = requests.get(url, params=params,  timeout=10)
    if r.status_code != 200:
        return []
    data = r.json()
    if "friendslist" not in data:
        return []
    return [f["steamid"] for f in data["friendslist"]["friends"]]

def extrair_ids(seeds, profundidade=1):
    G = nx.Graph()
    visitados = set()
    fila = [(s, 0) for s in seeds]
    
    while fila:
        atual_id, nivel = fila.pop(0)
        if atual_id in visitados or nivel > profundidade:
            continue
            
        visitados.add(atual_id)
        print(f"Rastreando amigos de: {atual_id} | Total acumulado: {len(visitados)}")
        
        try:
            amigos = get_friends(atual_id) 
            for amigo_id in amigos:
                # O NetworkX cria automaticamente o nó 'amigo_id' se ele não existir
                G.add_edge(atual_id, amigo_id)
            
                if nivel < profundidade:
                  fila.append((amigo_id, nivel + 1))
                    
        except Exception as e:
            print(f"Erro ao buscar dados do ID {atual_id}: {e}")
    
    return G

def salvar_grafo_csv(G, arquivo_nos='steam_nos.csv', arquivo_arestas='steam_arestas.csv'):
    """Salva os nós (com atributos dinâmicos) e arestas em CSV."""
    
    # Salvar Nós
    with open(arquivo_nos, mode='w', newline='', encoding='utf-8') as f:
        # Pega todas as chaves de atributos que existem no grafo para fazer o cabeçalho
        todas_chaves = set()
        for _, dados in G.nodes(data=True):
            todas_chaves.update(dados.keys())
            
        cabecalho = ['steam_id'] + list(todas_chaves)
        writer = csv.DictWriter(f, fieldnames=cabecalho)
        writer.writeheader()
        
        for steam_id, dados in G.nodes(data=True):
            linha = {'steam_id': steam_id}
            linha.update(dados)
            writer.writerow(linha)
            
    # Salvar Arestas
    with open(arquivo_arestas, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['source', 'target'])
        for u, v in G.edges():
            writer.writerow([u, v])
            
    print(f"Grafo salvo com sucesso em '{arquivo_nos}' e '{arquivo_arestas}'.")

if __name__ == "__main__":
    seeds = [
      "76561198320338168", 
      "76561199159522860"
    ]
    graph = extrair_ids(seeds, profundidade=2)
    
    salvar_grafo_csv(graph)
    print(f"Salvo {graph.number_of_nodes()} IDs para processamento.")