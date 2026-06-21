import requests
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import time
import os
import random
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("STEAM_API_KEY")
BASE = "https://api.steampowered.com"

def fetch_details(steam_id):
    """Busca detalhes com retry simples e logs de erro."""
    details = {"steam_id": steam_id, "pais": "N/A", "total_jogos": 0, "jogos_ids": ""}
    
    try:
        # 1. Resumo do Jogador
        r_sum = requests.get(f"{BASE}/ISteamUser/GetPlayerSummaries/v2/", 
                             params={"key": API_KEY, "steamids": steam_id}, timeout=10)
        
        if r_sum.status_code == 200:
            p = r_sum.json().get("response", {}).get("players", [])
            if p:
                details["pais"] = p[0].get("loccountrycode", "N/A")
        elif r_sum.status_code == 429:
            print("Rate limit atingido (429)! Reduza a velocidade.")

        # Pequena pausa entre chamadas para o mesmo ID para não sobrecarregar
        time.sleep(0.25)

        # 2. Jogos Possuídos
        r_games = requests.get(f"{BASE}/IPlayerService/GetOwnedGames/v0001/", 
                               params={"key": API_KEY, "steamid": steam_id}, timeout=10)
        
        if r_games.status_code == 200:
            resp = r_games.json().get("response", {})
            # Se o perfil for privado, 'response' pode estar vazio {}
            if resp:
                details["total_jogos"] = resp.get("game_count", 0)
                games = resp.get("games", [])
                details["jogos_ids"] = ";".join([str(g.get("appid")) for g in games])
            else:
                # Perfil Privado
                details["total_jogos"] = "PRIVADO" 

    except Exception as e:
        print(f"Erro de conexão no ID {steam_id}: {e}")
    
    return details

# def processar_bloco(lista_ids, thread_id):
#     print(f"Thread {thread_id} iniciada com {len(lista_ids)} registros.")
#     resultados = []
#     for steam_id in lista_ids:
#         # Garante que pegamos apenas o ID caso o input seja uma lista de listas
#         if isinstance(steam_id, list):
#             id_limpo = steam_id[0]
#         else:
#             id_limpo = steam_id

#         # Print para debug - remova se for muito volume
#         # print(f"Thread {thread_id} -> {id_limpo}")
        
#         resultados.append(fetch_details(id_limpo))
        
#         # ESSENCIAL: Pausa aleatória para evitar bloqueio da API
#         # Com 10 threads, isso média 10 requisições por segundo
#         time.sleep(random.uniform(0.2, 0.5))
        
#     return resultados
  
def processar_bloco(lista_ids, thread_id, progress, task_id):
    """
    lista_ids: lista de IDs
    thread_id: número da thread
    progress: objeto de progresso do Rich
    task_id: ID da barra específica desta thread
    """
    results = []
    for steam_id in lista_ids:
        # Atualiza a descrição com o ID atual
        progress.update(task_id, description=f"[cyan]Thread {thread_id}[/cyan] -> {steam_id}")
        
        if isinstance(steam_id, list):
            clean_id = steam_id[0]
        else:
            clean_id = steam_id
        results.append(fetch_details(clean_id))
        
        # Simula a chamada da API (substitua pelo seu fetch_details)
        time.sleep(random.uniform(0.2, 0.8))
        
        # Avança 1 unidade na barra
        progress.advance(task_id)
        
    return results

if __name__ == "__main__":
    # 1. Carregar IDs
    df_original = pd.read_csv('steam_nos.csv')
    
    # 2. Dividir em 10 partes
    blocos = np.array_split(df_original, 10)
    
    print(f"Iniciando processamento paralelo com 10 threads...")
    start_time = time.time()

    # 3. Executar Threads
    final_data = []
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    ) as progress:
      
      with ThreadPoolExecutor(max_workers=10) as executor:
          # Envia cada bloco para uma thread
          futures = []
          for i, bloco in enumerate(blocos):
              # Cria uma tarefa visual para cada thread
              t_id = progress.add_task(f"Thread {i} iniciando...", total=len(bloco))
              futures.append(executor.submit(processar_bloco, bloco.tolist(), i, progress, t_id))
          
          # Aguarda todas terminarem
          for future in futures:
              final_data.extend(future.result())
        
    # 4. Salvar resultado final
    df_final = pd.DataFrame(final_data)
    df_final.to_csv('steam_detalhes_completo.csv', index=False)
    
    end_time = time.time()
    print(f"Concluído em {end_time - start_time:.2f} segundos!")