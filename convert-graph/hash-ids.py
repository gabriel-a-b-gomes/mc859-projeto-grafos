import json
import os

pastas = ['reviews-comment-graph-v1', 'reviews-comment-graph-v2', 'reviews-comment-graph-v3']
arquivos_por_pasta = ['comment_edges', 'nodes', 'edges', 'review_edges']

print("Mapeando IDs únicos...")
ids_unicos = set()

for pasta in pastas:
    print("Pasta: " + pasta)
    for nome_arq in arquivos_por_pasta:
        caminho = os.path.join(pasta, f"{nome_arq}.json")
        print("Processando arquivo: " + caminho)
        if os.path.exists(caminho):
            with open(caminho, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                for item in dados:
                    for campo in ['id', 'src', 'dst', 'profile_owner']:
                        if campo in item:
                            ids_unicos.add(str(item[campo]))

mapeamento = {old_id: f"user_{i}" for i, old_id in enumerate(ids_unicos)}
print(f"Total de {len(ids_unicos)} usuários únicos encontrados.")

print("Iniciando a substituição e salvando novos arquivos...")

for pasta in pastas:
    pasta_saida = f"hash_{pasta}"
    if not os.path.exists(pasta_saida):
        os.makedirs(pasta_saida)
    
    for nome_arq in arquivos_por_pasta:
        caminho_in = os.path.join(pasta, f"{nome_arq}.json")
        caminho_out = os.path.join(pasta_saida, f"{nome_arq}.json")
        
        if os.path.exists(caminho_in):
            with open(caminho_in, 'r', encoding='utf-8') as f:
                dados = json.load(f)
            
            for item in dados:
                if 'id' in item:
                    item['id'] = mapeamento[str(item['id'])]
                if 'src' in item:
                    item['src'] = mapeamento[str(item['src'])]
                if 'dst' in item:
                    item['dst'] = mapeamento[str(item['dst'])]
                if 'profile_owner' in item:
                    item['profile_owner'] = mapeamento[str(item['profile_owner'])]
                
            with open(caminho_out, 'w', encoding='utf-8') as f:
                json.dump(dados, f, indent=2)
            print(f"Arquivo salvo: {caminho_out}")

print("\nProcesso concluído! Verifique as pastas 'hash_...'")