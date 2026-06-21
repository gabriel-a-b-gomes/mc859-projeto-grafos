import json

# Configurações
arquivo_entrada = 'steam_data.jsonl'
arquivo_saida = 'steam_data_hash.jsonl'

mapeamento = {}
contador_user = 0

def obter_anonimo(id_real):
    global contador_user, mapeamento
    id_str = str(id_real).strip()
    if id_str not in mapeamento:
        mapeamento[id_str] = f"user_{contador_user:06d}"
        contador_user += 1
    return mapeamento[id_str]

print("Iniciando a limpeza profunda e anonimização do .jsonl...")

with open(arquivo_entrada, 'r', encoding='utf-8') as f_in, \
     open(arquivo_saida, 'w', encoding='utf-8') as f_out:
    
    for linha in f_in:
        try:
            dados = json.loads(linha)
            
            if 'id' in dados:
                dados['id'] = obter_anonimo(dados['id'])
            
            if 'comments' in dados and isinstance(dados['comments'], list):
                for c in dados['comments']:
                    if 'author' in c:
                        c['author'] = obter_anonimo(c['author'])
                    if 'comment' in c:
                        c['comment'] = "[REMOVED_TEXT]"
            
            if 'reviews' in dados and isinstance(dados['reviews'], list):
                for r in dados['reviews']:
                    if 'author' in r:
                        r['author'] = obter_anonimo(r['author'])
                    
                    campos_texto_review = ['review', 'text', 'body', 'content']
                    for campo in campos_texto_review:
                        if campo in r:
                            r[campo] = "[REMOVED_TEXT]"
            
            f_out.write(json.dumps(dados, ensure_ascii=False) + '\n')
            
        except json.JSONDecodeError:
            continue

print(f"\nConcluído!")
print(f"Arquivo gerado: {arquivo_saida}")
print(f"Total de identidades protegidas: {len(mapeamento)}")