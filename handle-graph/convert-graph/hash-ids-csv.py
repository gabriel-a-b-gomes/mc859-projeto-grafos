import pandas as pd
import os

# Configurações
pasta_entrada = 'friendship-graph'
pasta_saida = 'hash-friendship-graph'

if not os.path.exists(pasta_saida):
    os.makedirs(pasta_saida)

print("Passo 1: Mapeando todos os IDs únicos...")
ids_unicos = set()
arquivos_csv = [f for f in os.listdir(pasta_entrada) if f.endswith('.csv')]

for arq in arquivos_csv:
    df = pd.read_csv(os.path.join(pasta_entrada, arq))
    for col in ['steam_id', 'source', 'target']:
        if col in df.columns:
            ids_unicos.update(df[col].astype(str).str.strip().unique())

mapeamento = {str(old_id): f"user_{i:05d}" for i, old_id in enumerate(sorted(ids_unicos))}
print(f"Total de {len(ids_unicos)} usuários mapeados.")

if ids_unicos:
    exemplo_id = list(ids_unicos)[0]
    print(f"Teste de mapeamento: ID {exemplo_id} vira -> {mapeamento[exemplo_id]}")

print("\nPasso 2: Gerando arquivos anonimizados...")
for arq in arquivos_csv:
    df = pd.read_csv(os.path.join(pasta_entrada, arq))
    
    colunas_encontradas = [c for c in ['steam_id', 'source', 'target'] if c in df.columns]
    
    for col in colunas_encontradas:
        df[col] = df[col].astype(str).str.strip().map(mapeamento)
    
    if df[colunas_encontradas].isnull().values.any():
        print(f"AVISO: Alguns IDs no arquivo {arq} não foram mapeados corretamente!")

    for sensivel in ['nickname', 'name']:
        if sensivel in df.columns:
            df = df.drop(columns=[sensivel])
    
    caminho_out = os.path.join(pasta_saida, arq)
    df.to_csv(caminho_out, index=False)
    print(f"Arquivo salvo: {arq}")

print("\nProcesso concluído!")