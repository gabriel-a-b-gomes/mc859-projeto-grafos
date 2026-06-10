import os
import json

def merge_json_files(src_folder, dst_file):
    """
    Junta todos os arquivos .json de uma pasta específica em um único arquivo final.
    """
    merged_data = {}
    processed_files = 0

    # Verifica se a pasta informada realmente existe
    if not os.path.exists(src_folder):
        print(f"❌ Erro: A pasta '{src_folder}' não foi encontrada.")
        return

    print(f"🔍 Buscando arquivos JSON em: {src_folder}...")

    # Listar e iterar por todos os arquivos da pasta
    for file_name in os.listdir(src_folder):
        # Filtrar apenas arquivos que terminam com .json
        if file_name.endswith('.json'):
            entire_path = os.path.join(src_folder, file_name)
            
            try:
                with open(entire_path, 'r', encoding='utf-8') as arquivo:
                    content = json.load(arquivo)
                    
                    # Garante que o conteúdo lido é um dicionário antes de mesclar
                    if isinstance(content, dict):
                        merged_data.update(content)
                        processed_files += 1
                    else:
                        print(f"⚠️ Aviso: O arquivo '{file_name}' foi pulado pois não está no formato de dicionário esperado.")
            
            except json.JSONDecodeError:
                print(f"❌ Erro: '{file_name}' não pôde ser lido. O arquivo pode estar corrompido ou mal formatado.")
            except Exception as e:
                print(f"❌ Erro inesperado ao ler '{file_name}': {e}")

    # Salvar o dicionário gigante gerado em um novo arquivo JSON
    if merged_data:
        try:
            with open(dst_file, 'w', encoding='utf-8') as arquivo_final:
                # indent=4 deixa o arquivo legível para humanos. 
                # ensure_ascii=False garante que emojis (como 🌩️) e acentos sejam salvos corretamente.
                json.dump(merged_data, arquivo_final, ensure_ascii=False, indent=4)
            
            print("\n" + "="*40)
            print(f"🎉 Sucesso! Processo concluído.")
            print(f"📁 Total de arquivos lidos: {processed_files}")
            print(f"💾 Arquivo final gerado em: {dst_file}")
            print("="*40)
            
        except Exception as e:
            print(f"❌ Erro ao salvar o arquivo final: {e}")
    else:
        print("ℹ️ Nenhum dado válido foi encontrado para ser mesclado.")

# --- CONFIGURAÇÃO E EXECUÇÃO ---
if __name__ == "__main__":
    # Substitua pelo caminho real da sua pasta (pode ser relativo ou absoluto)
    FILES_FOLDERS = "../../dataset/texts-processed" 
    
    # Nome do arquivo final que será gerado
    OUTPUT_FILE = "../../dataset/final-graph/texts_enriched_merged.json"

    # Executa a função
    merge_json_files(FILES_FOLDERS, OUTPUT_FILE)