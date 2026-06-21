import json
import hashlib
from concurrent.futures import ThreadPoolExecutor
import os

def generate_hash_id(user_id, text, date):
  raw_string = f"{str(user_id).strip()}_{str(text).strip()}_{str(date).strip()}"
  
  hash_object = hashlib.sha256(raw_string.encode('utf-8'))
  return hash_object.hexdigest()

def process_line(line):
  if not line.strip():
    return []
  
  try:
    data = json.loads(line)
    steam_id = data.get("id")
    formatted_items = []

    for review in data.get("reviews", []):
      rev_text = review.get("text", review.get("review", ""))
      rev_date = review.get("date", "")
      
      unique_id = generate_hash_id(steam_id, rev_text, rev_date)

      formatted_items.append({
        "id": unique_id,
        "text": rev_text,
        "userId": steam_id,
        "date": rev_date
      })

    for comment in data.get("comments", []):
      com_text = comment.get("comment", "")
      com_user = comment.get("author", "")
      com_date = comment.get("date", "")
      
      unique_id = generate_hash_id(com_user, com_text, com_date)

      formatted_items.append({
        "id": unique_id,
        "text": com_text,
        "userId": com_user,
        "date": com_date
      })

    return formatted_items
  except json.JSONDecodeError:
    return []

def process_steam_data(input_file, output_file, max_workers=4):
  final_results = []
  print(f"Iniciando processamento de '{input_file}'...")
  
  with open(input_file, 'r', encoding='utf-8') as f:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
      results = executor.map(process_line, f, chunksize=100)
      for result_list in results:
        if result_list:
          final_results.extend(result_list)

  print(f"Processamento concluído. Salvando {len(final_results)} itens em '{output_file}'...")
  with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(final_results, f, ensure_ascii=False, indent=2)
  
  print("Pronto!")

if __name__ == "__main__":
  input_filename = "../../collect-data/steam_data.jsonl"
  output_filename = "../results/texts/texts_mapped.json"
  
  cores = os.cpu_count() or 2
  threads = cores * 2

  print(threads)
  
  # process_steam_data(input_filename, output_filename, max_workers=threads)