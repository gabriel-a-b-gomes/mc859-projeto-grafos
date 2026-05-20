import json
import os

INPUT_FILE = "../../collect-data/steam_data.jsonl"
OUTPUT_FILE = "texts.txt"

def extract_texts():
  if not os.path.exists(INPUT_FILE):
    print(f"Erro: O arquivo {INPUT_FILE} não foi encontrado no diretório atual.")
    return

  texts = []
  
  print("Processando arquivo JSONL...")
  
  with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for index, line in enumerate(f, 1):
      if (index == 100):
        break

      if not line.strip():
        continue
      
      try:
        user_data = json.loads(line)
        
        if "reviews" in user_data and isinstance(user_data["reviews"], list):
          for r in user_data["reviews"]:
            review = r.get("review", "").strip()
            if review:
              aux = " ".join(review.split())
              texts.append(aux)
        
        if "comments" in user_data and isinstance(user_data["comments"], list):
          for c in user_data["comments"]:
            comment = c.get("comment", "").strip()
            if comment:
              aux = " ".join(comment.split())
              texts.append(aux)
                      
      except json.JSONDecodeError:
        print(f"Aviso: Falha ao ler a linha {index}. Pulando...")

  print(f"Salvando {len(texts)} textos em '{OUTPUT_FILE}'...")
  with open(OUTPUT_FILE, "w", encoding="utf-8") as f_out:
    for text in texts:
      f_out.write(text + "\n")
          
  print("Extração concluída com sucesso!")

if __name__ == "__main__":
  extract_texts()