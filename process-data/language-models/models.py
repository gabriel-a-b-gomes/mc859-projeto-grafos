import torch
from transformers import pipeline
import pandas as pd
from tabulate import tabulate
from tqdm import tqdm

device = 0 if torch.cuda.is_available() else -1

OUTPUT_PATH = "../results"
INPUT_FILE = "../texts/texts.txt"

print("Carregando Modelo 1 (CardiffNLP - Sentimento)...")
pipe_sentiment = pipeline(
    "text-classification", 
    model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
    return_all_scores=True,
    device=device
)

print("Carregando Modelo 2 (Unitary - Toxicidade)...")
pipe_toxic_unitary = pipeline(
    "text-classification", 
    model="unitary/unbiased-toxic-roberta",
    device=device
)

print("Carregando Modelo 3 (CNERG - Ódio/Agressividade)...")
pipe_toxic_cnerg = pipeline(
    "text-classification", model="Hate-speech-CNERG/dehatebert-mono-portugese", device=device
)

print("Carregando Modelo 4 (Facebook Dynabench R4)...")
pipe_toxic_fb = pipeline(
    "text-classification", 
    model="facebook/roberta-hate-speech-dynabench-r4-target", 
    device=device
)

def score_cardiff(text):
    if not text or str(text).strip() == "": return 5.0
    res = pipe_sentiment(text, truncation=True, max_length=512)
    scores = {item['label'].lower(): item['score'] for item in res}
    return round((scores.get('positive', 0.0) * 10) + (scores.get('neutral', 0.0) * 5), 2)

def score_unitary(text):
    if not text or str(text).strip() == "": return 5.0
    res = pipe_toxic_unitary(text, truncation=True, max_length=512)
    scores = {item['label'].lower(): item['score'] for item in res}
    score_toxic = scores.get('toxicity', scores.get('toxic', 0.0))
    return round((1 - score_toxic) * 10, 2)

def score_cnerg(text):
    if not text or str(text).strip() == "": return 5.0
    res = pipe_toxic_cnerg(text, truncation=True, max_length=512)
    scores = {item['label'].lower(): item['score'] for item in res}
    score_hate = scores.get('hate', 0.0)
    return round((1 - score_hate) * 10, 2)

def score_facebook(text):
    if not text or str(text).strip() == "": return 5.0
    res = pipe_toxic_fb(text, truncation=True, max_length=512)
    scores = {item['label'].lower(): item['score'] for item in res}
    score_hate = scores.get('hate', 0.0)
    return round((1 - score_hate) * 10, 2)


# with open(INPUT_FILE, "r", encoding="utf-8") as f:
#     steam_texts = [line.strip() for line in f if line.strip()]

# print(f"Total de textos carregados: {len(steam_texts)}")

# data = []

# for text in tqdm(steam_texts, desc="Progresso dos Modelos"):
#     data.append({
#         "Texto Original": text,
#         "Texto (Resumo)": text if len(text) <= 40 else text[:37] + "...",
#         "M1-Cardiff (Sentimento)": score_cardiff(text),
#         "M2-Unitary (Toxicidade)": score_unitary(text),
#         "M3-CNERG (Ódio)": score_cnerg(text),
#         "M4-Facebook (Moderação)": score_facebook(text)
#     })

# df = pd.DataFrame(data)

# df.to_csv("result_benchmark_models.csv", index=False, encoding="utf-8")

# df_html = df.drop(columns=["Texto (Resumo)"])
# df_html.to_html("result_benchmark_models.html", index=False, encoding="utf-8", classes="table table-striped")

# print("\n" + "="*70 + "\n          TABELA COMPARATIVA DE MODELOS (ESCALA 0 A 10)\n" + "="*70)

# df_terminal = df.drop(columns=["Texto Original"])

# print(tabulate(df_terminal, headers='keys', tablefmt='grid', showindex=False))

# print("\n[INFO] Os resultados completos foram salvos com sucesso em:")
# print(" -> CSV: 'resultado_benchmark_modelos.csv'")
# print(" -> HTML: 'resultado_benchmark_modelos.html' (Recomendado para ler os textos longos!)")
