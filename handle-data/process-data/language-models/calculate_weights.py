import json
import hashlib
from tqdm import tqdm

# Importa os modelos já carregados no seu models.py
from models import score_cardiff, score_cnerg, score_unitary, score_facebook

INPUT_EDGES  = "../results/graph_imed/edges.json"
OUT_TEXTS    = "../results/graph_imed/texts.json"
OUT_EDGES    = "../results/graph_imed/edges_enriched.json"

def make_text_id(text: str, user_id: str, date: str) -> str:
    """Gera um id único baseado no conteúdo + autor + data."""
    raw = f"{user_id}|{date}|{text}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def score_text(text: str, user_id: str, date: str) -> dict:
    s_cardiff = score_cardiff(text)

    # Aplicar modelo de tradução
    s_unitary   = score_unitary(text)

    s_cnerg   = score_cnerg(text)

    # Aplicar modelo de tradução
    s_facebook   = score_facebook(text)

    toxicity  = round(s_cardiff*0.35 + s_cnerg*0.30 + s_unitary*0.25 + s_facebook*0.10, 2)

    return {
        "id":             make_text_id(text, user_id, date),
        "text":           text,
        "m1_sentiment":   s_cardiff,
        "m2_unitary":     s_unitary,
        "m3_hate":        s_cnerg,
        "m4_facebook":    s_facebook,
        "toxicity_score": toxicity,
        "userId":         user_id,
        "date":           date,
    }


def enrich(edges: list) -> tuple[dict, list]:
    texts_map = {}
    seen_raw  = {}

    enriched_edges = []

    count = 0

    for edge in tqdm(edges, desc="Enriquecendo arestas"):

        if (count == 200):
            break

        type_edge = edge.get("type", "").strip()
        text = edge.get("text", "").strip()

        if not text or type_edge == "review":
            enriched_edges.append({**edge, "text": None})
            continue

        user_id = edge.get("src", "")
        date    = edge.get("src_date", "")

        cache_key = (user_id, date, text)

        if cache_key not in seen_raw:
            obj = score_text(text, user_id, date)
            seen_raw[cache_key]      = obj["id"]
            texts_map[obj["id"]]     = obj

        text_id = seen_raw[cache_key]
        enriched_edges.append({**edge, "text": text_id})

        count += 1

    return texts_map, enriched_edges


if __name__ == "__main__":
    print("Carregando arestas...")
    with open(INPUT_EDGES, "r", encoding="utf-8") as f:
        edges = json.load(f)
    print(f"  {len(edges):,} arestas carregadas.")

    texts_map, enriched_edges = enrich(edges)

    print(f"\nSalvando {len(texts_map):,} textos únicos em '{OUT_TEXTS}'...")
    with open(OUT_TEXTS, "w", encoding="utf-8") as f:
        json.dump(list(texts_map.values()), f, indent=2, ensure_ascii=False)

    print(f"Salvando {len(enriched_edges):,} arestas em '{OUT_EDGES}'...")
    with open(OUT_EDGES, "w", encoding="utf-8") as f:
        json.dump(enriched_edges, f, indent=2, ensure_ascii=False)

    skipped = sum(1 for e in enriched_edges if e.get("text") is None)
    print(f"\nPronto! ✓  ({skipped:,} arestas sem texto → text: null)")