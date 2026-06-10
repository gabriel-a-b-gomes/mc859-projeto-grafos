#!/usr/bin/env python3
"""
build_graph.py
==============
Processa um arquivo steam_data.jsonl e gera os arquivos de grafo calculando
os pesos das arestas baseando-se no arquivo de scores JSON unificado.
"""

import json
import subprocess
import sys
import os
import tempfile
import hashlib
from collections import Counter
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

INPUT_FILE   = sys.argv[1] if len(sys.argv) > 1 else "steam_data.jsonl"
TEMP_DIR     = tempfile.gettempdir()
REVIEWS_TEMP = os.path.join(TEMP_DIR, "steam_reviews_temp.tsv")
REVIEWS_SORT = os.path.join(TEMP_DIR, "steam_reviews_sorted.tsv")

WINDOW = 3

OUT_NODES    = "nodes.json"
OUT_EDGES    = "edges.json"
OUT_REVIEWS  = "review_edges.json"
OUT_COMMENTS = "comment_edges.json"

# --- NOVA CONFIGURAÇÃO DA ARQUIVO DE SCORES ---
SCORES_FILE  = sys.argv[2] if len(sys.argv) > 2 else "texts_enriched_merged.json"  # Arquivo gerado no passo anterior
SCORE_KEY    = "toxicity_score"  # Escolha a chave (ex: 'toxicity_score' ou 'm1_sentiment')
INVERT_SCORE = False  # True se nota ALTA significa ruim (ex: Toxicidade). False se nota ALTA significa bom.

# ---------------------------------------------------------------------------
# Helpers de Carregamento e Cálculo de Peso (Novos)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Helpers de Criptografia e Cálculo de Peso (Novos)
# ---------------------------------------------------------------------------

def get_text_hash(user: str, text: str, date_str: str) -> str:
    """Recreia o hash SHA-256 baseado na regra f'{user}_{text}_{date}'."""
    string_to_hash = f"{user}_{text}_{date_str}"
    return hashlib.sha256(string_to_hash.encode('utf-8')).hexdigest()


def load_scores(file_path: str) -> dict:
    """Carrega o JSON unificado mapeando diretamente o HASH_ID -> SCORE."""
    if not os.path.exists(file_path):
        print(f"⚠️ Aviso: Arquivo de scores '{file_path}' não encontrado. Pesos padrão (0) serão usados.")
        return {}
    
    print(f"📂 Carregando dicionário de scores por Hash ID de '{file_path}'...")
    with open(file_path, "r", encoding="utf-8") as f:
        dados = json.load(f)
    
    scores_map = {}
    for hash_id, info in dados.items():
        score = info.get(SCORE_KEY, None)
        if score is not None:
            scores_map[hash_id] = float(score)
            
    print(f"  Mapeados {len(scores_map):,} hashes únicos para scores.")
    return scores_map


def compute_edge_weight(s_src: float, s_dst: float) -> float:
    """Calcula o peso da aresta [-1, 1] baseado nos scores de origem e destino."""
    # 1. Normaliza para [0, 1] (onde 1 é semanticamente BOM e 0 é RUIM)
    if INVERT_SCORE:
        q_src = 1.0 - (s_src / 10.0)
        q_dst = 1.0 - (s_dst / 10.0)
    else:
        q_src = s_src / 10.0
        q_dst = s_dst / 10.0

    q_src = max(0.0, min(1.0, q_src))
    q_dst = max(0.0, min(1.0, q_dst))

    # 2. Translada para a escala [-1, 1]
    v_src = 2.0 * q_src - 1.0
    v_dst = 2.0 * q_dst - 1.0

    # 3. Aplicação da fórmula de oposição direcionada
    weight = v_dst * ((1.0 + v_src * v_dst) / 2.0)
    return round(weight, 4)


# ---------------------------------------------------------------------------
# Helpers de parse de data
# ---------------------------------------------------------------------------

MONTH_MAP = {
    "January":1,  "February":2,  "March":3,    "April":4,
    "May":5,       "June":6,      "July":7,     "August":8,
    "September":9, "October":10,  "November":11,"December":12,
    "Jan":1,  "Feb":2,  "Mar":3,  "Apr":4,  "Jun":6,
    "Jul":7,  "Aug":8,  "Sep":9,  "Oct":10, "Nov":11, "Dec":12,
}


def parse_review_date(s: str):
    s = s.strip()
    if "Last edited" in s:
        s = s[: s.index("Last edited")].strip()
    s = s.replace("Posted", "").replace(".", "").replace(",", "").strip()
    parts = s.split()
    if len(parts) == 3:
        try:
            return datetime(int(parts[2]), MONTH_MAP.get(parts[1], 0), int(parts[0]))
        except Exception:
            pass
    return None


def parse_comment_date(s: str):
    s = s.strip()
    try:
        date_part = s.split("@")[0].strip()
        parts = date_part.split()
        if len(parts) == 3:
            return datetime(
                int(parts[2]),
                MONTH_MAP.get(parts[1].replace(",", ""), 0),
                int(parts[0]),
            )
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# PASSO 1 — Primeira passagem (Coleção e salvamento de dados estruturados)
# ---------------------------------------------------------------------------

def pass_one(input_file: str):
    print(f"[1/3] Lendo {input_file} ...")
    users = {}
    comment_edges_raw = [] 
    total = 0

    with open(input_file, encoding="utf-8") as fin, \
         open(REVIEWS_TEMP, "w", encoding="utf-8") as rout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue

            uid = d.get("id", "").strip()
            if not uid:
                continue

            reviews  = d.get("reviews", []) or []
            comments = d.get("comments", []) or []

            if uid not in users:
                users[uid] = {"id": uid, "review_count": 0, "comment_count": 0}
            users[uid]["review_count"]  += len(reviews)
            users[uid]["comment_count"] += len(comments)

            # Salva reviews mantendo a data original sem quebras textuais no TSV
            for r in reviews:
                game = r.get("game", "").strip()
                raw_date = r.get("date", "").strip()
                dt   = parse_review_date(raw_date)
                text = r.get("review", "").strip()
                
                if game and dt:
                    # Escapa caracteres de quebra para não quebrar as linhas do arquivo TSV
                    escaped_text = text.replace("\n", "\\n").replace("\t", "\\t")
                    rout.write(f"{game}\t{uid}\t{dt.isoformat()}\t{escaped_text}\t{raw_date}\n")

            # Salva comentários retendo os metadados brutos em memória
            for c in comments:
                author = c.get("author", "").strip()
                raw_date = c.get("date", "").strip()
                date   = parse_comment_date(raw_date)
                text   = c.get("comment", "").strip()
                if author and date:
                    comment_edges_raw.append({
                        "profile_owner": uid,
                        "author":        author,
                        "date":          date.isoformat(),
                        "raw_date":      raw_date,
                        "text":          text,
                    })

            total += 1
            if total % 10_000 == 0:
                print(f"  {total:,} registros processados...")

    print(f"  Concluído: {len(users):,} usuários | {len(comment_edges_raw):,} arestas de comentário")
    return users, comment_edges_raw


# ---------------------------------------------------------------------------
# PASSO 2 — Ordenação e Construção de Arestas com Pesos por Hash
# ---------------------------------------------------------------------------

def sort_reviews():
    print("[2/3] Ordenando reviews por jogo e data ...")
    result = subprocess.run(
        ["sort", "-t", "\t", "-k1,1", "-k3,3", REVIEWS_TEMP, "-o", REVIEWS_SORT],
        capture_output=True,
    )
    if result.returncode != 0:
        print("  'sort' não encontrado, usando fallback Python ...")
        entries = []
        with open(REVIEWS_TEMP, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 4:
                    entries.append(parts)
        entries.sort(key=lambda x: (x[0], x[2]))
        with open(REVIEWS_SORT, "w", encoding="utf-8") as f:
            for parts in entries:
                f.write("\t".join(parts) + "\n")
    print("  Ordenação concluída.")


def build_review_edges(scores_map: dict):
    print("  Construindo arestas de review (Busca por Hash SHA-256) ...")
    review_edges = []
    seen = set()
    current_game = None
    current_entries = []

    def flush(game, entries):
      for i in range(len(entries)):
        u_curr, d_curr, t_curr, rd_curr = entries[i]
        
        start = max(0, i - WINDOW)

        for j in range(start, i):  # todos os anteriores
            u_prev, d_prev, t_prev, rd_prev = entries[j]

            if u_curr == u_prev:
                continue

            key = (u_prev, u_curr, game)

            if key not in seen:
                seen.add(key)
                # Reconstrói os hashes originais para buscar no mapa de score
                hash_src = get_text_hash(u_prev, t_prev, rd_prev)
                hash_dst = get_text_hash(u_curr, t_curr, rd_curr)
                
                s_src = scores_map.get(hash_src, 5.0)
                s_dst = scores_map.get(hash_dst, 5.0)
                
                weight = compute_edge_weight(s_src, s_dst)
                
                review_edges.append({
                    "src":        u_prev,
                    "dst":        u_curr,
                    "type":       "review",
                    "game":       game,
                    "src_date":   d_prev,
                    "dst_date":   d_curr,
                    "src_text":   t_prev,
                    "dst_text":   t_curr,
                    "src_score":  s_src,
                    "dst_score":  s_dst,
                    "weight":     weight
                })

    with open(REVIEWS_SORT, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 5:
                continue
            game, uid, dt, escaped_text, raw_date = parts[0], parts[1], parts[2], parts[3], parts[4]
            
            # Restaura o formato original do texto para bater com o Hash do arquivo unificado
            text = escaped_text.replace("\\n", "\n").replace("\\t", "\t")
            
            if game != current_game:
                if current_game and len(current_entries) > 1:
                    flush(current_game, current_entries)
                current_game = game
                current_entries = []
            current_entries.append((uid, dt, text, raw_date))

    if current_game and len(current_entries) > 1:
        flush(current_game, current_entries)

    print(f"  {len(review_edges):,} arestas de review geradas.")
    return review_edges


def build_comment_edges(comment_edges_raw: list, scores_map: dict) -> list:
    print("  Construindo arestas de comentário (Busca por Hash SHA-256) ...")

    from collections import defaultdict
    por_perfil = defaultdict(list)
    for entry in comment_edges_raw:
        por_perfil[entry["profile_owner"]].append(entry)

    comment_edges = []
    seen = set()

    for owner, entries in por_perfil.items():
        # Ordena por data
        entries_sorted = sorted(entries, key=lambda x: x["date"])

        # Cadeia: owner → entries[0], entries[0] → entries[1], ...
        chain = [(owner, None, "", "")] + [(e["author"], e["date"], e["text"], e["raw_date"]) for e in entries_sorted]

        for i in range(len(chain)):
            dst, dst_date, dst_text, dst_raw_date = chain[i]
            
            start = max(0, i - WINDOW)
            
            for j in range(start, i):  # todos os anteriores
                src, src_date, src_text, src_raw_date = chain[j]

                if src == dst:
                    continue

                key = (src, dst, owner)
                
                if key in seen:
                    continue
                seen.add(key)

                # Geração de hash dinâmico para os comentários consecutivamente encadeados
                hash_src = get_text_hash(src, src_text, src_raw_date) if src_raw_date else None
                hash_dst = get_text_hash(dst, dst_text, dst_raw_date)
                
                s_src = scores_map.get(hash_src, 5.0) if hash_src else 5.0
                s_dst = scores_map.get(hash_dst, 5.0)

                weight = compute_edge_weight(s_src, s_dst)

                edge = {
                    "src":            src,
                    "dst":            dst,
                    "type":           "comment",
                    "profile_owner":  owner,
                    "src_text":       src_text,
                    "dst_text":       dst_text,
                    "src_score":      s_src,
                    "dst_score":      s_dst,
                    "weight":         weight
                }
                if src_date:
                    edge["src_date"] = src_date
                if dst_date:
                    edge["dst_date"] = dst_date

                comment_edges.append(edge)

    print(f"  {len(comment_edges):,} arestas de comentário geradas.")
    return comment_edges


# ---------------------------------------------------------------------------
# PASSO 3 — Escrita de Resultados Estáveis
# ---------------------------------------------------------------------------

def save_outputs(users, comment_edges, review_edges):
    print("[3/3] Salvando arquivos de saída ...")

    nodes = list(users.values())
    comment_list = comment_edges 
    all_edges = review_edges + comment_list

    with open(OUT_NODES, "w", encoding="utf-8") as f:
        json.dump(nodes, f, indent=2, ensure_ascii=False)
    print(f"  → {OUT_NODES}  ({len(nodes):,} nós)")

    with open(OUT_REVIEWS, "w", encoding="utf-8") as f:
        json.dump(review_edges, f, indent=2, ensure_ascii=False)
    print(f"  → {OUT_REVIEWS}  ({len(review_edges):,} arestas)")

    with open(OUT_COMMENTS, "w", encoding="utf-8") as f:
        json.dump(comment_list, f, indent=2, ensure_ascii=False)
    print(f"  → {OUT_COMMENTS}  ({len(comment_list):,} arestas)")

    with open(OUT_EDGES, "w", encoding="utf-8") as f:
        json.dump(all_edges, f, indent=2, ensure_ascii=False)
    print(f"  → {OUT_EDGES}  ({len(all_edges):,} arestas no total)")

    out_deg = Counter(e["src"] for e in all_edges)
    in_deg  = Counter(e["dst"] for e in all_edges)
    print("\n=== RESUMO DO GRAFO ===")
    print(f"  Nós:              {len(nodes):>10,}")
    print(f"  Arestas review:   {len(review_edges):>10,}")
    print(f"  Arestas coment.:  {len(comment_list):>10,}")
    print(f"  Total arestas:    {len(all_edges):>10,}")


# ---------------------------------------------------------------------------
# Entrada da Execução Principal
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"Erro: arquivo '{INPUT_FILE}' não encontrado.")
        print("Uso: python build_graph.py [caminho_do_arquivo.jsonl]")
        sys.exit(1)

    # 1. Mapeia os Hashes SHA-256 contidos na fusão dos seus arquivos JSON
    scores_map = load_scores(SCORES_FILE)

    # 2. Extrai e gera os pipelines de dados temporários
    users, comment_edges_raw = pass_one(INPUT_FILE)
    sort_reviews()
    
    # 3. Processa grafos injetando o mapa com indexação criptográfica por Hash
    review_edges   = build_review_edges(scores_map)
    comment_edges  = build_comment_edges(comment_edges_raw, scores_map)
    save_outputs(users, comment_edges, review_edges)

    for tmp in [REVIEWS_TEMP, REVIEWS_SORT]:
        try:
            os.remove(tmp)
        except OSError:
            pass

    print("\nPronto! ✓")