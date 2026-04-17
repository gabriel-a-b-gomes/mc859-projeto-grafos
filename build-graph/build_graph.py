#!/usr/bin/env python3
"""
build_graph.py
==============
Processa um arquivo steam_data.jsonl e gera os arquivos de grafo:
  - nodes.json        → todos os usuários encontrados
  - review_edges.json → arestas direcionais entre usuários que reviewaram o mesmo jogo
  - comment_edges.json→ arestas do dono do perfil para quem comentou
  - edges.json        → todas as arestas juntas

Uso:
    python build_graph.py [caminho_do_arquivo.jsonl]

    Se nenhum caminho for passado, usa "steam_data.jsonl" no diretório atual.
"""

import json
import subprocess
import sys
import os
import tempfile
from collections import Counter
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

INPUT_FILE   = sys.argv[1] if len(sys.argv) > 1 else "steam_data.jsonl"
TEMP_DIR     = tempfile.gettempdir()
REVIEWS_TEMP = os.path.join(TEMP_DIR, "steam_reviews_temp.tsv")
REVIEWS_SORT = os.path.join(TEMP_DIR, "steam_reviews_sorted.tsv")

OUT_NODES    = "nodes.json"
OUT_EDGES    = "edges.json"
OUT_REVIEWS  = "review_edges.json"
OUT_COMMENTS = "comment_edges.json"

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
    """
    Formatos esperados:
      'Posted 10 September, 2025.'
      'Posted 10 September, 2025.\t\t\tLast edited 24 February.'
    """
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
    """
    Formato esperado: '3 Oct, 2025 @ 6:33pm'
    """
    s = s.strip()
    try:
        date_part = s.split("@")[0].strip().rstrip(",")
        parts = date_part.split()
        if len(parts) == 3:
            return datetime(
                int(parts[2].replace(",", "")),
                MONTH_MAP.get(parts[1], 0),
                int(parts[0]),
            )
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# PASSO 1 — Primeira passagem: coleta usuários, comentários e escreve reviews
# ---------------------------------------------------------------------------

def pass_one(input_file: str):
    print(f"[1/3] Lendo {input_file} ...")
    users = {}
    comment_edges = set()
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

            # Reviews → linha no arquivo temporário: game \t uid \t iso_date
            for r in reviews:
                game = r.get("game", "").strip()
                dt   = parse_review_date(r.get("date", ""))
                if game and dt:
                    rout.write(f"{game}\t{uid}\t{dt.isoformat()}\n")

            # Comentários → aresta dono-do-perfil → quem comentou
            for c in comments:
                author = c.get("author", "").strip()
                if author:
                    comment_edges.add((uid, author))

            total += 1
            if total % 10_000 == 0:
                print(f"  {total:,} registros processados...")

    print(f"  Concluído: {len(users):,} usuários | {len(comment_edges):,} arestas de comentário")
    return users, comment_edges


# ---------------------------------------------------------------------------
# PASSO 2 — Ordenar arquivo de reviews e construir arestas
# ---------------------------------------------------------------------------

def sort_reviews():
    """Ordena por game (col 1) e depois por data ISO (col 3)."""
    print("[2/3] Ordenando reviews por jogo e data ...")
    # 'sort' nativo do SO (Unix/Linux/macOS) é muito eficiente para arquivos grandes
    result = subprocess.run(
        ["sort", "-t", "\t", "-k1,1", "-k3,3", REVIEWS_TEMP, "-o", REVIEWS_SORT],
        capture_output=True,
    )
    if result.returncode != 0:
        # Fallback Python puro (mais lento, mas funciona no Windows também)
        print("  'sort' não encontrado, usando fallback Python ...")
        entries = []
        with open(REVIEWS_TEMP, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) == 3:
                    entries.append(parts)
        entries.sort(key=lambda x: (x[0], x[2]))
        with open(REVIEWS_SORT, "w", encoding="utf-8") as f:
            for parts in entries:
                f.write("\t".join(parts) + "\n")
    print("  Ordenação concluída.")


def build_review_edges():
    """
    Lê o arquivo ordenado e emite arestas consecutivas:
      review[i] → review[i+1]  (dentro do mesmo jogo, por ordem de data)

    Nota: pares consecutivos evitam explosão combinatorial em jogos populares
    (ex.: CS2 tem 16k reviews → 132M pares se todos forem conectados).
    """
    print("  Construindo arestas de review (pares consecutivos) ...")
    review_edges = []
    seen = set()
    current_game = None
    current_entries = []  # list of (uid, date_str)

    def flush(game, entries):
        for i in range(len(entries) - 1):
            u1, d1 = entries[i]
            u2, d2 = entries[i + 1]
            if u1 == u2:
                continue
            key = (u1, u2, game)
            if key not in seen:
                seen.add(key)
                review_edges.append({
                    "src":      u1,
                    "dst":      u2,
                    "type":     "review",
                    "game":     game,
                    "src_date": d1,
                    "dst_date": d2,
                })

    with open(REVIEWS_SORT, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 3:
                continue
            game, uid, dt = parts
            if game != current_game:
                if current_game and len(current_entries) > 1:
                    flush(current_game, current_entries)
                current_game = game
                current_entries = []
            current_entries.append((uid, dt))

    if current_game and len(current_entries) > 1:
        flush(current_game, current_entries)

    print(f"  {len(review_edges):,} arestas de review geradas.")
    return review_edges


# ---------------------------------------------------------------------------
# PASSO 3 — Salvar arquivos de saída
# ---------------------------------------------------------------------------

def save_outputs(users, comment_edges, review_edges):
    print("[3/3] Salvando arquivos de saída ...")

    nodes = list(users.values())
    comment_list = [{"src": s, "dst": d, "type": "comment"} for s, d in comment_edges]
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

    # Estatísticas de grau
    out_deg = Counter(e["src"] for e in all_edges)
    in_deg  = Counter(e["dst"] for e in all_edges)
    print("\n=== RESUMO DO GRAFO ===")
    print(f"  Nós:              {len(nodes):>10,}")
    print(f"  Arestas review:   {len(review_edges):>10,}")
    print(f"  Arestas coment.:  {len(comment_list):>10,}")
    print(f"  Total arestas:    {len(all_edges):>10,}")
    print("\n  Top 5 out-degree:")
    for uid, deg in out_deg.most_common(5):
        print(f"    {uid}: {deg}")
    print("\n  Top 5 in-degree:")
    for uid, deg in in_deg.most_common(5):
        print(f"    {uid}: {deg}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"Erro: arquivo '{INPUT_FILE}' não encontrado.")
        print("Uso: python build_graph.py [caminho_do_arquivo.jsonl]")
        sys.exit(1)

    users, comment_edges = pass_one(INPUT_FILE)
    sort_reviews()
    review_edges = build_review_edges()
    save_outputs(users, comment_edges, review_edges)

    # Limpeza dos temporários
    for tmp in [REVIEWS_TEMP, REVIEWS_SORT]:
        try:
            os.remove(tmp)
        except OSError:
            pass

    print("\nPronto! ✓")
