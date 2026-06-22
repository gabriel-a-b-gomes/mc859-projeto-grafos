import json
import time
import random
import argparse
import signal
import sys
from collections import deque, defaultdict

import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

class LightGraph:
    """
    Representação compacta usando listas de adjacência em dicionários.
    Muito mais rápida que nx.DiGraph para BFS repetido em grafos grandes.
    """
    def __init__(self):
        self.nodes      = []     
        self.node_set   = set()
        self.successors = defaultdict(list)  
        self.predecessors = defaultdict(list) 
        self.edge_data  = {}           

    def add_node(self, nid, **attrs):
        if nid not in self.node_set:
            self.nodes.append(nid)
            self.node_set.add(nid)

    def add_edge(self, u, v, **attrs):
        if u not in self.node_set:
            self.add_node(u)
        if v not in self.node_set:
            self.add_node(v)
        self.successors[u].append(v)
        self.predecessors[v].append(u)
        self.edge_data[(u, v)] = attrs

    def get_weight(self, u, v, attr="weight", default=0.0):
        return self.edge_data.get((u, v), {}).get(attr, default)

    def number_of_nodes(self):
        return len(self.nodes)

    def number_of_edges(self):
        return len(self.edge_data)


def load_light_graph(nodes_path: str, edges_path: str) -> LightGraph:
    with open(nodes_path, encoding="utf-8") as f:
        nodes_data = json.load(f)
    with open(edges_path, encoding="utf-8") as f:
        edges_data = json.load(f)

    G = LightGraph()
    for nd in nodes_data:
        G.add_node(nd["id"],
                   review_count=nd.get("review_count", 0),
                   comment_count=nd.get("comment_count", 0))
    for ed in edges_data:
        w = float(ed.get("weight", 0.0))
        
        toxic_cost = round(1.0 - max(0.0, -w), 6)
        G.add_edge(ed["src"], ed["dst"],
                   weight=w,
                   toxic_cost=toxic_cost,
                   src_score=float(ed.get("src_score", 5.0)),
                   dst_score=float(ed.get("dst_score", 5.0)))

    all_w = [G.edge_data[e]["weight"] for e in G.edge_data]
    n_tox = sum(1 for w in all_w if w < 0)
    print(f"\n  Nós    : {G.number_of_nodes():,}")
    print(f"  Arestas: {G.number_of_edges():,}")
    print(f"  Tóxicas: {n_tox:,} ({100*n_tox/len(all_w):.1f}%)")
    return G



def _brandes_single_source(G: LightGraph, source: str, scores: dict):
    """
    Executa Brandes a partir de uma única fonte e acumula em `scores`.
    Usa BFS (não ponderado) — adequado quando o grafo é grande e
    a topologia importa mais que os pesos para intermediação.
    """
    stack   = []
    pred    = defaultdict(list)  
    sigma   = defaultdict(int)    
    dist    = defaultdict(lambda: -1)
    delta   = defaultdict(float)  

    sigma[source] = 1
    dist[source]  = 0
    queue = deque([source])

    # BFS
    while queue:
        v = queue.popleft()
        stack.append(v)
        for w in G.successors[v]:
            if dist[w] < 0:                      
                queue.append(w)
                dist[w] = dist[v] + 1
            if dist[w] == dist[v] + 1:          
                sigma[w] += sigma[v]
                pred[w].append(v)

    # Acumulação reversa (back-propagation)
    while stack:
        w = stack.pop()
        for v in pred[w]:
            if sigma[w] > 0:
                delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w])
        if w != source:
            scores[w] += delta[w]



def run_betweenness_sampled(
    G: LightGraph,
    k: int = 200,
    top_k: int = 10,
    checkpoint_path: str = None,
    resume_path: str = None,
    seed: int = 42,
) -> dict:

    random.seed(seed)
    n = G.number_of_nodes()
    all_nodes = G.nodes

    # Amostra de fontes
    k_actual = min(k, n)
    sources = random.sample(all_nodes, k_actual)

    # Scores acumulados
    scores = defaultdict(float)
    start_idx = 0

    # Retomar de checkpoint
    if resume_path:
        try:
            with open(resume_path, encoding="utf-8") as f:
                ckpt = json.load(f)
            scores = defaultdict(float, {k: v for k, v in ckpt["scores"].items()})
            processed = set(ckpt["processed_sources"])
            sources = [s for s in sources if s not in processed]
            start_idx = ckpt["sources_done"]
            print(f"  Retomando checkpoint: {start_idx} fontes já processadas")
        except Exception as e:
            print(f"  Aviso: não foi possível carregar checkpoint ({e}), começando do zero")

    processed_sources = []
    interrupted = False

    def save_checkpoint(done_count):
        if not checkpoint_path:
            return
        ckpt = {
            "sources_done": start_idx + done_count,
            "k_total": k_actual,
            "processed_sources": processed_sources,
            "scores": dict(scores),
        }
        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(ckpt, f)

    def handle_interrupt(sig, frame):
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGINT, handle_interrupt)

    # ── Barra de progresso ────────────────────────────────────
    bar_fmt = (
        "{desc}\n"
        "  {percentage:5.1f}%  [{elapsed}<{remaining}]  "
        "{n}/{total} fontes  [{rate_fmt}]"
    )

    t_start = time.time()

    with tqdm(
        total=len(sources),
        bar_format="{l_bar}{bar}| {n}/{total} [{elapsed}<{remaining}, {rate_fmt}]",
        ncols=80,
        colour="cyan",
    ) as pbar:

        for i, source in enumerate(sources):
            if interrupted:
                print("\n\n  Interrompido pelo usuário — salvando checkpoint...")
                save_checkpoint(i)
                break

            _brandes_single_source(G, source, scores)
            processed_sources.append(source)

            # Atualiza descrição da barra a cada 5 fontes
            if i % 5 == 0 and scores:
                top3 = sorted(scores, key=scores.get, reverse=True)[:3]
                top3_str = " | ".join(f"…{nid[-6:]}" for nid in top3)
                pbar.set_description(f"Top-3 até agora: {top3_str}")

            # Checkpoint a cada 10 fontes
            if checkpoint_path and i % 10 == 0 and i > 0:
                save_checkpoint(i)

            pbar.update(1)

    if not interrupted:
        save_checkpoint(len(sources))


    total_done = start_idx + len(processed_sources)
    norm = (n - 1) * (n - 2)
    scale = (n / total_done) if total_done > 0 else 1.0  # correção amostral

    normalized = {
        node: (scores[node] * scale) / norm if norm > 0 else 0.0
        for node in all_nodes
    }

    # ── Enriquecer resultados ─────────────────────────────────
    node_details = []
    for node in all_nodes:
        bc = normalized.get(node, 0.0)
        out_weights = [G.edge_data[(node, v)]["weight"]
                       for v in G.successors[node]
                       if (node, v) in G.edge_data]
        in_weights  = [G.edge_data[(u, node)]["weight"]
                       for u in G.predecessors[node]
                       if (u, node) in G.edge_data]

        tox_idx = float(np.mean(out_weights)) if out_weights else 0.0
        toxic_out = sum(1 for w in out_weights if w < 0)
        toxic_in  = sum(1 for w in in_weights  if w < 0)

        node_details.append({
            "id":             node,
            "betweenness":    round(bc, 8),
            "toxicity_index": round(tox_idx, 4),
            "toxic_edges_out": toxic_out,
            "toxic_edges_in":  toxic_in,
            "in_degree":      len(G.predecessors[node]),
            "out_degree":     len(G.successors[node]),
        })

    node_details.sort(key=lambda x: x["betweenness"], reverse=True)
    top_nodes = node_details[:top_k]

    elapsed = time.time() - t_start
    print(f"\n  Tempo total   : {elapsed/60:.1f} min ({elapsed:.1f}s)")
    print(f"  Fontes usadas : {total_done} / {n} ({100*total_done/n:.1f}% do grafo)")
    print(f"\n  {'#':<4} {'ID (últimos 10)':<22} {'Betweenness':>12} "
          f"{'ToxIdx':>8} {'ToxOut':>7} {'In':>5} {'Out':>5}")
    print(f"  {'─'*65}")
    for i, nd in enumerate(top_nodes, 1):
        flag = " ⚠" if nd["toxicity_index"] < -0.1 else ""
        print(f"  {i:<4} …{nd['id'][-10:]:<21} {nd['betweenness']:>12.8f} "
              f"{nd['toxicity_index']:>8.4f} {nd['toxic_edges_out']:>7} "
              f"{nd['in_degree']:>5} {nd['out_degree']:>5}{flag}")
    print(f"\n  ⚠ = alta centralidade + índice tóxico negativo (vetor crítico)\n")

    return {
        "top_nodes":    top_nodes,
        "all_details":  node_details,
        "scores":       normalized,
        "sources_used": total_done,
        "elapsed_sec":  round(elapsed, 1),
    }



def plot_betweenness(result: dict, save_path: str = None):

    top      = result["top_nodes"]
    all_det  = result["all_details"]
    max_bc   = max(nd["betweenness"] for nd in all_det) if all_det else 1

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(
        f"Centralidade de Intermediação — Pontes de Contágio Tóxico\n"
        f"({result['sources_used']:,} fontes amostradas  |  "
        f"tempo: {result['elapsed_sec']/60:.1f} min)",
        fontsize=12, fontweight="bold"
    )

    COLORS = {
        "toxic":   "#E24B4A",
        "neutral": "#EF9F27",
        "healthy": "#1D9E75",
    }

    def node_color(tox_idx):
        if tox_idx < -0.1: return COLORS["toxic"]
        if tox_idx <  0.1: return COLORS["neutral"]
        return COLORS["healthy"]

    ax = axes[0]
    ids_s = [f"…{nd['id'][-10:]}" for nd in top]
    vals  = [nd["betweenness"]    for nd in top]
    tidxs = [nd["toxicity_index"] for nd in top]
    clrs  = [node_color(t) for t in tidxs]
    bars  = ax.barh(ids_s[::-1], vals[::-1], color=clrs[::-1], alpha=0.85, edgecolor="white")
    for bar, v in zip(bars, vals[::-1]):
        ax.text(v + max_bc * 0.01, bar.get_y() + bar.get_height()/2,
                f"{v:.6f}", va="center", fontsize=8) 
    ax.set_xlabel("Betweenness Centrality (normalizado, amostral)")
    ax.set_title(f"Top {len(top)} usuários ponte")
    ax.grid(axis="x", alpha=0.15)

    patches = [
        mpatches.Patch(color=COLORS["toxic"],   label="Índice tóxico (< -0.1)"),
        mpatches.Patch(color=COLORS["neutral"],  label="Neutro"),
        mpatches.Patch(color=COLORS["healthy"],  label="Saudável (> 0.1)"),
    ]
    ax.legend(handles=patches, fontsize=8, loc="lower right")

    ax2 = axes[1]
    sample = all_det[:2000]
    all_bc  = [d["betweenness"]    for d in sample]
    all_tox = [d["toxicity_index"] for d in sample]
    all_deg = [d["in_degree"] + d["out_degree"] for d in sample]
    sc_clrs = [node_color(t) for t in all_tox]
    sizes   = [max(10, min(150, dg * 5)) for dg in all_deg]
    ax2.scatter(all_tox, all_bc, c=sc_clrs, s=sizes, alpha=0.6,
                edgecolors="white", linewidths=0.2)
    ax2.axvline(0,    color="gray",          ls="--", lw=0.8, alpha=0.4)
    ax2.axvline(-0.1, color=COLORS["toxic"], ls=":",  lw=0.8, alpha=0.5)
    ymax = max(all_bc) if all_bc else 1
    ax2.fill_betweenx([0, ymax], -1, -0.1, alpha=0.05, color=COLORS["toxic"])
    ax2.text(-0.95, ymax * 0.92, "zona de risco\n(bridge tóxico)",
             fontsize=7, color=COLORS["toxic"], alpha=0.7)
    ax2.set_xlabel("Índice de toxicidade (média weights de saída)")
    ax2.set_ylabel("Betweenness Centrality")
    ax2.set_title("Betweenness × Toxicidade  (amostra de 2k nós)")
    ax2.legend(handles=patches, fontsize=8)
    ax2.grid(alpha=0.1)

    plt.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  Gráfico salvo: {save_path}")
    else:
        plt.show()
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Betweenness Centrality amostral com progresso para grafos grandes"
    )
    parser.add_argument("--nodes",      default="nodes.json")
    parser.add_argument("--edges",      default="edges.json")
    parser.add_argument("--k",          type=int, default=200,
                        help="Nº de fontes amostradas (mais = mais preciso, mais lento)")
    parser.add_argument("--top_k",      type=int, default=10)
    parser.add_argument("--seed",       type=int, default=42)
    parser.add_argument("--checkpoint", type=str, default="betweenness_ckpt.json",
                        help="Arquivo para salvar progresso automaticamente")
    parser.add_argument("--resume",     type=str, default=None,
                        help="Retomar de um checkpoint salvo anteriormente")
    parser.add_argument("--save_plot",  type=str, default=None,
                        help="Caminho para salvar o gráfico (ex: plot_bc.png)")
    parser.add_argument("--save_json",  type=str, default=None,
                        help="Caminho para exportar resultados (ex: bc_results.json)")
    args = parser.parse_args()

    print("=" * 52)
    print("  BETWEENNESS CENTRALITY — GRAFO STEAM")
    print("=" * 52)
    print(f"  k={args.k} fontes  |  top_k={args.top_k}  |  seed={args.seed}")
    if args.resume:
        print(f"  Retomando: {args.resume}")
    print()

    G = load_light_graph(args.nodes, args.edges)

    n = G.number_of_nodes()
    e = G.number_of_edges()
    est_sec = args.k * (n + e) / 200_000
    print(f"\n  Estimativa de tempo: ~{est_sec/60:.0f}–{est_sec*2/60:.0f} min "
          f"(varia com densidade da rede)")
    print(f"  Progresso salvo em : {args.checkpoint}")
    print(f"  Ctrl+C salva e sai com resultado parcial válido\n")

    result = run_betweenness_sampled(
        G,
        k=args.k,
        top_k=args.top_k,
        checkpoint_path=args.checkpoint,
        resume_path=args.resume,
        seed=args.seed,
    )

    if args.save_json:
        export = {
            "sources_used": result["sources_used"],
            "elapsed_sec":  result["elapsed_sec"],
            "top_nodes":    result["top_nodes"],
        }
        with open(args.save_json, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)
        print(f"  Resultados exportados: {args.save_json}")

    plot_betweenness(result, save_path=args.save_plot)


if __name__ == "__main__":
    main()