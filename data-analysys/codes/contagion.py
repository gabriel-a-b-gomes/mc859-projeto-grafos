import json
import argparse
import random
import math
import copy
from collections import defaultdict, deque
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx


def load_graph(nodes_path: str, edges_path: str) -> nx.DiGraph:
    with open(nodes_path, encoding="utf-8") as f:
        nodes_data = json.load(f)
    with open(edges_path, encoding="utf-8") as f:
        edges_data = json.load(f)

    G = nx.DiGraph()

    for nd in nodes_data:
        G.add_node(
            nd["id"],
            review_count=nd.get("review_count", 0),
            comment_count=nd.get("comment_count", 0),
        )

    for ed in edges_data:
        G.add_edge(
            ed["src"],
            ed["dst"],
            weight=float(ed.get("weight", 1.0)),
            dst_score=float(ed.get("dst_score", 5.0)),
            src_score=float(ed.get("src_score", 5.0)),
            game=ed.get("game", ""),
            src_date=ed.get("src_date", ""),
            dst_date=ed.get("dst_date", ""),
            src_text=ed.get("src_text", ""),
            dst_text=ed.get("dst_text", ""),
        )

    for n in list(G.nodes):
        if "review_count" not in G.nodes[n]:
            G.nodes[n]["review_count"] = 0
            G.nodes[n]["comment_count"] = 0

    print(f"\n[Grafo carregado]  nós={G.number_of_nodes()}  arestas={G.number_of_edges()}")
    return G


def toxicity_score(G: nx.DiGraph, node_id: str, threshold: float = 6.0) -> bool:
    scores = [
        data["dst_score"]
        for _, _, data in G.in_edges(node_id, data=True)
        if "dst_score" in data
    ]
    if not scores:
        return False
    return (sum(scores) / len(scores)) >= threshold


def seed_nodes(G: nx.DiGraph, n_seeds: int, threshold: float = 6.0) -> list:
    avg_score = {}
    for node in G.nodes():
        scores = [d["dst_score"] for _, _, d in G.in_edges(node, data=True) if "dst_score" in d]
        avg_score[node] = sum(scores) / len(scores) if scores else 0.0

    sorted_nodes = sorted(avg_score, key=avg_score.get, reverse=True)
    return sorted_nodes[:n_seeds]


def run_sis_sir(
    G: nx.DiGraph,
    mode: str = "sis",
    beta: float = 0.35,
    gamma: float = 0.15,
    tox_threshold: float = 6.0,
    n_seeds: int = 3,
    steps: int = 30,
    seed: int = 42,
) -> dict:
    random.seed(seed)
    nodes = list(G.nodes())
    states = {n: "S" for n in nodes}

    for n in seed_nodes(G, n_seeds, tox_threshold):
        states[n] = "I"

    history = {"S": [], "I": [], "R": []}

    def count():
        c = {"S": 0, "I": 0, "R": 0}
        for s in states.values():
            c[s] += 1
        return c

    for step in range(steps):
        c = count()
        history["S"].append(c["S"])
        history["I"].append(c["I"])
        history["R"].append(c["R"])

        new_states = copy.copy(states)

        for node in nodes:
            if states[node] == "S":
                p_infect = 0.0
                for pred in G.predecessors(node):
                    if states[pred] == "I":
                        w = G[pred][node].get("weight", 1.0)
                        p_infect += beta * w
                p_infect = min(p_infect, 0.999)
                if random.random() < p_infect:
                    new_states[node] = "I"

            elif states[node] == "I":
                if random.random() < gamma:
                    new_states[node] = "R" if mode == "sir" else "S"

        states = new_states

    c = count()
    history["S"].append(c["S"])
    history["I"].append(c["I"])
    history["R"].append(c["R"])

    r0 = beta / gamma if gamma > 0 else float("inf")
    peak_i = max(history["I"])
    peak_t = history["I"].index(peak_i)

    result = {
        "mode": mode.upper(),
        "beta": beta,
        "gamma": gamma,
        "R0": round(r0, 3),
        "peak_infected": peak_i,
        "peak_step": peak_t,
        "final_states": states,
        "history": history,
        "steps": steps,
    }

    print(f"\n{'='*50}")
    print(f"  MODELO {mode.upper()}")
    print(f"{'='*50}")
    print(f"  β (infecção)    = {beta}")
    print(f"  γ (recuperação) = {gamma}")
    print(f"  R₀              = {r0:.3f}  ({'endêmico' if r0 > 1 else 'se extingue'})")
    print(f"  Pico infectados = {peak_i} usuários (passo t={peak_t})")
    print(f"  Estado final    : S={c['S']}  I={c['I']}  R={c['R']}")

    return result


def compute_threshold(node_attrs: dict, base_theta: float, sigma: float) -> float:
    activity = (node_attrs.get("review_count", 0) + node_attrs.get("comment_count", 0)) / 50.0
    theta = random.gauss(base_theta, sigma) + activity * 0.15
    return max(0.02, min(0.95, theta))


def run_granovetter(
    G: nx.DiGraph,
    base_theta: float = 0.30,
    sigma: float = 0.12,
    n_seeds: int = 2,
    weighted: bool = True,
    tox_threshold: float = 6.0,
    max_rounds: int = 50,
    seed: int = 42,
) -> dict:
    random.seed(seed)
    nodes = list(G.nodes())

    thresholds = {
        n: compute_threshold(G.nodes[n], base_theta, sigma)
        for n in nodes
    }

    adopted = {n: False for n in nodes}
    for n in seed_nodes(G, n_seeds, tox_threshold):
        adopted[n] = True

    rounds_data = []
    round_num = 0
    changed = True

    while changed and round_num < max_rounds:
        changed = False
        new_adopted = copy.copy(adopted)
        adopted_this_round = []

        for node in nodes:
            if adopted[node]:
                continue

            in_neighbors = list(G.predecessors(node))
            if not in_neighbors:
                continue

            if weighted:
                total_w = sum(G[u][node].get("weight", 1.0) for u in in_neighbors)
                infected_w = sum(
                    G[u][node].get("weight", 1.0)
                    for u in in_neighbors if adopted[u]
                )
                frac = infected_w / total_w if total_w > 0 else 0.0
            else:
                infected_count = sum(1 for u in in_neighbors if adopted[u])
                frac = infected_count / len(in_neighbors)

            if frac >= thresholds[node]:
                new_adopted[node] = True
                adopted_this_round.append(node)
                changed = True

        adopted = new_adopted
        rounds_data.append(len(adopted_this_round))
        round_num += 1

    total_infected = sum(adopted.values())
    total_nodes = len(nodes)
    pct = 100.0 * total_infected / total_nodes if total_nodes > 0 else 0

    result = {
        "base_theta": base_theta,
        "sigma": sigma,
        "weighted": weighted,
        "total_infected": total_infected,
        "total_nodes": total_nodes,
        "infection_pct": round(pct, 2),
        "rounds": round_num,
        "rounds_data": rounds_data,
        "thresholds": thresholds,
        "final_adopted": adopted,
    }

    print(f"\n{'='*50}")
    print(f"  MODELO LIMIAR DE GRANOVETTER")
    print(f"{'='*50}")
    print(f"  Limiar base θ   = {base_theta}  (σ={sigma})")
    print(f"  Ponderado       = {weighted}")
    print(f"  Rodadas         = {round_num}")
    print(f"  Infectados      = {total_infected} / {total_nodes} ({pct:.1f}%)")
    print(f"  Maior cascata   = rodada {rounds_data.index(max(rounds_data))+1 if rounds_data else 0}"
          f" ({max(rounds_data) if rounds_data else 0} usuários)")

    return result


def run_betweenness(
    G: nx.DiGraph,
    top_k: int = 10,
    weight_attr: str = "weight",
    normalized: bool = True,
    k_approx: int = None,
) -> dict:
    print(f"\n{'='*50}")
    print(f"  CENTRALIDADE DE INTERMEDIAÇÃO")
    print(f"{'='*50}")

    n = G.number_of_nodes()
    if k_approx is None and n > 2000:
        k_approx = min(500, n)
        print(f"  Grafo grande ({n} nós) — usando k={k_approx} amostras")

    print(f"  Calculando... (nós={n}  arestas={G.number_of_edges()})")

    scores = nx.betweenness_centrality(
        G,
        normalized=normalized,
        weight=weight_attr,
        k=k_approx,
    )

    node_details = []
    for node, bc in scores.items():
        attrs = G.nodes[node]
        in_scores = [d["dst_score"] for _, _, d in G.in_edges(node, data=True) if "dst_score" in d]
        avg_tox = sum(in_scores) / len(in_scores) if in_scores else 0.0
        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)

        node_details.append({
            "id": node,
            "betweenness": round(bc, 6),
            "avg_tox_score": round(avg_tox, 3),
            "in_degree": in_deg,
            "out_degree": out_deg,
            "review_count": attrs.get("review_count", 0),
            "comment_count": attrs.get("comment_count", 0),
        })

    node_details.sort(key=lambda x: x["betweenness"], reverse=True)
    top_nodes = node_details[:top_k]

    print(f"\n  {'#':<4} {'ID':<22} {'Betweenness':>12} {'Tox.Score':>10} {'In':>5} {'Out':>5}")
    print(f"  {'-'*60}")
    for i, nd in enumerate(top_nodes, 1):
        print(f"  {i:<4} {nd['id']:<22} {nd['betweenness']:>12.6f} "
              f"{nd['avg_tox_score']:>10.3f} {nd['in_degree']:>5} {nd['out_degree']:>5}")

    result = {
        "scores": scores,
        "top_nodes": top_nodes,
        "top_k": top_k,
        "all_details": node_details,
    }
    return result


def plot_sis_sir(result: dict, save_path: str = None):
    fig, axes = plt.subplots(1, 2, figsize=(13, 4))
    fig.suptitle(f"Modelo {result['mode']} — Propagação de Toxicidade (R₀={result['R0']})",
                 fontsize=13, fontweight="bold")

    h = result["history"]
    t = list(range(len(h["S"])))

    ax = axes[0]
    ax.plot(t, h["S"], color="#378ADD", lw=2, label="Suscetíveis (S)")
    ax.plot(t, h["I"], color="#E24B4A", lw=2, label="Infectados (I)")
    if result["mode"] == "SIR":
        ax.plot(t, h["R"], color="#1D9E75", lw=2, label="Recuperados (R)")
    ax.axvline(result["peak_step"], color="#E24B4A", ls="--", alpha=0.5, lw=1)
    ax.set_xlabel("Passo de tempo")
    ax.set_ylabel("Número de usuários")
    ax.set_title("Curva epidemiológica")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.2)
    ax.set_xlim(0, result["steps"])

    ax2 = axes[1]
    fs = result["final_states"]
    counts = {"S": 0, "I": 0, "R": 0}
    for s in fs.values():
        counts[s] += 1
    labels = [k for k, v in counts.items() if v > 0]
    sizes  = [v for v in counts.values() if v > 0]
    colors = {"S": "#378ADD", "I": "#E24B4A", "R": "#1D9E75"}
    clrs   = [colors[l] for l in labels]
    full_labels = {"S": "Suscetíveis", "I": "Infectados", "R": "Recuperados"}
    ax2.pie(sizes, labels=[full_labels[l] for l in labels],
            colors=clrs, autopct="%1.1f%%", startangle=90,
            textprops={"fontsize": 9})
    ax2.set_title("Distribuição final dos estados")

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  Gráfico salvo: {save_path}")
    else:
        plt.show()
    plt.close()


def plot_granovetter(result: dict, save_path: str = None):
    fig, axes = plt.subplots(1, 2, figsize=(13, 4))
    fig.suptitle(
        f"Modelo de Limiar de Granovetter — θ={result['base_theta']} σ={result['sigma']}\n"
        f"{result['total_infected']}/{result['total_nodes']} usuários infectados "
        f"({result['infection_pct']}%) em {result['rounds']} rodadas",
        fontsize=11, fontweight="bold"
    )

    ax = axes[0]
    rd = result["rounds_data"]
    bars = ax.bar(range(1, len(rd)+1), rd, color="#E24B4A", alpha=0.8, edgecolor="white", lw=0.5)
    ax.set_xlabel("Rodada")
    ax.set_ylabel("Usuários que adotaram")
    ax.set_title("Cascata por rodada")
    ax.grid(axis="y", alpha=0.2)
    for bar in bars:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width()/2, h + 0.1, str(int(h)),
                    ha="center", va="bottom", fontsize=8)

    ax2 = axes[1]
    ths = list(result["thresholds"].values())
    adopted = result["final_adopted"]
    th_adopted  = [result["thresholds"][n] for n in adopted if adopted[n]]
    th_resisted = [result["thresholds"][n] for n in adopted if not adopted[n]]

    bins = np.linspace(0, 1, 20)
    ax2.hist(th_resisted, bins=bins, color="#378ADD", alpha=0.7, label="Resistiram")
    ax2.hist(th_adopted,  bins=bins, color="#E24B4A", alpha=0.7, label="Adotaram")
    ax2.axvline(result["base_theta"], color="black", ls="--", lw=1, label=f"θ base={result['base_theta']}")
    ax2.set_xlabel("Limiar individual θ")
    ax2.set_ylabel("Número de usuários")
    ax2.set_title("Distribuição de limiares")
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.2)

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  Gráfico salvo: {save_path}")
    else:
        plt.show()
    plt.close()


def plot_betweenness(G: nx.DiGraph, result: dict, save_path: str = None):
    top = result["top_nodes"]
    scores = result["scores"]
    all_det = result["all_details"]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Centralidade de Intermediação — Pontes de Contágio",
                 fontsize=13, fontweight="bold")

    ax = axes[0]
    ids_short = ["…" + nd["id"][-8:] for nd in top]
    vals = [nd["betweenness"] for nd in top]
    tox  = [nd["avg_tox_score"] for nd in top]
    colors = ["#E24B4A" if v > 0.7*max(vals) else
              "#EF9F27" if v > 0.35*max(vals) else "#378ADD"
              for v in vals]
    bars = ax.barh(ids_short[::-1], vals[::-1], color=colors[::-1], alpha=0.85, edgecolor="white")
    ax.set_xlabel("Betweenness (normalizado)")
    ax.set_title(f"Top {len(top)} usuários ponte")
    ax.grid(axis="x", alpha=0.2)
    for bar, v in zip(bars, vals[::-1]):
        ax.text(v + max(vals)*0.01, bar.get_y() + bar.get_height()/2,
                f"{v:.4f}", va="center", fontsize=8)

    ax2 = axes[1]
    all_bc  = [d["betweenness"]    for d in all_det]
    all_tox = [d["avg_tox_score"]  for d in all_det]
    all_deg = [d["in_degree"] + d["out_degree"] for d in all_det]
    scatter_colors = ["#E24B4A" if b > 0.7*max(all_bc) else
                      "#EF9F27" if b > 0.35*max(all_bc) else "#85B7EB"
                      for b in all_bc]
    sizes = [max(20, min(200, d*10)) for d in all_deg]
    ax2.scatter(all_tox, all_bc, c=scatter_colors, s=sizes, alpha=0.7, edgecolors="white", lw=0.3)
    ax2.set_xlabel("Score médio de toxicidade (dst_score)")
    ax2.set_ylabel("Betweenness Centrality")
    ax2.set_title("Betweenness × Toxicidade")
    ax2.grid(alpha=0.15)

    patches = [
        mpatches.Patch(color="#E24B4A", label="Alta centralidade"),
        mpatches.Patch(color="#EF9F27", label="Média centralidade"),
        mpatches.Patch(color="#85B7EB", label="Baixa centralidade"),
    ]
    ax2.legend(handles=patches, fontsize=8)

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  Gráfico salvo: {save_path}")
    else:
        plt.show()
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Análise de contágio comportamental no grafo Steam"
    )
    parser.add_argument("--nodes", default="nodes.json",  help="Caminho para nodes.json")
    parser.add_argument("--edges", default="edges.json",  help="Caminho para edges.json")
    parser.add_argument("--model", default="all",
                        choices=["all", "sis", "sir", "granovetter", "betweenness"],
                        help="Modelo a executar")

    parser.add_argument("--beta",    type=float, default=0.35,  help="Taxa de infecção β")
    parser.add_argument("--gamma",   type=float, default=0.15,  help="Taxa de recuperação γ")
    parser.add_argument("--steps",   type=int,   default=30,    help="Passos de simulação")
    parser.add_argument("--seeds",   type=int,   default=3,     help="Nós semente iniciais")
    parser.add_argument("--tox_threshold", type=float, default=6.0,
                        help="Score acima do qual o nó é considerado tóxico")

    parser.add_argument("--theta",   type=float, default=0.30,  help="Limiar base θ")
    parser.add_argument("--sigma",   type=float, default=0.12,  help="Dispersão σ do limiar")
    parser.add_argument("--weighted", action="store_true", default=True,
                        help="Ponderar influência pelo peso da aresta")

    parser.add_argument("--top_k",   type=int,   default=10,    help="Top-K nós ponte")
    parser.add_argument("--k_approx", type=int,  default=None,
                        help="Amostras para betweenness aproximado (None = exato)")

    parser.add_argument("--save_plots", action="store_true",
                        help="Salvar gráficos em PNG em vez de exibir")
    parser.add_argument("--save_json", action="store_true",
                        help="Exportar resultados em JSON")

    args = parser.parse_args()
    run_all = args.model == "all"

    G = load_graph(args.nodes, args.edges)

    results = {}

    if run_all or args.model == "sis":
        res = run_sis_sir(G, mode="sis", beta=args.beta, gamma=args.gamma,
                          tox_threshold=args.tox_threshold, n_seeds=args.seeds,
                          steps=args.steps)
        results["sis"] = res
        plot_sis_sir(res, save_path="plot_sis.png" if args.save_plots else None)

    if run_all or args.model == "sir":
        res = run_sis_sir(G, mode="sir", beta=args.beta, gamma=args.gamma,
                          tox_threshold=args.tox_threshold, n_seeds=args.seeds,
                          steps=args.steps)
        results["sir"] = res
        plot_sis_sir(res, save_path="plot_sir.png" if args.save_plots else None)

    if run_all or args.model == "granovetter":
        res = run_granovetter(G, base_theta=args.theta, sigma=args.sigma,
                              n_seeds=args.seeds, weighted=args.weighted,
                              tox_threshold=args.tox_threshold)
        results["granovetter"] = res
        plot_granovetter(res, save_path="plot_granovetter.png" if args.save_plots else None)

    if run_all or args.model == "betweenness":
        res = run_betweenness(G, top_k=args.top_k, k_approx=args.k_approx)
        results["betweenness"] = res
        plot_betweenness(G, res, save_path="plot_betweenness.png" if args.save_plots else None)

    if args.save_json:
        export = {}
        for key, val in results.items():
            if key in ("sis", "sir"):
                export[key] = {k: v for k, v in val.items() if k != "final_states"}
                export[key]["final_states_summary"] = {
                    s: list(val["final_states"].values()).count(s)
                    for s in ("S", "I", "R")
                }
            elif key == "granovetter":
                export[key] = {
                    "base_theta": val["base_theta"],
                    "sigma": val["sigma"],
                    "weighted": val["weighted"],
                    "total_infected": val["total_infected"],
                    "total_nodes": val["total_nodes"],
                    "infection_pct": val["infection_pct"],
                    "rounds": val["rounds"],
                    "rounds_data": val["rounds_data"],
                }
            elif key == "betweenness":
                export[key] = {"top_nodes": val["top_nodes"]}

        with open("results.json", "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)
        print("\n  Resultados exportados: results.json")

    print("\nConcluído.\n")


if __name__ == "__main__":
    main()
