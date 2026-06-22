import json
import copy
import random
import argparse
from pathlib import Path
from collections import defaultdict

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx

class WeightRange:
    def __init__(self, w_min: float, w_max: float, tox_cutoff: float = None):
        if w_min >= w_max:
            raise ValueError(f"w_min ({w_min}) deve ser menor que w_max ({w_max})")
        self.w_min = w_min
        self.w_max = w_max
        self.cutoff = tox_cutoff if tox_cutoff is not None else (w_min + w_max) / 2.0

    def toxic_signal(self, w: float) -> float:
        if w >= self.cutoff:
            return 0.0
        denom = self.cutoff - self.w_min
        if denom <= 0:
            return 0.0
        return min(1.0, (self.cutoff - w) / denom)

    def healthy_signal(self, w: float) -> float:
        if w <= self.cutoff:
            return 0.0
        denom = self.w_max - self.cutoff
        if denom <= 0:
            return 0.0
        return min(1.0, (w - self.cutoff) / denom)

    def is_toxic(self, w: float) -> bool:
        return w < self.cutoff

    def toxic_cost(self, w: float) -> float:
        return 1.0 - self.toxic_signal(w)

    def toxicity_index(self, weights: list) -> float:
        if not weights:
            return 0.0
        return float(np.mean([self.toxic_signal(w) for w in weights]))

    def signed_toxicity_index(self, weights: list) -> float:
        if not weights:
            return 0.0
        return float(np.mean([
            self.healthy_signal(w) - self.toxic_signal(w) for w in weights
        ]))

    def __repr__(self):
        return (f"WeightRange(w_min={self.w_min}, w_max={self.w_max}, "
                f"cutoff={self.cutoff})")


def make_weight_range(w_min: float, w_max: float, tox_cutoff: float = None) -> WeightRange:
    wr = WeightRange(w_min, w_max, tox_cutoff)
    print(f"\n  Intervalo de weight  : [{wr.w_min}, {wr.w_max}]")
    print(f"  Limiar de toxicidade : {wr.cutoff}  "
          f"(w < {wr.cutoff} = tóxico  |  w >= {wr.cutoff} = saudável)")
    return wr


def seed_toxic_nodes(
    G: nx.DiGraph,
    wr: WeightRange,
    n_seeds: int,
    use_kcore: bool = True,
    verbose: bool = False,
) -> list:
    
    G_check = G
    if nx.number_of_selfloops(G) > 0:
        G_check = G.copy()
        G_check.remove_edges_from(nx.selfloop_edges(G_check))

    tox = {}
    for n in G.nodes():
        out_w = [d.get("weight", wr.cutoff) for _, _, d in G.out_edges(n, data=True)]
        if out_w:
            tox[n] = wr.toxicity_index(out_w)
        else:
            in_w = [d.get("weight", wr.cutoff) for _, _, d in G.in_edges(n, data=True)]
            tox[n] = wr.toxicity_index(in_w)

    if not use_kcore:
        ranked = sorted(G.nodes(), key=lambda n: tox[n], reverse=True)
        return ranked[:n_seeds]

    cores = nx.core_number(G_check.to_undirected())

    n_distinct_cores = len(set(cores.values()))
    max_core = max(cores.values()) if cores else 0

    degenerate = n_distinct_cores <= 1 or max_core <= 1

    if degenerate:
        if verbose:
            print(f"  [Seeds] K-core degenerado (max_core={max_core}, "
                  f"{n_distinct_cores} camada(s) distintas) — "
                  f"grafo provavelmente esparso/tipo-árvore.")
            print(f"  [Seeds] Usando apenas toxicidade como critério (fallback).")
        ranked = sorted(G.nodes(), key=lambda n: tox[n], reverse=True)
    else:
        ranked = sorted(
            G.nodes(),
            key=lambda n: (cores[n], tox[n]),
            reverse=True,
        )
        if verbose:
            print(f"  [Seeds] K-core ativo (max_core={max_core}, "
                  f"{n_distinct_cores} camadas distintas)")

    selected = ranked[:n_seeds]

    if verbose:
        print(f"  [Seeds] {n_seeds} sementes selecionadas:")
        for i, s in enumerate(selected, 1):
            core_str = f"core={cores.get(s, '-')}" if not degenerate else "core=n/a"
            print(f"    {i}. …{s[-8:]}  {core_str}  tox={tox[s]:.4f}")

    return selected


def node_toxicity_index(G: nx.DiGraph, node: str, wr: WeightRange) -> float:
    out_edges = list(G.out_edges(node, data=True))
    weights = [d.get("weight", wr.cutoff) for _, _, d in out_edges]
    if not weights:
        in_edges = list(G.in_edges(node, data=True))
        weights = [d.get("weight", wr.cutoff) for _, _, d in in_edges]
    return wr.signed_toxicity_index(weights)


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
        w = float(ed.get("weight", 0.0))
        G.add_edge(
            ed["src"], ed["dst"],
            weight=w,
            src_score=float(ed.get("src_score", 5.0)),
            dst_score=float(ed.get("dst_score", 5.0)),
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

    return _print_graph_stats(G)


def load_graphml(graphml_path: str) -> nx.DiGraph:
    G = nx.read_graphml(graphml_path)

    if not G.is_directed():
        G = G.to_directed()

    for n in G.nodes():
        G.nodes[n].setdefault("review_count", 0)
        G.nodes[n].setdefault("comment_count", 0)

    for u, v, data in G.edges(data=True):
        if "weight" not in data:
            G[u][v]["weight"] = 0.0
        else:
            G[u][v]["weight"] = float(data["weight"])

    return _print_graph_stats(G)


def _print_graph_stats(G: nx.DiGraph) -> nx.DiGraph:
    all_weights = [d["weight"] for _, _, d in G.edges(data=True)]

    print(f"\n{'━'*52}")
    print(f"  GRAFO CARREGADO")
    print(f"{'━'*52}")
    print(f"  Nós     : {G.number_of_nodes():,}")
    print(f"  Arestas : {G.number_of_edges():,}")

    if not all_weights:
        print("  Aviso: grafo sem arestas com weight.")
        return G

    print(f"  Weight observado: mín={min(all_weights):.4f}  "
          f"máx={max(all_weights):.4f}  média={np.mean(all_weights):.4f}")
    print(f"  (Use --w_min/--w_max para definir o intervalo teórico de análise,")
    print(f"   que pode ser diferente do mín/máx observados nesta amostra)")

    return G


def report_toxicity_split(G: nx.DiGraph, wr: WeightRange):
    all_weights = [d["weight"] for _, _, d in G.edges(data=True)]
    if not all_weights:
        return
    n_toxic   = sum(1 for w in all_weights if wr.is_toxic(w))
    n_healthy = len(all_weights) - n_toxic
    print(f"\n  Classificação (cutoff={wr.cutoff}):")
    print(f"    Tóxicas   (w < {wr.cutoff}) : {n_toxic:,} "
          f"({100*n_toxic/len(all_weights):.1f}%)")
    print(f"    Saudáveis (w >= {wr.cutoff}): {n_healthy:,} "
          f"({100*n_healthy/len(all_weights):.1f}%)")


def run_sis_sir(
    G: nx.DiGraph,
    wr: WeightRange,
    mode: str = "sis",
    beta: float = 0.35,
    gamma: float = 0.15,
    n_seeds: int = 3,
    steps: int = 30,
    seed: int = 42,
    seed_strategy: str = "kcore",
    verbose_seeds: bool = False,
) -> dict:
    random.seed(seed)
    nodes = list(G.nodes())
    states = {n: "S" for n in nodes}

    for n in seed_toxic_nodes(G, wr, n_seeds,
                              use_kcore=(seed_strategy == "kcore"),
                              verbose=verbose_seeds):
        states[n] = "I"

    history = {"S": [], "I": [], "R": []}

    def count(s):
        c = {"S": 0, "I": 0, "R": 0}
        for st in s.values():
            c[st] += 1
        return c

    for _ in range(steps):
        c = count(states)
        history["S"].append(c["S"])
        history["I"].append(c["I"])
        history["R"].append(c["R"])

        new_states = copy.copy(states)

        for node in nodes:
            if states[node] == "S":
                p_infect = 0.0
                for pred in G.predecessors(node):
                    if states[pred] == "I":
                        w = G[pred][node].get("weight", wr.cutoff)
                        p_infect += beta * wr.toxic_signal(w)
                p_infect = min(p_infect, 0.999)
                if random.random() < p_infect:
                    new_states[node] = "I"

            elif states[node] == "I":
                in_healthy = [
                    wr.healthy_signal(G[u][node].get("weight", wr.cutoff))
                    for u in G.predecessors(node)
                ]
                recovery_boost = min(0.3, np.mean(in_healthy) * 0.3) if in_healthy else 0.0
                if random.random() < (gamma + recovery_boost):
                    new_states[node] = "R" if mode == "sir" else "S"

        states = new_states

    c = count(states)
    history["S"].append(c["S"])
    history["I"].append(c["I"])
    history["R"].append(c["R"])

    r0 = beta / gamma if gamma > 0 else float("inf")
    peak_i = max(history["I"])
    peak_t = history["I"].index(peak_i)

    print(f"\n{'━'*52}")
    print(f"  MODELO {mode.upper()}")
    print(f"{'━'*52}")
    print(f"  β (infecção)    = {beta}  |  γ (recuperação) = {gamma}")
    print(f"  R₀              = {r0:.3f}  "
          f"({'endêmico — contágio sustentado' if r0 > 1 else 'se extingue naturalmente'})")
    print(f"  Pico infectados = {peak_i} usuários (passo t={peak_t})")
    print(f"  Estado final    : S={c['S']}  I={c['I']}  R={c['R']}")

    return {
        "mode": mode.upper(), "beta": beta, "gamma": gamma, "R0": round(r0, 3),
        "peak_infected": peak_i, "peak_step": peak_t,
        "final_states": states, "history": history, "steps": steps,
    }


def compute_threshold(node_attrs: dict, base_theta: float, sigma: float) -> float:
    activity = (node_attrs.get("review_count", 0) + node_attrs.get("comment_count", 0)) / 50.0
    theta = random.gauss(base_theta, sigma) + activity * 0.15
    return max(0.02, min(0.95, theta))


def run_granovetter(
    G: nx.DiGraph,
    wr: WeightRange,
    base_theta: float = 0.30,
    sigma: float = 0.12,
    n_seeds: int = 2,
    max_rounds: int = 50,
    seed: int = 42,
    seed_strategy: str = "kcore",
    verbose_seeds: bool = False,
) -> dict:
    random.seed(seed)
    nodes = list(G.nodes())

    thresholds = {
        n: compute_threshold(G.nodes[n], base_theta, sigma)
        for n in nodes
    }

    adopted = {n: False for n in nodes}
    for n in seed_toxic_nodes(G, wr, n_seeds,
                              use_kcore=(seed_strategy == "kcore"),
                              verbose=verbose_seeds):
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

            predecessors = list(G.predecessors(node))

            if not predecessors:
                continue

            infected_preds = [u for u in predecessors if adopted[u]]
            
            if not infected_preds:
                continue

            toxic_signals = [
                wr.toxic_signal(G[u][node].get("weight", wr.cutoff))
                for u in infected_preds
            ]

            pressure_ratio = sum(toxic_signals) / len(infected_preds)

            if pressure_ratio >= thresholds[node]:
                new_adopted[node] = True
                adopted_this_round.append(node)
                changed = True

        adopted = new_adopted
        rounds_data.append(len(adopted_this_round))
        round_num += 1

    total_infected = sum(adopted.values())
    pct = 100.0 * total_infected / len(nodes) if nodes else 0

    print(f"\n{'━'*52}")
    print(f"  MODELO LIMIAR DE GRANOVETTER")
    print(f"{'━'*52}")
    print(f"  Limiar base θ = {base_theta}  (σ={sigma})")
    print(f"  Rodadas       = {round_num}")
    print(f"  Infectados    = {total_infected} / {len(nodes)} ({pct:.1f}%)")
    if rounds_data:
        peak_r = rounds_data.index(max(rounds_data)) + 1
        print(f"  Maior cascata = rodada {peak_r} ({max(rounds_data)} usuários)")

    return {
        "base_theta": base_theta, "sigma": sigma,
        "total_infected": total_infected, "total_nodes": len(nodes),
        "infection_pct": round(pct, 2), "rounds": round_num,
        "rounds_data": rounds_data, "thresholds": thresholds,
        "final_adopted": adopted,
    }


def run_betweenness(
    G: nx.DiGraph,
    wr: WeightRange,
    top_k: int = 10,
    k_approx: int = None,
) -> dict:
    n = G.number_of_nodes()
    if k_approx is None and n > 2000:
        k_approx = min(500, n)
        print(f"  Grafo grande ({n} nós) — usando k_approx={k_approx}")

    for u, v, data in G.edges(data=True):
        w = data.get("weight", wr.cutoff)
        G[u][v]["toxic_cost"] = wr.toxic_cost(w)

    print(f"\n{'━'*52}")
    print(f"  CENTRALIDADE DE INTERMEDIAÇÃO")
    print(f"{'━'*52}")
    print(f"  Calculando sobre {n} nós e {G.number_of_edges()} arestas...")

    scores = nx.betweenness_centrality(
        G, normalized=True, weight="toxic_cost", k=k_approx
    )

    node_details = []
    for node, bc in scores.items():
        attrs = G.nodes[node]
        tox_idx = node_toxicity_index(G, node, wr)
        out_weights = [d["weight"] for _, _, d in G.out_edges(node, data=True)]
        toxic_edges_out = sum(1 for w in out_weights if wr.is_toxic(w))

        node_details.append({
            "id": node,
            "betweenness": round(bc, 6),
            "toxicity_index": round(tox_idx, 4),
            "toxic_edges_out": toxic_edges_out,
            "in_degree": G.in_degree(node),
            "out_degree": G.out_degree(node),
            "review_count": attrs.get("review_count", 0),
            "comment_count": attrs.get("comment_count", 0),
        })

    node_details.sort(key=lambda x: x["betweenness"], reverse=True)
    top_nodes = node_details[:top_k]

    print(f"\n  {'#':<4} {'ID':<22} {'Betweenness':>12} {'Tox.Index':>10} {'ToxEdges':>9} {'In':>5} {'Out':>5}")
    print(f"  {'─'*66}")
    for i, nd in enumerate(top_nodes, 1):
        flag = " ⚠" if nd["toxicity_index"] < -0.1 and nd["betweenness"] > 0 else ""
        print(f"  {i:<4} {nd['id']:<22} {nd['betweenness']:>12.6f} "
              f"{nd['toxicity_index']:>10.4f} {nd['toxic_edges_out']:>9} "
              f"{nd['in_degree']:>5} {nd['out_degree']:>5}{flag}")

    print(f"\n  ⚠  = nó com alta centralidade E índice tóxico negativo (vetor crítico)")
    print(f"  (toxicity_index ∈ [-1, 1] — negativo=tóxico, positivo=saudável, "
          f"independente do range original do weight)")

    return {
        "scores": scores,
        "top_nodes": top_nodes,
        "top_k": top_k,
        "all_details": node_details,
    }


COLORS = {
    "S": "#378ADD", "I": "#E24B4A", "R": "#1D9E75",
    "toxic": "#E24B4A", "healthy": "#1D9E75", "neutral": "#AAAAAA",
    "high_bc": "#E24B4A", "mid_bc": "#EF9F27", "low_bc": "#85B7EB",
}


def plot_sis_sir(result: dict, save_path: str = None):
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
    r0_label = f"R₀={result['R0']} ({'endêmico' if result['R0'] > 1 else 'extingue'})"
    fig.suptitle(f"Modelo {result['mode']} — {r0_label}", fontsize=13, fontweight="bold")

    h = result["history"]
    t = list(range(len(h["S"])))

    ax = axes[0]
    ax.fill_between(t, h["S"], alpha=0.15, color=COLORS["S"])
    ax.fill_between(t, h["I"], alpha=0.15, color=COLORS["I"])
    ax.plot(t, h["S"], color=COLORS["S"], lw=2, label="Suscetíveis (S)")
    ax.plot(t, h["I"], color=COLORS["I"], lw=2, label="Infectados (I)")
    if result["mode"] == "SIR":
        ax.fill_between(t, h["R"], alpha=0.15, color=COLORS["R"])
        ax.plot(t, h["R"], color=COLORS["R"], lw=2, label="Removidos (R)")
    ax.axvline(result["peak_step"], color=COLORS["I"], ls="--", alpha=0.4, lw=1,
               label=f"Pico t={result['peak_step']}")
    ax.set_xlabel("Passo de tempo")
    ax.set_ylabel("Número de usuários")
    ax.set_title("Curva epidemiológica")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.15)
    ax.set_xlim(0, result["steps"])

    ax2 = axes[1]
    fs = result["final_states"]
    counts = {"S": 0, "I": 0, "R": 0}
    for s in fs.values():
        counts[s] += 1
    full = {"S": "Suscetíveis", "I": "Infectados", "R": "Removidos"}
    items = [(k, v) for k, v in counts.items() if v > 0]
    ax2.pie([v for _, v in items],
            labels=[f"{full[k]}\n({v})" for k, v in items],
            colors=[COLORS[k] for k, _ in items],
            autopct="%1.1f%%", startangle=90, textprops={"fontsize": 9})
    ax2.set_title("Distribuição final dos estados")

    plt.tight_layout()
    _save_or_show(fig, save_path)


def plot_granovetter(result: dict, save_path: str = None):
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
    fig.suptitle(
        f"Limiar de Granovetter  —  θ={result['base_theta']} σ={result['sigma']}\n"
        f"{result['total_infected']}/{result['total_nodes']} infectados "
        f"({result['infection_pct']}%)  em {result['rounds']} rodadas",
        fontsize=11, fontweight="bold"
    )

    ax = axes[0]
    rd = result["rounds_data"]
    clrs = [COLORS["toxic"] if v > 0 else COLORS["neutral"] for v in rd]
    bars = ax.bar(range(1, len(rd)+1), rd, color=clrs, alpha=0.85, edgecolor="white", lw=0.5)
    for bar in bars:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width()/2, h + 0.1, str(int(h)),
                    ha="center", va="bottom", fontsize=8)
    ax.set_xlabel("Rodada")
    ax.set_ylabel("Usuários que adotaram")
    ax.set_title("Cascata de adoção por rodada")
    ax.grid(axis="y", alpha=0.15)

    ax2 = axes[1]
    adopted = result["final_adopted"]
    th_adopted  = [result["thresholds"][n] for n in adopted if adopted[n]]
    th_resisted = [result["thresholds"][n] for n in adopted if not adopted[n]]
    bins = np.linspace(0, 1, 20)
    ax2.hist(th_resisted, bins=bins, color=COLORS["S"], alpha=0.7, label="Resistiram")
    ax2.hist(th_adopted,  bins=bins, color=COLORS["I"], alpha=0.7, label="Adotaram")
    ax2.axvline(result["base_theta"], color="black", ls="--", lw=1,
                label=f"θ base = {result['base_theta']}")
    ax2.set_xlabel("Limiar individual θ")
    ax2.set_ylabel("Nº de usuários")
    ax2.set_title("Distribuição de limiares (quem adotou vs resistiu)")
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.15)

    plt.tight_layout()
    _save_or_show(fig, save_path)


def plot_betweenness(G: nx.DiGraph, result: dict, save_path: str = None):
    top      = result["top_nodes"]
    all_det  = result["all_details"]
    max_bc   = max(nd["betweenness"] for nd in all_det) if all_det else 1

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Centralidade de Intermediação — Pontes de Contágio Tóxico",
                 fontsize=13, fontweight="bold")

    def node_color(tox_idx):
        if tox_idx < -0.1: return COLORS["toxic"]
        if tox_idx <  0.1: return COLORS["mid_bc"]
        return COLORS["healthy"]

    ax = axes[0]
    ids_short = [f"…{nd['id'][-8:]}" for nd in top]
    vals  = [nd["betweenness"]    for nd in top]
    tidxs = [nd["toxicity_index"] for nd in top]
    clrs  = [node_color(t) for t in tidxs]
    bars  = ax.barh(ids_short[::-1], vals[::-1], color=clrs[::-1], alpha=0.85, edgecolor="white")
    for bar, v in zip(bars, vals[::-1]):
        ax.text(v + max_bc * 0.01, bar.get_y() + bar.get_height()/2,
                f"{v:.4f}", va="center", fontsize=8)
    ax.set_xlabel("Betweenness Centrality (normalizado)")
    ax.set_title(f"Top {len(top)} usuários ponte")
    ax.grid(axis="x", alpha=0.15)
    patches = [
        mpatches.Patch(color=COLORS["toxic"],   label="Índice tóxico (< -0.1)"),
        mpatches.Patch(color=COLORS["mid_bc"],  label="Neutro"),
        mpatches.Patch(color=COLORS["healthy"], label="Saudável (> 0.1)"),
    ]
    ax.legend(handles=patches, fontsize=8, loc="lower right")

    ax2 = axes[1]
    all_bc  = [d["betweenness"]    for d in all_det]
    all_tox = [d["toxicity_index"] for d in all_det]
    all_deg = [d["in_degree"] + d["out_degree"] for d in all_det]
    sc_clrs = [node_color(t) for t in all_tox]
    sizes   = [max(20, min(200, d * 10)) for d in all_deg]
    ax2.scatter(all_tox, all_bc, c=sc_clrs, s=sizes, alpha=0.7, edgecolors="white", lw=0.3)
    ax2.axvline(0,    color="gray",          ls="--", lw=0.8, alpha=0.5)
    ax2.axvline(-0.1, color=COLORS["toxic"], ls=":",  lw=0.8, alpha=0.5)
    ax2.set_xlabel("Índice de toxicidade do nó [-1, 1]")
    ax2.set_ylabel("Betweenness Centrality")
    ax2.set_title("Betweenness × Toxicidade\n(tamanho = grau total)")
    ax2.grid(alpha=0.1)
    ax2.legend(handles=patches, fontsize=8)

    ymax = max(all_bc) if all_bc else 1
    ax2.fill_betweenx([0, ymax], -1, -0.1, alpha=0.05, color=COLORS["toxic"])
    ax2.text(-0.95, ymax * 0.95, "zona de risco\n(bridge tóxico)", fontsize=7,
             color=COLORS["toxic"], alpha=0.7)

    plt.tight_layout()
    _save_or_show(fig, save_path)


def _save_or_show(fig, save_path):
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  Gráfico salvo: {save_path}")
    else:
        plt.show()
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Análise de contágio comportamental no grafo Steam"
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--graphml", type=str, default=None,
                             help="Arquivo .graphml de comunidade")
    input_group.add_argument("--nodes",   type=str, default=None,
                             help="nodes.json do grafo completo")

    parser.add_argument("--edges", type=str, default="edges.json",
                        help="edges.json (usado com --nodes)")
    parser.add_argument("--model", default="all",
                        choices=["all", "sis", "sir", "granovetter", "betweenness"])

    parser.add_argument("--w_min", type=float, default=0.0,
                        help="Limite inferior do weight (padrão: -1.0, grafo completo)")
    parser.add_argument("--w_max", type=float, default=1.0,
                        help="Limite superior do weight (padrão: 1.0, grafo completo)")
    parser.add_argument("--tox_cutoff", type=float, default=0.6,
                        help="Limiar de toxicidade (padrão: ponto médio de w_min/w_max). "
                             "w < cutoff é considerado tóxico.")

    parser.add_argument("--beta",  type=float, default=0.35)
    parser.add_argument("--gamma", type=float, default=0.15)
    parser.add_argument("--steps", type=int,   default=30)
    parser.add_argument("--seeds", type=int,   default=3)
    parser.add_argument("--seed_strategy", default="kcore",
                        choices=["kcore", "toxicity"],
                        help="kcore: prioriza núcleo estrutural + toxicidade "
                             "(com fallback automático se o grafo for esparso). "
                             "toxicity: só toxicidade (comportamento antigo).")
    parser.add_argument("--verbose_seeds", action="store_true",
                        help="Mostra detalhes das sementes escolhidas e diagnóstico do k-core")

    parser.add_argument("--theta", type=float, default=0.30)
    parser.add_argument("--sigma", type=float, default=0.12)

    parser.add_argument("--top_k",    type=int, default=10)
    parser.add_argument("--k_approx", type=int, default=None,
                        help="Amostras para betweenness aproximado (None=exato)")

    parser.add_argument("--save_plots", action="store_true")
    parser.add_argument("--save_json",  action="store_true")

    args = parser.parse_args()
    run_all = args.model == "all"

    if args.graphml:
        G = load_graphml(args.graphml)
        out_prefix = Path(args.graphml).stem
    else:
        G = load_graph(args.nodes, args.edges)
        out_prefix = "plot"

    wr = make_weight_range(args.w_min, args.w_max, args.tox_cutoff)
    report_toxicity_split(G, wr)

    results = {}

    if run_all or args.model == "sis":
        res = run_sis_sir(G, wr, mode="sis", beta=args.beta, gamma=args.gamma,
                          n_seeds=args.seeds, steps=args.steps,
                          seed_strategy=args.seed_strategy,
                          verbose_seeds=args.verbose_seeds)
        results["sis"] = res
        plot_sis_sir(res, f"{out_prefix}_sis.png" if args.save_plots else None)

    if run_all or args.model == "sir":
        res = run_sis_sir(G, wr, mode="sir", beta=args.beta, gamma=args.gamma,
                          n_seeds=args.seeds, steps=args.steps,
                          seed_strategy=args.seed_strategy,
                          verbose_seeds=args.verbose_seeds)
        results["sir"] = res
        plot_sis_sir(res, f"{out_prefix}_sir.png" if args.save_plots else None)

    if run_all or args.model == "granovetter":
        res = run_granovetter(G, wr, base_theta=args.theta, sigma=args.sigma,
                              n_seeds=args.seeds,
                              seed_strategy=args.seed_strategy,
                              verbose_seeds=args.verbose_seeds)
        results["granovetter"] = res
        plot_granovetter(res, f"{out_prefix}_granovetter.png" if args.save_plots else None)

    if run_all or args.model == "betweenness":
        res = run_betweenness(G, wr, top_k=args.top_k, k_approx=args.k_approx)
        results["betweenness"] = res
        plot_betweenness(G, res, f"{out_prefix}_betweenness.png" if args.save_plots else None)

    if args.save_json:
        export = {}
        for key, val in results.items():
            if key in ("sis", "sir"):
                export[key] = {k: v for k, v in val.items() if k != "final_states"}
                export[key]["final_summary"] = {
                    s: list(val["final_states"].values()).count(s) for s in ("S","I","R")
                }
            elif key == "granovetter":
                export[key] = {k: v for k, v in val.items()
                               if k not in ("thresholds", "final_adopted")}
            elif key == "betweenness":
                export[key] = {"top_nodes": val["top_nodes"]}

        json_path = f"{out_prefix}_results.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)
        print(f"\n  Exportado: {json_path}")

    print("\nConcluído.\n")


if __name__ == "__main__":
    main()