"""Benchmark harness for mini-raft.

Everything here is measured on the deterministic simulator, so "time" is counted in
simulation ticks, not wall-clock milliseconds - a tick is one round of message
delivery plus one tick of every node. That keeps the numbers reproducible on any
machine (CI included) while still capturing the shape that matters: how election
time and commit latency scale with cluster size and packet loss.

Run: python bench/benchmark.py
Writes PNGs and a JSON summary into bench/results/.
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python" / "src"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from raft.cluster import Cluster

RESULTS = Path(__file__).resolve().parent / "results"
RESULTS.mkdir(exist_ok=True)

TRIALS = 200


def ticks_to_elect(size: int, seed: int, drop_prob: float = 0.0) -> int | None:
    c = Cluster(size, seed=seed, drop_prob=drop_prob)
    for t in range(1, 600):
        c.step()
        if c.leader() is not None:
            return t
    return None


def ticks_to_commit(size: int, seed: int) -> int | None:
    c = Cluster(size, seed=seed)
    leader = c.run_until_leader(300)
    if leader is None:
        return None
    leader.propose("x")
    for t in range(1, 300):
        c.step()
        if any(e.command == "x" for e in c.nodes[0].applied):
            return t
    return None


def pctile(data: list[int], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = min(len(s) - 1, round((p / 100) * (len(s) - 1)))
    return float(s[k])


def bench_election_by_size() -> dict:
    sizes = [3, 5, 7, 9]
    summary = {}
    means, p99s = [], []
    for size in sizes:
        samples = [t for s in range(TRIALS) if (t := ticks_to_elect(size, s)) is not None]
        summary[size] = {
            "mean": round(statistics.mean(samples), 2),
            "p50": pctile(samples, 50),
            "p99": pctile(samples, 99),
            "success_rate": round(len(samples) / TRIALS, 3),
        }
        means.append(statistics.mean(samples))
        p99s.append(pctile(samples, 99))

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(sizes, means, "o-", label="mean")
    ax.plot(sizes, p99s, "s--", label="p99")
    ax.set_xlabel("cluster size (nodes)")
    ax.set_ylabel("ticks to elect a leader")
    ax.set_title("mini-raft: election time vs cluster size")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(RESULTS / "election_by_size.png", dpi=110)
    plt.close(fig)
    return summary


def bench_election_under_loss() -> dict:
    drops = [0.0, 0.05, 0.10, 0.20, 0.30]
    summary = {}
    means, rates = [], []
    for d in drops:
        samples = [t for s in range(TRIALS) if (t := ticks_to_elect(5, s, d)) is not None]
        mean = statistics.mean(samples) if samples else 0.0
        summary[d] = {
            "mean": round(mean, 2),
            "p99": pctile(samples, 99),
            "success_rate": round(len(samples) / TRIALS, 3),
        }
        means.append(mean)
        rates.append(len(samples) / TRIALS)

    fig, ax1 = plt.subplots(figsize=(6, 4))
    ax1.plot([int(d * 100) for d in drops], means, "o-", color="tab:blue")
    ax1.set_xlabel("packet loss (%)")
    ax1.set_ylabel("mean ticks to elect", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax2 = ax1.twinx()
    ax2.plot([int(d * 100) for d in drops], [r * 100 for r in rates], "s--", color="tab:red")
    ax2.set_ylabel("elected within budget (%)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax1.set_title("mini-raft: election under packet loss (5 nodes)")
    ax1.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(RESULTS / "election_under_loss.png", dpi=110)
    plt.close(fig)
    return summary


def bench_commit_latency() -> dict:
    sizes = [3, 5, 7]
    summary = {}
    box_data = []
    for size in sizes:
        samples = [t for s in range(TRIALS) if (t := ticks_to_commit(size, s)) is not None]
        summary[size] = {
            "mean": round(statistics.mean(samples), 2),
            "p50": pctile(samples, 50),
            "p99": pctile(samples, 99),
            "p99_9": pctile(samples, 99.9),
        }
        box_data.append(samples)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.boxplot(box_data, tick_labels=[str(s) for s in sizes], showfliers=False)
    ax.set_xlabel("cluster size (nodes)")
    ax.set_ylabel("ticks from propose to commit")
    ax.set_title("mini-raft: commit latency distribution")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(RESULTS / "commit_latency.png", dpi=110)
    plt.close(fig)
    return summary


def main() -> None:
    summary = {
        "trials_per_point": TRIALS,
        "unit": "simulation ticks (one delivery round + one tick per node)",
        "election_by_size": bench_election_by_size(),
        "election_under_loss": bench_election_under_loss(),
        "commit_latency": bench_commit_latency(),
    }
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
