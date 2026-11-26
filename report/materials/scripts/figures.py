"""
Two extra figures from the raw results (run extract_facts.py first, it writes the CSVs this reads).

    python report/materials/scripts/figures.py

figures/subtask-load.{pdf,png}     records per parallel instance, source vs window, at 4, 6 and 8 cores
figures/container-cpu.{pdf,png}    mean CPU of each container during the 1-8 core runs
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).resolve().parents[1]
RUN = "20261124-132305-cores-1-8"
ACCENT, DARK, LIGHT, INK, MUTED = "#2563EB", "#4B5563", "#9CA3AF", "#111827", "#6B7280"

plt.rcParams.update({"font.size": 8, "axes.edgecolor": LIGHT, "axes.labelcolor": INK, "xtick.color": MUTED,
                     "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
                     "font.family": "serif"})


def save(fig, name):
    out = HERE / "figures"
    out.mkdir(exist_ok=True)
    fig.savefig(out / f"{name}.pdf", bbox_inches="tight")
    fig.savefig(out / f"{name}.png", dpi=300, bbox_inches="tight")
    print("saved", out / name)


def subtask_load():
    df = pd.read_csv(HERE / "tables" / "flink-subtasks.csv")
    df = df[(df.run == RUN) & df.task.isin(["source+router", "window"]) & df.cores.isin([4, 6, 8])]
    fig, axes = plt.subplots(2, 3, figsize=(7, 3.6), sharey=True)
    for col, cores in enumerate([4, 6, 8]):
        for row, (task, label, colour) in enumerate([("source+router", "Kafka source (reads partitions)", DARK),
                                                     ("window", "Window operator (after keyBy)", ACCENT)]):
            ax = axes[row, col]
            d = df[(df.cores == cores) & (df.task == task)].sort_values("subtask")
            m = d.records / 1e6
            ax.bar(d.subtask, m, color=colour, width=0.7)
            ax.axhline(m.mean(), color=INK, linewidth=0.8, linestyle="--")
            ax.text(d.subtask.max() + 0.45, m.mean(), f"mean {m.mean():.1f}", va="bottom", ha="right",
                    fontsize=6.5, color=INK)
            ax.set_xticks(d.subtask)
            ax.grid(axis="y", color="#E5E7EB", linewidth=0.6)
            ax.set_axisbelow(True)
            ax.set_title(f"{cores} cores, max/mean {m.max() / m.mean():.2f}", fontsize=7.5, color=INK)
            if col == 0:
                ax.set_ylabel("million trades", fontsize=7)
                ax.text(-0.32, 1.30, label, transform=ax.transAxes, fontsize=7.5, fontweight="bold", color=colour)
            if row == 1:
                ax.set_xlabel("subtask")
    fig.suptitle("Trades handled per parallel instance, weekday trades (59.4 M), 24 Nov run", fontsize=8.5)
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.75, top=0.8)
    save(fig, "subtask-load")


def container_cpu():
    df = pd.read_csv(HERE / "tables" / f"containers-{RUN}.csv")
    piv = df.pivot(index="cores", columns="container", values="cpu_mean")
    fig, ax = plt.subplots(figsize=(3.4, 2.3))
    width = 0.27
    x = range(len(piv))
    for i, (name, colour) in enumerate([("taskmanager", ACCENT), ("kafka", DARK), ("jobmanager", LIGHT)]):
        ax.bar([v + (i - 1) * width for v in x], piv[name] / 100, width=width * 0.92, color=colour, label=name)
    ax.scatter([v - width for v in x], piv.index, color=INK, marker="_", s=120, linewidths=1.2, label="cores given to TM", zorder=3)
    ax.set_xticks(list(x), piv.index)
    ax.set_xlabel("TaskManager cores (= parallelism)")
    ax.set_ylabel("mean CPU during run (cores)")
    ax.grid(axis="y", color="#E5E7EB", linewidth=0.6)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=6.5, loc="upper left")
    save(fig, "container-cpu")


if __name__ == "__main__":
    subtask_load()
    container_cpu()
