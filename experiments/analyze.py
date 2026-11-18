"""
Turns the raw experiment output into figures and tables for the report.

    python -m experiments.analyze                 # newest run of every experiment
    python -m experiments.analyze --scalability results/scalability/20261120-101500
    python -m experiments.analyze --latency results/latency/<timestamp> results/latency/tuned

Writes PNG figures to results/figures/ and the numbers behind them to results/summary.md
(plus CSVs), so every figure in the report can be traced back to a table.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

RESULTS = Path("results")
FIG = RESULTS / "figures"

# Colours by role (validated categorical slots 1-3 plus neutral inks), so every chart matches
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e1"

plt.rcParams.update({
    "figure.dpi": 150, "savefig.dpi": 200, "font.size": 9,
    "axes.edgecolor": MUTED, "axes.labelcolor": INK2, "axes.titlecolor": INK,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.spines.top": False,
    "axes.spines.right": False, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "xtick.color": INK2, "ytick.color": INK2, "legend.frameon": False, "lines.linewidth": 2,
})


def newest(kind: str) -> Path | None:
    d = RESULTS / kind
    runs = sorted(p for p in d.glob("*") if p.is_dir()) if d.exists() else []
    return runs[-1] if runs else None


def save(fig, name):
    FIG.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIG / name)
    plt.close(fig)
    return FIG / name


# ------------------------------------------------------------------ scalability

def cpu_mean(path: Path, container="taskmanager") -> float:
    if not path.exists():
        return float("nan")
    c = pd.read_csv(path)
    c = c[c["container"] == container]
    return float(pd.to_numeric(c["cpu_percent"], errors="coerce").mean()) if len(c) else float("nan")


def subtask_records(job_json: Path, name_part="window") -> list[int]:
    """Records each parallel instance of the window operator received: shows key skew."""
    if not job_json.exists():
        return []
    job = json.loads(job_json.read_text())
    for v in job["vertices"]:
        if name_part in v["name"].lower():
            return [s.get("metrics", {}).get("read-records", 0) for s in v.get("subtasks", [])]
    return []


def scalability(d: Path, lines: list[str], suffix: str = ""):
    runs = pd.read_csv(d / "runs.csv")
    runs = runs[runs["state"] == "FINISHED"].copy()
    events = json.loads((d / "producer.json").read_text())["events"]
    runs["seconds"] = runs["run_ms"] / 1000
    runs["throughput"] = events / runs["seconds"]
    runs["tm_cpu_percent"] = [cpu_mean(d / f"cpu-c{c}-r{r}.csv") for c, r in zip(runs["cores"], runs["repeat"])]
    g = runs.groupby("cores").agg(runs=("seconds", "size"), seconds_mean=("seconds", "mean"),
                                  seconds_std=("seconds", "std"), throughput_mean=("throughput", "mean"),
                                  throughput_std=("throughput", "std"),
                                  tm_cpu_percent=("tm_cpu_percent", "mean")).reset_index()
    base = g["seconds_mean"].iloc[0] * g["cores"].iloc[0]
    g["speedup"] = g["seconds_mean"].iloc[0] / g["seconds_mean"]
    g["efficiency"] = g["speedup"] / (g["cores"] / g["cores"].iloc[0])
    g["cpu_per_core_percent"] = g["tm_cpu_percent"] / g["cores"]
    g.to_csv(d / "summary.csv", index=False)

    settings = json.loads((d / "settings.json").read_text()) if (d / "settings.json").exists() else {}
    lines += [f"## Scalability ({d})", "",
              f"Input: {events:,} events ({settings.get('days', '?')}, {settings.get('events', '?')}, "
              f"exchanges {settings.get('exchanges', 'all')}). Host: {settings.get('host', '?')}, "
              f"Docker CPUs {settings.get('docker_cpus', '?')}.", "",
              g.round(3).to_markdown(index=False), ""]

    cores = g["cores"].to_numpy()
    fig, ax = plt.subplots(figsize=(4.2, 3))
    ax.errorbar(cores, g["throughput_mean"] / 1e3, yerr=g["throughput_std"].fillna(0) / 1e3,
                color=BLUE, marker="o", markersize=6, capsize=3, label="measured")
    ideal = g["throughput_mean"].iloc[0] / cores[0] * cores / 1e3
    ax.plot(cores, ideal, color=MUTED, linestyle="--", linewidth=1.2, label="linear scaling")
    ax.set(xlabel="TaskManager cores (= parallelism)", ylabel="thousand events / s",
           title="Throughput vs cores", xticks=cores)
    ax.set_ylim(bottom=0)
    ax.legend()
    figs = [save(fig, f"scalability-throughput{suffix}.png")]

    fig, ax = plt.subplots(figsize=(4.2, 3))
    ax.bar(cores, g["cpu_per_core_percent"], width=0.6, color=BLUE)
    ax.set(xlabel="TaskManager cores", ylabel="CPU used per core (%)", title="CPU utilisation",
           xticks=cores, ylim=(0, 105))
    figs.append(save(fig, f"scalability-cpu{suffix}.png"))

    maxc = int(cores.max())
    rec = subtask_records(d / f"job-c{maxc}-r1.json")
    if rec:
        fig, ax = plt.subplots(figsize=(4.2, 3))
        ax.bar(range(len(rec)), np.array(rec) / 1e6, width=0.6, color=BLUE)
        ax.axhline(np.mean(rec) / 1e6, color=MUTED, linestyle="--", linewidth=1.2)
        ax.set(xlabel="window operator instance", ylabel="million trades received",
               title=f"Load per parallel instance ({maxc} cores)", xticks=range(len(rec)))
        figs.append(save(fig, f"scalability-skew{suffix}.png"))
        lines += [f"Window operator load at {maxc} cores: max/mean = {max(rec) / np.mean(rec):.2f} "
                  f"(1.00 = perfectly even). Per instance: {rec}", ""]

    if (d / "validation.json").exists():
        v = json.loads((d / "validation.json").read_text())
        lines += ["Correctness of the Kafka-path output against the reference:", "", "```",
                  json.dumps({k: v[k] for k in v if not k.startswith("examples")}, indent=1), "```", ""]
    lines += [f"![]({p.relative_to(RESULTS)})" for p in figs] + [""]


def scalability_overview(dirs: list[Path], lines: list[str]):
    """One table across several scalability runs: what each replayed and how fast it went per core count."""
    rows = []
    for d in dirs:
        settings = json.loads((d / "settings.json").read_text()) if (d / "settings.json").exists() else {}
        events = json.loads((d / "producer.json").read_text())["events"]
        for _, r in pd.read_csv(d / "summary.csv").iterrows():
            rows.append({"run": d.name, "events_kind": settings.get("events", "?"),
                         "exchanges": settings.get("exchanges", "all"), "events": events, "cores": int(r["cores"]),
                         "seconds_mean": r["seconds_mean"], "throughput_mean": r["throughput_mean"],
                         "speedup": r["speedup"], "tm_cpu_percent": r["tm_cpu_percent"]})
    t = pd.DataFrame(rows)
    lines += ["## Scalability across runs", "", t.round(3).to_markdown(index=False), ""]


# ------------------------------------------------------------------ latency

def latency(d: Path, lines: list[str], suffix: str = ""):
    rows, per_run = [], []
    for res in sorted(d.glob("results-*.jsonl")):
        tag = res.stem.replace("results-", "")
        b = pd.read_csv(d / f"boundaries-{tag}.csv")
        prod = json.loads((d / f"producer-{tag}.json").read_text())
        r = pd.read_json(res, lines=True)
        m = r.merge(b, left_on="windowEnd", right_on="window_end_ms", how="inner")
        m["latency_ms"] = m["emitTs"] - m["wall_ms"]
        m["wait_ms"] = m["firedTs"] - m["wall_ms"]
        m["processing_ms"] = m["emitTs"] - m["firedTs"]
        cores = int(tag.split("-")[0][1:])
        row = {"cores": cores, "target_rate": prod["rate"], "achieved_rate": prod["events_per_second"],
               "windows": len(m)}
        for col in ("latency_ms", "wait_ms", "processing_ms"):
            for q in (50, 95, 99):
                row[f"{col.split('_')[0]}_p{q}"] = float(np.percentile(m[col], q))
        row["latency_max"] = float(m["latency_ms"].max())
        adv = m[m["signal"].fillna("") != ""]
        row["advisory_latency_p99"] = float(np.percentile(adv["latency_ms"], 99)) if len(adv) else float("nan")
        rows.append(row)
        per_run.append((cores, prod["events_per_second"], np.sort(m["latency_ms"].to_numpy())))
    if not rows:
        return
    t = pd.DataFrame(rows).sort_values(["cores", "achieved_rate"])
    t.to_csv(d / "summary.csv", index=False)
    lines += [f"## Latency ({d})", "",
              "Latency = time from a window becoming complete (first event of the next window sent) to its "
              "EMA result being produced. wait = until the window fired; processing = after that.", "",
              t.round(1).to_markdown(index=False), ""]

    figs = []
    for c, sub in t.groupby("cores"):
        fig, ax = plt.subplots(figsize=(4.6, 3))
        for q, col in zip((50, 95, 99), (BLUE, ORANGE, AQUA)):
            ax.plot(sub["achieved_rate"] / 1e3, sub[f"latency_p{q}"], color=col, marker="o", markersize=6)
            ax.annotate(f"p{q}", (sub["achieved_rate"].iloc[-1] / 1e3, sub[f"latency_p{q}"].iloc[-1]),
                        xytext=(5, 0), textcoords="offset points", color=INK2, va="center")
        ax.set(xlabel="input rate (thousand events / s)", ylabel="latency (ms)",
               title=f"Latency under load ({c} cores)")
        ax.set_ylim(bottom=0)
        figs.append(save(fig, f"latency-vs-rate-c{c}{suffix}.png"))

    fig, ax = plt.subplots(figsize=(4.6, 3))
    palette = [BLUE, ORANGE, AQUA]
    shown = sorted(per_run, key=lambda x: x[1])
    pick = [shown[0], shown[len(shown) // 2], shown[-1]] if len(shown) > 3 else shown
    for (c, rate, lat), col in zip(pick, palette):
        ax.plot(lat, np.linspace(0, 1, len(lat)), color=col, label=f"{rate / 1e3:,.0f}k ev/s, {c} cores")
    ax.set(xlabel="latency (ms)", ylabel="share of windows", title="Latency distribution")
    ax.set_xscale("log")
    ax.legend()
    figs.append(save(fig, f"latency-cdf{suffix}.png"))
    lines += [f"![]({p.relative_to(RESULTS)})" for p in figs] + [""]


# ------------------------------------------------------------------ out-of-order sweep

def ooo(d: Path, lines: list[str]):
    s = pd.read_csv(d / "sweep.csv")
    extra = []
    for b in s["bound_ms"]:
        v = json.loads((d / f"validation-ooo{b}.json").read_text())
        sig = v["signals_all"]
        extra.append({"bound_ms": b, "windows_wrong": v["windows_with_value_mismatch"] + v["windows_missing_in_flink"],
                      "signals_missed_or_wrong": sig["missed"] + sig["spurious"] + sig["wrong_direction"],
                      "signals_expected": sig["expected"]})
    s = s.merge(pd.DataFrame(extra), on="bound_ms")
    s.to_csv(d / "summary.csv", index=False)
    lines += [f"## Out-of-order bound sweep ({d})", "", s.to_markdown(index=False), ""]

    x = np.arange(len(s))
    figs = []
    for col, title, name in (("late_trades", "Trades that missed their window", "ooo-late.png"),
                             ("windows_wrong", "Windows differing from the answer key", "ooo-windows.png")):
        fig, ax = plt.subplots(figsize=(4.2, 3))
        ax.bar(x, s[col], width=0.6, color=BLUE)
        for xi, val in zip(x, s[col]):
            ax.annotate(f"{val:,}", (xi, val), xytext=(0, 3), textcoords="offset points", ha="center", color=INK2)
        ax.set(xticks=x, xticklabels=[f"{b / 1000:g}s" for b in s["bound_ms"]],
               xlabel="out-of-order bound", ylabel="count", title=title)
        figs.append(save(fig, name))
    lines += [f"![]({p.relative_to(RESULTS)})" for p in figs] + [""]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scalability", type=Path, nargs="+",
                    help="one or more scalability runs, e.g. all trades, every event type, one exchange")
    ap.add_argument("--latency", type=Path, nargs="+",
                    help="one or more latency runs, e.g. the default and the tuned one")
    ap.add_argument("--ooo", type=Path)
    ap.add_argument("--results-dir", type=Path, default=Path("results"))
    a = ap.parse_args()
    global RESULTS, FIG
    RESULTS, FIG = a.results_dir, a.results_dir / "figures"
    lines = ["# Experiment results", "",
             "Generated by `python -m experiments.analyze`. Numbers only; interpretation belongs in the report.", ""]
    for kind, fn, given in (("scalability", scalability, a.scalability), ("latency", latency, a.latency),
                            ("ooo-sweep", ooo, a.ooo)):
        if isinstance(given, list):
            # several runs of one experiment: one table each, figures suffixed with the run's folder name
            for d in given:
                print(f"{kind}: {d}")
                fn(d, lines, suffix=f"-{d.name}" if len(given) > 1 else "")
            if kind == "scalability" and len(given) > 1:
                scalability_overview(given, lines)
            continue
        d = given or newest(kind)
        if d:
            print(f"{kind}: {d}")
            fn(d, lines)
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "summary.md").write_text("\n".join(lines))
    print(f"wrote {RESULTS / 'summary.md'} and figures in {FIG}/")


if __name__ == "__main__":
    main()
