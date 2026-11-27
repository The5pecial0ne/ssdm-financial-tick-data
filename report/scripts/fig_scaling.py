"""Scaling figure for the report: speedup against cores (25 Nov rerun, 3 repeats), with the
ceiling that the Kafka partition split imposes, and the load balance of the source and
window operators. Inputs: results/scalability/20261125-223400-cores-1-8-r3/{summary.csv,job-c*-r1.json}.

    python report/scripts/fig_scaling.py        # from the repo root
"""
import csv, json, glob, re
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RUN = Path("results/scalability/20261125-223400-cores-1-8-r3")
rows = list(csv.DictReader(open(RUN / "summary.csv")))
cores = [int(r["cores"]) for r in rows]
t = {int(r["cores"]): float(r["seconds_mean"]) for r in rows}
sd = {int(r["cores"]): float(r["seconds_std"]) for r in rows}
speed = [t[1] / t[c] for c in cores]
# error bar on the speedup from the spread of the c-core runs (1-core mean taken as exact)
err = [t[1] / t[c] ** 2 * sd[c] for c in cores]

def balance(c, name_part):
    """max/mean of records received per subtask for the vertex whose name contains name_part."""
    j = json.load(open(RUN / f"job-c{c}-r1.json"))
    for v in j["vertices"]:
        if name_part in v["name"]:
            recs = [st["metrics"]["read-records"] if "Source" not in v["name"] else st["metrics"]["write-records"]
                    for st in v["subtasks"]]
            return max(recs) / (sum(recs) / len(recs))
    raise KeyError(name_part)

src = [balance(c, "Source") for c in cores]
win = [balance(c, "window") for c in cores]
ceiling = [c / b for c, b in zip(cores, src)]

plt.rcParams.update({"font.size": 8, "font.family": "serif"})
fig, (a, b) = plt.subplots(2, 1, figsize=(3.4, 3.6), sharex=True, gridspec_kw={"height_ratios": [1.6, 1]})
a.plot(cores, cores, "--", color="#9ca3af", lw=1, label="linear")
a.plot(cores, ceiling, ":", color="#b45309", lw=1.4, label="partition ceiling $c/$(source max/mean)")
a.errorbar(cores, speed, yerr=err, color="#2563eb", marker="o", ms=3.5, lw=1.8, capsize=2, label="measured (mean of 3)")
a.axvspan(4.5, 8.4, color="#f3f4f6", zorder=0)
a.text(6.45, 1.0, "beyond the 4\nperformance cores", ha="center", fontsize=7, color="#6b7280")
a.set_ylabel("speedup over 1 core")
a.set_ylim(0, 8.4); a.legend(frameon=False, fontsize=6.6, loc="upper left")
b.plot(cores, src, marker="s", ms=3, color="#374151", label="Kafka source (partitions)")
b.plot(cores, win, marker="o", ms=3, color="#2563eb", label="window operator (keyBy)")
b.axhline(1, color="#9ca3af", lw=0.8)
b.set_ylabel("load max/mean"); b.set_xlabel("TaskManager cores (= parallelism)")
b.set_ylim(0.9, 1.9); b.legend(frameon=False, fontsize=6.6, loc="upper left")
for ax in (a, b):
    ax.spines[["top", "right"]].set_visible(False); ax.grid(True, color="#e5e7eb", lw=0.6)
    ax.set_xticks(cores); ax.set_xlim(0.6, 8.4)
fig.tight_layout(h_pad=0.4)
fig.savefig("report/figures/scaling.pdf"); fig.savefig("report/figures/scaling.png", dpi=200)
for c, s, e, so, w, ce in zip(cores, speed, err, src, win, ceiling):
    print(f"{c}: speedup {s:.2f}±{e:.2f} source {so:.2f} window {w:.2f} ceiling {ce:.2f}")
