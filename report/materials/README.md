# Report materials

Facts, diagrams, tables and notes for writing the CS-E4780 report. Nothing in this folder is report
text: the analysis, interpretation and conclusions are for the authors to write. Every number is
taken from a file in this repository, and each sheet names its sources.

## What's here

| File | Use it for |
|---|---|
| [`section-map.md`](section-map.md) | Start here. Each required section, what it must cover, where the material is, what's still open |
| [`design-decisions.md`](design-decisions.md) | 24 design decisions with alternatives, trade-offs and evidence, plus how the design evolved from the October prototype |
| [`facts/dataset.md`](facts/dataset.md) | Dataset size, exchanges, trades vs events, long tail, data-quality quirks |
| [`facts/setup.md`](facts/setup.md) | Hardware, versions, containers, topics, producer and job settings, how cores were varied |
| [`facts/results.md`](facts/results.md) | Every experiment number in one place: correctness, scalability, busy/idle time per task, load per instance, CPU per container, latency, out-of-order sweep |
| [`facts/derived-facts.md`](facts/derived-facts.md) | The raw extra numbers behind `results.md` (generated) |
| `tables/*.csv` | The same numbers as CSV (per-subtask records, per-container CPU and memory) |

## Diagrams (`diagrams/`, PDF for LaTeX, PNG at 300 dpi, TikZ source in `diagrams/src/`)

| File | Shows | Fits |
|---|---|---|
| `01-architecture` | Every container, the four topics with partitions and formats, who writes and reads what | full width (`figure*`) |
| `02-flink-job-graph` | The job as Flink actually runs it: three chained tasks, two hash shuffles, side output, state per operator, record counts from the 8-core run | full width |
| `03-event-classification` | How each input line becomes a trade, quote, placeholder, marker or malformed event (the order of checks in `TickParser`) | one column, tall |
| `04-window-latency-timeline` | When a window fires, and what latency / wait / processing measure | full width |
| `05-experiment-procedure` | How the scalability, latency and out-of-order experiments run | full width |

The job graph was checked against the job plan Flink reported over its REST API
(`results/scalability/20261124-132305-cores-1-8/job-c8-r1.json`): three vertices, HASH edges, one
slot-sharing group, max parallelism 128.

## New data figures (`figures/`)

| File | Shows |
|---|---|
| `subtask-load` | Trades per parallel instance at 4, 6 and 8 cores: the Kafka source (fixed by the 8 partitions) next to the window operator (after `keyBy`) |
| `container-cpu` | Mean CPU of the TaskManager, Kafka and the JobManager during the 1-8 core runs |

## Using them in the report

Copy the PDFs into `figures/` next to `report.tex`, then:

```latex
\begin{figure*}[t]
  \centering
  \includegraphics[width=\textwidth]{02-flink-job-graph}
  \caption{...}
  \Description{...}
  \label{fig:jobgraph}
\end{figure*}
```

The diagrams use Latin Modern; the ACM template uses Libertine. If that bothers you, `\input` the
TikZ body into the report instead of the PDF (drop the `\documentclass`, `\begin{document}` lines,
and add `\usetikzlibrary{positioning,fit,arrows.meta,calc,backgrounds,shadows,shapes.geometric,decorations.pathreplacing}` and `\input{_style}`).
Mind the page limit: five full-width diagrams won't fit in 4-5 pages, so pick one or two.

## Rebuilding

```bash
python report/materials/scripts/extract_facts.py   # facts/derived-facts.md and tables/
python report/materials/scripts/figures.py         # figures/ (needs matplotlib, pandas)
report/materials/diagrams/src/build.sh             # diagrams/ (needs pdflatex with TikZ, and pdftocairo)
```
