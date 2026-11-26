# Report

- `report.pdf`: the submitted report (ACM `sigconf`, 6 pages including references).
- `report.tex`, `references.bib`, `figures/`: its source. Build with `latexmk -pdf report.tex`, or upload
  those three to Overleaf. The report uses `longtail-week.png`, `dashboard-top.png` and `scaling.pdf`.
  The other two PNGs in `figures/` belong to earlier drafts.
- `scripts/fig_scaling.py`: makes `figures/scaling.pdf` from
  `results/scalability/20261125-223400-cores-1-8-r3/` (speedup, the source-partition ceiling, and
  load per instance).
- `AI_USE.md`: the AI-use disclosure, the same text as the report's "Use of AI Tools" section.
- `materials/`: the working notes behind the report (design decisions, fact sheets, diagrams).
- `skeleton.tex`: the empty ACM skeleton the report started from.

Every number in the report comes from `results/`. Comments in `report.tex` name the source of each
section and table.
