# Report kit

- `report.tex`: the working report. Facts, setup, method, result tables and figures are filled in;
  the analysis, interpretation, conclusions and contributions are marked with red TODOs, each with a
  "FACT SHEET" comment above it listing the observations and candidate explanations. `report.pdf` is
  the current build. Figures it uses are in `figures/`. Build with `latexmk -pdf report.tex` or upload
  `report.tex`, `references.bib` and `figures/` to Overleaf.
- `skeleton.tex`: ACM `sigconf` skeleton with the sections the course asks for. Each section has
  comments listing what the grading criteria look for and where the evidence is in this repo. It
  contains headings only, because the course requires the analysis and conclusions to be your own writing.
- `references.bib`: the dataset citation and the core systems papers.
- `AI_USE.md`: template for the required AI-use disclosure.

Figures come from `python -m experiments.analyze` (written to `results/figures/`) and the
dashboard screenshot `docs/dashboard.png`. The numbers behind every figure are in `results/summary.md`.
Page limit: 4 to 5 pages.
