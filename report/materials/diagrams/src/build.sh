#!/usr/bin/env bash
# Builds every diagram: src/NN-name.tex -> ../NN-name.pdf and ../NN-name.png (300 dpi).
# Needs pdflatex (TeX Live with pgf/TikZ and standalone) and pdftocairo (poppler).
set -euo pipefail
cd "$(dirname "$0")"
for f in [0-9]*.tex; do
  n=${f%.tex}
  pdflatex -interaction=nonstopmode -halt-on-error "$f" >/dev/null || { echo "failed: $f (see $n.log)"; exit 1; }
  mv "$n.pdf" ..
  pdftocairo -png -r 300 -singlefile "../$n.pdf" "../$n"
  rm -f "$n.aux" "$n.log"
  echo "built $n"
done
