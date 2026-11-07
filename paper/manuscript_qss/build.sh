#!/bin/bash
# Build main.pdf.
# Usage: paper/manuscript_qss/build.sh   (run from anywhere; writes the PDF and auxiliary files here)
set -euo pipefail
cd "$(dirname "$0")"
if kpsewhich libertine.sty > /dev/null 2>&1; then
  TEX=pdflatex
else
  export OSFONTDIR="/usr/share/texmf/fonts/opentype:${HOME}/.local/share/fonts"
  TEX=lualatex
fi
run() { "$TEX" -interaction=nonstopmode -halt-on-error main > /dev/null; }
run; bibtex main > /dev/null
for _ in 1 2 3; do run; done
grep -H -E "undefined|Rerun to get" main.log || echo "main.pdf built; references resolved"
