#!/bin/bash
# Build main.pdf and supplement.pdf. The two documents reference each other (xr-hyper), so each needs the other's .aux.
# Usage: paper/manuscript_qss/build.sh   (run from anywhere; writes the PDFs and auxiliary files here)
set -euo pipefail
cd "$(dirname "$0")"
run() { pdflatex -interaction=nonstopmode -halt-on-error "$1" > /dev/null; }
run main; bibtex main > /dev/null; run supplement; bibtex supplement > /dev/null
for _ in 1 2 3; do run main; run supplement; done
grep -H -E "undefined|Rerun to get" main.log supplement.log || echo "main.pdf and supplement.pdf built; references resolved"
