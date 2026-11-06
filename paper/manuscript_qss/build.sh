#!/bin/bash
# Build main.pdf.
# Usage: paper/manuscript_qss/build.sh   (run from anywhere; writes the PDF and auxiliary files here)
set -euo pipefail
cd "$(dirname "$0")"
run() { pdflatex -interaction=nonstopmode -halt-on-error main > /dev/null; }
run; bibtex main > /dev/null
for _ in 1 2 3; do run; done
grep -H -E "undefined|Rerun to get" main.log || echo "main.pdf built; references resolved"
