#!/bin/bash
# Build the Zenodo archive of the replication package from a committed state of paper/.
# Leaves out paper/feedback/ (internal review notes), the compiled PDF, which may lag behind the .tex sources, and
# paper/v3_code/ (the 2021 R scripts, kept in the repository for the record; the current analyses do not use them).
# Usage: paper/make_zenodo_package.sh [commit] [output-dir]
set -euo pipefail
REV="${1:-HEAD}"
OUT="${2:-.}"
ROOT="$(git rev-parse --show-toplevel)"
SHORT="$(git -C "$ROOT" rev-parse --short "$REV")"
ZIP="$OUT/pip-replication-package-$SHORT.zip"
git -C "$ROOT" archive --format=zip --prefix="pip-replication-package/" -o "$ZIP" "$REV" -- paper \
  ':(exclude)paper/feedback' ':(exclude)paper/manuscript_qss/main.pdf' \
  ':(exclude)paper/v3_code'
echo "$ZIP"
