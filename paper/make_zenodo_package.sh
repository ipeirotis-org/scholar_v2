#!/bin/bash
# Build the Zenodo archive of the replication package from a committed state of paper/.
# Leaves out internal notes, legacy files, and the compiled PDF (which is built from manuscript_qss/main.tex).
# Usage: paper/make_zenodo_package.sh [commit] [output-dir]
set -euo pipefail
REV="${1:-HEAD}"
OUT="${2:-.}"
ROOT="$(git rev-parse --show-toplevel)"
SHORT="$(git -C "$ROOT" rev-parse --short "$REV")"
ZIP="$OUT/pip-replication-package-$SHORT.zip"
git -C "$ROOT" archive --format=zip --prefix="pip-replication-package/" -o "$ZIP" "$REV" -- paper \
  ':(exclude)paper/feedback' ':(exclude)paper/manuscript_qss/main.pdf' \
  ':(exclude)paper/v3_code' ':(exclude)paper/manuscript' \
  ':(exclude)paper/REVISION_PLAN.md' ':(exclude)paper/TYPOGRAPHY.md'
echo "$ZIP"
