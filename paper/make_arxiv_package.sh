#!/bin/bash
# Build the single-spaced, line-number-free arXiv upload archive (pip-arxiv-2103.16025v4.zip)
# and preview PDF (pip-arxiv-2103.16025v4.pdf) from paper/manuscript_qss/ and paper/figures/*.pdf.
# Usage: paper/make_arxiv_package.sh [output-dir]
set -euo pipefail

OUT="${1:-.}"
PAPER_DIR="$(cd "$(dirname "$0")" && pwd)"
MS="$PAPER_DIR/manuscript_qss"
FIGS="$PAPER_DIR/figures"

mkdir -p "$OUT"
OUT_ABS="$(cd "$OUT" && pwd)"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$WORK/source/figures" "$WORK/build/figures"
cp "$MS/references.bib" "$MS/qss-apa.bst" "$WORK/source/"
cp "$MS/references.bib" "$MS/qss-apa.bst" "$WORK/build/"

python3 - "$MS/main.tex" "$FIGS" "$WORK/source" "$WORK/build" <<'PY'
import pathlib, re, shutil, sys

tex_path = pathlib.Path(sys.argv[1])
figs_dir = pathlib.Path(sys.argv[2])
source_dir = pathlib.Path(sys.argv[3])
build_dir = pathlib.Path(sys.argv[4])

raw = tex_path.read_text(encoding="utf-8")

# Copy only the vector PDF figures actually referenced in main.tex.
referenced_figs = sorted(set(re.findall(r"\\includegraphics(?:\[[^\]]*\])?\{([^}]+)\}", raw)))
if not referenced_figs:
    raise SystemExit(f"No \\includegraphics directives found in {tex_path}")
for fig_name in referenced_figs:
    fig_src = figs_dir / fig_name
    if not fig_src.is_file():
        raise SystemExit(f"Referenced figure missing: {fig_src}")
    shutil.copy2(fig_src, source_dir / "figures" / fig_name)
    shutil.copy2(fig_src, build_dir / "figures" / fig_name)

common_replacements = [
    (
        "% Manuscript for Quantitative Science Studies. Build: paper/manuscript_qss/build.sh.",
        "% arXiv preprint source (2103.16025v4). Built via paper/make_arxiv_package.sh.",
    ),
    (
        "\\usepackage{setspace}\n",
        "",
    ),
    (
        "\\makeatletter\n\\DeclareRobustCommand{\\harvardand}{\\ifNAT@swa\\&\\else and\\fi}\n\\makeatother",
        "\\makeatletter\n\\@ifundefined{ifNAT@swa}\n"
        "  {\\DeclareRobustCommand{\\harvardand}{and}}\n"
        "  {\\DeclareRobustCommand{\\harvardand}{\\ifNAT@swa\\&\\else and\\fi}}\n\\makeatother",
    ),
    (
        "\\expandafter\\def\\expandafter\\UrlNoBreaks\\expandafter{\\UrlNoBreaks\\do\\:}",
        "\\makeatletter\n\\@ifundefined{UrlNoBreaks}{}{\\expandafter\\def\\expandafter\\UrlNoBreaks\\expandafter{\\UrlNoBreaks\\do\\:}}\n\\makeatother",
    ),
    (
        "\\usepackage[mathlines]{lineno}\n"
        "\\BeforeBeginEnvironment{equation}{\\begin{linenomath*}}\n"
        "\\AfterEndEnvironment{equation}{\\end{linenomath*}}\n"
        "\\linenumbers\n"
        "\\onehalfspacing\n"
        "\\graphicspath{{../figures/}}",
        "\\graphicspath{{figures/}}",
    ),
]

for old, new in common_replacements:
    if old not in raw:
        raise SystemExit(f"Expected snippet not found in {tex_path}:\n{old}")
    raw = raw.replace(old, new, 1)

square_line = "\\providecommand{\\square}{\\mdlgwhtsquare}"
if square_line not in raw:
    raise SystemExit(f"Expected \\square fallback not found in {tex_path}")

# Build copy retains the \ifPDFTeX ... \else ... \fi fallback in case local builds use LuaLaTeX.
build_tex = raw.replace(
    square_line,
    "\\ifPDFTeX\\providecommand{\\square}{\\leavevmode\\hbox{\\vrule\\vbox{\\hrule width 0.5em\\kern 0.5em\\hrule}\\vrule}}"
    "\\else\\providecommand{\\square}{\\mdlgwhtsquare}\\fi",
    1,
)
(build_dir / "main.tex").write_text(build_tex, encoding="utf-8")

# arXiv source copy removes the non-PDFTeX \setmainfont/\setsansfont/\setmonofont branch completely
# so arXiv Submission 1.5's static file scanner does not flag OpenType font names as missing files.
iftex_block = (
    "\\usepackage{iftex}\n"
    "\\ifPDFTeX\n"
    "  \\usepackage[T1]{fontenc}\n"
    "  \\usepackage[utf8]{inputenc}\n"
    "  \\usepackage{libertine}\n"
    "  \\usepackage[libertine]{newtxmath}\n"
    "\\else\n"
    "  \\usepackage{unicode-math}\n"
    "  \\setmainfont{Libertinus Serif}\n"
    "  \\setsansfont{Libertinus Sans}\n"
    "  \\setmonofont[Scale=MatchLowercase]{Libertinus Mono}\n"
    "  \\setmathfont{Libertinus Math}\n"
    "\\fi"
)
pdftex_only = (
    "\\usepackage[T1]{fontenc}\n"
    "\\usepackage[utf8]{inputenc}\n"
    "\\usepackage{libertine}\n"
    "\\usepackage[libertine]{newtxmath}"
)
if iftex_block not in raw:
    raise SystemExit("Expected \\ifPDFTeX font block not found in main.tex")

arxiv_tex = raw.replace(iftex_block, pdftex_only, 1).replace(
    square_line,
    "\\providecommand{\\square}{\\leavevmode\\hbox{\\vrule\\vbox{\\hrule width 0.5em\\kern 0.5em\\hrule}\\vrule}}",
    1,
)
(source_dir / "main.tex").write_text(arxiv_tex, encoding="utf-8")
PY

(
  cd "$WORK/build"
  if kpsewhich libertine.sty > /dev/null 2>&1; then
    TEX=pdflatex
  else
    export OSFONTDIR="/usr/share/texmf/fonts/opentype:${HOME}/.local/share/fonts"
    TEX=lualatex
  fi
  run() { "$TEX" -interaction=nonstopmode -halt-on-error main > /dev/null; }
  run
  bibtex main > /dev/null
  for _ in 1 2 3; do run; done
  if grep -E "undefined|Rerun to get" main.log; then
    echo "Unresolved references or rerun needed in arXiv build" >&2
    exit 1
  fi
)

cp "$WORK/build/main.bbl" "$WORK/source/main.bbl"
cp "$WORK/build/main.pdf" "$OUT_ABS/pip-arxiv-2103.16025v4.pdf"

ZIP="$OUT_ABS/pip-arxiv-2103.16025v4.zip"
rm -f "$ZIP"
(
  cd "$WORK/source"
  python3 - "$ZIP" <<'PY'
import pathlib, sys, zipfile

out_zip = pathlib.Path(sys.argv[1])
files = sorted(p for p in pathlib.Path(".").rglob("*") if p.is_file())
with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for p in files:
        zf.write(p, p.as_posix())
PY
)

echo "$ZIP"
echo "$OUT_ABS/pip-arxiv-2103.16025v4.pdf"
