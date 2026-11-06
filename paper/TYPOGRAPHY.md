# Final typography and layout checklist

Adapted from `writeup/TYPOGRAPHY.md` in `parasurama/adversarial_screener` (commit `f5cfb2a`) for the QSS manuscript in
`paper/manuscript_qss/` (`main.tex`). Only the build commands, file names, source conventions and
validation checks were changed.

Complete this audit before handing off manuscript changes or preparing a
submission. The goal is a professionally typeset academic paper with natural
paragraph endings and balanced page layout. A successful LaTeX build, a clean
log, or an automated short-line scan alone does not establish visual quality.

## 1. Build and inspect

- Build through the existing configuration: `paper/manuscript_qss/build.sh`, which compiles `main.tex`. Preserve a
  before PDF and log for comparison.
- Render **every page** at approximately **150–200 DPI** and visually inspect
  prose, proofs, footnotes, headings, lists, tables, captions, references,
  appendices, and page boundaries. Check both paragraph endings and whitespace.
- Also extract PDF text with **word bounding boxes**, using PyMuPDF or an
  equivalent tool, to flag suspiciously short final lines. Use both visual
  inspection and coordinate measurements; neither substitutes for the other.
- Measure line fill against the **available local text width**, accounting for
  columns, indentation, lists, table cells, and caption or note widths. Do not
  measure narrow table text against the full page width.
- Treat extracted lines as candidates. Confirm paragraph boundaries against
  the rendered page and source; PDF extraction can interleave footnotes,
  columns, equations, and captions. Record physical PDF page numbers and state
  how they relate to printed page numbers.

An optional rendering and extraction recipe, run from the repository root (needs `pip install pymupdf`):

```sh
paper/manuscript_qss/build.sh
python3 - <<'PY'
import json
from pathlib import Path
import pymupdf

# Choose a new directory for each iteration; refuse to mix old and new pages.
out = Path("build/typography/final")
out.mkdir(parents=True, exist_ok=False)
with pymupdf.open("paper/manuscript_qss/main.pdf") as pdf:
    pages = []
    for number, page in enumerate(pdf, start=1):
        page.get_pixmap(dpi=160, alpha=False).save(out / f"page-{number:03d}.png")
        pages.append({"pdf_page": number, "bounds": list(page.rect),
                      "words": page.get_text("words", sort=True)})
    (out / "words.json").write_text(json.dumps({
        "word_fields": ["x0", "y0", "x1", "y1", "text", "block", "line", "word"],
        "pages": pages,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
PY
```

This exports evidence; it does **not** detect paragraph boundaries or certify
the PDF. Use a fresh output directory for each iteration so obsolete page
images cannot be mistaken for the final document. Keep generated audit files
under ignored `build/typography/` or another local output directory.

## 2. Classify confirmed problems

| Priority | Paragraph-ending problem |
|---|---|
| Critical | Only one word, a mathematical symbol, a citation, or a short fragment remains, such as `harm.`, `Q.`, `tion 5.`, `listing.`, or `(3).` |
| High | Final line occupies less than 25% of the available text width |
| Medium | Final line occupies 25–35% of the available text width |

Critical classification takes precedence over the width thresholds. Also flag:

- Hyphenated words whose remainder creates a short final line.
- Headings stranded at page bottoms; captions separated from their figures or
  tables; equations or list items awkwardly separated from their explanations.
- Widows and orphans at page boundaries, isolated proof-ending symbols, and
  large or awkward gaps caused by paragraph, page, or forced float placement.

Distinguish genuine paragraph endings from headings, display mathematics,
intentional short statements or labels, and document-final lines. Complete
bibliographic fields and mathematical lead-ins need contextual review rather
than automatic rejection or blanket exemption. Explain retained exceptions.

## 3. Correct the source

Find the corresponding LaTeX passage and make the smallest effective change,
in this order of preference:

1. Adjust wording minimally while preserving the original meaning.
2. Remove unnecessary verbosity or use equivalent, more concise phrasing.
3. Use a paragraph-local adjustment such as `\looseness=-1` when appropriate.
4. Use carefully scoped `\emergencystretch` or other typographic adjustments
   only when needed.
5. Use nonbreaking spaces selectively to keep logically connected elements
   together, checking that they do not worsen line breaking elsewhere.

For a confirmed float gap, adjust that float's placement locally and recheck
ordering, caption attachment, and nearby text. Remove ineffective adjustments.

- Edit source files, **never the PDF or generated LaTeX files**.
- Do not change mathematical claims, definitions, substantive arguments,
  citations or their metadata, equation numbering, labels, or cross-references.
- Preserve the source's line wrapping (about 120 columns) and blank lines between paragraphs.
- Do not force prose wrapping with manual `\\` breaks, arbitrary spacing, or
  excessive `\mbox{}`. Do not change margins, font sizes, or global layout
  parameters merely to hide a local problem. Do not suppress box warnings to
  make the log appear clean.

## 4. Iterate and verify the final PDF

- Recompile after corrections, rerender, and inspect affected **and downstream
  reflowed pages**. A fixed paragraph can create a new short line, stranded
  heading, split caption, or gap elsewhere.
- Compare logs for overfull/underfull boxes and other warnings. Resolve new
  regressions and investigate remaining warnings; inspect the page even when
  the log is clean.
- Repeat until **all confirmed critical issues are resolved** and
  **high-priority issues are minimized**. Consider medium issues, retaining
  them when further changes would impair precision or overall layout.
- Finish with a scan of the whole final PDF and visual coverage of every page.
  Previously inspected page images may be reused only if pixel comparison
  confirms they are unchanged; directly inspect every changed page.
- Verify that formulas, citation content, equation numbering, and
  cross-references remain intact. Run `paper/manuscript_qss/build.sh` and check that every citation key and
  cross-reference resolves (no `undefined` warnings in either log) and that no number, citation, equation or
  cross-reference changed unintentionally. Any reworded sentence that carries a citation needs a new entry in
  `literature/verifications.md` (see the citation-needed skill). Report blocked checks honestly.

Do not stop at identifying defects or passing compilation. If a critical
problem remains, explicitly mark the typography audit incomplete.

## 5. Report the result

Keep a concise report tied to the exact final PDF (identify the build or file
hash). Include:

- Each confirmed problem's page, offending text, and priority.
- The source file and line or stable label, plus the correction applied.
- Any unresolved or deliberately retained issue, with its reason.
- Representative before/after text or image comparisons.
- Page coverage, rendering DPI, coordinate-check method, build/validation
  results, and any limits on verification.

Separate intentional short lines and extraction false positives from defects.
Check that reported after-text and page numbers match the actual final PDF,
especially when footnotes or floats have moved. Keep the checklist recurring:
an earlier audit does not certify a later revision.
