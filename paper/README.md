# Percentile in Percentile: Evaluating Scholars Across Career Stages — Replication Package

Replication package for *Percentile in Percentile: Evaluating Scholars Across Career Stages* (Panos Ipeirotis and Sen Tian). An interactive demonstration of the metric on Google Scholar profiles is available at [pip-score.org](https://www.pip-score.org/).

## Layout

- **`manuscript_qss/`** holds the LaTeX manuscript (`main.tex` and `references.bib`). Build it with `build.sh`. Every number in the manuscript comes from `FINDINGS.md` and `results/`.
- **`figures/`** holds the figure scripts (`make_figures.py`, `make_tau_sweep.py`), the export of their data from BigQuery (`export_figure_data.sh`, output in `figures/data/`) and the rendered PDF/PNG files.
- **`results/`** holds CSV exports of the result tables that the queries write (aggregates only; see `results/README.md`). The per-author and per-article tables stay in the BigQuery datasets `scholar-version2.paper_rp` and `scholar-version2.paper_rp_noself`, readable by anyone signed in to Google Cloud.
- **`COMPUTATION.md`** explains how the SQL evaluates the integral that defines PiP (40 percentile classes), how `sql/25_pip_exact.sql` computes the exact integral, and how much the two differ.
- **`lean/`** holds the Lean 4 + Mathlib (`v4.35.0`) formalization of the mathematical properties and integration identities in Section 4, `COMPUTATION.md`, and Appendix C (`lake build` compiles all proofs and checks axioms).
- **`checks/`** holds the numerical simulation scripts checking the properties of PiP (`sym_properties.py`) and the productivity-normalized area (`pip_properties.py`).
- **`honors/`** holds the scripts and curated data for the later-recognition analysis (`fetch_wikidata_honors.py`, `analyze_honors.py`, `analyze_gs_honors.py`).
- **`sql/`** holds the analysis queries, numbered in run order. They write to the BigQuery dataset `scholar-version2.paper_rp` (and `scholar-version2.paper_rp_noself` for the self-citation-free pipeline).

## Data decisions

- **Source.** All analyses use the Semantic Scholar (S2) Academic Graph snapshot of release 2026-10-27, loaded 2026-11-01. Citing year 2026 is partial, so citation counts stop at the end of 2025. Google Scholar profiles serve as a validation check on author disambiguation and later recognition.
- **Age.** A paper's age $t$ covers citations received through calendar year `pub_year + t`. Age 0 is the publication year itself, plus any citations dated earlier, which come from preprints.
- **Reference sets.** Paper percentiles compare a paper with papers from the same publication year:
  - `all` includes every S2 paper.
  - `article` includes papers typed `JournalArticle`, `Conference`, `Review`, `Study`, `ClinicalTrial`, `MetaAnalysis` or `CaseReport`, and excludes `Editorial`, `News` and `LettersAndComments`. About half of S2 records carry no type; those are cited far less often (36% against 86% for journal articles, 1990–2019).
- **Uncited papers stay in.** Unlike the live web application, the paper's tables keep uncited papers, and no filter depends on information from after the evaluation year.
