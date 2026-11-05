# Rank percentiles and PiP-AUC: paper workspace

Revision of Tian & Ipeirotis, *On the Predictability of Utilizing Rank Percentile to Evaluate Scientific Impact*
([arXiv:2103.16025](https://arxiv.org/abs/2103.16025), v3, December 2021). The revision merges the paper with the
PiP-AUC score behind [pip-score.org](https://www.pip-score.org/), reruns the analyses on Semantic Scholar (S2) bulk
data, and targets *Quantitative Science Studies*.

## Layout

- **`manuscript_qss/`** holds the new manuscript for *Quantitative Science Studies* (`main.tex`, `references.bib`,
  compiled `main.pdf`). Build with `latexmk -pdf main.tex`. Every number in it comes from `FINDINGS.md`; open
  decisions for the authors are marked `TODO(authors)`.
- **`figures/`** holds the figure scripts (`make_figures.py`, `make_tau_sweep.py`), the export of their data from
  BigQuery (`export_figure_data.sh`, output in `figures/data/`) and the rendered PDF/PNG files.
- **`manuscript/`** holds the arXiv v3 LaTeX source (`ms.tex` includes `introduction`, `results`, `discussion` and
  `supplemental`), with its figures from the original Google Scholar dataset.
- **`results/`** holds CSV exports of the result tables the queries write (aggregates only; see
  `results/README.md`). The per-author and per-article tables stay in the BigQuery datasets `paper_rp` and
  `paper_rp_noself`, readable by anyone signed in to Google Cloud.
- **`v3_code/`** holds the v3 R scripts, copied from
  [sentian/SciImpactRanking](https://github.com/sentian/SciImpactRanking) (commit `2aaf31d`, December 2021) for the
  record. The current analyses don't use them.
- **`sql/`** holds the analysis queries, numbered in run order. They write to the BigQuery dataset
  `scholar-version2.paper_rp`, which is kept separate from the app's `statistics` and `gs_statistics` datasets.
- **`REVISION_PLAN.md`** lists the reviewer concerns from PLOS ONE (PONE-D-21-10383, 2021–2022) and how the revision
  addresses each one.
- **`feedback/`** holds the pre-submission reviews of the QSS manuscript (`review_A.md`, `review_B.md`) and
  `TRIAGE.md`, which decides what to fix, in what order, and what to decline.

## Data decisions

- **Source.** All analyses use the S2 snapshot of release 2026-10-27, loaded 2026-11-01. Citing year 2026 is
  partial, so citation counts stop at the end of 2025. Google Scholar profiles serve only as a robustness check for
  authors who appear in both sources.
- **Age.** A paper's age t covers citations received through calendar year `pub_year + t`. Age 0 is the publication
  year itself, plus any citations dated earlier, which come from preprints.
- **Reference sets.** Paper percentiles compare a paper with papers from the same publication year:
  - `all` includes every S2 paper.
  - `article` includes papers typed JournalArticle, Conference, Review, Study, ClinicalTrial, MetaAnalysis or
    CaseReport, and excludes Editorial, News and LettersAndComments. About half of S2 records carry no type; those
    are cited far less often (36% against 86% for journal articles, 1990–2019).
- **Uncited papers stay in.** Unlike the app's tables, the paper's tables keep uncited papers, and no filter depends
  on information from after the evaluation year. The app's reference sets do depend on it: papers cited as of today,
  authors with 6 or more papers as of today, and active authors defined by their current h-index.
