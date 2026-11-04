# Revision plan

Status as of 2026-11-04:

- **History.** PLOS ONE sent three "revision required" decisions: April 2021, August 2021 and January 2022.
- **Lapse.** The third revision was not submitted by its July 2022 deadline.
- **New target.** The revision goes to *Quantitative Science Studies* as a new paper.
- **Merge.** The scholar indicator S_P5 is replaced by PiP-AUC.

## Reviewer concerns and how the revision answers them

### 1. Summing paper percentiles (S_P5)

**Concern.** This was the main objection in rounds 2 and 3. A sum lets many weak papers outrank one strong paper.
Reviewers also objected that it mixes quantity and quality in an unbounded way, that no prior work justifies sums of
percentiles, and that the indicator is inconsistent in the sense of Waltman & van Eck (2012).

**Answer.** PiP-AUC replaces the sum. Sort the author's papers by citation percentile, y_1 >= ... >= y_n, and let F_c
be the distribution of paper counts in the author's career-start cohort, with p_c(k) = F_c(k) - F_c(k-1). Then:

    PiP-AUC ≈ Σ_k y_k p_c(k)
            = F_c(n) × [Σ_k y_k p_c(k) / F_c(n)]
            = productivity percentile × weighted mean paper percentile

Both factors are bounded percentiles. The score also has an equivalent threshold form: ∫_0^1 F_c(n(t)) dt, where n(t)
is the number of the author's papers above percentile t.

The theory section will cover:

- the decomposition above and its bounds;
- monotonicity in added papers;
- a frank check of consistency;
- the relation to I3 (Leydesdorff & Bornmann 2011), an established indicator that sums percentiles;
- the Mann–Whitney reading: the mean percentile is the probability that the author's paper beats a random same-year
  paper.

### 2. Comparing across time points

**Concern.** Papers of age t were compared with older papers evaluated at their own age t, so the comparison mixes
calendar years. Scholars were compared across career-start years.

**Answer.** Paper percentiles are computed within the publication year at each age. Scholar percentiles are computed
within the career-start year at each evaluation year. No percentile pools calendar years.

### 3. Selection effects and survivorship

**Concern.** The data covered only faculty at the top-10 US universities.

**Answer.** S2 covers all authors, including those who stopped publishing. The revision adds a test of whether an
early-career score predicts who is still publishing later. Benchmarks are defined from information available in the
evaluation year, never from current status.

### 4. Field normalization

**Concern.** The paper should be placed within the field-normalization literature (Waltman & van Eck 2015, Zitt 2005),
and the stability of field normalization should be addressed.

**Answer.** S2 gives a field for each paper. Results will be reported per field, alongside the pooled results.

### 5. Citation windows

**Concern.** The 5-year window for papers, the 10-year horizon for scholars and the 30-year comparison need
justification, a minimum age for papers, and a sensitivity analysis.

**Answer.** S2 has yearly citations for every paper, so windows from 1 to 30 years can be compared by field.

### 6. Data availability and author disambiguation

**Concern.** The original data link held only Git LFS pointers, and reviewers asked how good the author
disambiguation is.

**Answer.** S2 data is ODC-BY, and the derived tables will be deposited. Disambiguation is checked against Google
Scholar profiles and against S2 authors with an ORCID.

### 7. Presentation

**Concern.** Reviewers asked for:

- percentile background (Hazen, Bornmann & Williams 2020, the Leiden Manifesto, beamplots);
- consistent wording ("scholars" against "tenured professors");
- simpler notation and clearer figure captions;
- justification for the 9:1 split and the 75 model fits;
- an explanation of the non-monotone R^2 for future-work prediction;
- a conventional structure and a limitations section.

**Answer.** Rewrite during the revision.

## Analyses (S2), mapped to the v3 figures

1. **Cohort stationarity (Fig. 3).** Scholar scores at age t by career-start year: full distributions, not only
   medians.
2. **Paper percentile stability (Figs. 4–6).** P(t1) against P(t2) by publication year and by field.
3. **Scholar score stability (Fig. 7).** PiP-AUC(t1) against PiP-AUC(t2), and the future-work version.
4. **Prediction models (Fig. 8).** Autoregressive and Markov baselines against gradient boosting.
5. **New: survivorship.** Does early-career PiP-AUC predict continued publishing?
6. **New: per-field results** for 1–4.
7. **New: citation-window sensitivity.**
8. **New: S2 against Google Scholar** for overlapping authors.

## Build status

| Step | Table | Status |
|---|---|---|
| 01 | `paper_rp.paper_citations_by_age` | built 2026-11-04: 226.0M papers 1950–2025 (71.5M articles, 146M with a field, 105M cited by 2025); validated (see below) |
| 02 | `paper_rp.citation_percentile_lookup` | built 2026-11-04: 25.1M rows, 94,550 partitions; validated (see below) |
| 03 | `paper_rp.paper_percentile_stability` | built 2026-11-04: results in `FINDINGS.md` §1 |
| 04 | `paper_rp.article_percentiles` | built 2026-11-04: 71.5M articles; per-age arrays of citations, `below` (share with fewer citations) and Hazen percentiles |
| 05 | `paper_rp.author_articles` | built 2026-11-04: 277.8M author–article links; career start = first article |
| 06 | `paper_rp.author_pip_by_age` (`sql/run_06.sh`) | built 2026-11-04: per author and career age 1–30, counts, h-index, productivity percentile, step-form PiP-AUC; two peer rules (`active` 111.6M rows, `min3` 192.7M rows) |
| 07 | `paper_rp.author_percentiles_by_age` | built 2026-11-04: within-cohort percentiles of PiP, h-index, citations, v3 sum, mean paper percentile, article count |
| 08 | `paper_rp.author_stability` | built 2026-11-04: `FINDINGS.md` §5 |
| 09 | `paper_rp.survivorship_base` / `_auc` / `_deciles` | built 2026-11-04: `FINDINGS.md` §5 |

### What the validation checked

**Table 01:**

- Cumulative counts never decrease with age.
- Ages that can't be observed yet are NULL.
- Citations through 2025 sum to 97.5% of S2's own `citationcount` (2000–2005 papers), with a correlation of 0.999.
  The remainder is 2026 citations and citing papers that have no year.

**Table 02:**

- Each partition's count equals the number of observable papers in table 01.
- The paper-weighted mean Hazen percentile is exactly 0.5 in every partition.

### Example: the 2000 cohort at age 5

| Reference set | Uncited by age 5 | Citations at the 67th–71st percentile | Citations at the ~95th percentile |
|---|---|---|---|
| `all` | 63% | 1 citation is the 67th | 50 citations is the 98.6th |
| `article` | 30% | 10 citations is the 71st | 50 citations is the 95th |
