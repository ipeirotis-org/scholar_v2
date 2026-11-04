# Findings so far

These are results on Semantic Scholar (S2) data, release 2026-10-27, with citations through 2025. Table names refer to
the BigQuery dataset `scholar-version2.paper_rp`.

## 1. Paper percentiles settle within a few years (S2 version of v3 Figs. 4–6)

`paper_percentile_stability` comes from `sql/03_paper_percentile_stability.sql`. It uses the `article` reference set
and Hazen percentiles within (field, publication year) at each age. A paper of age t has its citations counted through
calendar year `pub_year + t`.

Correlating percentiles is the same as a Spearman correlation of raw citation counts, because the percentiles are
mid-ranks.

### Pooled fields, articles published 1980–1995 (7.2M papers)

| Ages | r | OLS slope | Mean absolute change |
|---|---|---|---|
| 1 vs 10 | 0.724 | 0.846 | 0.161 |
| 2 vs 10 | 0.850 | 0.911 | 0.115 |
| 3 vs 10 | 0.908 | 0.944 | 0.086 |
| 5 vs 10 | 0.959 | 0.975 | 0.051 |
| 3 vs 30 | 0.835 | 0.882 | 0.125 |
| 5 vs 30 | 0.889 | 0.918 | 0.100 |
| 10 vs 30 | 0.942 | 0.956 | 0.068 |
| 20 vs 30 | 0.984 | 0.989 | 0.030 |

### Stability over cohorts

r(3, 10) is between 0.875 and 0.910 for every cohort from 1960 to 1995. For the 5-against-30 comparison, r is between
0.880 and 0.905.

The early ages have become more informative over time. r(1, 10) was 0.72 for cohorts before 1995, 0.76 in 2000, 0.79
in 2005, 0.82 in 2010 and 0.83 in 2015. Faster citation dynamics and preprints are the likely reasons; this needs
checking.

### By field, cohorts 1985–1995

| Field | r(3, 10) | r(5, 30) |
|---|---|---|
| Biology | 0.928 | 0.885 |
| Chemistry | 0.916 | 0.879 |
| Medicine | 0.911 | 0.887 |
| Physics | 0.911 | 0.886 |
| Computer Science | 0.908 | 0.883 |
| Mathematics | 0.891 | 0.843 |
| Engineering | 0.882 | 0.856 |
| Economics | 0.872 | 0.863 |
| Sociology | 0.858 | 0.861 |
| Business | 0.838 | 0.842 |
| Political Science | 0.836 | 0.819 |
| Law | 0.812 | 0.795 |
| History | 0.786 | 0.730 |
| Art | 0.753 | 0.673 |

What this means for the reviewers:

- **A field-specific citation window.** This answers round 1, Reviewer 2 (Wang 2013 shows the window depends on the
  field). A window of 3 years reaches r ≈ 0.9 against age 10 in the natural and life sciences. The humanities and law
  need about 5 years for the same level.
- **The bias toward the middle is mild.** The OLS slope is about 0.9 in every field, so early percentiles pull only
  slightly toward the middle.

## 2. PiP-AUC properties (`checks/pip_properties.py`)

These are random-simulation checks on 2,000–5,000 cases, with cohort count distributions drawn as lognormals.

- **Step form equals threshold form.** Σ_k y_k [F(k) − F(k−1)] = ∫₀¹ F(n(t)) dt, to within the numerical grid error
  (1e-5).
- **Bounds.** y_min · F(n) ≤ PiP ≤ y_max · F(n) held in every case. Equivalently, PiP = F(n) × a weighted mean of the
  paper percentiles.
- **Monotonicity.** Adding a paper never lowered the score, in either the step form or the app's trapezoid form.
- **Trapezoid against step.** The app's trapezoid rule (`stats_author_pip_scores_current.sql`) is always at least the
  step form. The gap has a median of 0.008 and a maximum of 0.05, and it is largest for authors with few papers and
  widely spread percentiles. **Open decision:** the identity and the decomposition hold exactly only for the step
  form. The paper should either define PiP-AUC as the step form and switch the app, or keep the trapezoid and state
  the decomposition as approximate.
- **Reviewers' example (round 2).** A has 50 papers at the 1st percentile; B has one paper at the 25th. A scores 0.009
  and B scores 0, because F(1) = 0 among active authors. Both land at the bottom instead of A outranking B.
