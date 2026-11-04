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

## 3. Author-level setup (`sql/04`–`07`)

Each author's career start is the year of their first article. At evaluation year Y = start + age, an author's peers
are authors with the same start year who meet the peer rule using only information up to Y.

### Peer rules

- **`active`** (main): at least 3 articles and an h-index of at least 3 at Y. This is the as-of-year version of the
  app's `active_authors`, which uses current values instead.
- **`min3`**: at least 3 articles at Y.

Article counts and the h-index never decrease, so once an author is a peer, they remain one at every later age.

### What the tables hold

- **Paper percentile y:** the share of articles from the same publication year with strictly fewer citations at Y,
  so uncited papers score 0.
- **PiP-AUC:** step form, Σ_k y_(k) p(k), where p(k) is the share of peers with exactly k articles.

### Size and validation

- `author_pip_by_age`: 111.6M author-age rows for 11.1M authors under `active`; 192.7M rows for 19.2M authors under
  `min3`.
- The bounds y_min·F(n) ≤ PiP ≤ y_max·F(n) never fail.
- Within-cohort percentiles average exactly 0.5 in all 1,665 peer groups.

### Why `active` is the main rule

`min3` puts a large mass at PiP = 0: 54M of 193M rows. When every peer has 3 or more papers, p(1) = p(2) = 0, so a
3-article author scores y₍₃₎·p(3), which is zero whenever their third-best article is uncited. This is the threshold
form at work: a paper above threshold t counts only if the author has more papers above t than some peers have in
total. Under `active`, every third-ranked paper is cited, so every score is positive.

## 4. A raw PiP score means the same thing across cohorts (answers round 3, R1, on Fig. 3)

PiP-AUC quantiles at career age 10, `active` peers:

| Start year | Peers | 25th | Median | 75th | 90th | Mean h-index |
|---|---|---|---|---|---|---|
| 1960 | 4,031 | 0.132 | 0.287 | 0.483 | 0.670 | 4.4 |
| 1970 | 11,926 | 0.147 | 0.293 | 0.491 | 0.678 | 4.7 |
| 1980 | 25,383 | 0.145 | 0.284 | 0.479 | 0.671 | 4.8 |
| 1990 | 61,422 | 0.145 | 0.278 | 0.474 | 0.663 | 5.0 |
| 2000 | 122,114 | 0.136 | 0.274 | 0.485 | 0.681 | 5.8 |
| 2010 | 269,202 | 0.124 | 0.261 | 0.482 | 0.685 | 6.2 |
| 2015 | 392,360 | 0.122 | 0.262 | 0.482 | 0.686 | 6.1 |

The raw PiP distribution is nearly constant over 55 years of cohorts: medians 0.26–0.29, 90th percentiles 0.66–0.69.
Over the same cohorts the raw h-index at age 10 drifts up by 40%.

## 5. Scholar indicators: stability, survivorship, and what PiP measures

### Stability (`author_stability`, v3 Fig. 7)

Correlation of within-cohort percentiles between career ages, `active` peers, start years 1975–1995. Pearson on
percentiles equals Spearman on the raw metric.

| Ages | Articles | v3 sum (S_P5) | PiP-AUC | Mean paper pct. | Citations | h-index |
|---|---|---|---|---|---|---|
| 5 vs 10 | 0.847 | 0.829 | 0.772 | 0.703 | 0.798 | 0.604 |
| 5 vs 30 | 0.642 | 0.628 | 0.565 | 0.545 | 0.451 | 0.370 |
| 10 vs 30 | 0.800 | 0.783 | 0.742 | 0.736 | 0.620 | 0.599 |
| 20 vs 30 | 0.946 | 0.936 | 0.923 | 0.912 | 0.886 | 0.877 |

The 5-vs-30 row covers 434K authors. PiP is more stable over long horizons than citations or the h-index, and less
stable than the article count. Part of the h-index's low stability early on comes from heavy ties at h = 3–4.

### Survivorship (`survivorship_*`, the limitation v3 promised to study)

Population: `active` authors at age 5, start years 1980–2005, 1.13M authors. The table gives the ROC AUC for still
publishing at ages 11–15 and 16–20.

| Indicator at age 5 | Publishing at 11–15 | Publishing at 16–20 |
|---|---|---|
| Articles | 0.683 | 0.663 |
| v3 sum | 0.675 | 0.655 |
| PiP-AUC | 0.647 | 0.630 |
| h-index | 0.575 | 0.565 |
| Citations | 0.540 | 0.537 |
| Mean paper percentile | 0.400 | 0.410 |

By PiP decile at age 5, the share still publishing at 11–15 rises from 39% in the bottom decile to 79% in the top.

The mean paper percentile is inversely related to continuing. Authors with a few well-cited papers are more likely to
stop appearing; these include students and short-term contributors on strong teams. S2 splitting one person into
several author profiles can also look like stopping.

### What PiP measures

Cross-sectional correlations at age 10 (`active`, start years 1975–2010, 3.43M authors), between within-cohort
percentiles:

| Pair | Correlation |
|---|---|
| PiP vs articles | 0.897 |
| PiP vs h-index | 0.924 |
| PiP vs citations | 0.717 |
| PiP vs plain mean paper percentile | 0.131 |
| v3 sum vs articles | 0.941 |
| Plain mean paper percentile vs articles | −0.271 |

Variance of log PiP = var(log F(n)) + var(log weighted-mean y) + 2·cov:

| Age | Productivity share | Quality share | Covariance share |
|---|---|---|---|
| 5 | 0.77 | 0.30 | −0.07 |
| 10 | 0.67 | 0.24 | +0.09 |
| 20 | 0.59 | 0.21 | +0.20 |

The weighted-mean factor is not flat: its 10th / 50th / 90th percentiles are 0.32 / 0.61 / 0.85. It correlates about
0.8 with the plain mean.

**Takeaway, and an open decision.** On S2, PiP-AUC behaves mostly like a productivity percentile, adjusted by a
quality factor that explains about a quarter of its log variance. It sits close to the h-index (r = 0.92) and far from
mean quality. This bears directly on the round 3 objection that "combining quality and quantity … has not been done
successfully". The paper should report the decomposition openly. A further option is to vary the balance, for
example F(n)^α · W^(1−α), and test which α predicts later outcomes best.

## 6. Variant: age-matched paper percentiles (`sql/10_age_matched_paper_percentiles.sql`)

In this variant a paper's percentile is computed only against papers from the same year by same-cohort active peers,
so papers are compared with others written at the same career stage. Population: active authors at career age 10,
start years 1975–2010, 3.43M authors.

| | Same-year articles (main) | Same-cohort peers' papers |
|---|---|---|
| Average paper percentile of these authors | 0.61 | 0.49 |
| Weighted-mean factor W, 10th / 50th / 90th | 0.32 / 0.60 / 0.84 | 0.17 / 0.43 / 0.75 |
| Share of log-PiP variance: productivity | 0.65 | 0.47 |
| Share of log-PiP variance: quality | 0.24 | 0.36 |
| PiP rank vs article-count rank | 0.896 | 0.839 |
| PiP rank vs mean-quality rank | 0.131 | 0.304 |
| Correlation of the two PiP rankings | — | 0.989 |

The variant spreads the quality factor and moves weight toward quality. The overall ranking hardly changes.

Two caveats:

- A paper's percentile would depend on its author's career stage, so co-authors at different stages would see
  different percentiles for the same paper.
- Part of the effect may come from the stronger reference set rather than from matching career age, since active
  authors' papers average the 61st percentile of all articles. That still needs testing against all active authors'
  papers from the same year.

**Decision (2026-11-04):** the app keeps same-year percentiles. The paper reports this as a variant, next to the
F(n)^α · W^(1−α) weighting.

## 7. Predicting future work: PiP vs h-index, raw vs percentile (`sql/11_prediction.sql`)

The predictors are the indicators at evaluation year Y. The outcomes come from the author's articles published in
Y+1 .. Y+10, each scored at its own age 5 within its publication year, so no article counts toward both. Population:
`active` peers.

Within one cohort, a raw value and its percentile rank people identically. So the comparison pools cohorts in two
settings:

- **Same career age:** everyone at career age 5, 10 or 15, start years 1975 .. 2010 − age (the tenure-committee case).
- **Same calendar year:** everyone active in 2000 or 2005, at career ages 2–30 (comparing people of different
  seniority).

### Spearman correlation with the number of future top-10% articles

| Predictor | Same age: age 5 | age 10 | age 15 | Same year: 2000 | 2005 |
|---|---|---|---|---|---|
| PiP raw | 0.354 | **0.434** | 0.453 | **0.398** | **0.434** |
| PiP percentile | **0.356** | 0.433 | 0.453 | 0.393 | 0.433 |
| h-index raw | 0.268 | 0.352 | 0.384 | 0.269 | 0.284 |
| h-index percentile | 0.256 | 0.352 | 0.385 | 0.328 | 0.363 |
| Citations percentile | 0.250 | 0.297 | 0.331 | 0.291 | 0.312 |
| Articles raw | 0.348 | 0.437 | **0.464** | 0.337 | 0.366 |
| Articles percentile | 0.343 | 0.433 | 0.461 | **0.402** | **0.439** |

### ROC AUC for at least one future top-1% article

| Predictor | Same age: age 5 | age 10 | age 15 | Same year: 2000 | 2005 |
|---|---|---|---|---|---|
| PiP raw | **0.683** | **0.756** | **0.792** | **0.751** | **0.770** |
| h-index percentile | 0.647 | 0.721 | 0.760 | 0.721 | 0.737 |
| h-index raw | 0.646 | 0.719 | 0.758 | 0.682 | 0.691 |
| Articles percentile | 0.661 | 0.739 | 0.781 | 0.737 | 0.757 |
| Citations percentile | 0.671 | 0.716 | 0.750 | 0.723 | 0.733 |

### Spearman correlation with the mean percentile of future articles (pure quality)

| Indicator | Range across settings |
|---|---|
| Citations percentile | 0.28–0.32 (best) |
| h-index | 0.15–0.27 |
| PiP | 0.10–0.24 |
| Articles | about 0 |

### What this shows

1. **PiP beats the h-index, raw or percentile, in every setting and on both impact outcomes.** At age 10, PiP's
   Spearman correlation is 0.43 against 0.35 for the h-index. In 2005, PiP's AUC is 0.77 against 0.74 for the
   h-index percentile.
2. **Raw PiP needs no further normalization.** PiP raw and PiP percentile are equivalent, even across seniority
   (2005: 0.434 vs 0.433). PiP is built from a cohort-relative x-axis and year-relative paper percentiles, and its raw
   distribution is already stable across cohorts (§4).
3. **The h-index has to be converted to a cohort percentile to compare people of different seniority.** At the same
   career age the conversion makes no difference (age 5: raw 0.268 vs percentile 0.256). In 2005 the
   percentile reaches 0.363 against 0.284 for raw h, and the AUC 0.737 against 0.691. Even converted, it stays behind
   PiP.
4. **Article count is a strong baseline for future counts,** as expected from PiP's productivity weight (§5). It ties
   PiP on the number of future top-10% articles. PiP is better at identifying who will produce a top-1% article, and
   at predicting future article quality (0.19 vs 0).
5. **Future quality alone is best predicted by the citations percentile.** PiP is not the best pure-quality
   predictor.

Corrections (2026-11-04, from the Codex review of PR #179):

- §7, age-5 column: the future-article lookup missed authors who started in 2004–2005. Fixed, and the column was
  rerun.
- §6: the reference distribution counted authorships instead of distinct papers. Now it counts distinct papers.

Neither correction changes the conclusions.
