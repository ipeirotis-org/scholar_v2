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
| PiP rank vs article-count rank | 0.897 | 0.840 |
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

## 8. PiP variants: symmetric and τ-cut (`sql/12`–`14`)

All variants use the threshold form on a 40-point grid t = j/40. Here n(t) is the number of the author's articles at
or above percentile t.

| Variant | Definition |
|---|---|
| `pip` | Current step form: the average over t of F(n(t)), where F is the share of peers whose *total* count is at most n(t) |
| `pip_tau25`, `pip_tau50` | The same, restricted to t ≥ τ and rescaled by 1/(1−τ). Papers below τ earn nothing; the score is still monotone. |
| `sym` | The average over t of G_t(n(t)), where G_t is the share of peers with strictly fewer articles *above the same t* |
| `sym_tau50` | Symmetric, restricted to t ≥ 0.5 |

Check: the grid reproduces the exact `pip` with correlation 0.9999 (+0.0075, as expected from the left-edge sum).

### S2, active peers, career age 10, start years 1975–2010 (3.43M authors)

Within-cohort Spearman with each characteristic:

| Variant | Articles | Mean paper pct. | h-index | Top-2.5% articles |
|---|---|---|---|---|
| pip | 0.897 | 0.131 | 0.924 | 0.421 |
| pip_tau25 | 0.844 | 0.228 | 0.932 | 0.450 |
| pip_tau50 | 0.722 | 0.382 | 0.898 | 0.498 |
| sym | 0.856 | 0.214 | 0.919 | 0.491 |
| sym_tau50 | **0.637** | **0.466** | 0.838 | **0.602** |

### Cohort stability: median / 90th percentile of the raw score at age 10

| Start year | pip | pip_tau50 | sym | sym_tau50 |
|---|---|---|---|---|
| 1960 | 0.29 / 0.67 | 0.24 / 0.61 | 0.44 / 0.87 | 0.41 / 0.85 |
| 1980 | 0.28 / 0.67 | 0.17 / 0.56 | 0.42 / 0.86 | 0.39 / 0.85 |
| 2000 | 0.27 / 0.68 | 0.13 / 0.56 | 0.40 / 0.85 | 0.37 / 0.85 |
| 2010 | 0.26 / 0.68 | 0.11 / 0.56 | 0.40 / 0.86 | 0.38 / 0.85 |

`pip_tau50` drifts: its median halves across cohorts. The asymmetric cut compares a partial count against peers' total
counts, and those totals grow over time. The symmetric variants stay stable.

### Prediction of future work (setup as in §7)

Values are Spearman with future top-10% count / AUC for any future top-1% / Spearman with future mean quality.

| Setting | h raw | pip | pip_tau25 | sym | sym_tau50 |
|---|---|---|---|---|---|
| Age 5 | .268/.646/.155 | .354/.683/.108 | .362/.690/.148 | .375/.698/.140 | **.378/.715/.264** |
| Age 10 | .352/.719/.230 | .434/.756/.190 | .437/.760/.225 | **.445**/.766/.223 | .427/**.772/.321** |
| Age 15 | .384/.758/.270 | .453/.792/.236 | .453/.794/.266 | **.460/.799**/.264 | .436/**.799/.343** |
| Year 2000 | .269/.682/.152 | .398/.751/.186 | **.399**/.755/.215 | .393/.753/.199 | .381/**.761/.288** |
| Year 2005 | .284/.691/.174 | .434/.770/.201 | **.435**/.773/.235 | .429/.772/.219 | .407/**.776/.310** |

### Live GS data (`sql/14`, 15,900 active authors in cohorts of at least 30)

The live PiP percentile correlates **0.968** with the cited-work count, which is even more productivity-driven than
on S2.

| | Live PiP | sym | sym_tau50 |
|---|---|---|---|
| Rank correlation with live | — | 0.983 | 0.903 |
| Median shift in cohort percentile | — | 3 points | 7 points (90th percentile: 22) |
| Rank vs top-2.5% works | 0.568 | 0.659 | 0.761 |
| Rank vs cited-work count | 0.968 | 0.923 | 0.799 |
| Score scale, 50th / 75th / 90th | 0.22 / 0.40 / 0.59 | — | 0.47 / 0.72 / 0.88 |

### Takeaways

- Every variant beats the h-index in every setting.
- **`sym_tau50` is the most balanced** and rewards top papers most. It is the best at predicting future top-1% papers
  and future quality in every setting, and it is stable across cohorts.
- **The cost is a small loss on predicting the count of future top-10% papers** (2005: 0.407 against 0.434), which is
  expected once productivity weighs less.
- **The asymmetric cut (`pip_tau*`) is not usable,** because it drifts across cohorts.
- **Open questions:** the cut point τ (0.25 / 0.5 / 0.75) for the symmetric version, and whether GS's small cohorts
  need pooled windows of ±2 years.
- §6 and §8 (Codex review): the within-cohort Spearman correlations now use Hazen percentiles, (N + rank ascending −
  rank descending) / 2N, which gives average ranks for ties and normalizes for cohort size so that cohorts pool
  correctly. Before, `PERCENT_RANK` gave tied values their minimum rank. The numbers moved by 0.01 or less, apart from
  the GS top-work row, where each entry rose by about 0.02.

## 9. Choosing τ for the symmetric variant (`sql/12`–`14`, all cutoffs)

The symmetric variant averages G_t(n(t)) over the thresholds t ≥ τ.

### S2, career age 10

The first four columns are within-cohort Spearman correlations. The next three are prediction results at age 10, with
the 2005 setting in parentheses. The last column is the median score at age 10 for the 1960 and 2010 cohorts.

| τ | vs articles | vs mean quality | vs h | vs top-2.5% | Future top-10% count | Any future top-1% (AUC) | Future quality | Median 1960 → 2010 |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.856 | 0.214 | 0.919 | 0.491 | **0.445** (0.429) | 0.766 (0.772) | 0.223 (0.219) | 0.44 → 0.40 |
| 0.25 | 0.776 | 0.329 | 0.907 | 0.537 | 0.443 (0.425) | 0.770 (**0.776**) | 0.265 (0.259) | 0.43 → 0.39 |
| 0.5 | 0.637 | 0.466 | 0.838 | 0.602 | 0.427 (0.407) | **0.772** (**0.776**) | 0.321 (0.310) | 0.41 → 0.38 |
| 0.6 | 0.573 | 0.506 | 0.789 | 0.633 | 0.415 (0.396) | 0.771 (0.775) | 0.340 (0.325) | 0.41 → 0.37 |
| 0.75 | 0.473 | **0.536** | 0.694 | 0.688 | 0.388 (0.369) | 0.766 (0.769) | **0.356** (**0.336**) | 0.38 → 0.33 |
| 0.9 | 0.370 | 0.486 | 0.562 | **0.781** | 0.347 (0.332) | 0.750 (0.753) | 0.338 (0.316) | **0.37 → 0.16** |
| Current PiP | 0.897 | 0.131 | 0.924 | 0.421 | 0.434 (0.434) | 0.756 (0.770) | 0.190 (0.201) | 0.29 → 0.26 |
| h-index (raw) | — | — | — | — | 0.352 (0.284) | 0.719 (0.691) | 0.230 (0.174) | — |

At ages 5 and 15 the pattern is the same: the top-1% AUC is flat from τ = 0.25 to 0.6, and future quality peaks at
τ = 0.75.

At τ = 0.9 the score is zero for 42% of recent cohorts, and the median drifts. Only 4 grid thresholds remain, and many
authors have no paper in the top 10%.

### Live GS data

| τ | Rank corr. with live | Median / p90 shift in cohort pct. | vs top-2.5% works | vs cited-work count | 50/75/90th pct. | PA9La6oAAAAJ |
|---|---|---|---|---|---|---|
| Live PiP | 1 | — | 0.568 | 0.968 | 0.22/0.40/0.58 | 0.751 |
| 0 | 0.983 | 0.029 / 0.091 | 0.659 | 0.923 | 0.48/0.72/0.88 | 0.781 |
| 0.25 | 0.956 | 0.047 / 0.146 | 0.703 | 0.873 | 0.47/0.72/0.88 | 0.764 |
| 0.5 | 0.903 | 0.073 / 0.217 | 0.761 | 0.799 | 0.47/0.72/0.88 | 0.789 |
| 0.6 | 0.871 | 0.085 / 0.251 | 0.788 | 0.760 | 0.46/0.72/0.88 | 0.810 |
| 0.75 | 0.804 | 0.107 / 0.310 | 0.837 | 0.688 | 0.45/0.71/0.88 | 0.829 |
| 0.9 | 0.693 | 0.134 / 0.388 | 0.907 | 0.580 | 0.41/0.71/0.88 | 0.861 |

### Reading

τ moves the score smoothly from productivity-heavy to quality-heavy. On the way it gives up the ability to predict the
*count* of future top-10% papers and gains the ability to predict their *quality*. The top-1% AUC stays flat across
0.25–0.6.

The usable range is τ ∈ [0.25, 0.6], where cohort stability holds. At 0.75 the median drifts mildly, and 0.9 is
unusable.

τ = 0.5 is a natural default with a one-line explanation: only papers in the top half of their year earn credit.
Moving to τ = 0.25 halves the shift for live GS authors, which matters if the app switches.

**Decision (2026-11-04):** τ = 0.5 is the paper's default, so only papers in the top half of their year earn credit.
The paper presents τ as a dial: lower τ puts more weight on quantity, higher τ more weight on impact. Figure:
`figures/tau_sweep.pdf`, from `figures/make_tau_sweep.py` with data in `figures/data/`.

## 10. The main indicator (symmetric PiP, τ = 0.5) through the scholar-level tests (`sql/15`, `sql/16`)

`sym_by_age` holds the symmetric scores for every active author at every career age, 111.6M rows. They match `pip_variants`
exactly wherever both exist (17.0M rows, maximum difference 0).

### Stability across a career

Correlation of within-cohort percentiles between two career ages, start years 1975–1995, compared with §5:

| Indicator | 5 vs 10 | 5 vs 15 | 5 vs 30 | 10 vs 30 | 20 vs 30 |
|---|---|---|---|---|---|
| Article count | 0.847 | 0.754 | 0.642 | 0.800 | 0.946 |
| Symmetric τ = 0 | 0.778 | 0.682 | 0.569 | 0.742 | 0.921 |
| **Symmetric τ = 0.5** | 0.781 | 0.687 | 0.563 | 0.726 | 0.911 |
| Current PiP | 0.772 | 0.674 | 0.565 | 0.742 | 0.923 |
| Citations | 0.798 | 0.659 | 0.451 | 0.620 | 0.886 |
| h-index | 0.604 | 0.496 | 0.370 | 0.599 | 0.877 |

The new indicator is as stable as the current PiP and more stable than citations or the h-index over long horizons.

### Survivorship

ROC AUC of the age-5 percentile for still publishing at career ages 11–15 / 16–20, pooled:

| Indicator | 11–15 | 16–20 |
|---|---|---|
| Articles | 0.683 | 0.663 |
| Current PiP | 0.647 | 0.630 |
| Symmetric τ = 0 | 0.647 | 0.631 |
| **Symmetric τ = 0.5** | 0.616 | 0.603 |
| h-index | 0.575 | 0.565 |
| Citations | 0.540 | 0.537 |

Continuing to publish is a productivity outcome, so it is predicted best by the indicators that weigh productivity
most.

### Factorization on S2

sym_τ50 = F(n) × Q, for active authors with a positive score, start years 1975–2005. The shares are of the variance
of log sym_τ50.

| Age | Productivity log F(n) | Quality log Q | Covariance | Q > 1 | Q 10th / 50th / 90th | Score = 0 |
|---|---|---|---|---|---|---|
| 5 | 0.30 | 0.82 | −0.11 | 30% | 0.17 / 0.76 / 1.46 | 0.1% |
| 10 | 0.29 | 0.79 | −0.09 | 35% | 0.16 / 0.83 / 1.53 | 0.5% |
| 20 | 0.30 | 0.74 | −0.04 | 36% | 0.18 / 0.88 / 1.51 | 1.2% |

The current PiP is the reverse: productivity accounts for 0.59–0.77 of its log variance (§5). The within-cohort
Spearman view in §9 is more balanced (0.64 with article count, 0.47 with mean quality), because the log variance
gives extra weight to the low scores.

### Theory checks (`checks/sym_properties.py`, simulated cohort of 2,000 peers)

- **Range, monotonicity and τ-invariance:** no violations in 1,500 random authors. The score stayed in [0, 1], never
  fell when a paper was added or a percentile raised, and never changed when a paper below τ was added.
- **Q > 1:** 24% of the simulated authors.
- **Consistency in the sense of Waltman & van Eck:** adding the same paper to two authors reversed their order in 0.7%
  of 3,000 pairs, against 1.3% for the current PiP.
- **v3's synthetic careers:**

  | Career | sym_τ50 | Current PiP |
  |---|---|---|
  | One top paper (99th percentile) | 0.45 | 0 |
  | One average paper | 0 | 0 |
  | 40 weak papers | 0 | 0.10 |

The draft theory section is `manuscript/theory.tex`. It is not yet included in `ms.tex`.

## 11. Self-citations (`sql/00_noself_citations.sql`, `sql/run_noself.sh`; dataset `paper_rp_noself`)

A citation counts as a self-citation when the citing and the cited paper share at least one S2 author. Self-citations
are 8.1% of all S2 citations. The self-free table totals 2,737,478,329 citations, the same total as
`s2_data.paper_citations_by_year`, so the construction matches the loader. The whole pipeline (steps 01–07 and 11–13)
was rerun on self-free citations: percentiles, peers (h-index ≥ 3 now without self-citations), scores and outcomes.

Prediction benchmarks, with self-citations / without:

| Setting | Indicator | Future top-10% count | Future top-1% (AUC) | Future quality |
|---|---|---|---|---|
| Age 5 | sym_τ50 | .378 / .378 | .715 / .712 | .264 / .234 |
| | current PiP | .354 / .355 | .683 / .681 | .108 / .093 |
| | h-index | .268 / .257 | .646 / .640 | .155 / .148 |
| Age 10 | sym_τ50 | .427 / .427 | .772 / .771 | .321 / .298 |
| | current PiP | .434 / .435 | .756 / .755 | .190 / .171 |
| | h-index | .352 / .340 | .719 / .714 | .230 / .221 |
| Age 15 | sym_τ50 | .436 / .437 | .799 / .799 | .343 / .325 |
| | current PiP | .453 / .454 | .792 / .790 | .236 / .219 |
| | h-index | .384 / .374 | .758 / .754 | .270 / .261 |
| 2005 | sym_τ50 | .407 / .407 | .776 / .777 | .310 / .291 |
| | current PiP | .434 / .437 | .770 / .771 | .201 / .188 |
| | h-index | .284 / .277 | .691 / .689 | .174 / .169 |

### What it shows

- **The ordering of the indicators, and PiP's lead over the h-index, do not depend on self-citations.** The count and
  top-1% results move by at most 0.003 for the PiP variants.
- **The h-index loses slightly more without self-citations** (top-10% count at age 10: 0.352 → 0.340).
- **Predicting future quality drops by about 0.02 for every indicator.** Part of the persistence of quality runs
  through authors citing their own work.
- **This rules out the most mechanical Matthew-effect channel, but not reputation in general.** Other people also
  cite well-known names, and separating that needs an external design (see `REVISION_PLAN.md`).

## 12. Fields (`sql/17_fields.sql`, `sql/18_field_normalized.sql`)

An author's main field is the most frequent S2 field of study among their articles by career age 10 (whole
counting).

### Pooled scores favor high-citation fields

Share of each field's authors in their cohort's top 10% on sym_τ50, career age 10, start years 1975–2010. A
field-neutral score would put 10% of every field there.

| Field | Authors | sym_τ50 median / 90th | In the cohort top 10% |
|---|---|---|---|
| Physics | 76,009 | 0.46 / 0.90 | 14.7% |
| Psychology | 29,885 | 0.42 / 0.87 | 12.1% |
| Medicine | 1,905,585 | 0.41 / 0.87 | 11.7% |
| Biology | 536,964 | 0.43 / 0.84 | 9.3% |
| Computer Science | 303,231 | 0.26 / 0.82 | 8.3% |
| Chemistry | 184,019 | 0.32 / 0.77 | 5.8% |
| Mathematics | 8,210 | 0.23 / 0.70 | 3.7% |
| Engineering | 129,387 | 0.16 / 0.64 | 2.9% |
| Economics | 13,928 | 0.26 / 0.66 | 2.1% |
| Education | 14,791 | 0.18 / 0.58 | 1.4% |

### The prediction advantage holds inside every field

Career age 10, start years 1975–2000; 15 fields with at least 3,000 authors.

- **sym_τ50 against the h-index:** sym_τ50 is better on all three outcomes in every field.
- **sym_τ50 against the current PiP:** sym_τ50 is better on top-1% AUC and on future quality in every field. The
  current PiP is better on the future top-10% count in about half the fields, as it is overall.

Examples (top-10% count / top-1% AUC / quality):

| Field | sym_τ50 | Current PiP | h-index |
|---|---|---|---|
| Medicine | .437/.777/.317 | .443/.762/.207 | .355/.723/.235 |
| Computer Science | .485/.807/.358 | .513/.804/.264 | .415/.761/.267 |
| Physics | .371/.695/.168 | .335/.669/.086 | .277/.642/.116 |
| Economics | .393/.741/.274 | .369/.695/.167 | .311/.665/.166 |
| Mathematics | .383/.749/.283 | .344/.697/.168 | .294/.682/.190 |

### Field-normalized variant (`fn_sym50`)

Paper percentiles are computed within (the author's main field, publication year) when the paper carries that field,
and pooled otherwise. Peers are authors with the same main field and start year. Outcomes are scored both pooled and
field-normalized. Population: 1.45M authors.

| Outcome | Score | Top-10% count | Top-1% (AUC) | Quality |
|---|---|---|---|---|
| Pooled | pooled score | **.427** | **.772** | **.321** |
| Pooled | field-normalized score | .414 | .770 | .298 |
| Field-normalized | pooled score | .405 | .769 | .300 |
| Field-normalized | field-normalized score | **.407** | **.772** | **.316** |
| Field-normalized | h-index | .333 | .717 | .209 |

- **Each version predicts its own kind of outcome best**, by 0.01–0.02. Both versions are far ahead of the h-index.
- **The field-normalized score removes the field bias.** Each field's share of the cohort top 10% is 6.9–11.6%,
  against 1.4–14.7% for the pooled score.
- **Within a field, the ranking barely changes.** The correlation with the pooled score is 0.91–0.99, so
  normalization moves fields relative to each other rather than reordering authors inside a field.
- **The remaining spread**, for example Agricultural and Food Sciences at 6.9%, comes from papers that do not carry
  the author's main field, which keep their pooled percentile.

## 13. S2 against Google Scholar for the same people (`sql/19_s2_gs_match.sql`)

### Matching

A GS work matches an S2 paper on normalized title (at least 25 characters) and year ±1. The S2 counterpart of a
profile is the same-surname S2 author holding the most matched works.

- **Confident matches:** 14,511 of 16,218 GS profiles (89%), meaning at least 5 matched works and at least 30% of the
  profile's eligible titles.

### S2 author disambiguation (the reviewers' question)

- **Coverage:** for the median profile, 76% of the GS works are found in S2.
- **Concentration:** 90% of the found works sit under the main S2 ID; the other 10% are scattered across other IDs.
- **Splitting:** 64% of people have at least 2 of their works under at least 2 S2 IDs with the same surname and first
  initial, and 42% under at least 3. Median 2 IDs, upper quartile 4, maximum 84.
- **Smaller records in S2:** S2 holds a median 67% as many articles as the GS profile lists cited works, and its
  h-index is a median 78% of GS's.
- **Career start:** S2's start year (first article in S2) is within one year of GS's for 48% of people, and later for
  47%, because early work is missing or sits under split IDs.

### Agreement of rankings across the two sources

9,796 people with scores in both sources (S2 start year ≥ 1995, evaluation year 2025). Each source uses its own
reference population: GS faculty profiles against all S2 active authors.

| Indicator | Spearman between sources |
|---|---|
| h-index | 0.824 |
| Current PiP | 0.782 |
| sym_τ50 | 0.778 |
| sym_τ50, people with at most one S2 ID | 0.757 |
| sym_τ50, people split across 3+ S2 IDs | 0.733 |

### What it means

The rankings agree well across two independent data sources with different reference populations, but S2's
fragmentation is a real limitation. The paper should:

- report both sources;
- note that S2 shortens careers and splits profiles;
- treat GS (author-curated profiles) as the check on S2.

## 14. PiP against percentile-based indicators (`sql/20_percentile_baselines.sql`)

The prediction setup is that of §7. The baselines use the same paper percentiles as PiP (`below` at the evaluation
year):

- the number of articles in the top 10% (Leiden), also divided by career age (Bornmann & Marx 2014);
- the number of articles in the top 1%;
- I3, the sum of the percentiles (Leydesdorff & Bornmann 2011);
- the mean percentile.

Each enters raw and as a within-cohort Hazen percentile. PiP and the h-index reproduce §7–§8 exactly.

### Results

Values are future top-10% count (Spearman) / any future top-1% (AUC) / future mean percentile (Spearman). Age 10 uses
raw values; 2005 uses within-cohort percentiles.

| Indicator | Age 10 | 2005 |
|---|---|---|
| PiP (τ = 0.5) | .427 / **.772** / .321 | .416 / **.781** / .318 |
| Original PiP-AUC | .434 / .756 / .190 | .433 / .770 / .201 |
| h-index | .352 / .719 / .230 | .363 / .737 / .241 |
| Articles | .437 / .740 / .000 | **.439** / .757 / .019 |
| Top-10% count | .360 / .748 / .338 | .354 / .762 / .324 |
| Top-10% per year of career (raw) | = top-10% count | .379 / .771 / .345 |
| Top-1% count | .250 / .641 / .228 | .262 / .698 / .225 |
| I3 | **.438** / .755 / .182 | .434 / .769 / .189 |
| Mean percentile | −.043 / .523 / **.394** | −.042 / .526 / **.404** |

### What it shows

- **No baseline beats PiP (τ = 0.5) on more than one outcome, in any of the five settings.** None beats it on the
  top-1% AUC.
- **The mean percentile** is the best predictor of future quality, but it carries no information about quantity.
- **I3 and the article count** match PiP on the future count but are weak on quality.
- **The top-10% count** is the closest competitor. It is better on quality (.338 against .321 at age 10) and worse on
  the count (.360 against .427) and on the top-1% AUC.
- **The τ family forms a frontier.**
  - τ = 0 matches or beats I3 and the original PiP-AUC on all three outcomes in every setting.
  - τ = 0.75 matches or beats the top-10% count in every setting.
  - τ from 0.25 to 0.75 beats the h-index on all three outcomes.
  - The mean percentile lies beyond the frontier, at the pure-quality end. The article count lies just beyond it at
    age 15 and in 2000. The top-10% count per year lies on the frontier next to τ = 0.75.
- **The knee of the frontier is at τ = 0.5.** From τ = 0.25 to 0.5, quality gains 0.056 and the count loses 0.016.
  Past 0.5, quality gains less for each unit of count lost.

### Raw against percentile PiP

In the same-year settings, PiP's cohort percentile predicts slightly better than the raw score (2005: .416 / .781 /
.318 against .407 / .776 / .310). The manuscript previously said no conversion was needed; that sentence is
corrected. The gain is much smaller than for the h-index (+0.08).

### Replication across 26 cohorts (age 10, start years 1975–2000, 17,867 to 122,114 authors)

Sampling error is below 0.001 with more than a million authors per setting, so the check that matters is whether the
results hold cohort by cohort.

- **Against the h-index,** PiP wins on all three outcomes in all 26 cohorts. On the count it leads by 0.055–0.102.
- **Against the top-10% count,** PiP wins on the count (by 0.050–0.076) and on the top-1% AUC in all 26 cohorts. It
  loses on quality in 20 cohorts (by −0.074 to +0.009). At τ = 0.6 it loses on quality in 15.
- **Against I3, the article count and the original PiP-AUC,** PiP wins on the top-1% AUC and on quality in all 26
  cohorts. On the count it wins in 11–14 cohorts, with differences between −0.033 and +0.044.

## 15. Early detection (`sql/21_early_detection.sql`)

The question is how soon each indicator picks out the scholars who will do well later.

### Setup

- **Fixed populations:** authors active at career age 3 (306,854) and, separately, at age 5 (711,986). Start years
  are 1975–2000. Activity persists, so every member has every indicator at every later age.
- **Measurement:** each indicator is measured at ages s..10 as a within-cohort Hazen percentile.
- **Fixed outcome window:** articles published at career ages 11–20, each scored at its own age 5.
- **Future star:** the top 10% of the cohort (within the population) by the number of top-10% articles at ages 11–20.
  Ties are broken by the mean percentile, then by a hash of the author id.

### Population active at age 3

Each cell is precision of the cohort's top 10% at finding stars (chance 0.10) / AUC for at least one top-1% article.

| Measured at | PiP | Original PiP-AUC | I3 | Articles | Top-10% count | h-index | Citations |
|---|---|---|---|---|---|---|---|
| Age 3 | **.211** / **.629** | .204 / .611 | .203 / .615 | .196 / .612 | .196 / .616 | .176 / .560 | .174 / .598 |
| Age 5 | **.299** / **.705** | .292 / .693 | .283 / .691 | .268 / .685 | .278 / .679 | .248 / .660 | .224 / .663 |
| Age 10 | **.486** / **.817** | .486 / .811 | .460 / .803 | .430 / .793 | .469 / .794 | .423 / .782 | .368 / .768 |

- **PiP leads at every age on both AUC measures** (stars and top-1%). On precision it leads at every age except 9
  and 10, where the original PiP-AUC matches it (.451 against .450, then .486 against .486).

### Lead time

How many years earlier PiP reaches each indicator's accuracy, taken as the range over target ages 5–10:

| Compared with | Star precision | Star AUC | Top-1% AUC |
|---|---|---|---|
| h-index | 1.2–1.8 | 1.1–1.6 | 1.3–1.9 |
| Citations | 1.7–3.2 | 1.4–2.8 | 1.2–2.5 |
| Articles | 0.7–1.6 | 0.3–0.9 | 0.6–1.3 |
| Top-10% count | 0.5–0.6 | 1.0–1.3 | 0.8–1.3 |
| I3 | 0.4–0.7 | 0.2–0.4 | 0.4–0.8 |
| Original PiP-AUC | 0.0–0.2 | 0.0–0.2 | 0.3–0.4 |

For the population active at age 5, the leads are similar: 1.2–1.8 years over the h-index and 1.6–2.9 over
citations.

### Other outcomes

On the count of future top-10% papers, the article count is slightly ahead of PiP at the earliest ages. On future
mean quality, the mean percentile is best at every age. This is the same ordering as in §7 and §14.

### Reading

PiP's early-detection advantage is in finding the scholars at the top, who combine volume with highly cited work, and
in anticipating exceptional papers. It reaches the h-index's age-10 accuracy at about age 8.

