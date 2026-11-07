# How the code computes PiP

The paper defines PiP as an integral over impact thresholds (Section 4, "Definition"). The SQL evaluates that integral
on 40 percentile classes. This note explains how the production code computes the score, what the 40 classes compute
exactly, how the exact integral is computed for comparison, and how much the two differ. Every number here comes from
the result tables in `results/paper_rp/`, which the queries named in each section write.

## Notation

- **y_j**: the percentile of article j at evaluation year Y, the share of articles from its publication year with
  strictly fewer citations through Y. Uncited articles have y_j = 0. In the SQL this is `below[OFFSET(Y - p)]` of
  `article_percentiles` (`04_article_percentiles.sql`), carried into `author_articles` (`05_author_articles.sql`).
- **Peers**: the eligible authors with the same start year at the same career age, the author included.
- **n_a(t)**: the number of author a's articles with y_j >= t.
- **G_t(m)**: the share of peers with strictly fewer than m articles at or above t. The author's standing at t is
  G_t(n_a(t)).
- **PiP_tau(a)** = 1/(1 - tau) × ∫ from tau to 1 of G_t(n_a(t)) dt. The paper uses tau = 1/2.

## The production computation: 40 percentile classes

`12_pip_variants.sql` (career ages 5, 10 and 15, and career ages 2–30 in 2000 and 2005) and `15_sym_by_age.sql` (every
career age from 1 to 30) evaluate the integral at the 40 thresholds t_j = j/40, j = 0, ..., 39:

    PiP_tau(a) = 1/(40 (1 - tau)) × Σ over j >= 40 tau of G_{t_j}(n_a(t_j)),
    so PiP_{1/2}(a) = (1/20) × Σ_{j=20}^{39} G_{t_j}(n_a(t_j)).

For each peer group (start year, career age) the queries:

1. put each article in its class b = min(39, ⌊40 y⌋) and count the author's articles per class (`buckets`);
2. get n_a(t_j), the number of articles in class j or above, as a running sum from j = 39 down (`nj`);
3. count, for each class j, how many peers have each value of n(t_j), and turn the counts into the share of peers
   strictly below each value, G_{t_j}(m) (`dist_j`), in one pass over the peers;
4. average the author's standings G_{t_j}(n_a(t_j)) over the classes at or above the cutoff (`scores`).

The work per peer group is one sort of the peers' counts per class, so the whole computation, 111.6 million
author-age records for 11.1 million authors, runs as ordinary SQL in BigQuery.

### What the 40 classes compute

On the interval (j/40, (j+1)/40] an article counts at threshold t exactly when y >= j/40. The sum above is therefore
not an approximation of some other quantity: it equals the integral exactly, with every percentile rounded up to the
next multiple of 1/40,

    y  ->  (⌊40 y⌋ + 1) / 40.

The reported score is PiP on 40 percentile classes, and the properties of the paper's Section 4 (range,
monotonicity, nothing for articles below the cutoff, the reading as a win probability against a random peer, the mean
score of a peer group) hold for it as stated, with thresholds drawn from the 40 classes.

The classes have two visible effects. Scored against the simulated cohort of 2,000 peers in
`checks/sym_properties.py`:

- Articles in the top 2.5% of their year are not told apart: one article at the 97.5th, 99th or 99.9th percentile
  scores 0.449 on the classes, against 0.422, 0.452 and 0.472 on a 400-point midpoint grid, which is close to the
  exact integral. The paper quotes the second set.
- An article at or just above the median (0.5 <= y < 0.525) earns a full class of credit: one article at the 50th
  percentile scores 0.004 on the classes and 0 on the fine grid.

## The exact integral

`25_pip_exact.sql` computes the integral without a grid, from order statistics. Sort the author's percentiles
y_(1) >= y_(2) >= ... On (y_(k+1), y_(k)] the author has exactly k articles at or above t, and a peer b has fewer than
k exactly when its own k-th best percentile Y_k^b is below t (Y_k^b = 0 when b has fewer than k articles). Hence

    ∫ over (x0, x1] of G_t(k) dt = H_k(x1) - H_k(x0),   H_k(x) = (1/|peers|) × Σ over peers b of max(x - Y_k^b, 0),

and

    PiP_tau(a) = 1/(1 - tau) × Σ_k [ H_k(y_(k)) - H_k(max(tau, y_(k+1))) ],
                 over the k with y_(k) > max(tau, y_(k+1)).

H_k is evaluated by sorting the peers' k-th best percentiles together with the query points within each (start year,
career age, k) and keeping a running count and sum (`asof`). The cost grows with the number of articles, not with the
number of thresholds.

The same query computes the score on K classes by first rounding every percentile up to (⌊K y⌋ + 1)/K; K = 0 means
no rounding, the exact integral. `run_25.sh grid` runs it for the eligible authors at career age 10 (5,279,295 authors,
start years 1960–2015) with K = 40, 200, 1000 and exact, at tau = 0.5, 0.75 and 0.9. It takes about 1.5 minutes.

**Validation.** With K = 40 the query reproduces the production scores of `12_pip_variants.sql` for every one of these
authors, to within 8 × 10⁻¹⁵ (`grid_validation`). This confirms the rounding identity above.

## The classes against the exact integral

`26_grid_convergence.sql` compares the scores at career age 10.

**Scores**, start years 1975–2010, 3.43 million authors (`grid_convergence`). Differences are class score minus exact
score; the rank correlation is within start-year cohorts; the overlap is the share of each cohort's exact top 10%
that the class score also puts in the top 10%.

| tau | Classes | Mean difference | Mean absolute difference | Largest difference | Rank correlation | Top-10% overlap |
|---|---|---|---|---|---|---|
| 0.5 | 40 | +0.0096 | 0.0113 | 0.055 | 0.9994 | 96.2% |
| 0.5 | 200 | +0.0022 | 0.0025 | 0.012 | 0.99998 | 99.2% |
| 0.5 | 1000 | +0.0004 | 0.0005 | 0.002 | 0.999999 | 99.9% |
| 0.75 | 40 | +0.0187 | 0.0206 | 0.108 | 0.9985 | 93.1% |
| 0.9 | 40 | +0.0395 | 0.0415 | 0.251 | 0.9960 | 84.7% |

**Prediction**, start years 1975–2000, 1.45 million authors (`grid_prediction`): Spearman correlation with the number
of future top-10% articles / ROC AUC for at least one future top-1% article / Spearman correlation with the mean
percentile of future articles, as in the paper's prediction tables.

| tau | 40 classes | Exact |
|---|---|---|
| 0.5 | 0.427 / 0.772 / 0.321 | 0.426 / 0.773 / 0.325 |
| 0.75 | 0.388 / 0.766 / 0.356 | 0.386 / 0.766 / 0.357 |
| 0.9 | 0.347 / 0.750 / 0.338 | 0.343 / 0.749 / 0.337 |

No result moves by more than 0.004 at any of the three cutoffs, so the paper reports the 40-class scores.

**Cohort drift at high cutoffs** (`grid_cohorts`). The paper reports that above tau = 0.6 the score's distribution
drifts across cohorts. The drift is not caused by the classes: with the exact integral, the median at tau = 0.9 falls
from 0.32 in the 1960 cohort to 0.12 in 2010, against 0.37 to 0.16 on the classes (the paper's cutoff table). It comes
from authors with no article in the top 10% of its year, who all score zero at tau = 0.9: 6% of the 1960 cohort and
42% of the 2010 cohort. At tau = 0.5 the exact median moves only from 0.41 to 0.37.

## Numerical checks of the properties

`checks/sym_properties.py` checks the properties of PiP on a simulated cohort of 2,000 peers, on the 40 classes and
on the 400-point midpoint grid:

- No violation of the range, of monotonicity (adding an article or raising a percentile) or of the invariance to
  articles below the cutoff, over 1,500 random authors, on either grid.
- Adding the same article to two authors reverses their order in 22 of 3,000 random pairs (0.7%), on either grid.
- Over the 2,000 simulated peers the two grids agree to a rank correlation of 0.9995 (top-10% overlap 97%).

`checks/pip_properties.py` checks the earlier score, PiP-AUC: that its step form equals its threshold form, its bounds
y_min × F(n) <= PiP-AUC <= y_max × F(n), its monotonicity when an article is added, and the gap between the trapezoid
rule the website used before November 2026 and the step form.

## Machine-checked proofs in Lean 4

`lean/` contains a self-contained Lean 4 + Mathlib (`v4.35.0`) package (`PiP`) that formally proves the mathematical
properties and computational identities above (also summarized in Appendix C of the manuscript):

- **`PiP/Basic.lean`**: bounds `0 <= S(a) <= (N - 1)/N < 1`, zero contribution below the cutoff, conditional
  monotonicity (updating the focal author inside the peer cohort), paper-level corollaries, the sawtooth constant-count
  lemma, and an exact rational counterexample to raw-citation monotonicity.
- **`PiP/CohortMean.lean`**: the ordered-pair cohort-mean tie identity `1/N × Σ_a S(a) = 1/(2K) × Σ_k (1 - Σ_m p_{k,m}^2)`
  and its continuous-integral counterpart (Section 4 and Appendix C of the manuscript).
- **`PiP/GridIntegral.lean`**: the cell equivalence `R_M(y) >= t <-> y >= k/M` for `t in (k/M, (k+1)/M]` under strict
  upward rounding `R_M(y) = (⌊M y⌋ + 1)/M` and the exact equality between the finite grid average and the continuous
  interval integral of rounded records.
- **`PiP/ExactIntegral.lean`**: the order-statistics equivalence `n_b(t) < k <-> Y_k^b < t`, the interval integral
  identity `∫_{(x0, x1]} G_t(k) dt = H_k(x1) - H_k(x0)`, and the running count-and-sum representation
  `H_k(x) = (c_k(x) x - s_k(x)) / |peers|` used by the `asof` step in `sql/25_pip_exact.sql`.
