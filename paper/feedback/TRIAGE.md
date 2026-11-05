# Pre-submission reviews: what to fix

Status 2026-11-05. Two reviews of `manuscript_qss/main.pdf` as built at main@c7efb5d (26 pages):

- **Review A** ([`review_A.md`](review_A.md)): a referee-style report. Major revision.
- **Review B** ([`review_B.md`](review_B.md)): a pre-submission review addressed to Panos. Major revision before
  submitting.

Labels: A2.1–A2.5 are Review A's major comments and A-m1–A-m6 its minor ones; B1–B7 are Review B's major comments,
B-theory its theory section and B-table its table of corrections. Locations are `main.tex` lines, with the PDF lines
the reviewers cite in parentheses.

## Decision

Fix almost all of it. The reviews overlap on the main points (the eligibility filter, the grid, ties, field
normalization, fractional counting, the website sentence, the contribution count), and their statements about the
manuscript and the code hold when checked, with two exceptions listed below. Four requests are declined, each for a
stated reason.

Three new analyses come first, because each could change what the paper can claim:

1. **Eligibility without the h-index condition** (A2.4, B1). Early detection and same-age prediction among authors
   with at least three articles and no h-index condition.
2. **Grid convergence** (A2.1, B2). The 40-class computation against finer grids, and a definition that matches the
   SQL.
3. **Baselines that isolate the construction** (B4). The cutoff-matched percentile sum T_τ and a combination of the
   article-count and mean-percentile cohort percentiles.

If any of these goes against PiP (say T_τ predicts as well as PiP, or the lead over the h-index shrinks once the
h ≥ 3 condition is gone), the abstract and conclusion follow the result. The wording fixes below can go in now; the
ones whose numbers these runs could change are marked "after the runs".

## Status (2026-11-05, first pass)

Done; results in `FINDINGS.md` §18–21, manuscript and figures updated:

- **The three required runs.** Grid convergence (§18), T_τ and the count + mean-percentile mix (§19), eligibility
  without the h-index condition (§20).
- **Section 1** (eligibility): all items.
- **Section 2** (grid): all items. The 40 classes stay; the definition, claims and property checks now match the SQL.
- **Section 3** (ties): the tie paragraph, B's identity and the factorization wording. Not done: the tie series by
  cohort, beyond the share at zero at τ = 0.9, which settles the direction question.
- **Section 4** (cohort claim): wording and the probability reading. Not done: calibration by PiP band.
- **Section 5** (baselines): run, then reframed by the authors. The cutoff-matched sum appears only as PiP-simple, a
  variant of PiP that compares with peers once, to show what the comparison at every threshold adds; it is not
  listed among the competing indicators. The count + mean-percentile mix is left out of the paper (results in
  `FINDINGS.md` §19).
- **The first, asymmetric form of the score** ("original PiP-AUC", the blog post and pip-score.org) is no longer
  compared in the results, by the authors' decision: it is the authors' own earlier version, not a published
  indicator. Section 4.5 still explains why the symmetric form replaces it. Its numbers stay in `FINDINGS.md`.
- **Section 6** (fields): wording only. Not done: paper-field rule and ablation.
- **Section 7** (counting, document types): not started.
- **Section 8** (validation claims): all wording items. Not done: upper-tail transitions; dated honors as the main
  column.
- **Section 9** (uncertainty, data, reproducibility): tie-aware precision, the reconstruction wording, the start-year
  rule and the dependence caveat. Not done: bootstrap, S2–GS top-decile agreement, start-year sensitivity, archive.
- **Section 10** (theory): all items.
- **Section 11** (editorial): all items except the three marked "Authors" (title, CRediT, website deployment).
- **Also noticed:** both fixed.

What the runs changed in the paper's claims:

- **PiP against PiP-simple** (the cutoff-matched sum). The comparison with peers at every threshold adds to predicting
  future citation impact (+0.02 to +0.04 in every cohort) and the top-1% outcome (+0.002 to +0.011). It adds nothing to
  the count of future top papers and little to early detection, where PiP-simple is within 0.3 years of PiP.
- **Without the h-index condition.** PiP still beats the h-index everywhere, by half to two thirds of the margin. In
  early detection, I3, T_0.5 and the original PiP-AUC catch up with it from about career age 6–7.
- **The grid.** It does not matter for any result (prediction changes at most 0.004). The τ = 0.9 drift is real: it
  comes from ties at zero, which grow from 6% to 42% of authors across cohorts.

## What the checks found

Checked against the code and data while triaging:

- **Grid (B2, confirmed).** `sql/15_sym_by_age.sql` and `sql/18_field_normalized.sql` evaluate PiP on θ_k = k/40,
  k = 0..39. `checks/sym_properties.py` uses a 400-point midpoint grid. Rerunning that script's simulated cohort
  (2,000 peers, seed 1) on the production grid:

  | Record | Production grid (k/40) | 400-point midpoint | 4,000-point midpoint |
  |---|---|---|---|
  | One article at the 50.0th percentile | 0.004 | 0 | 0 |
  | One article at the 97.5th | 0.449 | 0.422 | 0.422 |
  | One article at the 99.0th | 0.449 | 0.452 | 0.452 |
  | One article at the 99.9th | 0.449 | 0.472 | 0.470 |

  Over 1,500 simulated authors, the production grid against the 4,000-point grid: Spearman 0.9995, mean absolute
  difference 0.012 (largest 0.049), 97% overlap of the top 10%. Rankings will barely move. The manuscript's claims
  about the top 1% and about a median article scoring 0 do not hold for the implemented score.
- **What the grid computes.** On (k/40, (k+1)/40], an article counts at θ exactly when y ≥ k/40. So the left Riemann
  sum equals the integral of Eq. 4 applied to percentiles rounded up to the next multiple of 1/40, (⌊40y⌋ + 1)/40.
  The implemented score is PiP on 40 percentile classes, and the properties in Section 4.4 hold for it exactly. This
  is the "define the operational indicator directly" option in B2.
- **Mid-ranks (A2.2).** With ties counted half, an author whose five articles all sit below the median scores 0.22 in
  the same simulated cohort, against 0 under the strict rule.
- **Tie-breaks in early detection (B7).** The top-decile pick breaks ties at the boundary by a hash of the author id
  (`sql/21_early_detection.sql:87`), so the reported precision rests on one random tie-break. AUCs use mid-ranks.
- **Prediction windows (A-m3, B7, confirmed).** Same-age settings use start years 1975 to 2010 − age
  (`sql/11_prediction.sql:25`).
- **Publication-only rule.** `author_pip_by_age` (step 06) already has the `min3` rule (192.7M rows, 19.2M authors).
  `sym_by_age` (step 15) was built only for `active`.
- **Self-citations (B-table, confirmed).** Removing them lowers the future-quality correlation by 0.018–0.030 for
  PiP, 0.013–0.019 for the original PiP-AUC and 0.005–0.009 for the h-index (Table 7).
- **Tables 4, 7, 8 (B-table, confirmed).** PiP in 2005 is 0.416 / 0.781 / 0.318 in Table 4 (cohort percentile) and
  .407 / .776 / .310 in Tables 7 and 8 (raw).
- **Figure 3(d) (B-table, confirmed).** The panel title is "(d) Identifies future top-1% authors"
  (`figures/make_tau_sweep.py:114`).
- **Honors (B6).** `honors/analyze_honors.py` already computes the dated-honors outcome (`honored_dated`).
- **Exception 1 (A2.2).** Review A says the share of peers tied at zero shrinks in later cohorts and drives the drift
  at high τ. Our numbers point the other way: at τ = 0.9, 42% of recent authors score zero, and the cohort mean at
  τ = 0.5 falls slightly (0.44 to 0.42). By Review B's identity (the cohort mean of g is (1 − Σ p²)/2), a falling
  mean means more ties, not fewer. Ties are the right mechanism; compute the series before stating a direction.
- **Exception 2 (A2.4).** "1960s–1970s, when citation rates were 40% lower": the 40% in the paper is the rise in the
  mean h-index at career age 10, not a citation rate. No action, but the response should not repeat it.

## Decisions

### 1. Eligibility filter (A2.4, B1)

- **"Includes everyone who ever published."** main.tex:129 (PDF 86), 837 (PDF 452). **Fix.** "All authors who meet
  the eligibility rule at the evaluation year, including those who later stop publishing."
- **"Active" → "eligible".** Throughout. **Fix.** Once met, the rule holds for good, so "active" reads oddly next to
  the survivorship test of who is still publishing. Paper only; the app keeps `active_authors`.
- **The one-landmark-paper example lies outside the population.** main.tex:76-77, 94-96, 266-269. **Fix.** Say that
  this case is shown in simulation and that the empirical results cover eligible authors (three articles, h-index of
  three).
- **Publication-only eligibility.** **Fix + run, required.** Build step 15 for `min3`, then rerun step 21 (populations
  at ages 3 and 5) and the same-age prediction (steps 11 and 20). Report the lead times under both rules.
- **Share excluded by the h ≥ 3 condition, by field and career age.** **Fix + run** (BQ on existing tables: `min3`
  against `active` in `author_pip_by_age`, fields from `author_field_age10`).
- **Describe early detection as among authors eligible at age 3.** main.tex:642-650. **Fix.**

### 2. The 40-class grid (A2.1, B2)

- **Definition.** main.tex:243-250, 315-319 (PDF 212). **Fix.** Keep Eq. 4 and define the reported score as Eq. 4 on
  40 percentile classes (the rounding identity above), so the properties are stated for what we compute.
- **Claims that depend on resolution.** main.tex:94-96 (PDF 57), 266-269 (PDF 174-176). **Fix.** "Top 1%" becomes
  "top 2.5%" where the claim is about the highest threshold. The one-article examples get production-grid numbers
  (0.449, and 0.004 for a median article), unless the pipeline moves to a finer grid.
- **Convergence.** **Fix + run, required.** Career age 10 (1.45M authors): PiP on k/40, k/200 and k/1000, plus exact
  breakpoints for two or three cohorts. Report score differences, Spearman, top-decile changes and the Table 4
  metrics. If no prediction metric moves by more than about 0.005, keep 40 classes; if one does, move the pipeline to
  the finer grid and rerun.
- **Property checks on the production grid.** **Fix, local.** Make the grid a parameter in `checks/sym_properties.py`
  and report both grids.
- **τ = 0.9 with four grid points.** **Partly.** Include τ = 0.75 and 0.9 in the convergence run. The statement that
  τ is usable up to 0.6 (main.tex:435-442) stays only if the drift survives the finer grid.
- **Tail-refined grid (A2.1, option a).** **Decide after the convergence run.**

### 3. Ties (A2.2, B-theory)

- **Explain the tie rule.** main.tex:149-151, 221-225. **Fix.** The standing counts peers with strictly fewer
  articles, so an author with nothing above θ gets zero there. Mid-ranks would give 0.22 to five sub-median articles
  in the simulated cohort.
- **Mid-ranks as the main rule.** **Decline** (reason above). Optional variant: ties at m ≥ 1 count half, zero stays
  zero.
- **Drift at τ > 0.6.** main.tex:435-437, 476-477. **Fix + run** (BQ, small). Add B's identity for the cohort mean and
  report Σ p² and the share at zero, p₀(θ), by start year at θ = 0.5, 0.75 and 0.9. That explains the drift directly.
- **F uses ≤, g uses <.** main.tex:271-283 (PDF 178-184). **Fix.** Q is below 1 for an author whose standing is the
  same at every threshold, so "Q > 1 for 30–36%" depends on the convention. State the convention or drop the
  statistic (see also section 10).

### 4. "Means the same thing in every cohort" (B3)

- **Narrow the claim.** Heading main.tex:457 (PDF 264); abstract 44-45 (PDF 13); conclusion 902-903; "extends to
  comparisons across seniority" 480-481 (PDF 276). **Fix.** "The distribution of PiP is stable across start-year
  cohorts." Comparability means the same rarity. A cohort percentile of the h-index also gives a standardized scale,
  so the case for PiP rests on what it summarizes and how well it predicts.
- **PiP = Pr[n_a(Θ) > n_B(Θ)]**, with B a random peer and Θ uniform on [τ, 1]. Section 4.3. **Fix.** It follows from
  the definition in one line and gives users a plain-language reading.
- **Calibration by PiP band.** **Fix + run, second wave** (BQ, from `prediction_base`). Future top-10% count and share
  with a future top-1% article by band of raw PiP (width 0.1), by start-year group and career age. The cohort section
  claims only what this shows.

### 5. Baselines (B4)

- **T_τ, the cutoff-matched sum.** **Fix + run, required** (extend `sql/20_percentile_baselines.sql`). Cohort
  percentile of Σ_j max(y_j − τ, 0), computed on the same 40 classes as PiP, so the only difference is G_θ inside the
  integral. Add it to Tables 4 and 5 and Figure 6.
- **Count plus mean-percentile combination.** **Fix + run, required** (same query). w·pct(articles) + (1 − w)·pct(mean
  percentile) with w = 0.5, plus 0.25 and 0.75 as a curve on Figure 6.
- **"At most one of three outcomes" and the frontier wording.** main.tex:47-48, 598-600, 615-625, 907-908. **Fix,
  after the runs.** Present the result as a trade-off among these three outcomes.
- **Equal normalization for comparators.** **Partly.** Every comparator is already a cohort percentile.
  Field-normalized versions of T_τ and the top-10% count go into the field run (section 6).
- **"Holdout check" → "retrospective temporal robustness check".** main.tex:444 (PDF 252). **Fix.**

### 6. Field normalization (A2.3, B5)

- **Into the abstract and the main argument.** **Fix.** One sentence in the abstract, and the field-normalized row
  in Table 4.
- **Paper-field rule.** main.tex:784-786 (PDF 413-416). **Fix + run, second wave** (rerun step 18). Percentile within
  each of the article's fields, averaged over its fields; untagged articles keep the pooled percentile. Coauthors then
  share a percentile. Keep the current rule as a variant.
- **Ablation.** **Fix + run** (same run): paper normalization only, field peers only, and both.
- **"The remaining spread comes from papers outside the author's main field."** main.tex:788-789 (PDF 417-418).
  **Fix.** Delete it, unless the ablation shows it.
- **Finer subfields (OpenAlex, MeSH, arXiv).** **Decline.** S2 has 23 fields; another taxonomy means another data
  source. Discuss the coarseness as a limitation (main.tex:867-870).

### 7. Counting and document types (A2.5, B5, B7)

- **Fractional counting.** main.tex:879-881. **Fix + run, second wave** (BQ, career age 10 only). Each article counts
  1/(number of authors) in the author's counts, the peers' counts and the outcomes; report the main rows of Table 4.
  This replaces "we have not tested it".
- **Reviews, meta-analyses and letters.** main.tex:180-184 (PDF 127-129). **Fix + run** (BQ, small). Share of each S2
  type among the 71.5M articles, and a sample of LettersAndComments titles to see whether research letters sit there.
  Optional: the age-10 prediction with reviews and meta-analyses dropped.
- **Retention by year and field.** **Fix + run** (BQ, small). Articles over all records, by decade and by field, as an
  appendix table.

### 8. What the validation supports (B6)

- **Upper-tail transitions.** **Fix + run, second wave** (BQ, `article_percentiles`). For 1980–1995 articles in the
  top 10% or top 1% at ages 1, 2, 3 and 5: the share still there at age 10, and the reverse. Add one sentence on the
  mean absolute change of 0.086.
- **"The main source of PiP's lead in early detection."** main.tex:851-853 (PDF 463-465). **Fix.** State it as the
  likely reason. Optional test: PiP without articles younger than two years.
- **Honors: weight the comparable populations.** Abstract main.tex:49-50, introduction 117-119, 764-766. **Fix.**
  Within notable groups the lead over the h-index is 0.018 and 0.008, and the top-10% count does as well or better.
  Say so wherever the all-authors AUC is quoted.
- **Honors: dated honors first.** **Fix, local** (`honored_dated` already exists).
- **Honors caveats.** **Fix.** A missing record does not confirm that someone was not honored; older cohorts had more
  time; compare within start-year decades where the counts allow.
- **"Understate how well any indicator tracks merit."** main.tex:767-768 (PDF 400). **Fix.** Delete.
- **"Since the effect applies to every indicator we compare, it does not explain the differences."** main.tex:862-863
  (PDF 475). **Fix.** Replace: indicators can absorb cumulative advantage to different degrees, so it may account for
  part of the differences.
- **"Quality" → "citation impact".** Throughout. **Fix.** "Quality threshold" → "percentile threshold"; "future
  quality" → "mean percentile of future articles" or "future citation impact"; "quality multiplier" → "impact
  multiplier". Keep "quality" only where the text discusses how citation impact relates to it.

### 9. Uncertainty, data and reproducibility (B7, A-m3, A-m4)

- **Paired uncertainty.** **Fix + run, second wave** (export, then local). Paired bootstrap over authors for PiP minus
  each comparator in Table 4 and for the lead times in Table 5 (the age-3 early-detection base is about 2.5M rows).
  Shared papers make author-level resampling optimistic, so the 26-cohort replication stays the main evidence.
- **Tie rule for top-decile precision.** main.tex:647-648. **Fix + run** (BQ, small). State the hash rule and add the
  expected precision under random tie-breaking: authors tied at the boundary count with weight (slots left) / (number
  tied).
- **"Using only information available at each evaluation year."** main.tex:43 (PDF 12), 100, 192-193 (PDF 138).
  **Fix.** A retrospective reconstruction that limits publication and citing years to the evaluation year; author
  records, document types and fields come from the 2026 snapshot.
- **Start-year rule for prediction.** main.tex:523-527 and the Table 4 caption. **Fix.** Start years 1975 to
  2010 − age, so that ten years of future articles each get five years of citations by 2025.
- **S2 against GS: top-decile agreement and large discrepancies.** **Fix + run** (BQ, step-19 tables).
- **Start-year sensitivity (B7, A-m4).** **Partly.** Drop authors whose first article came three or more years before
  their second, and rerun the age-10 prediction and early detection. Redefining the career start through the whole
  pipeline: **decline** unless this check moves the results.
- **Archive.** main.tex:912-916. **Fix before submission.** Zenodo deposit of the code, queries, derived tables and
  PDF, and a git tag on the commit that built it.

### 10. Theory (B-theory)

- **Short proofs.** Section 4.4. **Fix.** Range, monotonicity and zero credit below τ take a line each for a fixed
  reference set. The author is one of the peers, which gives the strict upper bound. Keep the simulations as
  implementation checks.
- **Factorization as description.** main.tex:271-283. **Fix.** Keep the identity. Drop "the quality part accounts for
  0.74–0.82 of the variance", or present it as a description of how the score varies, not as the share that is
  quality.
- **Double rank analysis (Brito & Rodríguez-Navarro).** Section 2. **Fix.** Cite and distinguish in two sentences.
  Check the reference details before adding it to `references.bib`.

### 11. Editorial

| Item | Where | Decision |
|---|---|---|
| "Four contributions", six listed (A-m1, B-table) | main.tex:101 (PDF 62) | Fix |
| Title: "all their papers" (B-table) | main.tex:27 | Authors: drop "all", or retitle, e.g. "… at every level of paper impact" |
| "Every paper … earns credit" → "can earn credit" (B-table) | main.tex:41-42, 94 (PDF 56) | Fix |
| Table 4 caption: raw values and cohort percentiles agree only within a start-year cohort (B-table) | main.tex:544-548 | Fix |
| Raw against cohort-percentile PiP labeled in Tables 4, 7, 8 (B-table) | main.tex:540-572, 940-990 | Fix |
| Figure 3(d) title (B-table) | `figures/make_tau_sweep.py:114` | Fix: "(d) Any future top-1% paper"; re-render |
| "As stable as productivity" → the observed order (B-table) | main.tex:108-109 (PDF 68), 903 (PDF 507) | Fix |
| Self-citations: "about 0.02 for every indicator" (B-table) | main.tex:814-815 (PDF 431), 861 | Fix: give the drops for PiP and the h-index separately |
| "Without reordering scholars within a field" (B-table) | main.tex:122 (PDF 81) | Fix: "largely preserves the order" |
| The website computes the original PiP-AUC (A-m2, B-table) | main.tex:130, 909-910 (PDF 514-515) | Fix the text; deploying the new score on pip-score.org is the authors' call |
| CRediT statement (A-m5, B-table) | main.tex:927-928 (PDF 527) | Authors |
| Appendix B heading alone at the foot of p. 25 (A-m6) | main.tex:969-990 (PDF 613) | Fix: `\clearpage` before Appendix B |

### Also noticed (in neither review)

- main.tex:841-842 (PDF 456-457): "PiP leads on the top-1% and quality outcomes despite this." PiP does not lead on
  future quality overall: the mean percentile and the top-10% count are ahead (Table 4). Name the indicators it leads,
  the article count and the original PiP-AUC, which predict survival best.
- main.tex:906 (conclusion): "about two years earlier than the h-index". Table 5 gives 1.1–1.9 years depending on the
  measure. Give the range, or tie it to the h-index's accuracy at age 10 as the abstract does.

## Order of work

1. Wording fixes that do not depend on new numbers.
2. Required runs: publication-only eligibility, grid convergence, T_τ and the combination baseline. Then update the
   abstract, introduction and conclusion from the results.
3. Second wave: the paper-field rule and ablation; fractional counting at age 10; calibration bands; the tie series;
   upper-tail transitions; the data audit; S2–GS top-decile agreement; tie-aware precision; the bootstrap; dated
   honors first.
4. Optional: the mid-rank variant with zero kept at zero; the isolated-first-article check; early detection without
   recent articles; reviews dropped.
5. Before submission: re-read the numbers in the abstract and conclusion against `FINDINGS.md`; CRediT; archive and
   tag.

## Declined, with reasons (for a response letter)

- **Mid-ranks as the main tie rule (A2.2).** They give positive scores to authors with no article above the cutoff
  (0.22 for five sub-median articles in the simulated cohort), which removes the property that weak papers earn
  nothing. The revision explains the strict rule and uses the tie identity to account for the drift.
- **Finer field schemes from another database (A2.3).** S2 offers 23 fields; OpenAlex subfields or MeSH would need a
  second pipeline. The revision discusses the coarseness of the fields and adds the paper-field rule.
- **Redefining the career start throughout (A-m4).** This requires rebuilding steps 05–21. The revision runs the
  isolated-first-article check and reports the S2–GS start-year comparison.
- **Review A's direction for the drift (A2.2).** Not adopted, since our numbers suggest the opposite direction. The
  revision reports the tie series instead.
- **New baselines as competitors (B4).** The authors decided not to present constructed scores as competing indicators.
  The cutoff-matched sum is reported as PiP-simple, a variant of PiP, which answers B's question of what the comparison
  at every threshold adds; the count + mean-percentile mix stays out of the paper. Both results are in `FINDINGS.md`
  §19 for a response letter: PiP beats the mix by about 0.10 on the future count and 0.055 on the top-1% AUC, with
  equal future impact.
