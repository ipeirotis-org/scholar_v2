# Editing pass of 5 November 2026: status and open decisions

Branch `claude/citation-audit-qss`. Commits on `paper/manuscript_qss/main.tex` and `references.bib`, in order:

| Commit | What |
|---|---|
| `f0dc0be`, `60b7d1a` | Citation fixes from the citation-needed audit (ledger: `literature/verifications.md`) |
| `abb574f` to `ce2f5e9` | Blue-pencil pass for a general scientific audience, one commit per section |
| `7010816` | Your decisions: every held-back sentence added; Hazen tie clause dropped. The proposed cut ("Detecting performance early was one of the motivations for this work.") was not applied, so that sentence stays. "Against 20\% for the $h$-index" keeps "for the $h$-index". |
| `8c79ccf` | Numbers corrected against the pipeline outputs (Section 2a) |
| `c0f3309` | Claims scoped to their evidence; definitions, callouts and sources (Section 2b) |
| `231d654` | Three Related-work glosses; the $\tau = 0.75$ frontier claim scoped |

Every changed number was recomputed from the files the pipeline writes (`paper/figures/data/`, `paper/honors/data/`,
`paper/FINDINGS.md`, or one query of `paper_rp.baseline_prediction`); the commit messages give the values.

## 1. Decisions that need you

Each item gives the exact text of each option. Reply with the item and the letter (for example "D1 A, D2 B, D3 B").

**Status after your reply of 5 November 2026 (D1 A, D2 B, D3 B, D6 A, D7 A, D8 A):** those six are applied as written
(D6 sets *papers* in italics). Later replies: D12 A (applied: the earlier Conclusion sentence is deleted) and D4 C
(no change: the rule stays unexplained), D9 A and D10 A (no change). Still open: D5 (the website's export is not the
data the paper uses; see D5 below) and D11 (inputs).

**D5, update.** The export at \url{https://www.pip-score.org/data} (`scholar_data_share/all_authors_stats.csv`, last
modified 14 March 2026) holds 16,953 Google Scholar profiles with author-level statistics (h-index, citations,
i10-index, publication counts, start year, their percentiles and the asymmetric PiP-AUC), with profile dates from
February 2024 to May 2025. The paper uses 16,218 profiles from the November 2026 crawl, with per-article citations, and
the symmetric PiP. Options:
- **A.** Refresh the export from the current crawl before submission, then write: "Author-level statistics for the
  Google Scholar profiles can be downloaded from \url{https://www.pip-score.org/data}." (Writing the file changes the
  live website's download; I would do it only on your go-ahead.)
- **B.** Keep the export as it is and write: "Author-level statistics for an earlier snapshot of the website's Google
  Scholar profiles can be downloaded from \url{https://www.pip-score.org/data}; the per-article profile data used here
  are not redistributed."
- **C.** Both A and B's second clause: refresh the export and say the per-article data are not redistributed.

**D12. Conclusion overlap after D8.** The Conclusion now says twice that the comparison at every threshold improves
the prediction of citation impact and exceptional papers.
- **A.** Delete the earlier sentence, "The prediction of future citation impact and of exceptional papers improves
  when scholars are compared with peers at every threshold rather than once." The D8 sentence keeps the claim.
- **B.** Keep both.

**D1. Abstract length.** A web search reports that QSS limits abstracts to 200 words (MIT Press submission guidelines;
the page itself returned 403 to my fetch, so please confirm). The current abstract has about 350 words.
- **A.** Replace it with this 199-word version (every number checked against the manuscript):
  > Committees judge scholars early in their careers, before citations accumulate, but citation counts and the
  > $h$-index depend on field, grow with seniority and drift with the calendar. We propose PiP (percentile in
  > percentile). Each paper is ranked within its publication year. At every impact threshold above a cutoff, a
  > scholar's count of papers at or above it is ranked against peers who started publishing in the same year; PiP
  > averages these standings. We compute PiP for 11.1 million Semantic Scholar authors with data up to each evaluation
  > year. A paper's percentile at age 3 correlates at 0.91 with its percentile at age 10, and PiP's distribution
  > barely changes across cohorts from 1960 to 2015, while the mean $h$-index at career age 10 rose by about 40\%. PiP
  > predicts a scholar's next ten years (number of top papers, chance of an exceptional paper, citation impact) better
  > than the $h$-index in every setting and in each of 15 fields. It finds future high performers as accurately as the
  > $h$-index does at career age 10, but 1.6 to 1.8 years sooner, and better anticipates academy memberships,
  > fellowships and prizes (AUC 0.75 against 0.71 at career age 5). The results hold without self-citations.

  It drops: the "one corner" sentence, the $\tau$ dial, Spearman 0.43 against 0.35, the three-article-rule clause,
  the comparison with other percentile indicators, "though only slightly within already notable groups" and the
  Google Scholar agreement ($\rho = 0.78$).
- **B.** Keep the current abstract (if the limit is not 200 words).
- **C.** You cut it yourself; I check every number and claim against the body.

**D2. Title.** "by the impact of all their papers", while PiP gives nothing to papers below the cutoff.
- **A.** Keep: "Percentile in Percentile: rating scholars against their career cohort by the impact of all their papers"
- **B.** "Percentile in Percentile: rating scholars against their career cohort by the impact of their papers"
- **C.** "Percentile in Percentile: rating scholars against their career cohort at every impact threshold"

**D3. Introduction, unsourced career stages.** "six years past the doctorate" and "three to eight years into their
careers" count from the doctorate, and no source was found for the range.
- **A.** Keep both as illustrations.
- **B.** Replace with: "...whether a candidate's record is strong for someone at that career stage" and "...judge
  scholars in the first years of their careers, when most of their papers are recent...".
- **C.** Keep them and name sources (for example the eligibility windows of specific grant schemes); I verify them.

**D4. Data, why the eligibility rule exists.** The paper never says why authors need three articles and an $h$-index
of three. `FINDINGS.md` (section 3) says the rule is the as-of-year version of the website's `active_authors`
benchmark.
- **A.** Add: "The rule is the benchmark population of the website \url{https://www.pip-score.org}, which leaves out
  records too short to rank meaningfully."
- **B.** Add a reason in your own words.
- **C.** Leave it unexplained.

**D5. Availability of the Google Scholar profile data** (now a `% TODO` in Data and code availability).
- **A.** "The Google Scholar profiles were collected from public profile pages through a commercial API and are not
  redistributed; the collection code is in the \texttt{gs\_crawler/} directory of the same repository."
- **B.** Release per-profile scores (PiP and the comparison indicators) without the profile contents, and say so.
- **C.** "Available from the authors on request."

**D6. "Papers" and "articles".** Data defines articles as the typed subset; Results mostly say "papers" for the
same thing.
- **A.** Keep "papers" in Results and add one sentence at the end of the Data paragraph on articles: "From here on,
  papers means articles unless stated otherwise."
- **B.** Replace "papers" with "articles" wherever the counted unit is meant in Sections 4.1 to 4.9 (about 40
  places). Front matter and Discussion keep "papers".

**D7. Whole-counting limitation.** The bullet does not say which way whole counting moves scores.
- **A.** Add: "Compared with fractional counting, it favors authors who publish in large teams."
- **B.** Leave as is.

The other two bullets that lack a direction (untyped records; retrospective reconstruction) have no evidence in the
repository either way; I left them unchanged.

**D8. Conclusion synthesis.**
- **A.** Add after the sentence on 1.6 to 1.8 years: "Most of PiP's gain over the $h$-index comes from ranking papers
  within their publication year with a cutoff; comparing scholars with peers at every threshold adds the gain on
  citation impact and exceptional papers."
- **B.** Keep the conclusion as a summary.

**D9. Discussion, self-citation numbers.**
- **A.** Keep "by 0.02 to 0.03 and the $h$-index's by less than 0.01".
- **B.** Replace them with a pointer: "...lowers PiP's prediction of future citation impact more than the
  $h$-index's (Section~\ref{sec:robustness})."

**D10. Subgroup agreements (Robustness).** For PiP, both subgroups (0.76 with one record, 0.73 with three or more)
are below the 0.78 for all 9,796 people. The text now names PiP and gives the values.
- **A.** Leave it there.
- **B.** I query the matched table (`sql/19_s2_gs_match.sql`) for the spread of scores in each subgroup and, if a
  narrower spread explains the lower values, add one sentence saying so.

**D11. Inputs only you have** (no options possible): Sen Tian's current affiliation (the PDF shows NYU Stern); Sen
Tian's competing interests; confirmation of "No specific funding was received for this work."; CRediT roles for both
authors; the Zenodo DOI once the derived tables are deposited.

## 2. What was fixed

### 2a. Numbers (commit `8c79ccf`)

| # | Finding | Fix | Source |
|---|---|---|---|
| 1 | 40\% rise, 2010 or 2015 endpoint | Section 4.3: "rises by 40\%, from 4.4 in the 1960 cohort to 6.2 in the 2010 cohort (6.1 in 2015)"; abstract and caption: "about 40\%". The Introduction's 2010 comparison was right (+40.7\%). | `fig2_cohorts.csv` |
| 2 | 1.6 to 1.9 years | At target age 10 the leads are 1.79 (precision), 1.58 (star AUC), 1.90 (top-1\% AUC). Abstract, contribution 4 and Conclusion (future high performers): 1.6 to 1.8. Section 4.6 keeps 1.6 to 1.9 "across the three measures" and adds "(1.6 to 1.8 on the two measures for future stars)". | `fig8_early.csv` with the interpolation in `make_figures.py` |
| 3 | Honors text against the table | Only the article count does worst among all authors (I3 is above the $h$-index there); notable-group AUCs at age 10 span 0.54 to 0.65; "level at age 5" now gives the paired differences ($-0.008$ [$-0.023$, 0.007]; $+0.002$ [$-0.010$, 0.013]), which the table does not show; the Google Scholar sentence says citations and the top-10\% count tie PiP on honors (intervals include zero) and lead by 0.005 and 0.004 on Wikipedia. | `honors_results.csv`, `gs_honors_results.csv` |
| 4 | fig:curves "about half" | "stands above 49\% to 63\% of the peers" | `fig3_curves.csv` |
| 5 | 16,218 against 15,890 | Caption: 15,890 are the profiles that meet the eligibility rule and belong to a start-year cohort of at least 30. | `sql/24_gs_honors.sql` |
| 6 | 55 cohorts | 56 | `fig2_cohorts.csv` |
| 7 | Field thresholds | 3,000 (prediction, start years 1975 to 2000) and 5,000 (figure, start years 1975 to 2010) are different populations; the caption now says so, and the 6.9\% to 11.6\% sentence no longer says "1.45 million" (that range comes from the 1975 to 2010 population). 20,000 counts articles in another analysis. | `sql/18_field_normalized.sql`, `export_figure_data.sh` |
| 8 | 0.72 "before 1995" | "for publication years 1980--1995" (0.724 in the table) | `FINDINGS.md` section 1 |
| 9 | Subgroup agreements | Named as PiP's; "at most one record" | `FINDINGS.md` section 13 |
| | Chemistry 0.91 | 0.92 (0.916) | `fig1_fields.csv` |
| | Exclusion figures | All three ages cover start years 1975 to 2000; philosophy ties history at 97\% | `FINDINGS.md` section 20 |

### 2b. Claims, callouts and terms (commits `c0f3309`, `231d654`)

| # | Finding | Fix |
|---|---|---|
| 10 | "Area under the whole curve" | "PiP reads the whole curve above the cutoff". Title: D2. |
| 11 | "Do not depend on the skewed tail" | "are less influenced than means by the skewed tail", the source's own comparative claim |
| 12 | Seniority in the Discussion | Third property now rests on Results: stable rarity at the same career age (Section 4.3) and raw PiP predicting almost as well as its cohort percentile when seniorities are pooled (Section 4.5). Responsible use unchanged. |
| 13 | $\tau$ up to 0.6 against 0.75 for honors | Honors section adds that above 0.6 the raw score drifts, so such scores should be read as cohort percentiles, as in the honors test. |
| 14 | "Ordering of the indicators" without self-citations | Only PiP, the asymmetric form and the $h$-index were rerun. Robustness and Discussion now say PiP stays ahead of the $h$-index on all three outcomes in every setting (Appendix A). |
| 15 | "Needs only publication years and citation counts" | "needs only each scholar's articles, with their publication years and citations by year" |
| 16 | Step or trapezoid in the blog post | The post says "area under the curve" without the form. The formula is now tied to the website ("as the website computes it"); the website has used the step form since 4 November 2026. |
| 17 | Three-article rule scope | It was run only at the same career age and for early detection; abstract, contribution 3 and Conclusion say "at the same career age". |
| 18 | Contribution 4 | Scoped to the main eligibility rule; Section 4.6 says PiP-simple edges ahead on future stars from age 6 or 7 under the three-article rule (by at most 0.003). |
| 19 | Conclusion coverage | Adds self-citations, Google Scholar agreement and the field-normalized version. The abstract: D1. |
| 21 | Wikidata and Wikipedia | New Data paragraph ("Honors") with a citation (`wikidata2026`, CC0, queried 5 November 2026); Wikidata availability added; Google Scholar data: D5. |
| 22 | Replication package | Defined as the `paper/` directory in Data and code availability; Section 3 points there. |
| 23 | tab:predictionall uncited | Cited in Section 4.5, with the observation that raw and percentile forms nearly agree at the same career age. |
| 24 | "All three measures (Figure early)" | Says what the figure shows and that Table early covers all three. |
| 26 | fig:frontier legend | Names the grey squares and the two off-chart indicators; total citations are not plotted. |
| 27 | Doctorate-based ages | D3 |
| 28 | Academic age | "career age" in Related work; the sources mean years since the first publication. |
| 29 | Future high performers | Section 4.6 defines "future star, or future high performer". |
| 30 | ROC AUC, GS | ROC AUC defined at first use in Section 4.5 (rank-sum form with ties at one half, as `sql/11_prediction.sql` computes it); "GS" spelled out in the honors caption. |
| 31 | Smaller variants | "Three-article rule" defined in Data and used throughout; whole counting of fields glossed; "exceptional paper" in the Limitations is now "top paper"; "peer group" for the mean-score results; "articles" in Section 3. Papers against articles in Results: D6. |
| 32 | Asymmetric single-article zero | Exact: the simulation scores records against a cohort they do not join, whose members all have at least three articles (`checks/sym_properties.py`). The text now says so, here and in Credit for the best work. |
| 33 | TODOs | D11 |

Also from the section questions: Intro states the eligibility rule and points to Section 3.6 for the grid; Related work
says PiP shares the $h$-index's inconsistency, glosses mean normalization, Abramo and D'Angelo's measure and double
rank analysis from the archived sources, and links citation windows to Section 4.1; Data gives a career-age example,
says the $h$-index condition implies three articles, and notes that Section 4.7 uses the profiles; Section 3 gives
$n_a(0.9)$ as an example, states that most of the variance comes from the impact multiplier, and points the 0.004 to
Section 3.6; Section 4.2 points to Section 4.5, names the "knee" of the frontier and fixes the fig:tau start years;
Section 4.4 scopes "much more stable" to horizons that start early; Section 4.5 explains scoring at age 5, excludes the
top-1\% count from "each is the best on one outcome", moves "best predictor of a future top-1\% paper in every setting"
to the paragraph it summarizes, and says $\tau = 0.75$ matches the top-10\% count on all three outcomes in all five
settings (checked in `paper_rp.baseline_prediction`); Section 4.7 says undated honors count as later honors
(`sql/22_honors.sql`) and that 41,272 counts items, honored or not; Section 4.8 names the two questions the field
versions answer and presents the two sources of remaining spread as candidates; Robustness uses one term
(counterpart); Discussion names the cutoff in the second property, replaces "external source of variation", says what
slow fields mean for a scholar's score, and adds merged records and the $h$-index's inconsistency to the Limitations.

## 3. Checked and left as is

- Abstract clauses (a) to (f) of the blue-pencil questions: every number attaches to the intended quantity; (e) the
  65 honors include prizes.
- Spearman and ROC AUC both depend only on order, ties included (average ranks in `sql/11_prediction.sql`); a raw
  indicator and its cohort percentile order people identically within a cohort.
- "Every indicator is computed from the author's articles and citations up to the evaluation year" holds for citations
  and the article count.
- The five settings are career ages 5, 10, 15 and the years 2000 and 2005.
- Raw 2005 PiP values 0.41, 0.78, 0.31 and the citation count's 2.6 to 3.2 years (against its own age-10 accuracy):
  correct.
- "The 2000 cohort" of records means records published in 2000 (records have no start year); the text already says so.
- "Self-citation" as a channel of cumulative advantage matches `FINDINGS.md` section 11; unchanged.
- "Coauthor" is the house spelling; untouched.
- The disambiguation sentence appears in both Data and Robustness; kept as a reminder in Robustness.
- PS2 "publication year" (not cohort), and all numerical moves in Sections 4.1 to 4.4: values and referents unchanged.

## 4. Citation status

All 75 claim-source pairs in the current text have a ledger entry for their exact wording: 70 supported, 5 partially
supported, none unsupported or unverifiable. Hazen is now supported (the tie clause is gone). The partial rows: DORA in
Responsible use (the co-cited Leiden Manifesto covers expert judgment); Merton on comparable work (the co-cited
Petersen et al. covers citations); Waltman and Schreiber twice (co-cited sources cover the rest); Leydesdorff and
Bornmann on I3 growing without bound (your own inference). The new source `wikidata2026` is archived in the private
bucket (Wikidata's Licensing and Introduction pages, fetched 5 November 2026).
