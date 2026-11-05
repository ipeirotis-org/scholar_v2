# Editing pass of 5 November 2026: decisions for the authors

Two passes changed `paper/manuscript_qss/main.tex` and `references.bib` on branch `claude/citation-audit-qss`:

1. **Citation fixes** (commits `f0dc0be`, `60b7d1a`): the problems found by the citation-needed audit, with the author's approval.
   The ledger is `literature/verifications.md`.
2. **Blue-pencil pass for a general scientific audience** (commits `abb574f` to `ce2f5e9`, one per section, at
   first-draft depth). Numbers, citations, cross-references, equations and table and figure bodies are unchanged; this was
   checked mechanically against the pre-edit text. Sentences that would add an interpretation, and cuts that need your
   confirmation, were held back (Section 1). Revert any section with `git revert <commit>`.

Line numbers in Section 2 refer to the manuscript before the blue-pencil pass (commit `60b7d1a`); Section 3 uses each
reviser's paragraph labels.

## 1. Held back: confirm to add (or reject)

Each sentence below was proposed by the editing pass but not applied. Say which to add.

- **Introduction**
  - Added, held back: "At the level of single papers, percentiles make that comparison."
  - Added, held back: "A paper one year old thus competes only with other papers one year old."
  - Proposed cut, not applied (kept): "Detecting performance early was one of the motivations for this work." It repeats the
    sentence before it.
- **Related work and Data**
  - "The second condition means that at least three of their articles have at least three citations each."
  - "We compute PiP only for eligible authors and compare each with their peers, the eligible authors who share their start
    year."
  - "Automatic disambiguation can split one person across several records or merge different people." (taken from Robustness)
- **Results, Sections 4.1 to 4.4**
  - "A fixed rule treats every start year alike only if the indicator's distribution does not change with the start year."
  - Before Eq. mean, kept your clause instead of: "The standing counts peers with strictly fewer articles, so a tie earns
    neither peer credit, whereas two random peers who do not tie are equally likely to be ahead."
  - Kept "PiP uses percentiles at every age, including recent papers" instead of: "PiP uses each paper's percentile at whatever
    age the paper has reached, so in slow fields part of a scholar's score rests on percentiles that are still settling".
- **Results, prediction and early detection**
  - "We compare PiP with the $h$-index, total citations and the number of articles, and with several indicators built from the
    same paper percentiles (defined below)."
  - "Every indicator is computed from the author's articles and citations up to the evaluation year."
  - "The Spearman correlation and the ROC AUC both depend only on how an indicator orders people."
  - "Together, the two kinds of pooling give five settings."
  - Kept "against 20\% for the $h$-index"; the pass had dropped "for the $h$-index".
- **Results, recognition, fields and robustness**
  - "For each indicator, the ROC AUC measures how well its ranking separates these future honorees from the others."
  - "Part of an indicator's predictive power could run through authors citing their own work."
  - "To check that the results are not artifacts of this algorithm, we use the author-curated Google Scholar profiles."
- **Discussion**
  - "PiP-simple uses the same percentiles and cutoff, and so shares the credit for recent papers, but compares the scholar with
    peers only once."
  - "Part of PiP's lead on that outcome therefore runs through authors citing their own work, but the lead remains."
- **Conclusion**
  - "It also anticipates later academy memberships, fellowships and prizes better than that index, though only slightly within
    already notable groups."
  - "and of exceptional papers", in "The prediction of future citation impact improves when scholars are compared with peers at
    every threshold rather than once."
- **The PiP score; Abstract**: nothing held back. The abstract grew from about 260 to 330 words; check it against the QSS
  abstract limit.

## 2. Cross-section consistency findings (whole-paper check, 41 items)

**Numbers that disagree (decide which is right)**
1. The 40\% rise in the mean $h$-index at career age 10: 1960 to 2010 in the Introduction (l. 64-66), 1960 to 2015 in Section
   5.3, the abstract and the fig:cohorts caption.
2. "1.6 to 1.9 years" for future high performers (abstract, contribution 4, conclusion): in tab:early the 1.9 comes from the
   top-1\% column; for future stars alone the age-10 lead is 1.6 to 1.8. The conclusion also drops "age-10".
3. Section 5.7 against tab:honors: I3 (0.721, 0.788) is above the $h$-index in both all-author columns, although the text says
   the article count and I3 "do worst apart from the mean percentile"; "every indicator ... (AUC 0.59-0.65)" leaves out 0.569,
   0.570 and 0.535; "the top-10\% count do[es] as well or better" when it scores 0.709 against PiP's 0.712; "level with it at
   age 5" for the notable groups, whose columns are at age 10 only.
4. fig:curves caption "about half" of peers from 0.85 to 0.95 against the text's 63\% at the 90th percentile.
5. 16,218 Google Scholar profiles in Data and Robustness against 15,890 in tab:honors.
6. "Over 55 cohorts" (fig:cohorts) for start years 1960 to 2015, which are 56.
7. Three field thresholds: 3,000 eligible authors (15 fields), 5,000 authors (fig:fields), 20,000 articles (fig:paperstability).
8. "0.72 for publication years before 1995" against 0.724 for 1980-1995 in tab:paperstability.
9. Subgroup agreements 0.76 and 0.73 are both below the pooled 0.78 and do not name their indicator.

**Claims that say more than the section behind them**
10. "PiP measures the area under the whole curve" (l. 95) against the cutoff; the title's "all their papers" against "gives
    nothing to papers below the cutoff".
11. Introduction's absolute "do not depend on the skewed tail" against Related work's comparative "less sensitive to outliers",
    both citing bornmann2013use.
12. Discussion: PiP's scale "does not depend on the cohort, so scholars of different seniority can be compared" rests on
    Section 5.3 at one career age; Responsible use recommends comparing scholars "at a similar career stage".
13. Section 5.2 advises raising $\tau$ only "up to about 0.6"; Section 5.7 recommends $\tau = 0.75$ "or higher" for recognition.
14. "The ordering of the indicators does not change" without self-citations: Appendix A shows only PiP and the $h$-index.
15. "Needs only publication years and citation counts by year" (conclusion) leaves out author-paper links and the type filter.
16. The step-curve formula is attributed to the 2024 blog post (l. 137-138, 365-369), but the repository's CLAUDE.md records the
    website moving from a trapezoidal to the step form on 2026-11-04. Which form does the post describe?
17. "The lead narrows but remains when eligibility does not depend on citations" next to "every setting and each of 15
    fields": the three-article rule is reported only for the same-age settings and early detection.
18. Contribution 4 "at least as accurately as the other indicators": under the three-article rule I3 matches or exceeds PiP on
    the AUC for future stars from about age 6, and PiP-simple's direction is not stated.

**Coverage and framing**
19. The conclusion omits later recognition (now proposed; see Section 1), self-citations and the Google Scholar check; the
    abstract omits the field-normalized version that Responsible use recommends.
20. (Addressed in the editing pass.) The abstract now lists prizes and ties $\rho = 0.78$ to PiP scores.
21. Wikidata and English Wikipedia appear first in Section 5.7, with no citation, snapshot date, Data-section entry or
    availability statement; the availability statement does not cover the Google Scholar profile data.
22. "The replication package" is never identified with the GitHub `paper/` directory; the Zenodo deposit is a TODO.

**Callouts**
23. tab:predictionall (Appendix B) is never cited in the text, and its caption does not say it reports raw PiP in the same-year
    rows.
24. "All three measures (Figure~\ref{fig:early})": the figure shows two; the AUC for future stars is only in tab:early.
25. (Partly addressed.) Section 5.2 used "citation impact" before Section 5.5 defines it; the editing pass now glosses it in 5.2.
26. fig:frontier's legend does not name the article count and the top-10\% count per year of career.

**Terminology**
27. "Six years past the doctorate" and "three to eight years into their careers" count from the doctorate; career age counts
    from the first article, and the 3-8 range has no source.
28. "Academic age" (Related work) against "career age" (Results) for the same Bornmann and Marx indicator.
29. "Future high performers" (front matter, Discussion) against the defined "future star".
30. ROC AUC is never defined, and "AUC" is also part of the name PiP-AUC; "GS" is never spelled out (tab:honors).
31. Smaller variants: papers against articles; "cohort" for publication years (partly fixed); "whole counting" in two senses;
    "exceptional paper" for top 1\% and top 2.5\%; five names for the three-article rule; "field version".
32. (Raised by the PiP-score reviser.) The asymmetric form is said to give one 99th-percentile article a score of zero; if the
    author is among their own peers, $F(1) - F(0) \ge 1/|\Peers|$, so the score is small but positive. Exact or rounded?

**Placeholders and back matter**
33. TODOs: Sen Tian's affiliation (the PDF currently shows NYU Stern), competing interests, funding, CRediT roles (an empty
    section), the Zenodo DOI.

## 3. Questions from each section's editing pass (verbatim)

### Abstract

- The revised abstract runs about 360 words, up from about 290, because it now defines what the $h$-index leaves out, the paper percentile, the standing and the three outcomes. Does it fit the QSS abstract limit? If it must shrink without losing a claim, would you rather drop the $h$-index gloss after the colon (about 12 words), fold the percentile and standing definitions back into one sentence (about 8), or drop "for comparison" and "author-curated" (3)?
- Numerical clauses rephrased, for your review: (a) "(correlation 0.91 between ages 3 and 10)" -> "its values 3 and 10 years after publication correlate at 0.91"; (b) "across cohorts from 1960 to 2015" moved to the head of its own sentence as "Across career cohorts from 1960 to 2015"; (c) "PiP predicts the next ten years of work better than the $h$-index ... (at career age 10, Spearman 0.43 against 0.35 ...)" -> "PiP predicts all three better ...", with the same parenthesis; (d) "the $h$-index's age-10 accuracy ... 1.6 to 1.9 years sooner" -> "as accurately as the $h$-index does at career age 10, but 1.6 to 1.9 years sooner"; (e) "(AUC 0.75 against 0.71 at career age 5)" now attaches to "academy memberships, fellowships and prizes", which assumes the AUC is computed over all 65 honors, prizes included, as lines 825-828 describe; (f) "agree with Google Scholar profiles ($\rho = 0.78$)" -> "PiP scores agree with those from author-curated Google Scholar profiles ($\rho = 0.78$)". Does each number still attach to the quantity you intend?
- Is "it ignores less-cited papers and the extra citations of the most-cited ones" the reading of "one corner" you want in the abstract? The Introduction (lines 77-78) also says a recent paper counts only once it has collected $h$ citations, which ties straight to the abstract's first sentence about early careers. Should that be added (about 12 words)?
- The abstract still ends on a robustness check, with no sentence on what PiP lets a committee do. The manuscript has candidates: Section~\ref{sec:cohorts}'s reading that scores at the same career age "can be read as statements of rarity, whatever their start year" (lines 576-577), or the Conclusion's "needs only publication years and citation counts by year". Do you want one of them as the closing sentence, at some cost in length?
- "Setting" (same career age or same calendar year), "Spearman" and "AUC" stay unexplained for a general reader, and the main eligibility rule is not stated (naming it would add numbers, which this pass cannot do). Should any of them be defined in the abstract?
- For the consistency check: the abstract, Introduction and Conclusion say "future high performers" where Section~\ref{sec:early} defines "future star". Is the plain phrase in the front matter intentional?
---

### Introduction

- P1 and P2: "six years past the doctorate" and "three to eight years into their careers" place scholars by years since the doctorate or by an unstated measure, while the paper's career age (now defined in P6 as years since the first paper) counts from the first article, and the citation audit found no source for "three to eight years". I left both numbers unchanged. Do you want to cite a source for the range, restate it in the paper's own career-age terms, or keep it as an illustrative description?
- P1: the introduction says the mean $h$-index at career age 10 is "40% higher for those who started in 2010 than for those who started in 1960", but Section 5.3 reports it rising from 4.4 to 6.2 across start years 1960 to 2015, and the abstract and the cohorts figure caption say it rose or grew by 40%. I changed nothing. Is 2010 the intended endpoint, or should the introduction use the same 1960 to 2015 span as Section 5.3?
- P5a: the sentence before it says PiP averages the curve only over thresholds above the cutoff, but "PiP measures the area under the whole curve" says the whole curve. Did you mean the whole curve to the right of the cutoff (so "the area under the curve above the cutoff" would be exact), or do you want to keep the contrast with "one corner" as written?
- L3 and P7 use "eligibility" and "an eligibility rule" before the introduction says what the rule is. Adding it would bring a new claim and an extra $h$-index into the introduction, so I left it to you. Should P6 state the rule, for example from Section 3: authors with at least three articles and an $h$-index of at least three, both computed up to the evaluation year?
- P5b: "every threshold of the computation" refers to the 40 percentile classes of Section 4.6, which a general reader has not met yet. A pointer would be a new \ref{sec:computation}, which the protected-content check would have to allow. Do you want to add that pointer, or keep the phrase as it is?
- I restructured four sentences that carry numbers, without changing any number or wording of the numbers: P1 (the 40% sentence now stands alone instead of following a colon), P5b (the top-2.5% sentence is now separate from the "With $\tau = 0.5$" sentence), P6 (a career-age definition follows "11.1 million authors at every career age from 1 to 30"), and L4 ("It reaches the accuracy that the $h$-index has at career age 10, and does so 1.6 to 1.9 years sooner"). Does each still say what you mean?
- I added two bridge sentences: "At the level of single papers, percentiles make that comparison." and "A paper one year old thus competes only with other papers one year old." (the second from Section 4.1). Do you confirm both as accurate descriptions of what paper percentiles do?
- P2: I cut "Detecting performance early was one of the motivations for this work." because it repeated the sentence before it, and L4 now carries early detection. Do you want the motivation stated explicitly somewhere in the introduction?
- Terminology: the introduction counts "papers", while the body's unit is "articles" (L2 already says "article count"); L4 says "future high performers", while Section 5.6 defines "future star"; and the introduction never names the asymmetric form PiP-AUC, which the body names in its equation. Should the introduction keep these plain-language variants, or align with the body's terms?
- For the consistency pass, outside this unit: the two phrasings repaired here recur in the body, "more articles than a random peer above a random impact threshold" (Section 4.3) and "at age 10 1.6 to 1.9 years earlier" (Section 5.6). Should those be aligned with the introduction's wording?

### Related work and Data

- [Related work, $h$-index paragraph] The paragraph ends on "The $h$-index is also inconsistent", but Section~\ref{sec:properties} (Consistency) and the Limitations say PiP is not consistent either, so a reader here will assume PiP fixes it. Should the sentence say that PiP shares the property (which needs a new cross-reference, a protected change I did not make)?
- [Related work, two places] "Academic age" describes the $m$-quotient family and \citet{bornmann2014individual}, yet Section~\ref{sec:prediction} describes the same Bornmann indicator as "divided by career age", the paper's defined term. Should Related work also say "career age", or do the cited works mean something different?
- [Related work, Percentiles and From papers to scholars] A reader outside bibliometrics will not know what "normalization by mean citation rates" does, what the "inputs" of \citet{abramo2014productivity} are, or what "the relation between the two" ranks is in double rank analysis, and the manuscript explains none of them. Would you add a short plain gloss for each?
- [Related work, Citation windows] The paragraph does not say why windows matter for PiP, which scores recent papers by their early percentiles (Section~\ref{sec:paperstability} measures how fast those settle). Would you add that link, which needs a new cross-reference?
- [Data, Eligibility] I added "The second condition means that at least three of their articles have at least three citations each," from the Introduction's definition of the $h$-index. Is it right? Because it shows that the $h$-index condition already implies three articles, should the text say that the article condition is listed so that the relaxed rule reads as dropping one condition?
- [Data, Eligibility] I added "We compute PiP only for eligible authors and compare each with their peers, the eligible authors who share their start year," from Section~\ref{sec:pip} and the 11.1 million figure. Does it hold for every analysis under the main rule?
- [Data, Eligibility] The manuscript never says why the rule exists. Why are authors without three articles cited at least three times each left out (for example, records too short to rank), and could you state the reason in one sentence here?
- [Data, Authors and careers] I added "Automatic disambiguation can split one person across several records or merge different people," taken from Section~\ref{sec:robustness} (Author disambiguation), which now says the same thing. Keep both, or shorten the copy in Robustness?
- [Data, Articles] The 63\% against 30\% comparison has no stated consequence. What would the non-article records have done to article percentiles (a reader may guess that many uncited records at the bottom of each year would lift every article), and should the text say it?
- [Data, Articles; numerical flag] I changed "In the 2000 cohort, 63\% of all records are uncited at age 5, but only 30\% of articles" to "Of the records published in 2000, 63\% are uncited at age 5, but only 30\% of the articles from that year", to keep "cohort" for start-year cohorts. Does "the 2000 cohort" mean records published in 2000?
- [Data, Eligibility; numerical flag] I split the exclusion sentence into the claim and its evidence, and "(start years 1975--2000)" still sits in the age-3 clause. Do the age-10 figures (43\%; 22\%, 64\%, 97\%) cover the same start years, and are biology and history the lowest and highest fields, as "from ... to" implies?
- [Data, Google Scholar profiles] The paragraph presents the profiles only as a check on disambiguation, but Section~\ref{sec:honors} also uses them (the faculty subgroup and the current-score columns of Table~\ref{tab:honors}). Should this paragraph mention that use?
- [Data, Authors and careers] Figure~\ref{fig:curves} has a ready example for career age (start year 1995, evaluated in 2005, career age 10). Would you add it here, given that it adds numbers and so needs your approval?
- [Cross-section] Section~\ref{sec:paperstability} also uses "cohort" for publication years ("For each cohort from 1960 to 1995"). Should the consistency pass keep "cohort" for start-year cohorts there as well?

### The PiP score

- The definition of $n_a(\theta)$ has no worked value. Would you add one line that ties a threshold to a familiar share, for example that $n_a(0.9)$ is the number of the author's articles in the top 10% of their year? I did not add it because it brings new math and a new number into the text.
- The text uses only the median author of Figure 1. Would one sentence on how the curves of the 25th- and 90th-percentile authors differ (their article counts, or the thresholds where their curves rise) help readers see what moves a score? The manuscript gives no values for those two authors, so I could not write it.
- The Figure 1 caption says the median author stands above "about half" of the peers at thresholds from 0.85 to 0.95, while the text says this author wins against 63% of peers at the 90th percentile. Is "about half" an average over that range, and should the caption say so?
- In Credit for the best work, one article at the 50th percentile scores 0.004 on the grid but 0 with the exact integral; the reason (an article in the class just above the median earns a full class of credit) appears only in the Computation subsection. Would you add a pointer to Section~\ref{sec:computation} there? I did not, because it is a new cross-reference.
- Should the Factorization paragraph say in words what the variance shares show, for example that most of the variance of $\log \PiP_{1/2}$ comes from $\log Q$ (0.74–0.82, against about 0.30 from $\log F$), so that the contrast with the asymmetric form, where the productivity percentile accounts for 0.59–0.77, is explicit?
- The asymmetric form is said to give an author with one article at the 99th percentile a score of zero. If that author belongs to their own peer group, as in Range and Factorization, then $F(1) - F(0) \ge 1/|\Peers|$ and the score is small but positive; is the zero exact under the simulation's setup, or rounded?
- The unit uses "cohort" and "peer group" for the same set in places ("the mean score of a peer group", then "the mean score of a cohort"; the game draws "a scholar from the same cohort", the formula "a peer"), while the Figure 1 caption separates "the 1995 cohort" from its "eligible authors", and Section 4.3 also says "the mean score of a cohort". Should "cohort" mean all authors with one start year and "peer group" its eligible members, with the text following that split?
- Section 3 mixes "paper" and "article" ("top-10% papers", "a single paper", "weak papers") although Section 2 defines articles as a typed subset of records. Should Section 3 say "article" throughout, or is "paper" an intended informal synonym?
- Numerical review: three rephrased sentences carry numbers or a figure reference. They are the median-author example (now "wins against 63% of peers when the threshold is at the 90th percentile ..., against 28% at the 75th percentile, and against none at the 97.5th percentile", with "close to the median of the cohort" added from the caption), "The whole computation covers 111.6 million author-age records", and the split of the Factorization paragraph, which moved its data sentences into their own paragraph. Are all values, pairings and scopes as you intend?

### Results, Sections 4.1 to 4.4

- In PS2, "For each cohort from 1960 to 1995" now reads "for each publication year from 1960 to 1995", because
  elsewhere in the paper a cohort is a scholar's start year; is publication year the intended meaning?
- Numerical text was moved or reworded in these places; do all numbers still attach to the right referents: (a) PS2's
  last two sentences, now their own paragraph in reversed order; (b) T2's drift sentences, now their own paragraph;
  (c) T4, where the choice-cohort differences now precede the test-sample result and "(1975--1990)" and "(1991 and
  later)" read "(start years 1975--1990)" and "(start years 1991 and later)"; (d) C1's cohort-size sentence, moved
  before the PiP distribution; (e) K1's reordered sentences, including "The article count alone is more stable than PiP
  at every horizon"; (f) the tab:paperstability caption, "7.2 million papers" changed to "7.2 million articles"?
- Do the three sentences on the Added bridges line (C1 rule-of-thumb sentence, C2 tie sentence, PS3 slow-field
  consequence) say what you mean, or would you reword them?
- PS1 now explains Hazen mid-ranks as "not the strict shares used in the score but Hazen mid-ranks, which also count
  half of the tied papers"; is that the definition you want a general reader to take away?
- ROC AUC appears in this unit (the "Top-1% AUC" column of Table tab:tau and the retrospective check) and is not
  defined in plain language anywhere in the paper; would you add a one-line definition at its first use, here or in the
  prediction section?
- T2 now glosses citation impact as the mean percentile of the future articles before the prediction section defines
  the outcomes; would you also add a pointer such as Section~\ref{sec:prediction} after "the author's future articles"
  (a new cross-reference is protected content, so I did not add one)?
- In T4, "the quantity--impact trade-off turns at a slightly higher cutoff": what does "turns" mean for a reader outside
  the field (the knee of the frontier in Figure~\ref{fig:frontier}, or the cutoff where future citation impact is best
  predicted), and should the sentence say so?
- In T4, "whether the choice depends on them" now reads "whether the choice depends on which cohorts it is made from",
  and "the rule above" is now stated as "the cutoff with the highest AUC for a future top-1% paper among cutoffs whose
  distribution is stable across cohorts"; are both readings right?
- K1 says "PiP is much more stable than the h-index", and Table tab:careerstability gives 0.911 against 0.877 between
  ages 20 and 30; does "much" hold at every horizon, or should it be scoped to horizons that start early in a career?
- In PS3, "chemistry, physics, medicine and computer science (0.91)": does 0.91 apply to all four fields or only to
  computer science?
- Cross-section: the Introduction says the mean h-index at career age 10 is 40% higher for scholars who started in 2010
  than for those who started in 1960, the fig:cohorts caption says it "grows by 40%", and C1 reports 4.4 to 6.2 over
  start years 1960 to 2015; which start year has the mean of 6.2, and do the three statements agree?
- The fig:cohorts caption says "over 55 cohorts", but start years 1960 to 2015 inclusive span 56; is 55 intended?
- The fig:tau caption gives start years 1975--2010 for every panel, while Table tab:tau says the prediction columns
  use 1.45 million authors with start years 1975--2000; should the caption give the narrower range for panels b--d?
- The new Results opener describes subsections outside this unit (prediction through robustness); does it still match
  their order and content after the other units are revised?

### Results, prediction and early detection

- The manuscript never says how to read a ROC AUC or where chance lies, yet the prediction and early-detection results lean on it (0.77 against 0.72; 0.63 against 0.56). Would you add a plain reading at its first use in Section `sec:prediction` (for example, the probability that a randomly chosen author with the outcome is ranked above one without it, with 0.5 as chance) and, if useful, a note that a Spearman correlation of 0 means no relation?
- Added bridge in P2: is "The Spearman correlation and the ROC AUC both depend only on how an indicator orders people." the reasoning behind "order people identically, so the comparison pools cohorts", and does it hold for the measures as computed, including the handling of ties?
- Added sentence in P1: "Every indicator is computed from the author's articles and citations up to the evaluation year." restates Section `sec:data` for this test. Does it hold for every compared indicator, including total citations and the article count?
- The same-career-age settings also pool start years (1975 to $2010 - A$), so raw values and cohort percentiles differ there too, and Table `tab:predictionall` reports the $h$-index both ways at ages 5, 10 and 15. Why does the text compare both forms only in the same-year settings, and should it say why?
- Numerical move in P3: the raw 2005 PiP values "0.41, 0.78 and 0.31" now sit in the "gap is larger" sentence instead of the conversion sentence, with their values unchanged. Is that placement right?
- Numerical rephrasings: in P10b "the accuracy that the $h$-index has at age 10 1.6 to 1.9 years earlier" became "the $h$-index's age-10 accuracy 1.6 to 1.9 years earlier", and "the accuracy of the citation count 2.6 to 3.2 years earlier" became "the citation count's 2.6 to 3.2 years earlier", read as the citation count's age-10 accuracy, as in the next sentence; in P11 "against 20% for the $h$-index" became "against 20%". Does each number still refer to what you meant, and in particular is 2.6 to 3.2 years measured against the citation count's age-10 accuracy?
- New count in P2: "Together, the two kinds of pooling give five settings." Are the five settings career ages 5, 10 and 15 and the years 2000 and 2005?
- In P5, "Each is the best or nearly the best on one outcome and weak on another" and "Each of them serves one of the three outcomes" also cover the top-1% count, which is far from best on every outcome (0.250, 0.641 and 0.228 at age 10). Should these sentences exclude the top-1% count?
- Why are future articles scored at age 5? Since a general reader will ask, would you add at the first mention a pointer to Section `sec:paperstability` (correlation 0.96 between ages 5 and 10)?
- The PiP-simple gloss "through the rank of its score in the cohort" comes from Section `sec:pip` ("compared with peers only through its rank in the cohort"). Is this what "compares the scholar with peers only once" means?
- The outcomes are built from articles (the defined record type), but the results alternate "future articles" with "future top-10% papers" and "future papers". Is "papers" a synonym for articles here, and may the unit use "articles" throughout?
- In P6, does "with $\tau = 0.75$ it does at least as well as the top-10% count in every setting" mean on all three outcomes, as the $\tau = 0$ clause about I3 does?
- The Figure `fig:frontier` caption now reads "Grey squares are the $h$-index and the indicators built from the same paper percentiles", and P6 places the article count against the frontier. Are the article count and total citations among the grey squares, and should the caption name them?
- P7 ends with a claim about settings ("PiP is the best predictor of a future top-1% paper in every setting") inside a paragraph about cohorts. Would the claim sit better at the end of P5, next to "none beats it on the chance of a future top-1% paper"?
- P10a says PiP is the most accurate "on all three measures (Figure `fig:early`)", but the figure shows only precision and the top-1% AUC. Where can a reader see the AUC for future stars at each age?
- In P12's "the gain comes mainly from what the two forms share", is the gain PiP's lead over the $h$-index, as in the Discussion, or its lead over every indicator?
- For the cross-section consistency check: the Introduction (contribution 4) has the same number collision ("at career age 10 1.6 to 1.9 years sooner"); Sections `sec:tau` and `sec:robustness` call the first design the "same-age settings" while this unit says "Same career age"; and Section `sec:tau` uses "citation impact" before this unit defines it. Should these be aligned?

### Results, recognition, fields and robustness

- Wikidata is neither described nor cited anywhere in the paper, so a general reader does not know what a "Wikidata item" is or why its coverage of honors is uneven; would you add a short gloss, with a citation or URL, at its first use in the Later recognition subsection?
- The two linkage counts use different units, "41,272 items" and "12,434 honorees"; does 41,272 count every Wikidata item with a Semantic Scholar author ID (the "Wikidata" population of the honors table) or only honorees, and could the sentence say which?
- How are people whose honors carry no date treated in the main analysis, kept as future honorees or dropped? "Restricting to honors with a recorded date gives the same pattern" implies they are kept; could the design paragraph say so?
- ROC AUC is not glossed anywhere for a general reader, and its first use is in the Predicting future work subsection (outside this unit); would you add a one-clause plain-language gloss there, including the value that means chance, so that the honors AUCs (and "only weakly" at 0.59 to 0.65) can be read without it?
- LR2 says "the article count and I3, which lean towards volume, do worst apart from the mean percentile", but in the honors table I3 (0.721, 0.788) is above the h-index (0.710, 0.782) at both ages and above citations (0.701) at age 5, so in the two "All" columns only the article count fits; should the sentence name only the article count, or point to the notable-group columns, where I3 is near the bottom?
- LR3 says "every indicator separates future honorees only weakly (AUC 0.59--0.65)", but the table has the article count at 0.569 (Wikidata) and the mean percentile at 0.570 and 0.535; does the range exclude these indicators, and should the sentence say so?
- On the Google Scholar profiles the text says "total citations and the top-10% count do as well or better", but for holding an honor the top-10% count has 0.709 against PiP's 0.712; is "as well" meant within the bootstrap interval, or should the wording change?
- In R4, do the subgroup agreements 0.73 (three or more records) and 0.76 (one record) refer to PiP or to the h-index? If to PiP, both lie below the overall 0.78; is that intended (for example because the subgroups span narrower ranges of scores), or do the subgroup figures come from a different subset?
- The Fields text uses fields with at least 3,000 eligible authors (fifteen fields), but the fig:fields caption says "Fields with at least 5,000 authors"; are both thresholds intended, and should the caption or the text explain the difference?
- In the main-field rule, does "(whole counting)" mean that an article with several fields counts in full for each of them? The Limitations use "whole counting" for co-authors' credit, so may we add a short gloss here to keep a general reader from conflating the two senses?
- R3 uses "counterpart", "main author record" and "main record"; are these the same Semantic Scholar record, and if so, may we use one term throughout?
- F3 opens "The two versions answer different questions" but never names the questions; which are they, so the topic sentence can state them?
- In the second Fields result paragraph I joined "We have not separated the sources of the remaining spread" to the next sentence with a colon, which presents the pooled-percentile fallback and the field differences in ties as those sources; is that your intended reading, or are they only two of several candidates?
- Please confirm the three added framing sentences on the Added bridges line (the AUC description in the honors design paragraph, the self-citation worry, and the purpose sentence of the disambiguation check); each is built from your own text (the caption and LR3; the end of R1; R4, the Data section and the Introduction), but does each say what you mean?
- For human review, numerical clauses were moved or rephrased with every value unchanged: (a) LR3, the age 5 and age 10 comparison with the h-index and its two intervals now trail in one clause; (b) R1, "move by at most 0.003 for PiP" became "Removing self-citations moves PiP's predictions ... by at most 0.003"; (c) R5, the survival AUCs now follow the bias and open with "Measured at career age 5"; (d) Fields, the 6.9% to 11.6% result now opens its own paragraph, and "this brings" became "the field-normalized score brings"; (e) R2, "conditions on citations" became "selects the population by citations"; (f) the honors table caption was reworded around "Columns 2--5". Does each still state what you measured?

### Discussion

- [Added bridge, P2] I brought Section robustness's reading into the Discussion: "Part of PiP's lead on that outcome therefore runs through authors citing their own work, but the lead remains." Do you want that interpretation stated here, and is "but the lead remains" the strength you intend?
- [Added bridge, P1b] "PiP-simple uses the same percentiles and cutoff, and so shares the credit for recent papers, but compares the scholar with peers only once." Does this state the design, and why the comparison isolates the peer comparison, the way you intend?
- [Numerical, P2] The self-citation sentence was split and "and lowers" became "but lowers". The figures 0.02 to 0.03 and less than 0.01 are unchanged and still refer to future citation impact. Can you confirm? Separately, since the sentence now states their consequence, would you drop these Results figures from the Discussion and point to Section~\ref{sec:robustness} instead? That removes two numbers and adds a \ref, which are protected changes I did not make.
- [Logic, P1] P1a says "Three properties explain most of the gain in prediction", yet P1b credits "percentiles within the publication year and the cutoff" with most of the gain, and the cutoff is not one of the three properties. Should the cutoff join the list, or should the opening claim change?
- [Scope tension, P1 against P4] P1 says PiP's cohort-free scale lets "scholars of different seniority" be compared, while P4 says PiP is meant for "scholars at a similar career stage". Which scope do you want the Discussion to recommend?
- [Evidence, P1] The third property is the only one without a pointer. Should it point to the same-year comparison in Section~\ref{sec:prediction}, where converting the $h$-index to a cohort percentile narrows the gap but does not close it? That would add a \ref.
- [Missing consequence, L2] Which way does whole counting move scores, and for whom (for example, authors who publish in large teams), and should the bullet say so?
- [Missing consequence, L5] What happens to a scholar whose real articles carry no publication type (a smaller record and a lower score?), and does this hit some fields harder than others?
- [Missing consequence, L4] Can you say which way the retrospective reconstruction could bias the results, for example whether the 2026 author records are cleaner than the records an evaluator would have seen at the time?
- [L1] Section robustness says automatic disambiguation can also merge different people. Should the bullet name merged records as well as split ones?
- [L6] Section properties notes that the $h$-index shares this inconsistency. Should the bullet say so? Saying it would add an $h$ math span, a protected change I did not make.
- [P2, jargon] "An external source of variation" is econometric shorthand. Can you name the kind of variation you have in mind, so that a reader outside econometrics can picture it?
- [P2] "The most mechanical channel of this feedback is self-citation" keeps the link in your original between "channel" and reputation feeding itself. Is self-citation meant as a channel of cumulative advantage, or of citations feeding on themselves more generally?
- [P3b, cross-section] Results (Section paperstability, end of the field paragraph) promises that the Discussion explains "what this means for scholars in slow fields". P3b gives the evaluator's remedy but not the consequence for the scholar. Should it say that the credit for recent papers, which P1 links to the early-detection lead, is least reliable in those fields?
- [Copyedit, L2] I changed "co-author" to "coauthor" to match "coauthored" and "coauthors" in Section fields. Is "coauthor" the house spelling?
- [L5] I replaced "the counts" with "the paper counts", the wording of Section data. Should the bullet also name the $h$-index, as Section data does ("the reference sets, the paper counts and the $h$-index")?

### Conclusion

- I added the later-recognition finding ("It also anticipates later academy memberships, fellowships and prizes better than that index, though only slightly within already notable groups") and, in the sentence on comparing at every threshold, "and of exceptional papers". Both are stated in the abstract, in Introduction contributions 3 and 5 and in Sections~\ref{sec:prediction} and~\ref{sec:honors}. Did you leave either out of the conclusion on purpose?
- The conclusion is still a summary. The Discussion's first paragraph holds the synthesis (lines 991-994: percentiles within the publication year and the cutoff carry most of the gain over the $h$-index in counting future top papers and in early detection; the comparison at every threshold adds the gain on citation impact and exceptional papers). Should one sentence of it lead the conclusion, even though it repeats the Discussion?
- Numerical clauses rephrased, for your review: "On 11.1 million Semantic Scholar authors its distribution changes little across cohorts since 1960" now reads "For 11.1 million Semantic Scholar authors, the distribution of PiP changes little across career cohorts since 1960", and "It reaches the $h$-index's accuracy ... 1.6 to 1.9 years earlier" now opens with "PiP". Do both keep their numbers on the quantities you intend?
- The last sentence now contrasts the website's earlier form with "PiP itself" computed by the replication code. Does the replication package compute the symmetric PiP at full scale, as the Data and code availability section implies?
- The earlier score is "a first, asymmetric form" in the Introduction, "the first form" in the subsection on relations to other indicators (where its formula is named PiP-AUC), and "the earlier, asymmetric form" here and in Competing interests. Should one wording be used throughout?

## 4. Citation status after both passes

All 73 claim-source pairs in the current text have a ledger entry for their exact wording: 67 supported, 6 partially
supported, none unsupported or unverifiable. The partial rows: DORA in Responsible use (the co-cited Leiden Manifesto covers
expert judgment); Merton on comparable work (the co-cited Petersen et al. covers citations); Waltman and Schreiber twice
(co-cited sources cover the rest); Leydesdorff and Bornmann on I3 growing without bound (your own inference); and Hazen,
where the editing pass added "which also count half of the tied papers", which Hazen 1914 does not discuss.
