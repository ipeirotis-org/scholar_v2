<!-- Review B. Source: pre-submission review addressed to Panos, pasted in chat on 2026-11-05. Written against
     manuscript_qss/main.pdf as of main@c7efb5d (26 pages); page and line numbers are PDF numbers. The text is unchanged.
     Converted to Markdown: headings, lists, tables and LaTeX math. Removed: a tool citation marker after the third
     paragraph and the placeholders where three links (to the production SQL, the property checks and the prediction
     SQL) were lost in the paste. -->

# Pre-submission review (Review B)

Panos, I would recommend major revision for QSS. There is a credible publication here, but I would revise this version
before submitting it. The construction is interesting, the empirical work is substantial, and the reported predictive
results are promising. The main weaknesses concern the population being evaluated, the interpretation of
comparability, and whether the comparisons establish the added value of PiP’s particular construction.

The topic fits QSS well: the journal explicitly welcomes novel methodological approaches to studying science and
scientific indicators.

I read all 26 pages, inspected the figures and tables, and checked selected files in the linked replication
repository. I have not reproduced the full database analysis. Page, section, and table references below refer to the
supplied manuscript.

## Overall assessment

The strongest contribution is comparing authors with their career peers separately at each paper-impact threshold,
then aggregating those comparisons. That is a coherent response to an actual measurement problem: publication counts,
citation counts, mean impact, and counts above a single threshold emphasize different parts of a publication record.

Several features make the paper stronger:

* The predictive outcomes use newly published papers. This avoids the obvious mechanical advantage involved in
  predicting a future cumulative indicator that already contains the present record.
* Future papers receive a common citation window. Scoring each at age five is a sensible way to make the outcomes more
  comparable.
* The comparisons extend beyond the h-index. Table 4 makes the quantity–impact trade-off visible and acknowledges that
  other indicators outperform PiP on individual outcomes.
* The paper investigates consequential limitations. Field differences, self-citations, author disambiguation,
  continued publication, and institutional recognition all receive attention.
* The early-detection design fixes the outcome and evaluation population while changing the measurement age. That is
  the right basic structure for asking how much earlier an indicator becomes informative.

My concern is that the manuscript repeatedly moves from these defensible findings to broader statements: that the
score means the same thing across cohorts and seniorities, measures quality, evaluates everyone who ever published, and
establishes a generally superior quantity–quality frontier. Those conclusions need either stronger tests or more
precise wording.

## Major comments

### 1. The eligibility filter materially limits the early-career claim and may affect the comparison with the h-index

Sections 3, 5.6, and 5.9; pp. 4–5, 15–17, and 20.

The analytic population requires at least three articles and an h-index of at least three at the evaluation date. The
early-detection analysis imposes that requirement by career age three.

This has three important consequences.

First, the population is narrower than the manuscript sometimes claims. The statement that the population “includes
everyone who ever published” is incorrect as a description of the analytic sample. The study retains people who
subsequently stop publishing, which addresses one form of survivorship selection. It nevertheless excludes everyone who
has not crossed the publication-and-citation threshold.

The appropriate description is something like:

> The analysis includes all authors who meet the eligibility criteria at the evaluation date, including those who
> subsequently stop publishing.

Second, the most appealing motivating example falls outside the operational population. The paper emphasizes the
ability to recognize someone with one exceptional paper. Yet that person is excluded by the eligibility rule. The
mathematical score can be evaluated against a reference cohort for such a person; the empirical analysis does not
validate that use.

Third, selection uses the competing indicator itself. Restricting the sample to $h\geq3$, especially at age three,
removes the lower part of the h-index distribution and can leave substantial bunching at three and four. The manuscript
acknowledges this bunching in Section 5.4. It therefore matters when interpreting an early predictive advantage over
the h-index.

This does not prove that the comparison is unfair or that the advantage would disappear. It establishes a specific
sensitivity analysis that is needed.

What I would request:

1. Repeat the main early-detection comparison using a publication-based eligibility rule without an h-index
   requirement—for example, at least three articles.
2. Report the proportion of authors excluded by the current rule, by field and career age.
3. Show how the time advantage changes under these alternative populations.
4. Describe the existing result explicitly as early detection among authors already meeting the eligibility threshold
   at age three.

Repeating the analysis among authors active at age five is useful, but it retains the citation-based selection
mechanism.

I would also replace “active” with “eligible” throughout. An author remains in this category permanently after
crossing the threshold, even if they never publish again. “Eligible” describes the rule more accurately.

### 2. The implemented score and the mathematical definition are different objects

Section 4.6, p. 8; properties in Section 4.4.

Equation 4 defines a continuous integral. Section 4.6 then uses

$$
\widehat{\mathrm{PiP}}_{0.5}(a)
=
\frac{1}{20}\sum_{k=20}^{39}g_a(k/40).
$$

This is a numerical approximation, and the distinction has observable consequences.

The implementation cannot distinguish papers above the 97.5th percentile. Holding everything else fixed, a paper at the
97.5th percentile and one at the 99.9th percentile contribute identically to all sampled thresholds. That is relevant to
the manuscript’s emphasis on exceptional papers.

The boundary at the median behaves differently. Under the integral, a paper at exactly $y=0.5$ contributes over an
interval of zero length. Under the discrete rule, it enters the threshold at 0.5 and can increase the score.

I also checked the public replication files. The production SQL uses the 40-point grid described in the paper, whereas
the numerical property checks use a 400-point midpoint grid. Consequently, those checks do not directly validate the
implemented discretization.

This is a confirmed definition–implementation discrepancy. It does not establish that the reported predictive results
are materially wrong.

What I would request:

* Clearly distinguish the integral definition from its approximation.
* Compare the implemented score with finer grids and, on a manageable sample, exact integration over the relevant
  breakpoints.
* Report score differences, rank agreement, top-decile membership changes, and changes in the main predictive
  comparisons.
* Apply the checks to the actual implementation.
* Reassess the high-$\tau$ findings with adequate resolution: at $\tau=0.9$, four sampled thresholds are a particularly
  coarse representation.

If the discrete version is the intended operational indicator, defining it directly would also be defensible. Its
properties and interpretation should then be stated for that version.

### 3. “Means the same thing in every cohort” needs a narrower interpretation

Section 5.3, pp. 10–12; related claims in the abstract and discussion.

The cohort comparison is informative, but the current wording gives distributional stability more substantive meaning
than it establishes.

There is a useful mathematical observation here. Let $p_{\theta,m}$ be the fraction of a cohort with $m$ papers above
threshold $\theta$. Because the standing uses the fraction with strictly fewer papers,

$$
\frac{1}{|\mathcal P|}\sum_{a\in\mathcal P}g_a(\theta)
=
\frac12\left(1-\sum_m p_{\theta,m}^{\,2}\right).
$$

Thus,

$$
\overline{\mathrm{PiP}_{\tau}}
=
\frac{1}{2(1-\tau)}
\int_\tau^1
\left(1-\sum_m p_{\theta,m}^{\,2}\right)\,d\theta.
$$

This follows directly from the definitions: among two randomly selected peers, the probability that one strictly
exceeds the other is half the probability that they do not tie.

The implication is that the cohort mean is strongly anchored by the normalization and determined by the prevalence of
ties. The full distribution is not mechanically fixed—the manuscript correctly acknowledges that—but stable means are
partly a structural property of the ranking procedure.

Three claims should be distinguished:

1. A score has approximately the same rarity in different cohorts.
2. A score represents approximately the same relative position on a defined citation-based construct.
3. A score implies approximately the same expected future performance.

Figure 4 mainly supports the first. The construction provides a particular interpretation of the second. The third
requires calibration evidence.

For example, does a PiP of 0.6 correspond to similar future top-paper counts across career ages, fields, and historical
cohorts? An overall predictive correlation does not answer that question.

What I would request:

* Change the heading to something like “The distribution of PiP is approximately stable across start-year cohorts.”
* State precisely what cross-cohort comparability means.
* If equal predictive meaning is intended, show future outcomes by PiP band, separately by cohort and career age.
* Keep the distinction between normalization and predictive usefulness explicit. A cohort percentile of the h-index
  also supplies a standardized relative scale; PiP’s additional contribution must come from what it summarizes and how
  well it predicts.

There is also a particularly clear interpretation available from your definition:

$$
\mathrm{PiP}_{\tau}(a)
=
\Pr\!\left[n_a(\Theta)>n_B(\Theta)\right],
$$

where $B$ is a randomly selected peer and $\Theta$ is uniformly selected from $[\tau,1]$.

In words: PiP is the probability that the author exceeds a randomly selected peer in the number of papers above a
randomly selected impact threshold.

That interpretation would improve the theory section. It also helps explain why a PiP of 0.6 need not be the 60th
percentile of authors’ overall PiP scores.

### 4. The benchmark set does not yet isolate the value of threshold-specific peer normalization

Sections 4.5 and 5.5; Tables 2 and 4; Figure 6.

The existing comparisons are useful. However, the paper’s most important methodological question remains:

> How much does comparing authors with peers separately at every threshold add beyond simpler combinations of the same
> information?

The comparison against ordinary I3 mixes two differences: PiP removes credit below the cutoff and applies peer
normalization separately at each threshold. A particularly informative missing baseline is a sum of percentile excesses
above the same cutoff:

$$
T_\tau(a)
=
\frac{1}{1-\tau}\sum_{j\in a}\max(y_j-\tau,0)
=
\frac{1}{1-\tau}\int_\tau^1 n_a(\theta)\,d\theta.
$$

One can then convert $T_\tau$ into a cohort percentile.

This baseline uses the same paper percentiles, ignores papers below the same cutoff, and combines publication volume
with citation standing. Its comparison with PiP would directly test the added value of applying $G_\theta$ inside the
integral.

A second useful baseline would be a simple combination of the cohort percentile of publication count and the cohort
percentile of mean paper impact. A prespecified equal-weight combination would be a reasonable starting point; a small
weight grid could provide a corresponding trade-off curve.

These are more informative additions than an extensive catalogue of h-index variants.

The “beats PiP on at most one of three outcomes” criterion also needs restraint. It establishes a pattern across the
selected outcomes. It does not establish a generally optimal indicator, because the relative importance of those
outcomes has not been defined.

What I would request:

* Add the cutoff-matched sum and a simple quantity–impact combination.
* Give each comparator equivalent opportunities for cohort and field normalization.
* Present the result as a trade-off among specific predictive objectives.
* Reserve claims about a superior frontier until these close alternatives have been tested.

The temporal split in Section 5.2 is useful, but because the default and its justification were developed after
examining the pooled results, I would call it a retrospective temporal robustness check. It does not provide the same
protection against specification selection as an untouched holdout.

### 5. Field normalization should be more central, and its remaining assumptions need examination

Section 5.8 and Figure 8, pp. 18–19.

The manuscript does investigate field bias, which is a strength. Its own results make this issue central: under the
pooled score, the proportion of authors appearing in the cohort top decile ranges from 1.4% in education and 2.1% in
economics to 14.7% in physics.

Given the introduction’s motivation involving comparisons across departments, the field-normalized analysis should
appear more prominently in the main argument and abstract.

The current field treatment also has an unusual feature: a paper is normalized against the author’s main field when it
carries that field, otherwise against the pooled reference set. Consequently, the same coauthored paper can receive
different percentiles for different authors. That may be a deliberate contextual choice, but it requires justification
and a sensitivity analysis.

I would ask for comparison with a rule based on the paper’s field assignments, together with a transparent treatment of
multidisciplinary papers.

There is a further inference that needs correction. The manuscript attributes the remaining differences in field
representation—6.9% to 11.6%—to papers outside the author’s main field retaining pooled percentiles. That explanation is
plausible, but it has not been established by the reported analysis. Averaging threshold-specific standings can yield
different final distributions across fields because of differences in ties and the dependence between standings at
different thresholds.

An ablation separating paper normalization, peer-group restriction, and the pooled fallback would identify what each
contributes.

Finally, whole counting matters for the interpretation of the score as an individual indicator. Every coauthor
receives full credit, and future outcomes use the same convention. A fractional-counting sensitivity analysis would
help establish how much the results depend on persistent collaboration patterns. Field normalization addresses
differences in publication, collaboration, and citation practices only to the extent that the chosen construction
captures them.

### 6. The validation supports prediction of citation outcomes and recognition; several claims about quality and mechanisms go further

Sections 5.1, 5.7, and 6.

#### Paper-percentile stability

The reported correlation of 0.908 between ages three and ten is encouraging. However, an overall rank correlation does
not directly show how reliably early data identify the upper tail, which is especially relevant to PiP.

The mean absolute percentile change of 0.086 in Table 1 is also worth interpreting: considerable individual movement can
coexist with a high overall correlation.

I would add a compact transition analysis for early versus later top-10% and top-1% status, including the youngest
paper ages actually used in the indicator. This would connect the stability evidence more directly to the proposed
application.

The discussion also attributes the early-detection advantage mainly to giving prompt credit to recent papers. That is a
plausible explanation, but the current comparisons do not isolate it. A version excluding the most recent papers, or a
targeted normalization ablation, would make that explanation testable.

#### Honors validation

Table 6 is valuable, and the presentation already acknowledges that honors are imperfect outcomes. The interpretation
should nevertheless give greater weight to the more comparable populations.

At career age ten:

| Evaluation population | PiP | h-index | Top-10% count |
|---|---|---|---|
| All eligible authors | 0.816 | 0.782 | 0.816 |
| Authors with a Wikidata item | 0.628 | 0.610 | 0.644 |
| Matched Google Scholar faculty | 0.631 | 0.623 | 0.634 |

The broad-population result shows useful discrimination. Within populations closer to the faculty-evaluation
motivation, the improvement over the h-index is modest and the top-10% count performs at least as well.

That is still a useful result. It supports convergent validity for institutional recognition, with clear limits.

I would also address:

* Incomplete outcome ascertainment: an author without a recorded honor is not necessarily a confirmed nonrecipient.
* Unequal follow-up: older cohorts have had more opportunity to receive senior honors.
* Undated honors: dated future honors should be the primary predictive analysis, with undated cases treated separately.
* Differences in opportunity: where feasible, compare within relevant field and cohort groups.

Two sentences should be changed outright.

On p. 18, the claim that uneven coverage and nomination networks imply that the AUCs “understate how well any indicator
tracks merit” is unsupported. These processes can strengthen or weaken the observed association; the direction is not
established.

On p. 21, the claim that cumulative advantage cannot explain differences between indicators because it affects all of
them is also incorrect. Indicators can capture or amplify the same underlying process to different degrees.

Throughout, I would use “citation impact,” “citation standing,” or “future citation outcomes” for the measured
quantities. The paper can discuss their relationship to research quality without treating the two as interchangeable.

### 7. Uncertainty, data quality, and reproducibility need a more decision-focused treatment

Sections 3, 5.5, 5.6, 5.9, and the availability statement.

There are several related issues here.

The large author count does not automatically make all relevant uncertainty negligible. Authors share publications and
citations, so observations are dependent. Moreover, the most important uncertainty concerns whether the gains persist
across populations and whether an individual’s placement is reliable.

The cohort-by-cohort comparisons are therefore useful. I would supplement them with paired uncertainty estimates for
the principal indicator differences and for the “years earlier” quantities. Precision at the top-decile selection
boundary also needs an explicit tie rule, particularly for the h-index. Fractional inclusion or expected precision
under random tie-breaking would avoid arbitrary choices.

The underlying bibliographic filtering needs a clearer audit. Only 71.5 million of 226 million records enter the
article population, and untyped records are excluded. The paper should show how retention varies by publication year
and field, and clarify whether the excluded letters category contains original research contributions.

The author-matching findings identify a substantial limitation of the main input. The manuscript reports that Semantic
Scholar start years are later than Google Scholar start years for 47% of matched people. This matters directly because
the start year determines the comparison cohort.

The cross-source correlation of 0.78 provides useful agreement evidence, but it does not establish that individual
rankings are accurate or that the predictive advantage replicates under corrected author records. Useful additions
would include top-decile agreement, large rank discrepancies, and sensitivity to plausible changes in the observed start
year.

The historical reconstruction should be described precisely. Restricting publication and citing years prevents direct
use of future publications and citations. It does not establish that the author identities, coverage,
classifications, and records in the 2026 database were available historically. I would describe this as a
retrospective reconstruction using temporally restricted publication and citation information.

One issue I checked is reassuring: the prediction SQL restricts start years to 2010 − age, allowing ten years of
future publications and another five years for their citation measurement. The manuscript should state this
upper-bound rule explicitly. I would treat this as a reporting clarification.

Finally, the public main-branch manuscript I inspected differs from the attached revision. Before submission, archive
the exact code, queries, derived outputs, and manuscript version underlying this PDF, including the newer
early-detection and recognition analyses.

## The theory section could be stronger with a few focused changes

Beyond the discretization issue, I would improve three aspects.

Provide short analytic arguments for the properties. Boundedness, weak monotonicity, and zero contribution below the
cutoff can be established directly under clearly specified fixed reference sets. Numerical checks are useful
implementation checks, but do not establish general mathematical properties.

Treat the factorization as descriptive. Since

$$
Q_\tau(a)=\frac{\mathrm{PiP}_\tau(a)}{F(n_a(0))},
$$

the decomposition is an identity obtained by defining the multiplier as a ratio. Its log-variance decomposition does
not independently establish that 74–82% of the indicator represents research quality. The multiplier is useful for
describing standing relative to productivity standing; its interpretation should remain at that level.

Sharpen the novelty statement. The distinctive element is threshold-specific comparison with career peers. There is
related literature on percentile-based double-rank analysis, including Brito and Rodríguez-Navarro’s work on
institutional and country publication distributions. The published description suggests a different construction, but
a short explicit comparison would help distinguish the contribution.

Tie handling also deserves substantive discussion rather than being treated as a minor implementation choice. Its
implications for percentile-based indicators and field comparability are established in the literature you already
cite.

## Specific corrections before submission

These are relatively straightforward and would prevent avoidable reviewer objections.

| Location | Correction |
|---|---|
| Introduction, line 62 | “Four contributions” is followed by six numbered contributions. |
| Title and recurring descriptions | “All their papers” overstates the default score’s coverage. The cutoff gives no contribution to papers below it. |
| Abstract/introduction | “Every paper … earns credit” suggests strictly positive marginal credit. The proved property is weak monotonicity; adding a qualifying paper need not change any relative standing. |
| Table 4 caption | Raw values and cohort percentiles rank authors identically within a start-year cohort, not across all authors of one career age pooled across start years. Table 8 shows differences between the pooled rankings. |
| Tables 4, 7, and 8 | Label raw versus cohort-percentile PiP consistently. The 2005 values differ across tables because different versions are being reported. |
| Figure 3(d) | “Future top-1% authors” should describe authors producing at least one future top-1% paper. |
| Section 5.4 and conclusion | “As stable … as productivity” is too strong. Table 3 reports 0.563 for PiP versus 0.642 for article count at ages 5 versus 30. State the observed ordering. |
| Section 5.9 | The claim that removing self-citations lowers future-quality prediction by about 0.02 “for every indicator” does not match Table 7. The h-index changes are generally smaller. |
| Field-ranking claims | Correlations of 0.91–0.99 support largely preserved rankings, not an absence of reordering. |
| Conclusion and website description | Clarify that the website implements the original PiP-AUC, as the competing-interests statement says, unless the new construction has been deployed. |
| Author contributions | The section is empty. |

## What I would prioritize before submission

I would concentrate the next revision on five tasks:

1. Resolve the definition–implementation discrepancy and document grid convergence.
2. Repeat the early-detection analysis without citation-based eligibility, while preserving the fixed population and
   future outcome design.
3. Add the cutoff-matched percentile-sum baseline and a simple quantity–impact combination.
4. Bring field normalization into the main framing, with clearer justification of the reference-set choices.
5. Narrow the interpretation of cohort stability, quality, and recognition, and synchronize the exact replication
   release.

The paper’s most defensible positioning is:

> PiP is an interpretable, cohort-relative citation-impact indicator that integrates author standings across
> paper-impact thresholds. In the reported retrospective evaluations, it offers a useful balance between predicting
> future publication volume and citation impact, consistently outperforming the h-index while exhibiting trade-offs
> against other percentile-based indicators.

That is a substantial methodological claim. Establishing it carefully would give QSS reviewers a strong basis for
evaluating the contribution.
