# Literature verifications

store: gs://scholar-version2-literature/sources/
maintained-by: citation-needed skill
refresh-interval: 12 months

<!-- Append-only. Archived copies live in the private bucket above (public access prevention enforced); this repo is
public, so never commit source PDFs here. Entry format: citation-needed references/verification-ledger.md. -->

## [2026-11-05T16:07Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:63
claim: "Bibliometric indicators enter these decisions alongside expert judgment, and the most common ones, citation counts and the $h$-index \citep{hirsch2005index}, are hard to read in this setting."
claim-hash: 8e5cf960ae3b
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "I propose the index h, defined as the number of papers with citation number higher or equal to h, as a useful index to characterize the scientific output of a researcher." (abstract)
notes: Cited as the source of the h-index, which this paper proposes. 'Most common' is the manuscript's own characterization and is not attributed to this source (coordinator override of the worker's 'partially supported', which treated 'most common' as attributed). [coordinator changed the worker's 'partially supported']

## [2026-11-05T16:07Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:82
claim: "At the level of single papers, the field has settled on percentiles as the preferred normalization \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}: a paper is compared with papers of the same publication year, and possibly the same field, by the share of them it out-cites."
claim-hash: 1e67a85063ec
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Percentiles and PR have been established in bibliometrics as an important alternative" (sec. 5 Discussion, first paragraph); "With a reference set it is possible to determine, for example, whether a publication belongs to the 1% or 10% of the most cited" (sec. 2 The calculation of percentiles); "The factors which are commonly considered in bibliometrics to have a significant impact on citations are the subject category/area, the publication year and the document type of a publication." (sec. 1 Using reference sets)
notes: The source supports comparing a paper with a same-year, same-field reference set via percentiles, but describes percentiles as an important alternative to mean-based indicators, not as the field's settled preferred normalization. The paper also stresses unresolved problems (ties, Hazen vs Gringorten formulas, class sizes). Its percentile is a rank within the reference set (share of papers a paper out-cites, inverted in InCites), which fits the 'share it out-cites' phrasing only loosely.

## [2026-11-05T16:07Z] cite:waltman2016review — paper/manuscript_qss/main.tex:82
claim: "At the level of single papers, the field has settled on percentiles as the preferred normalization \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}: a paper is compared with papers of the same publication year, and possibly the same field, by the share of them it out-cites."
claim-hash: 1e67a85063ec
source: doi:10.1016/j.joi.2016.02.007
version-read: preprint (arXiv:1507.02099v3)
archived: gs://scholar-version2-literature/sources/waltman2016review--10.1016_j.joi.2016.02.007-f65cd1b4.pdf sha256:dbfc29bc5a786a45fd14c9a7ab47d7b40949adff0dce04f0a3c540631491f907
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Nowadays, the idea of calculating the proportion of publications that belong to the top 10% most highly cited in their field plays an important role both in the CWTS Leiden Ranking and in the SCImago Institutions Rankings" (sec. 6.2, p. 25); "In the literature, there is no agreement which of the above two normalized variants of the average number of citations per publication is to be preferred." (sec. 6.1, p. 24)
notes: Sec. 6.2 supports that percentile-based (top-10%) classification of papers in field is influential and describes same-field/same-year normalization, but the review presents it as one approach among several alongside mean-based normalization and never says the field has settled on percentiles as the preferred normalization of single papers.

## [2026-11-05T16:07Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:82
claim: "At the level of single papers, the field has settled on percentiles as the preferred normalization \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}: a paper is compared with papers of the same publication year, and possibly the same field, by the share of them it out-cites."
claim-hash: 1e67a85063ec
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Normalized indicators are required, and the most robust normalization method is based on percentiles: each paper is weighted on the basis of the percentile to which it belongs in the citation distribution of its field" (p. 431, principle 6)
notes: Names percentile-based normalization of single papers as the most robust method.

## [2026-11-05T16:07Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:86
claim: "Sums of percentiles, such as I3 \citep{leydesdorff2011integrated}, grow without bound with the number of papers, so many weak papers can outweigh one strong paper."
claim-hash: 10ac1de9b88c
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Using the sum total of the frequencies (f) in each percentile, however, accounts for both the size and shape of the distribution." (p. 6-7, section on percentile rank classes (I3 definition)); "number of publications with less impact matters for the overall impact of two otherwise comparable documents sets." (p. 16-17, results (comparison of I3 with h-index-like top segment))
notes: The source defines I3 as a sum over percentile values and says it accounts for size, so that the number of lower-impact publications matters. It does not state that the sum grows without bound or that many weak papers can outweigh one strong paper; that is the manuscript's own inference from the summation.

## [2026-11-05T16:07Z] cite:waltman2012leiden — paper/manuscript_qss/main.tex:88
claim: "Counts of papers in the top 10\% \citep{waltman2012leiden,bornmann2014individual} use one threshold and discard everything below it."
claim-hash: f390765f78e1
source: doi:10.1002/asi.22708
version-read: preprint (arXiv:1202.3941v1)
archived: gs://scholar-version2-literature/sources/waltman2012leiden--10.1002_asi.22708-420e6186.pdf sha256:463b811dfb698626177a2533bda112c3cc0def73dd694eb2ba606c068d4c54f9
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the PPtop 10% indicator only takes into account whether a publication belongs to the top 10% of its field or not." (sec. on MNCS vs PPtop 10% comparison (Fig. 2 discussion)); "The indicator is insensitive to the exact number of citations of a publication." (same paragraph)
notes: The source defines a top-10% indicator that uses a single cutoff and ignores citation counts beyond it (and below it, publications simply do not count), matching the single-threshold attribution. Note the Leiden indicator is a proportion (PPtop 10%) rather than a raw count, but it is the same top-10% threshold idea.

## [2026-11-05T16:07Z] cite:bornmann2014individual — paper/manuscript_qss/main.tex:88
claim: "Counts of papers in the top 10\% \citep{waltman2012leiden,bornmann2014individual} use one threshold and discard everything below it."
claim-hash: f390765f78e1
source: doi:10.1007/s11192-013-1161-y
version-read: preprint (arXiv:1302.3697v2)
archived: gs://scholar-version2-literature/sources/bornmann2014individual--10.1007_s11192-013-1161-y-22e3c955.pdf sha256:f53354eb37b3efb9676ae2f64101071c8aa1d7ffe88e12df5d581f88ea210334
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Publications which are among the 10% most cited publications in their subject area are as a rule called highly cited or excellent" (sec. 3.2, Ptop 10% paragraph); "Ptop 10% offers another advantage in that it does not use an arbitrary threshold" (sec. 3.2)
notes: The paper proposes the count of top-10% papers (Ptop 10%) as an indicator, defined by the 10% most-cited cutoff, so it is a single-threshold count. It argues this threshold is not arbitrary; that does not contradict the manuscript's description of it as one threshold. The paper does not say the count discards papers below the threshold, but that follows from the definition.

## [2026-11-05T16:07Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:101
claim: "We compute PiP from the Semantic Scholar Academic Graph \citep{kinney2023semantic} for 11.1 million authors at every career age from 1 to 30, reconstructing peers and percentiles in each evaluation year from the publications and citations up to that year."
claim-hash: df6594505e99
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "form is to build and distribute the Semantic Scholar" (sec. 2, Platform Overview (continues 'Academic Graph, or S2AG')); "Download links to monthly snapshots of our knowl" (sec. 4.2, Datasets)
notes: The paper defines the Semantic Scholar Academic Graph (S2AG) and describes its distribution as downloadable datasets, so it is a valid description of the data source.

## [2026-11-05T16:07Z] cite:tian2021predictability — paper/manuscript_qss/main.tex:132
claim: "An earlier version of this work \citep{tian2021predictability} studied the predictability of paper and scholar percentiles on a smaller dataset of faculty at top US universities and rated scholars by the sum of their paper percentiles."
claim-hash: 68c943e804c7
source: arXiv:2103.16025 (DataCite doi:10.48550/arXiv.2103.16025)
version-read: preprint (arXiv:2103.16025v3)
archived: gs://scholar-version2-literature/sources/tian2021predictability--on_the_predictability_of_utilizing_rank_percentile_to_evalua-681c8251.pdf sha256:b937c4c4ca0316809d0d9133cf4a82700a26d04511b1788436d7edee67ad64ed
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "The dataset utilized for this study is from Google Scholar and includes active faculty members (assistant, associate, and full professors) in multiple disciplines from the top 10 universities in the United States, which totals 14, 358 scholars." (p. 2, Dataset); "The publication percentile is highly stable over time, while the scholar percentile exhibits short-term stability and can be predicted via a simple linear regression model." (p. 1, Abstract); "The evaluation metric for scholar i is determined by aggregating the performance of all N(t)" (p. 3, The Rank Percentile Indicator for Scholars); "that is ∑ Pcj (5)" (p. 3, The Rank Percentile Indicator for Scholars)
notes: The paper studies predictability of paper and scholar percentiles on Google Scholar faculty from top 10 US universities (14,358 scholars), and its scholar indicator S^P5 sums paper rank percentiles. Whether the dataset is 'smaller' depends on the manuscript's own dataset, which this source cannot speak to.

## [2026-11-05T16:07Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:138
claim: "A first, asymmetric form of the score was described by \citet{ipeirotis2024pip} and is used by the website \url{https://www.pip-score.org}; Section~\ref{sec:pip} explains why we replace it."
claim-hash: 4f2d22b52b14
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: partially supported
evidence: "we have a nice new score that we have given the thoroughly boring name "Percentile in Percentile Area Under the Curve" score, or PiP-AUC for short" (post body, paragraph introducing the score); "Instead of showing the raw number of papers on the x-axis, we normalize paper productivity against the percentile benchmark shown above." (post body, x-axis normalization paragraph)
notes: The post does describe the PiP-AUC score (citation percentiles on y, productivity-percentile-normalized x), supporting 'described by Ipeirotis (2024)'. It never calls the form asymmetric and never mentions pip-score.org; it points to the app at scholar.ipeirotis.org, so 'used by pip-score.org' is not addressed (asymmetry is the manuscript's own characterisation).

## [2026-11-05T16:07Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:144
claim: "The $h$-index \citep{hirsch2005index} combines productivity and impact in one number and has been studied, criticized and modified at length."
claim-hash: 8837a706d201
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "to propose a single number, the" (p. 1, introduction); "Instead, the proposed h-index measures the broad impact of an individual" (p. 1, after the list of criteria)
notes: Hirsch proposes h as a single number measuring broad impact and lists paper counts as the productivity criterion it improves on; he does not say h combines productivity and impact. The paraphrase is an inference from the definition.

## [2026-11-05T16:07Z] cite:egghe2006theory — paper/manuscript_qss/main.tex:145
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by academic age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 71e92edb885f
source: doi:10.1007/s11192-006-0144-7
version-read: none
archived: none
license: n/a — nothing archived
updates: none
verdict: unverifiable
evidence: none — No text or abstract reachable; the legal Hasselt copy was down (503).
notes: No text or abstract reachable; the legal Hasselt copy was down (503). retry Hasselt green OA copy (hdl.handle.net/1942/981) or supply the PDF

## [2026-11-05T16:07Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:145
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by academic age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 71e92edb885f
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "of h versus n, the parameter m, should provide a useful yardstick to compare scientists of different seniority." (p. 3, after Eq. (6))
notes: m is the slope of h versus career length n (h ~ mn), proposed to compare scientists of different seniority. Hirsch calls it the m parameter; 'm-quotient' is later usage.

## [2026-11-05T16:07Z] cite:sidiropoulos2007generalized — paper/manuscript_qss/main.tex:145
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by academic age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 71e92edb885f
source: doi:10.1007/s11192-007-1722-z
version-read: preprint (arXiv:cs/0607066v1)
archived: gs://scholar-version2-literature/sources/sidiropoulos2007generalized--10.1007_s11192-007-1722-z-93dc68b1.pdf sha256:926776aa9e2f6d8a4f93cf91421b1c18070eba8be4192c44955d37ef2b6de9dc
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "This way, an old article gradually loses its “value”, even if it still gets citations." (p. 4, The contemporary h-index (after Eq. 2)); "If we set δ=1, then S c (i) is the number of citations that the article i has received, divided by the “age” of the article." (p. 4, The contemporary h-index (after Eq. 2))
notes: The contemporary h-index scores each article by citations discounted by article age, so old papers lose weight. The source frames the motivation as recency and inactivity, and as distinguishing senior from young scientists, rather than as an explicit 'career length' correction; this is a minor framing difference and the mechanism attributed matches.

## [2026-11-05T16:07Z] cite:harzing2014hia — paper/manuscript_qss/main.tex:145
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by academic age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 71e92edb885f
source: doi:10.1007/s11192-013-1208-0
version-read: accepted manuscript (author version, December 2013)
archived: gs://scholar-version2-literature/sources/harzing2014hia--10.1007_s11192-013-1208-0-17a20b4c.pdf sha256:c9490f3f542009fd086dc526c1de10836280b53afbf58252d29aafc5cdd44f1d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "We therefore follow Hirsch’s original suggestion to correct for career length by dividing the h‐index by the number of years an academic has been publishing." (p. 5); "the hIa‐index is thus calculated by dividing Harzing’s Publish or Perish hI‐norm (Harzing, 2007) by the number of years that an academic has been publishing" (p. 5)
notes: hIa divides an author-normalized h-index by years publishing, i.e. by academic age.

## [2026-11-05T16:07Z] cite:kaur2013universality — paper/manuscript_qss/main.tex:145
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by academic age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 71e92edb885f
source: doi:10.1016/j.joi.2013.09.002
version-read: preprint (arXiv:1305.6339v2)
archived: gs://scholar-version2-literature/sources/kaur2013universality--10.1016_j.joi.2013.09.002-74fa8e45.pdf sha256:784a714cf7633ff9623c1b53b56491fb53fa5b7d0ab70b1592b7b50a29317267
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "hs index is proposed here as a normalization of the h index by the average h of the authors in the same discipline." (sec. 2, metric definitions (hs index))
notes: Kaur et al. define the hs index as the h index divided by the average h of authors in the same discipline, i.e. a rescaling by the field average, as the manuscript states.

## [2026-11-05T16:07Z] cite:bornmann2011metaanalysis — paper/manuscript_qss/main.tex:149
claim: "Most variants correlate highly with the original \citep{bornmann2011metaanalysis}."
claim-hash: cb0ee89f8235
source: doi:10.1016/j.joi.2011.01.006
version-read: none
archived: none
license: n/a — nothing archived
updates: none
verdict: unverifiable
evidence: none — No full text or abstract legally reachable; secondary snippets not used.
notes: No full text or abstract legally reachable; secondary snippets not used. paywalled: supply the PDF (J. Informetrics 5(3):346-359, doi:10.1016/j.joi.2011.01.006)

## [2026-11-05T16:07Z] cite:waltman2012inconsistency — paper/manuscript_qss/main.tex:152
claim: "The $h$-index is also inconsistent: adding the same paper to two scholars can reverse their order \citep{waltman2012inconsistency}."
claim-hash: eaf1fd89bfc1
source: doi:10.1002/asi.21678
version-read: preprint (arXiv:1108.3901v1)
archived: gs://scholar-version2-literature/sources/waltman2012inconsistency--10.1002_asi.21678-42823f3e.pdf sha256:8afcd8681c22f8267eb5442a4ca26783f88c7206b05a96133294a97a369198ca
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Suppose next that the two scientists jointly produce two new publications. These publications each receive eight citations. This does not change the h-index of scientist X. However, the h-index of scientist Y increases from four to six" (sec. 3.2, Example 2, p. 7); "This means that the ranking of the two scientists in terms of overall impact has reversed." (sec. 3.2, Example 2, p. 7); "our conclusion therefore is that the h-index has provided inconsistent rankings of scientists X and Y." (sec. 3.2, Example 2, p. 7)
notes: Example 2 shows exactly that adding the same (joint) publications to two scientists reverses their h-index order, and the authors call the h-index inconsistent on that basis.

## [2026-11-05T16:07Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:155
claim: "Percentile-based indicators compare each paper with a reference set of papers from the same year and field \citep{bornmann2013use,leydesdorff2011turning,waltman2013percentile,bornmann2020evaluation}."
claim-hash: d27f5b663419
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Each publication by the four universities is assigned to a percentile corresponding to its reference set (after normalization for the respective subject category, publication year, and document type)." (sec. 2, paragraph introducing Figure 1); "The factors which are commonly considered in bibliometrics to have a significant impact on citations are the subject category/area, the publication year and the document type of a publication." (sec. 1 Using reference sets)
notes: The source describes percentile indicators as comparing each publication with a reference set defined by subject category and publication year (plus document type), which matches the claim as written; document type is an extra criterion the manuscript omits but does not contradict.

## [2026-11-05T16:07Z] cite:leydesdorff2011turning — paper/manuscript_qss/main.tex:155
claim: "Percentile-based indicators compare each paper with a reference set of papers from the same year and field \citep{bornmann2013use,leydesdorff2011turning,waltman2013percentile,bornmann2020evaluation}."
claim-hash: d27f5b663419
source: doi:10.1002/asi.21534
version-read: preprint (arXiv:1101.3863v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011turning--10.1002_asi.21534-ac5b1ce1.pdf sha256:b1a31ad0ca4bf8d0011741c5ca72e2fd794a9dfc37c7a30815d8459d68af291d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "With percentile ranks, the citation of each paper is rated in terms of its percentile in the citation distribution." (abstract); "Our scheme requires one normalization or another against a reference set for each paper" (Methods, reference-set paragraph); "the choice of using journals or fields for the normalization matters" (Methods, reference-set paragraph); "against the set of all papers in the same journal in the same year and of the same document type" (Discussion, p. 14)
notes: The paper defines percentile indicators as rating each paper against a reference set of papers from the same publication year. Field is named as one admissible reference set alongside journals, but the worked example uses same journal, year and document type, so the field part is a general option rather than the paper's own implementation.

## [2026-11-05T16:07Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:155
claim: "Percentile-based indicators compare each paper with a reference set of papers from the same year and field \citep{bornmann2013use,leydesdorff2011turning,waltman2013percentile,bornmann2020evaluation}."
claim-hash: d27f5b663419
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "A percentile-based bibliometric indicator is an indicator that values publications based on their position within the citation distribution of their field." (abstract); "we collected all publications of the document type article that appeared in 1999" (sec. 2, Some empirical context)
notes: The source supports comparison against the citation distribution of the paper's field; it does not state that the reference set is restricted to papers of the same year (the 1999 restriction appears only in its empirical illustration), so the same-year part is not addressed as a general definition.

## [2026-11-05T16:07Z] cite:bornmann2020evaluation — paper/manuscript_qss/main.tex:155
claim: "Percentile-based indicators compare each paper with a reference set of papers from the same year and field \citep{bornmann2013use,leydesdorff2011turning,waltman2013percentile,bornmann2020evaluation}."
claim-hash: d27f5b663419
source: doi:10.1007/s11192-020-03512-7
version-read: version of record
archived: gs://scholar-version2-literature/sources/bornmann2020evaluation--10.1007_s11192-020-03512-7-1f24fb42.pdf sha256:3ea8c359d01836ba0507727a744a394ae2b0778cfc91682e8bafe7b5676bc29e
license: CC-BY-4.0 (Springer open access)
updates: none
verdict: supported
evidence: "if a focal paper has a PR of 90, then 90% of the papers in the publication year and subject category have a citation impact which is (at or) below the impact of the focal paper." (p. 1458, Introduction); "Suppose various publication sets from a database that each contain all papers published in a specific combination of publication year and subject category." (p. 1458, Introduction)
notes: The paper defines percentile ranks as a paper's standing relative to all papers from the same publication year and subject category (field), matching the attributed claim.

## [2026-11-05T16:07Z] cite:schubert1986relative — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers than mean-based normalization \citep{schubert1986relative,Radicchi2008,waltman2019field} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d12175b7d721
source: doi:10.1007/bf02017249
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/schubert1986relative--10.1007_bf02017249-0d8903d0--abstract--2026-11-05-26c0d74a.html sha256:26c0d74a3353e9680a72faffcf7efd140021c30bfc6d95ac61256fdc19e6c693
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: unverifiable
evidence: none — Only the abstract was reachable; it describes relative indicators and relational charts for cross-field comparison and does not state or contradict that percentiles are less sensitive to outliers than mean-based normalization.
notes: Only the abstract was reachable; it describes relative indicators and relational charts for cross-field comparison and does not state or contradict that percentiles are less sensitive to outliers than mean-based normalization. paywalled: please supply the PDF of Schubert & Braun 1986 (Scientometrics 9(5-6):281-291) if this claim is to be checked

## [2026-11-05T16:07Z] cite:Radicchi2008 — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers than mean-based normalization \citep{schubert1986relative,Radicchi2008,waltman2019field} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d12175b7d721
source: doi:10.1073/pnas.0806977105
version-read: preprint (arXiv:0806.0974v2)
archived: gs://scholar-version2-literature/sources/radicchi2008--10.1073_pnas.0806977105-fa509e28.pdf sha256:6dde217e74a426a0d1f6d900d74f0fce3452ab8c500260979751d86d06d7c5a0
license: unknown (treated as restricted; private store)
updates: none
verdict: unsupported
evidence: "For the normalization of the relative indicator," (p. 2, sec. on universal scaling (two-column layout interleaves this sentence)); "we have considered the average number c0 of citations" (p. 2, continuation); "as an unbiased relative indicator of scientific impact" (p. 1, introduction (cf = c/c0)); "to conclude that cf is an unbiased indicator for" (p. 3, after Figs. 2-3)
notes: The paper proposes and endorses mean-based normalization (cf = c/c0) as unbiased across fields; the full text contains no discussion of outliers, robustness, or percentiles being less sensitive than mean-based normalization, so it does not carry this attribution (it argues for the mean-based approach). [worker quotes spanned interleaved columns; replaced with verbatim fragments carrying the same content]

## [2026-11-05T16:07Z] cite:waltman2019field — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers than mean-based normalization \citep{schubert1986relative,Radicchi2008,waltman2019field} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d12175b7d721
source: doi:10.1007/978-3-030-02511-3_11
version-read: preprint (arXiv:1801.09985v1)
archived: gs://scholar-version2-literature/sources/waltman2019field--10.1007_978-3-030-02511-3_11-357ae0ed.pdf sha256:af5d1381889500d4cb5876fa62907d470b34613ed4b7db6a647e6a3d252aa77a
license: unknown (treated as restricted; private store)
updates: none
verdict: unsupported
evidence: "Percentile-based impact indicators value publications based on their position in the citation distribution of their field and publication year" (sec. 4.2, Indicators based on percentiles)
notes: Section 4.2 describes percentile-based indicators (top-10% classes, ties) but nowhere says they are less sensitive to outliers than mean-based normalization; a search of the chapter found no discussion of outliers or robustness.

## [2026-11-05T16:07Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers than mean-based normalization \citep{schubert1986relative,Radicchi2008,waltman2019field} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d12175b7d721
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the most robust normalization method is based on percentiles" (p. 431, principle 6); "A single highly cited publication slightly improves the position of a university in a ranking that" (p. 431, principle 6 (sentence continues after a column break)); "is based on percentile indicators, but may propel the university from the middle to the top of a ranking built on citation averages" (p. 431, principle 6, continuation)
notes: Recommends percentile-based normalization; its example also bears on outlier robustness. [worker quote spanned a column break; split into the two verified halves]

## [2026-11-05T16:07Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:158
claim: "Ties and the definition of the percentile need care \citep{waltman2013percentile}; we use the share of the reference set with strictly fewer citations for scoring, so that uncited papers have percentile zero, and Hazen's mid-rank \citep{hazen1914storage} for correlations (Section~\ref{sec:properties} discusses the choice)."
claim-hash: 7943a4ac5286
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "A difficulty in the calculation of percentile-based indicators is caused by the discrete nature of citation distributions combined with the presence of many publications with the same number of citations." (abstract); "Usually, there are multiple publications in a field that have the same number of citations. The presence of such ‘ties’ may aggravate the above problem." (sec. 1, Introduction)
notes: The whole paper is about how ties and the discrete citation distribution complicate the definition of percentile-based indicators, which matches the attribution.

## [2026-11-05T16:07Z] cite:hazen1914storage — paper/manuscript_qss/main.tex:158
claim: "Ties and the definition of the percentile need care \citep{waltman2013percentile}; we use the share of the reference set with strictly fewer citations for scoring, so that uncited papers have percentile zero, and Hazen's mid-rank \citep{hazen1914storage} for correlations (Section~\ref{sec:properties} discusses the choice)."
claim-hash: 7943a4ac5286
source: doi:10.1061/taceat.0002563
version-read: version of record (scan of Trans. ASCE vol. 77, 1914; pages 1629-1740 of the archive.org volume scan, extracted)
archived: gs://scholar-version2-literature/sources/hazen1914storage--10.1061_taceat.0002563-1fbb5981.pdf sha256:e80f3c2b37f161d8ac843c6c0e1a78db86343bb4e08fff8db8f9324db8dfc5de
license: public domain (1914 US publication)
updates: none
verdict: supported
evidence: "the figure for each term, arranged in its order of magnitude, is plotted at the percentage which corresponds to the middle of its strip" (printed p. 1549, 'Probability Paper'); "The first term will be plotted at the middle of the first 2% strip; that is, on the 1% line" (printed p. 1549)
notes: Hazen places the m-th of n ordered values at the middle of its 1/n strip, i.e. (2m-1)/2n: the mid-rank plotting position. He calls it a plotting position on probability paper, not a percentile.

## [2026-11-05T16:07Z] cite:bornmann2014individual — paper/manuscript_qss/main.tex:160
claim: "At the level of a scholar, \citet{bornmann2014individual} propose counting papers in percentile classes and normalizing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 655c46aee2e9
source: doi:10.1007/s11192-013-1161-y
version-read: preprint (arXiv:1302.3697v2)
archived: gs://scholar-version2-literature/sources/bornmann2014individual--10.1007_s11192-013-1161-y-22e3c955.pdf sha256:f53354eb37b3efb9676ae2f64101071c8aa1d7ffe88e12df5d581f88ea210334
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "we would like to propose using the number of years as an active researcher (Ptop 10% quotient) to normalise Ptop 10% for age" (sec. 3.2, Ptop 10% paragraphs)
notes: The paper proposes counting papers in the top-10% percentile class (Ptop 10%) and normalising by years as an active researcher, which matches the age-normalisation part. It uses a single class (top 10%), plus average percentile as a separate indicator; I found no counting of papers across several percentile classes, so 'percentile classes' (plural) overstates it slightly.

## [2026-11-05T16:07Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:160
claim: "At the level of a scholar, \citet{bornmann2014individual} propose counting papers in percentile classes and normalizing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 655c46aee2e9
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "One can also consider hundred percentiles as a continuous random variable and sum these values" (p. 5-6, section on percentile ranks); "The citations are weighted in accordance with the percentile" (p. 7, I3 definition (formula I3 = sum_i x_i * f(x_i)))
notes: I3 is defined as the sum over percentile ranks of the documents in a set (the frequency-weighted sum of the percentile values). The source applies it to document sets such as journals and nations; the manuscript's use for a scholar's set of papers is a straightforward extension (the paper itself mentions individuals).

## [2026-11-05T16:07Z] cite:rousseau2012basic — paper/manuscript_qss/main.tex:160
claim: "At the level of a scholar, \citet{bornmann2014individual} propose counting papers in percentile classes and normalizing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 655c46aee2e9
source: doi:10.1002/asi.21684
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/rousseau2012basic--10.1002_asi.21684-660d9303--abstract--2026-11-05-c17f011c.json sha256:c17f011c6bfe3e3e0bd0544abf7df6ba45c0d6aab62927dedf6ecd5754fb2bf4
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: unverifiable
evidence: "the Integrated Impact Indicator (I3), introduced by Leydesdorff and Bornmann ( 2011 ), is a strictly congruous indicator of absolute performance" (abstract)
notes: Only the abstract was reachable; it discusses I3 as an indicator of absolute performance but neither states nor contradicts that I3 sums percentiles over a set of papers, so the claim cannot be judged. paywalled (Wiley, no open copy found): please supply the PDF to verify C15

## [2026-11-05T16:07Z] cite:ioannidis2019standardized — paper/manuscript_qss/main.tex:163
claim: "Composite indicators such as the one of \citet{ioannidis2019standardized} combine several citation counts, and \citet{abramo2014productivity} weight field-normalized citations by inputs."
claim-hash: 1693ade30422
source: doi:10.1371/journal.pbio.3000384
version-read: version of record
archived: gs://scholar-version2-literature/sources/ioannidis2019standardized--10.1371_journal.pbio.3000384-89de78a2.xml sha256:68b36d5eddd9616ed488af0c2dd001e879b5a380d2d6059023cd6c338ff901a8
license: CC-BY-4.0 (PLOS)
updates: new_version doi:10.1371/journal.pbio.3000384 (2019-08-19, publisher)
verdict: supported
evidence: "composite indicator that considers six citation metrics (total citations; Hirsch h-index; coauthorship-adjusted Schreiber hm-index; number of citations to papers as single author; number of citations to papers as single or first author; and number of citations to papers as single, first, or last author)" (Main text, database construction paragraph)
notes: The source defines its composite indicator as combining six citation-based metrics, which matches the attribution that it combines several citation counts.

## [2026-11-05T16:07Z] cite:abramo2014productivity — paper/manuscript_qss/main.tex:163
claim: "Composite indicators such as the one of \citet{ioannidis2019standardized} combine several citation counts, and \citet{abramo2014productivity} weight field-normalized citations by inputs."
claim-hash: 1693ade30422
source: doi:10.1007/s11192-014-1269-8
version-read: preprint (arXiv:1810.12830v1)
archived: gs://scholar-version2-literature/sources/abramo2014productivity--10.1007_s11192-014-1269-8-95b9efa5.pdf sha256:04b35f0064c50e7c47e8a5d72e4caff628f477c4fc8658039293823fb32e5f37
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we measure FSSR, a proxy of the average yearly productivity over a period of time, accounting for the cost of labor." (sec. Labor productivity at the individual level, p. 7); "To measure the cost efficiency or research units, one should normalize its output by the cost of labor." (sec. Labor productivity in research activity and the FSS, p. 5-6); "average of the distribution of citations received for all cited publications" (definition of c-bar in Eq. [1], p. 7)
notes: FSS (Eq. 1) sums each paper's citations divided by the mean citations of same-year, same-subject-category papers (field normalization), with fractional authorship weights, and divides by the researcher's salary (cost of labor) and years worked, i.e. normalizes to inputs. The manuscript's 'weight ... by inputs' is loose wording for dividing by labor cost, but the attribution is accurate.

## [2026-11-05T16:07Z] cite:rodriguez2018double — paper/manuscript_qss/main.tex:166
claim: "For countries and institutions, double rank analysis gives each of a unit's papers a rank within the unit and a rank among all papers in the world, and reads the unit's output at very high percentiles from the relation between the two \citep{rodriguez2018double,brito2018percentile}."
claim-hash: de03b22ec35b
source: doi:10.1016/j.joi.2017.11.004
version-read: none
archived: none
license: n/a — nothing archived
updates: none
verdict: unverifiable
evidence: none — No legal full text or abstract of this closed-access paper was reachable, so the attribution (rank within the unit plus world rank, reading very high percentiles from their relation) could not be checked against its text.
notes: No legal full text or abstract of this closed-access paper was reachable, so the attribution (rank within the unit plus world rank, reading very high percentiles from their relation) could not be checked against its text. paywalled: please supply the PDF of Rodriguez-Navarro & Brito, J. Informetrics 12(1) 31-41 (doi 10.1016/j.joi.2017.11.004), or its abstract

## [2026-11-05T16:07Z] cite:brito2018percentile — paper/manuscript_qss/main.tex:166
claim: "For countries and institutions, double rank analysis gives each of a unit's papers a rank within the unit and a rank among all papers in the world, and reads the unit's output at very high percentiles from the relation between the two \citep{rodriguez2018double,brito2018percentile}."
claim-hash: de03b22ec35b
source: doi:10.1016/j.joi.2018.01.011
version-read: preprint (arXiv:1712.00399v2)
archived: gs://scholar-version2-literature/sources/brito2018percentile--10.1016_j.joi.2018.01.011-2027d857.pdf sha256:7ebab94bcec8cf9f7d71333a6783dadd0107ff16314c4b89a211681f1d8508ba
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "In the double rank analysis of research publications, the local rank position of a country or institution publication is expressed as a function of the world rank position." (Abstract); "the ranking number of a publication in a country or institution is expressed as a function of its world ranking number. The resulting function can be well fitted by a power law, which allows estimating the likelihood of any country or institution producing a very highly cited paper" (Sec. 1, Introduction)
notes: The source defines double rank analysis as giving each country or institution paper a local rank as a function of its world rank, and uses the resulting power-law relation to estimate output at very high citation levels. This matches the attribution; the percentile version in this paper is a refinement but the sentence describes the general method.

## [2026-11-05T16:07Z] cite:hirsch2007does — paper/manuscript_qss/main.tex:171
claim: "\citet{hirsch2007does} found that the $h$-index predicts a scholar's later $h$-index and citations better than counts of papers or citations do."
claim-hash: 3e0aad428bd5
source: doi:10.1073/pnas.0707962104
version-read: preprint (arXiv:0708.0646v2)
archived: gs://scholar-version2-literature/sources/hirsch2007does--10.1073_pnas.0707962104-c362526c.pdf sha256:a56674327d7e8c6fb528c1518969af379c2deb402e21fd217d4e120b74428285
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Our findings indicate that the h-index is better than other indicators considered (total citation count, citations per paper, and total paper count) in predicting future scientific achievement." (Abstract)
notes: The abstract (echoed in Sec. VI) states that the h-index predicts future achievement (its own later value or later total citations) better than citation count, paper count and mean citations per paper. Caveat: the advantage over total citations is small (Sec. V: 'a small consistent advantage of the h-index compared to Nc') and is lost for cumulative citations (Fig. 2 vs 4); the sentence's wording does not overstate this.

## [2026-11-05T16:07Z] cite:acuna2012future — paper/manuscript_qss/main.tex:173
claim: "Later work predicted future $h$-index values from features of a career \citep{acuna2012future}, questioned how much of that predictability is mechanical, since the future $h$-index contains the current one \citep{penner2013predictability}, and modeled impact as a stable individual parameter applied to papers of random quality \citep{sinatra2016quantifying}; \citet{clauset2017datadriven} review the area."
claim-hash: c12500b400bc
source: doi:10.1038/489201a
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/acuna2012future--10.1038_489201a-fecad52c--abstract--2026-11-05-16b7e5fc.json sha256:16b7e5fc7b3345de7571bf808d2abcc200b80f2ee40021e88e7d1c8b270580c9
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: unverifiable
evidence: "present a formula to estimate the future h-index of life scientists" (abstract (Nature standfirst / S2 abstract))
notes: Only the one-sentence standfirst was reachable; it is consistent with predicting future h-index but does not state the 'features of a career' part, and the body text that would carry it is paywalled. paywalled: please supply the PDF of Nature 489:201-202 if a body-text check is wanted

## [2026-11-05T16:07Z] cite:penner2013predictability — paper/manuscript_qss/main.tex:173
claim: "Later work predicted future $h$-index values from features of a career \citep{acuna2012future}, questioned how much of that predictability is mechanical, since the future $h$-index contains the current one \citep{penner2013predictability}, and modeled impact as a stable individual parameter applied to papers of random quality \citep{sinatra2016quantifying}; \citet{clauset2017datadriven} review the area."
claim-hash: c12500b400bc
source: doi:10.1038/srep03052
version-read: version of record
archived: gs://scholar-version2-literature/sources/penner2013predictability--10.1038_srep03052-5af70d94.xml sha256:0ec5949cf8c9968d00edb312c484c4008ff9fb57f89ce2fa9fb1a3d89800c0a4
license: CC-BY-NC-SA-3.0 (Scientific Reports)
updates: none
verdict: supported
evidence: "cumulative non-decreasing measures like the h -index contain intrinsic autocorrelation, resulting in significant overestimation of their “predictive power”" (Abstract); "linear regression models are not so much predicting future impact as they are picking up on a correlation intrinsic to cumulative measures" (Results, 'Modeling non-cumulative measures')
notes: Penner et al. argue that regression models of future h-index overstate predictive power because h is cumulative and non-decreasing, so the future value mechanically contains the current one; this matches the attribution.

## [2026-11-05T16:07Z] cite:sinatra2016quantifying — paper/manuscript_qss/main.tex:173
claim: "Later work predicted future $h$-index values from features of a career \citep{acuna2012future}, questioned how much of that predictability is mechanical, since the future $h$-index contains the current one \citep{penner2013predictability}, and modeled impact as a stable individual parameter applied to papers of random quality \citep{sinatra2016quantifying}; \citet{clauset2017datadriven} review the area."
claim-hash: c12500b400bc
source: doi:10.1126/science.aaf5239
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/sinatra2016quantifying--10.1126_science.aaf5239-d62f25d9--abstract--2026-11-05-3225445d.json sha256:3225445df027c1a52b24c06e7e83c6b979dfed83e018047b06d5bf26a3124be0
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: unverifiable
evidence: "The authors developed a quantitative model of impact, based on an element of randomness, productivity, and a factor Q that is particular to each scientist and remains constant during the scientist's career." (Crossref abstract (Science editor's summary))
notes: Only the abstract was reachable. It supports the 'stable individual parameter' half (Q constant over a career) and mentions an element of randomness, but it does not state that papers are of random quality (the paper-level random factor); the full text that would carry that detail was not reached, so no firmer verdict is possible. Full text not legally reachable from this environment: please supply the PDF of Sinatra et al. 2016 (Science 354:aaf5239) if a verdict on 'papers of random quality' (the random p factor in the Q-model) is needed.

## [2026-11-05T16:07Z] cite:clauset2017datadriven — paper/manuscript_qss/main.tex:173
claim: "Later work predicted future $h$-index values from features of a career \citep{acuna2012future}, questioned how much of that predictability is mechanical, since the future $h$-index contains the current one \citep{penner2013predictability}, and modeled impact as a stable individual parameter applied to papers of random quality \citep{sinatra2016quantifying}; \citet{clauset2017datadriven} review the area."
claim-hash: c12500b400bc
source: doi:10.1126/science.aal4217
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/clauset2017datadriven--10.1126_science.aal4217-d398d626--abstract--2026-11-05-38148b09.json sha256:38148b0952091e52d3f159586b431d815156e4ffc442364283c0f9c9e23bc401
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: unverifiable
evidence: "In this Essay, we survey the emerging and interdisciplinary field of the “science of science” and what it teaches us about the predictability of scientific discovery." (abstract)
notes: Only the abstract was reachable. It describes a survey of the science of science and the predictability of scientific discovery, which is consistent with 'review the area' but does not itself say the essay covers predicting scholarly impact or the h-index; the full text was not reached. No legal open full text found: please supply the PDF of Clauset, Larremore and Sinatra, Science 355:477-480 (2017) if a full-text check is wanted.

## [2026-11-05T16:07Z] cite:wang2013window — paper/manuscript_qss/main.tex:179
claim: "How long a paper needs before its citations reflect its impact depends on the field \citep{wang2013window,abramo2011window}."
claim-hash: 45e823b00572
source: doi:10.1007/s11192-012-0775-9
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/wang2013window--10.1007_s11192-012-0775-9-75debdfb--abstract--2026-11-05-f17b6d3a.html sha256:f17b6d3ab6d0670af9ce40602a21905818e18caac9745fb0a33d0d808ade16d9
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: supported
evidence: "There are significant differences in citation ageing between different research fields, document types, total citation counts, and publication months." (abstract)
notes: The abstract states that citation ageing differs significantly between research fields, which is what the sentence attributes to this source (the window needed depends on field); the abstract also says within-group differences are more striking, a caveat not in the manuscript sentence. Judged from the abstract only.

## [2026-11-05T16:07Z] cite:abramo2011window — paper/manuscript_qss/main.tex:179
claim: "How long a paper needs before its citations reflect its impact depends on the field \citep{wang2013window,abramo2011window}."
claim-hash: 45e823b00572
source: doi:10.1016/j.joi.2011.06.004
version-read: preprint (arXiv:1811.01705v1)
archived: gs://scholar-version2-literature/sources/abramo2011window--10.1016_j.joi.2011.06.004-41cc2290.pdf sha256:04250c83863be14b590bc141f178dd00300a98f320b4f4668fd51f2d15cfe5cd
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Apart from Mathematics then, given the data, we argue that the accuracy in measurement of publication impact falls within acceptable levels for citation windows of two years for the first cluster of disciplines noted and three years for the second group." (p. 12-13, sec. 4 Conclusions); "The error is notably less for all the other disciplines. Convergence arrives quickest in Physics, followed by Biomedical research and Engineering." (sec. 3.3, text after Table 4)
notes: The study finds that the citation window needed for reliable impact measurement differs by discipline (2 years for one cluster, 3 for another, with Mathematics slower), which is what the sentence attributes to it.

## [2026-11-05T16:07Z] cite:merton1968matthew — paper/manuscript_qss/main.tex:180
claim: "Citations also feed on themselves: well-known scholars receive more attention for the same work \citep{merton1968matthew,petersen2014reputation}, which affects any citation-based indicator and any test of its predictive validity."
claim-hash: 764823a51cf0
source: doi:10.1126/science.159.3810.56
version-read: version of record (scan of Science 159(3810):56-63, 1968; OCR text)
archived: gs://scholar-version2-literature/sources/merton1968matthew--10.1126_science.159.3810.56-0577b48e.pdf sha256:95fc1cb4a84cf1c563b8085c0c8adca9f44c0162ae343368c688c1f5b64e5209
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "entist may have greater stimuius vaiue" (p. 61 (OCR of 'cognitive material presented by an outstanding scientist may have greater stimulus value')); "than roughly the same kind of mater" (p. 61, next line); "ial presented by an obscure one-a" (p. 61)
notes: Material from an outstanding scientist gets more notice than roughly the same material from an obscure one.

## [2026-11-05T16:07Z] cite:petersen2014reputation — paper/manuscript_qss/main.tex:180
claim: "Citations also feed on themselves: well-known scholars receive more attention for the same work \citep{merton1968matthew,petersen2014reputation}, which affects any citation-based indicator and any test of its predictive validity."
claim-hash: 764823a51cf0
source: doi:10.1073/pnas.1323111111
version-read: preprint (arXiv:1303.7274v4)
archived: gs://scholar-version2-literature/sources/petersen2014reputation--10.1073_pnas.1323111111-aab695c0.pdf sha256:e9a2fedc3721a39f5ec0a686fb3d15f9ebaf99579c5bc1b844dce252f36e2205
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "a new publication may gain a significant early advantage corresponding to roughly a 66% increase in the citation rate for each tenfold increase in Ci" (Abstract)
notes: The paper shows an author's reputation (total citations) raises the citation rate of that author's new publications, all else equal. The effect is concentrated in publications below a citation crossover and becomes negligible for highly cited ones, so 'more attention for the same work' holds mainly for young or low-cited papers.

## [2026-11-05T16:07Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:186
claim: "We use the Semantic Scholar Academic Graph \citep{kinney2023semantic}, release of 27 October 2026, which is distributed under the ODC-BY license."
claim-hash: 4dae5e8290d8
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: unsupported
evidence: "obtain an authentication key, at no charge, subject to" (sec. 4.1, APIs (followed by 'terms of use'))
notes: I read the sections on APIs and datasets (4, 4.2) and searched the full text: the paper never mentions ODC-BY or any named license for S2AG, saying only that full datasets require a free key subject to terms of use. The 27 October 2026 release is also not covered, since the paper predates it.

## [2026-11-05T16:07Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:328
claim: "Mid-ranks, which count half of the tied peers, are a common choice for the percentile of a single paper \citep{waltman2013percentile}, but here they would give credit to authors with nothing above a threshold: at high thresholds most peers have no article, and an author tied with them at zero would stand at half their share."
claim-hash: 8be462dc0505
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Pudovkin and Garfield put the tied publications in a random order and calculate their average percentile." (sec. 3, Pudovkin and Garfield approach); "This is done based on the number of publications with fewer citations than the publication of interest." (sec. 3, Leydesdorff et al. approach)
notes: The source describes an average-percentile treatment of ties (Pudovkin and Garfield, roughly mid-rank) as one of several approaches, next to the strictly-fewer approach of Leydesdorff et al. and the National Science Board; it never calls mid-ranks a common choice nor uses the phrase mid-rank, and it argues for a different fractional approach.

## [2026-11-05T16:07Z] cite:waltman2012inconsistency — paper/manuscript_qss/main.tex:350
claim: "PiP is not consistent in the sense of \citet{waltman2012inconsistency}: adding the same article to two authors can reverse their order, because $G_\theta$ is not linear."
claim-hash: 5688699dc7e2
source: doi:10.1002/asi.21678
version-read: preprint (arXiv:1108.3901v1)
archived: gs://scholar-version2-literature/sources/waltman2012inconsistency--10.1002_asi.21678-42823f3e.pdf sha256:8afcd8681c22f8267eb5442a4ca26783f88c7206b05a96133294a97a369198ca
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "If two scientists achieve the same absolute performance improvement, their ranking relative to each other should remain unchanged." (sec. 3.2, Example 2, p. 7); "Scientists X and Y have both achieved exactly the same performance improvement (i.e., two publications with six citations each), but despite of this their ranking has reversed." (sec. 3.2, Example 2, p. 7)
notes: The source's consistency property (same added output must not reverse order) matches the manuscript's paraphrase. The paper frames this for size-dependent indicators and notes that scoring rules (sums of per-publication scores) are consistent; the application to PiP and the claim about G_theta non-linearity are the manuscript's own.

## [2026-11-05T16:07Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:361
claim: "The first form of the score \citep{ipeirotis2024pip}, which the website \url{https://www.pip-score.org} uses, is asymmetric."
claim-hash: 69ad69b39f7d
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: partially supported
evidence: "we created a small app at https://scholar.ipeirotis.org/ that allows anyone to search for a Google Scholar profile" (post body, second paragraph)
notes: The post describes the first form of the score (PiP-AUC), which is what the citation identifies. It does not mention pip-score.org; its app link is scholar.ipeirotis.org. That does not contradict the claim (the site may have moved), so the website clause is unaddressed rather than contradicted. Coordinator changed 'unsupported' to 'partially supported'. [coordinator changed the worker's 'unsupported']

## [2026-11-05T16:07Z] cite:hazen1914storage — paper/manuscript_qss/main.tex:418
claim: "For every article published from 1980 to 1995, we compute its percentile within its publication year at each age, using Hazen mid-ranks \citep{hazen1914storage}, and correlate the percentiles at two ages within each publication year."
claim-hash: a29e246430ee
source: doi:10.1061/taceat.0002563
version-read: version of record (scan of Trans. ASCE vol. 77, 1914; pages 1629-1740 of the archive.org volume scan, extracted)
archived: gs://scholar-version2-literature/sources/hazen1914storage--10.1061_taceat.0002563-1fbb5981.pdf sha256:e80f3c2b37f161d8ac843c6c0e1a78db86343bb4e08fff8db8f9324db8dfc5de
license: public domain (1914 US publication)
updates: none
verdict: supported
evidence: "if there are 50 terms in the series, each is taken to represent 2% of the whole space" (printed p. 1549); "the figure for each term, arranged in its order of magnitude, is plotted at the percentage which corresponds to the middle of its strip" (printed p. 1549)
notes: The mid-strip rule converts ranks into percentage positions, a way to compute a percentile within a set.

## [2026-11-05T16:07Z] cite:wang2013window — paper/manuscript_qss/main.tex:464
claim: "The speed differs by field (Figure~\ref{fig:paperstability}b), in line with \citet{wang2013window} and \citet{abramo2011window}."
claim-hash: 22078ba6331f
source: doi:10.1007/s11192-012-0775-9
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/wang2013window--10.1007_s11192-012-0775-9-75debdfb--abstract--2026-11-05-f17b6d3a.html sha256:f17b6d3ab6d0670af9ce40602a21905818e18caac9745fb0a33d0d808ade16d9
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: supported
evidence: "There are significant differences in citation ageing between different research fields, document types, total citation counts, and publication months." (abstract); "many papers in the slowest ageing field may still age faster than many papers in the fastest ageing field" (abstract)
notes: The abstract reports field differences in citation ageing, consistent with the manuscript's finding that percentile settling speed differs by field; Wang studies raw citation ageing, not percentile stability, and stresses large within-field spread, so 'in line with' holds only at that level. Judged from the abstract only.

## [2026-11-05T16:07Z] cite:abramo2011window — paper/manuscript_qss/main.tex:464
claim: "The speed differs by field (Figure~\ref{fig:paperstability}b), in line with \citet{wang2013window} and \citet{abramo2011window}."
claim-hash: 22078ba6331f
source: doi:10.1016/j.joi.2011.06.004
version-read: preprint (arXiv:1811.01705v1)
archived: gs://scholar-version2-literature/sources/abramo2011window--10.1016_j.joi.2011.06.004-41cc2290.pdf sha256:04250c83863be14b590bc141f178dd00300a98f320b4f4668fd51f2d15cfe5cd
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "The citation speed is also quite different for clusters of disciplines." (p. 12, sec. 4 Conclusions); "Convergence arrives quickest in Physics, followed by Biomedical research and Engineering." (sec. 3.3, text after Table 4)
notes: Abramo et al. show that convergence of citation-based classification to the benchmark proceeds at different speeds across disciplines, consistent with the claim that the speed of settling differs by field. Note their measure is quartile-class shift, not percentiles per se, and they analyse WoS disciplines for Italian 2001 publications.

## [2026-11-05T16:07Z] cite:waltman2012leiden — paper/manuscript_qss/main.tex:685
claim: "The closest alternatives to PiP use the same paper percentiles but combine them differently: the number of papers in the top 10\% \citep{waltman2012leiden}, also divided by career age \citep{bornmann2014individual}; the number in the top 1\%; I3, the sum of the percentiles \citep{leydesdorff2011integrated}; and their mean."
claim-hash: 4cdb06c0a9e9
source: doi:10.1002/asi.22708
version-read: preprint (arXiv:1202.3941v1)
archived: gs://scholar-version2-literature/sources/waltman2012leiden--10.1002_asi.22708-420e6186.pdf sha256:463b811dfb698626177a2533bda112c3cc0def73dd694eb2ba606c068d4c54f9
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Proportion top 10% publications (PPtop 10%). The proportion of the publications of a university that, compared with other similar publications, belong to the top 10% most frequently cited" (sec. 3, indicator list)
notes: The source presents top-10% publications as a citation-impact indicator, supporting that top-10% paper counting is an existing indicator; it is defined as a proportion for universities, not a count for individual authors, so the 'number of papers' form is a close variant rather than verbatim.

## [2026-11-05T16:07Z] cite:bornmann2014individual — paper/manuscript_qss/main.tex:685
claim: "The closest alternatives to PiP use the same paper percentiles but combine them differently: the number of papers in the top 10\% \citep{waltman2012leiden}, also divided by career age \citep{bornmann2014individual}; the number in the top 1\%; I3, the sum of the percentiles \citep{leydesdorff2011integrated}; and their mean."
claim-hash: 4cdb06c0a9e9
source: doi:10.1007/s11192-013-1161-y
version-read: preprint (arXiv:1302.3697v2)
archived: gs://scholar-version2-literature/sources/bornmann2014individual--10.1007_s11192-013-1161-y-22e3c955.pdf sha256:f53354eb37b3efb9676ae2f64101071c8aa1d7ffe88e12df5d581f88ea210334
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we would like to propose using the number of years as an active researcher (Ptop 10% quotient) to normalise Ptop 10% for age" (sec. 3.2, Ptop 10% paragraphs); "With a value of 2.8, Researcher 2 published around twice as many Ptop 10% as Researcher 3" (sec. 3.2, end)
notes: The Ptop 10% quotient is the number of top-10% papers divided by years as an active researcher, which is career age as the manuscript says.

## [2026-11-05T16:07Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:685
claim: "The closest alternatives to PiP use the same paper percentiles but combine them differently: the number of papers in the top 10\% \citep{waltman2012leiden}, also divided by career age \citep{bornmann2014individual}; the number in the top 1\%; I3, the sum of the percentiles \citep{leydesdorff2011integrated}; and their mean."
claim-hash: 4cdb06c0a9e9
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Because this indicator is based on a summation, the value for the EU-27" (p. 25, nations decomposition); "The integrals in this stepwise" (p. 5, percentile rank section)
notes: I3 is the sum of percentile ranks (sum over x_i * f(x_i)), which matches 'I3, the sum of the percentiles'.

## [2026-11-05T16:07Z] cite:aksnes2003macro — paper/manuscript_qss/main.tex:928
claim: "These are 8.1\% of all citations; such rates depend strongly on the definition and the citation window \citep{aksnes2003macro}."
claim-hash: 928cce965a4f
source: doi:10.1023/a:1021919228368
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/aksnes2003macro--10.1023_a_1021919228368-e0aafd03--abstract--2026-11-05-ddd47d56.html sha256:ddd47d560ca73384c79e53bbfc12e5e4461a9e186e1dcdaedcd8f01b714c41f5
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: unverifiable
evidence: "Using a three-year citation window we find that 36% of all citations represent author self-citations. However, this percentage is decreasing when citations are traced for longer periods." (abstract)
notes: Only the abstract was reachable. It is consistent with the citation-window half of the claim (self-citation share falls as the window lengthens) but says nothing about dependence on the definition of self-citation, and the body text that would carry that was not reached, so the claim is not judged supported or unsupported. paywalled: please supply the PDF of Aksnes 2003 (Scientometrics 56(2):235-246) to check the 'definition' part of the claim

## [2026-11-05T16:07Z] cite:merton1968matthew — paper/manuscript_qss/main.tex:993
claim: "Citations reward visibility as well as research quality: well-known scholars attract more citations for comparable work \citep{merton1968matthew,petersen2014reputation}."
claim-hash: f97695df5eec
source: doi:10.1126/science.159.3810.56
version-read: version of record (scan of Science 159(3810):56-63, 1968; OCR text)
archived: gs://scholar-version2-literature/sources/merton1968matthew--10.1126_science.159.3810.56-0577b48e.pdf sha256:95fc1cb4a84cf1c563b8085c0c8adca9f44c0162ae343368c688c1f5b64e5209
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "entist may have greater stimuius vaiue" (p. 61); "been carried out on pnciscly this question." (footnote 32 ('So far as I know, no investigation has yet ...')); "cited 30 times more often in the 5 years" (footnote 32 (laureates-to-be, 'at best suggestive'))
notes: Visibility for comparable work is supported; Merton says no study had tested whether eminent scientists get more citations for comparable work, so the citation part is not a finding of this source.

## [2026-11-05T16:07Z] cite:petersen2014reputation — paper/manuscript_qss/main.tex:993
claim: "Citations reward visibility as well as research quality: well-known scholars attract more citations for comparable work \citep{merton1968matthew,petersen2014reputation}."
claim-hash: f97695df5eec
source: doi:10.1073/pnas.1323111111
version-read: preprint (arXiv:1303.7274v4)
archived: gs://scholar-version2-literature/sources/petersen2014reputation--10.1073_pnas.1323111111-aab695c0.pdf sha256:e9a2fedc3721a39f5ec0a686fb3d15f9ebaf99579c5bc1b844dce252f36e2205
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the author’s reputation is found to dominate the annual citation rate" (Abstract); "the reputation effect becomes negligible for highly cited publications" (Abstract)
notes: Author reputation boosts citations of comparable (c < c×) publications, supporting 'citations reward visibility'. The paper notes quality cannot be separated from citation counts, so 'comparable work' rests on a model with other things equal, and the effect fades for highly cited papers.

## [2026-11-05T16:07Z] cite:zitt2005relativity — paper/manuscript_qss/main.tex:1005
claim: "Field normalization brings its own choices: the field scheme, the assignment of multi-field papers and the assignment of an author to a main field \citep{zitt2005relativity,waltman2015field, waltman2019field}."
claim-hash: ef7935434dda
source: doi:10.1007/s11192-005-0218-y
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/zitt2005relativity--10.1007_s11192-005-0218-y-bf0707e5--abstract--2026-11-05-ce9c72cb.html sha256:ce9c72cbeba9eee1d4c552b5f9ffd9d3754059dec1c034a9abad75b2c54304b1
license: restricted — publisher abstract snapshot, private store
updates: none
verdict: unverifiable
evidence: "fields may be defined at various levels, from small research areas to broad academic disciplines, and thus normalisation values are expected to vary" (abstract)
notes: Only the abstract was reachable. It is consistent with the field-scheme part of the claim (choice of level of field definition changes normalised values) but neither states nor contradicts the assignment of multi-field papers or the assignment of an author to a main field, so the full claim cannot be judged. paywalled: please supply the PDF of Zitt, Ramanana-Rahary & Bassecoulard (2005), Scientometrics 63(2):373-401, to check the multi-field paper and author main-field parts of the claim

## [2026-11-05T16:07Z] cite:waltman2015field — paper/manuscript_qss/main.tex:1005
claim: "Field normalization brings its own choices: the field scheme, the assignment of multi-field papers and the assignment of an author to a main field \citep{zitt2005relativity,waltman2015field, waltman2019field}."
claim-hash: ef7935434dda
source: doi:10.1016/j.joi.2015.08.001
version-read: preprint (arXiv:1501.04431v1)
archived: gs://scholar-version2-literature/sources/waltman2015field--10.1016_j.joi.2015.08.001-8988ca16.pdf sha256:acc87a4b752fbf52994b18aa703b1561c84124c780e9de46a66ff8a0341fc2a6
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "fields are defined by the WoS journal subject categories." (sec. 4 (empirical analysis, data and methods), p. 19); "A fractional counting approach is taken to handle publications belonging to multiple subject categories." (footnote 6, p. 19)
notes: The paper uses a field scheme (WoS subject categories) and handles multi-field publications by fractional assignment, which fits the first two choices as an example, but it does not discuss them as choices that field normalization brings, and it never addresses assigning an author to a main field (its units of analysis are authors, organizations and countries, with publications as the normalized items); at low aggregation it even calls field normalization itself questionable.

## [2026-11-05T16:07Z] cite:waltman2019field — paper/manuscript_qss/main.tex:1005
claim: "Field normalization brings its own choices: the field scheme, the assignment of multi-field papers and the assignment of an author to a main field \citep{zitt2005relativity,waltman2015field, waltman2019field}."
claim-hash: ef7935434dda
source: doi:10.1007/978-3-030-02511-3_11
version-read: preprint (arXiv:1801.09985v1)
archived: gs://scholar-version2-literature/sources/waltman2019field--10.1007_978-3-030-02511-3_11-357ae0ed.pdf sha256:af5d1381889500d4cb5876fa62907d470b34613ed4b7db6a647e6a3d252aa77a
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "normalized indicators are quite sensitive to methodological choices, such as the choice of a field classification system" (sec. 7.2); "Another issue in the calculation of the normalized citation score of a publication is the way in which publications belonging to multiple fields are handled." (sec. 5.1)
notes: The chapter supports the field-scheme choice and the handling of multi-field publications as choices in field normalization; it does not address assigning an individual author to a main field (it treats institutions, groups and publications).

## [2026-11-05T16:07Z] cite:waltman2015field — paper/manuscript_qss/main.tex:1019
claim: "A fractional version would weight each paper's contribution to $n_a(\theta)$ by the author's share \citep{waltman2015field}; we have not tested it."
claim-hash: 1abf12bdc8bf
source: doi:10.1016/j.joi.2015.08.001
version-read: preprint (arXiv:1501.04431v1)
archived: gs://scholar-version2-literature/sources/waltman2015field--10.1016_j.joi.2015.08.001-8988ca16.pdf sha256:acc87a4b752fbf52994b18aa703b1561c84124c780e9de46a66ff8a0341fc2a6
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "In the fractional counting case, a publication is fractionally assigned to each co-author." (sec. 2.1, pp. 1-2); "publication is assigned to a co-author indicates the share of the publication allocated to that co-author." (sec. 2.1, pp. 1-2)
notes: Author-level fractional counting weights each publication by the author's share (e.g. 1/5 each for five authors), which is what the manuscript attributes; the source does not test it for a PiP-type metric, but the manuscript only cites it for the weighting concept.

## [2026-11-05T16:07Z] cite:waltman2012inconsistency — paper/manuscript_qss/main.tex:1032
claim: "\emph{Consistency.} PiP is not consistent in the sense of \citet{waltman2012inconsistency}, although reversals are rare in simulation."
claim-hash: bc165d2a7ef1
source: doi:10.1002/asi.21678
version-read: preprint (arXiv:1108.3901v1)
archived: gs://scholar-version2-literature/sources/waltman2012inconsistency--10.1002_asi.21678-42823f3e.pdf sha256:8afcd8681c22f8267eb5442a4ca26783f88c7206b05a96133294a97a369198ca
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "If two scientists achieve the same absolute performance improvement, their ranking relative to each other should remain unchanged." (sec. 3.2, Example 2, p. 7); "the h-index fails to provide consistent rankings of individual scientists and research groups" (sec. 3.3, Example 3, p. 8)
notes: Only the definition of consistency is attributed to this source (the simulation claim is the manuscript's own); the source states the property.

## [2026-11-05T16:07Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:1037
claim: "PiP is meant to inform comparisons among scholars at a similar career stage, alongside reading the work itself and expert judgment, as the Leiden Manifesto \citep{hicks2015bibliometrics} and DORA \citep{dora2012} recommend."
claim-hash: 9be3e3cde409
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Quantitative evaluation should support qualitative, expert assessment." (p. 430, principle 1); "Reading and judging a researcher’s work is much more appropriate than relying on one number." (p. 431, principle 7)
notes: Indicators should support expert assessment and reading the work.

## [2026-11-05T16:07Z] cite:dora2012 — paper/manuscript_qss/main.tex:1037
claim: "PiP is meant to inform comparisons among scholars at a similar career stage, alongside reading the work itself and expert judgment, as the Leiden Manifesto \citep{hicks2015bibliometrics} and DORA \citep{dora2012} recommend."
claim-hash: 9be3e3cde409
source: https://sfdora.org/read/
version-read: version of record (web page snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/dora2012--san_francisco_declaration_on_research_assessment-3c8fb843--2026-11-05-a05800ad.html sha256:a05800adef16b8b0a984caf5ba249467fb2a404b7c3ff50146172fed9ce269db
license: CC-BY-SA-4.0 (stated on page)
accessed: 2026-11-05
verdict: partially supported
evidence: "the scientific content of a paper is much more important than publication metrics or the identity of the journal in which it was published" (Recommendations 2 and 4 (funding agencies; institutions)); "consider a broad range of impact measures including qualitative indicators of research impact" (Recommendations 3 and 5); "Use a range of article metrics and indicators on personal/supporting statements, as evidence of the impact of individual published articles and other research outputs" (Recommendation 17 (for researchers))
notes: DORA supports assessing the work's content and using a range of article-level metrics and qualitative indicators rather than relying on metrics alone, which fits the 'alongside reading the work itself' part. It does not mention expert judgment or peer review as a complement to indicators, and says nothing about comparing scholars at a similar career stage (the only career-stage reference is protecting early-stage investigators from metric-driven evaluation); its main thrust is rejecting journal-based metrics, so it does not specifically recommend using author-level indicators to inform comparisons.

## [2026-11-05T16:07Z] bib:hirsch2005index
entry-hash: bbb647b39d9a
source: doi:10.1073/pnas.0507655102 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'An index to quantify an individual's scientific research output', Proceedings of the National Academy of Sciences 102(46) 16569-16572; doi: missing from bib; registrar DOI 10.1073/pnas.0507655102

## [2026-11-05T16:07Z] bib:bornmann2013use
entry-hash: 5473de822a5d
source: doi:10.1016/j.joi.2012.10.001 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'The use of percentiles and percentile rank classes in the analysis of bibliometric data: Opportunities and limits', Journal of Informetrics 7(1) 158-165; doi: missing from bib; registrar DOI 10.1016/j.joi.2012.10.001

## [2026-11-05T16:07Z] bib:waltman2016review
entry-hash: 6a5f32a82ab5
source: doi:10.1016/j.joi.2016.02.007 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'A review of the literature on citation impact indicators', Journal of Informetrics 10(2) 365-391; doi: missing from bib; registrar DOI 10.1016/j.joi.2016.02.007

## [2026-11-05T16:07Z] bib:hicks2015bibliometrics
entry-hash: e5d83fb4711c
source: doi:10.1038/520429a — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Bibliometrics: The Leiden Manifesto for research metrics', Nature 520(7548) 429-431; doi: missing from bib; registrar DOI 10.1038/520429a

## [2026-11-05T16:07Z] bib:leydesdorff2011integrated
entry-hash: b381d51bac24
source: doi:10.1002/asi.21609
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Integrated impact indicators compared with impact factors: An alternative research design with policy implications', Journal of the American Society for Information Science and Technology 62(11) 2133-2146; title, authors, venue, volume, issue, pages, year and DOI match the entry

## [2026-11-05T16:07Z] bib:waltman2012leiden
entry-hash: 9e48e744b9ec
source: doi:10.1002/asi.22708 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'The Leiden ranking 2011/2012: Data collection, indicators, and interpretation', Journal of the American Society for Information Science and Technology 63(12) 2419-2432; doi: missing from bib; registrar DOI 10.1002/asi.22708

## [2026-11-05T16:07Z] bib:bornmann2014individual
entry-hash: ef0d7c8b5eca
source: doi:10.1007/s11192-013-1161-y — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'How to evaluate individual researchers working in the natural and life sciences meaningfully? A proposal of methods based on percentiles of citations', Scientometrics 98(1) 487-509; doi: missing from bib; registrar DOI 10.1007/s11192-013-1161-y

## [2026-11-05T16:07Z] bib:kinney2023semantic
entry-hash: 79d72e0f8161
source: arXiv:2301.10140 — doi:10.48550/arXiv.2301.10140 matched; proposed addition
registrar: DataCite (arXiv)
verdict: discrepant
evidence: arXiv API and DataCite record for doi:10.48550/arXiv.2301.10140: title matches the entry, year matches; the entry has no DOI — proposed addition doi:10.48550/arXiv.2301.10140

## [2026-11-05T16:07Z] bib:tian2021predictability
entry-hash: 3202194ddc53
source: arXiv:2103.16025 — doi:10.48550/arXiv.2103.16025 matched; proposed addition
registrar: DataCite (arXiv)
verdict: discrepant
evidence: arXiv API and DataCite record for doi:10.48550/arXiv.2103.16025: title matches the entry, year matches; the entry has no DOI — proposed addition doi:10.48550/arXiv.2103.16025

## [2026-11-05T16:07Z] bib:ipeirotis2024pip
entry-hash: fb45735653a6
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html (accessed 2026-11-05)
registrar: none (URL-backed)
verdict: discrepant
evidence: the cited URL is the work itself (blog post, 18 January 2024, accessed 2026-11-05); page title 'The PiP-AUC score for research productivity: A somewhat new metric for paper citations and number of papers' — the entry drops the subtitle; author-controlled page

## [2026-11-05T16:07Z] bib:egghe2006theory
entry-hash: 3c7077747eaa
source: doi:10.1007/s11192-006-0144-7 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Theory and practise of the g-index', Scientometrics 69(1) 131-152; doi: missing from bib; registrar DOI 10.1007/s11192-006-0144-7

## [2026-11-05T16:07Z] bib:sidiropoulos2007generalized
entry-hash: c1ddfa13e512
source: doi:10.1007/s11192-007-1722-z — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Generalized Hirsch h-index for disclosing latent facts in citation networks', Scientometrics 72(2) 253-280; doi: missing from bib; registrar DOI 10.1007/s11192-007-1722-z

## [2026-11-05T16:07Z] bib:harzing2014hia
entry-hash: 2cad36acb08f
source: doi:10.1007/s11192-013-1208-0 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'hIa: an individual annual h-index to accommodate disciplinary and career length differences', Scientometrics 99(3) 811-821; doi: missing from bib; registrar DOI 10.1007/s11192-013-1208-0

## [2026-11-05T16:07Z] bib:kaur2013universality
entry-hash: 71aeeab683d7
source: doi:10.1016/j.joi.2013.09.002 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Universality of scholarly impact metrics', Journal of Informetrics 7(4) 924-932; doi: missing from bib; registrar DOI 10.1016/j.joi.2013.09.002

## [2026-11-05T16:07Z] bib:bornmann2011metaanalysis
entry-hash: d26b7db635ba
source: doi:10.1016/j.joi.2011.01.006 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'A multilevel meta-analysis of studies reporting correlations between the h index and 37 different h index variants', Journal of Informetrics 5(3) 346-359; doi: missing from bib; registrar DOI 10.1016/j.joi.2011.01.006

## [2026-11-05T16:07Z] bib:waltman2012inconsistency
entry-hash: 41e05a930052
source: doi:10.1002/asi.21678
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'The inconsistency of the h‐index', Journal of the American Society for Information Science and Technology 63(2) 406-415; title, authors, venue, volume, issue, pages, year and DOI match the entry

## [2026-11-05T16:07Z] bib:leydesdorff2011turning
entry-hash: ba5bcc271217
source: doi:10.1002/asi.21534 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Turning the tables on citation analysis one more time: Principles for comparing sets of documents', Journal of the American Society for Information Science and Technology 62(7) 1370-1381; doi: missing from bib; registrar DOI 10.1002/asi.21534

## [2026-11-05T16:07Z] bib:waltman2013percentile
entry-hash: 89b462c14f5d
source: doi:10.1002/asi.22775 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'On the calculation of percentile‐based bibliometric indicators', Journal of the American Society for Information Science and Technology 64(2) 372-379; doi: missing from bib; registrar DOI 10.1002/asi.22775

## [2026-11-05T16:07Z] bib:bornmann2020evaluation
entry-hash: fe11cd78856c
source: doi:10.1007/s11192-020-03512-7 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'An evaluation of percentile measures of citation impact, and a proposal for making them better', Scientometrics 124(2) 1457-1478; number: bib 'None' vs registrar '2'; doi: missing from bib; registrar DOI 10.1007/s11192-020-03512-7

## [2026-11-05T16:07Z] bib:schubert1986relative
entry-hash: 688c892cef35
source: doi:10.1007/bf02017249 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Relative indicators and relational charts for comparative assessment of publication output and citation impact', Scientometrics 9(5-6) 281-291; doi: missing from bib; registrar DOI 10.1007/bf02017249

## [2026-11-05T16:07Z] bib:Radicchi2008
entry-hash: 95f28c40172a
source: doi:10.1073/pnas.0806977105 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Universality of citation distributions: Toward an objective measure of scientific impact', Proceedings of the National Academy of Sciences 105(45) 17268-17272; doi: missing from bib; registrar DOI 10.1073/pnas.0806977105

## [2026-11-05T16:07Z] bib:waltman2019field
entry-hash: 78f287d212e3
source: doi:10.1007/978-3-030-02511-3_11 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Field Normalization of Scientometric Indicators', Springer Handbooks None(None) 281-300; doi: missing from bib; registrar DOI 10.1007/978-3-030-02511-3_11

## [2026-11-05T16:07Z] bib:hazen1914storage
entry-hash: 904f2297622e
source: doi:10.1061/taceat.0002563 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Storage to be Provided in Impounding Municipal Water Supply', Transactions of the American Society of Civil Engineers 77(1) 1539-1640; doi: missing from bib; registrar DOI 10.1061/taceat.0002563; note: the paper's own title page (Trans. ASCE Paper No. 1308, archive.org scan) reads 'Storage to be Provided in Impounding Reservoirs for Municipal Water Supply' — both the entry and the registrar omit 'Reservoirs for'

## [2026-11-05T16:07Z] bib:rousseau2012basic
entry-hash: 26fa25ed986b
source: doi:10.1002/asi.21684 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Basic properties of both percentile rank scores and the I3 indicator', Journal of the American Society for Information Science and Technology 63(2) 416-420; doi: missing from bib; registrar DOI 10.1002/asi.21684

## [2026-11-05T16:07Z] bib:ioannidis2019standardized
entry-hash: f93042b4b40c
source: doi:10.1371/journal.pbio.3000384 — matched by search; proposed addition
registrar: Crossref
updates: new_version doi:10.1371/journal.pbio.3000384 (2019-08-19, publisher)
verdict: discrepant
evidence: registrar record: 'A standardized citation metrics author database annotated for scientific field', PLOS Biology 17(8) e3000384; doi: missing from bib; registrar DOI 10.1371/journal.pbio.3000384

## [2026-11-05T16:07Z] bib:abramo2014productivity
entry-hash: 48683d4c2d3f
source: doi:10.1007/s11192-014-1269-8 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'How do you define and measure research productivity?', Scientometrics 101(2) 1129-1144; doi: missing from bib; registrar DOI 10.1007/s11192-014-1269-8

## [2026-11-05T16:07Z] bib:rodriguez2018double
entry-hash: 7e6cd1e0417e
source: doi:10.1016/j.joi.2017.11.004
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Double rank analysis for research assessment', Journal of Informetrics 12(1) 31-41; title, authors, venue, volume, issue, pages, year and DOI match the entry

## [2026-11-05T16:07Z] bib:brito2018percentile
entry-hash: 7a44d89ef3ca
source: doi:10.1016/j.joi.2018.01.011
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Research assessment by percentile-based double rank analysis', Journal of Informetrics 12(1) 315-329; title, authors, venue, volume, issue, pages, year and DOI match the entry

## [2026-11-05T16:07Z] bib:hirsch2007does
entry-hash: 98e56f45d206
source: doi:10.1073/pnas.0707962104 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Does the h index have predictive power?', Proceedings of the National Academy of Sciences 104(49) 19193-19198; doi: missing from bib; registrar DOI 10.1073/pnas.0707962104

## [2026-11-05T16:07Z] bib:acuna2012future
entry-hash: 80786095a743
source: doi:10.1038/489201a — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Predicting scientific success', Nature 489(7415) 201-202; title: bib 'Future impact: Predicting scientific success' vs registrar 'Predicting scientific success' (sim 0.818); pages: bib '201' vs registrar '201-202'; doi: missing from bib; registrar DOI 10.1038/489201a

## [2026-11-05T16:07Z] bib:penner2013predictability
entry-hash: df440035fef8
source: doi:10.1038/srep03052 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'On the Predictability of Future Impact in Science', Scientific Reports 3(1) 3052; number: bib 'None' vs registrar '1'; doi: missing from bib; registrar DOI 10.1038/srep03052

## [2026-11-05T16:07Z] bib:sinatra2016quantifying
entry-hash: f10b0ad60aa4
source: doi:10.1126/science.aaf5239 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Quantifying the evolution of individual scientific impact', Science 354(6312) aaf5239; doi: missing from bib; registrar DOI 10.1126/science.aaf5239

## [2026-11-05T16:07Z] bib:clauset2017datadriven
entry-hash: 69b726f4f99d
source: doi:10.1126/science.aal4217 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Data-driven predictions in the science of science', Science 355(6324) 477-480; doi: missing from bib; registrar DOI 10.1126/science.aal4217

## [2026-11-05T16:07Z] bib:wang2013window
entry-hash: 367a1ee967bb
source: doi:10.1007/s11192-012-0775-9 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Citation time window choice for research impact evaluation', Scientometrics 94(3) 851-872; doi: missing from bib; registrar DOI 10.1007/s11192-012-0775-9

## [2026-11-05T16:07Z] bib:abramo2011window
entry-hash: aea3b4473043
source: doi:10.1016/j.joi.2011.06.004 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Assessing the varying level of impact measurement accuracy as a function of the citation window length', Journal of Informetrics 5(4) 659-667; doi: missing from bib; registrar DOI 10.1016/j.joi.2011.06.004

## [2026-11-05T16:07Z] bib:merton1968matthew
entry-hash: 9a1adb55fcf6
source: doi:10.1126/science.159.3810.56 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'The Matthew Effect in Science', Science 159(3810) 56-63; doi: missing from bib; registrar DOI 10.1126/science.159.3810.56

## [2026-11-05T16:07Z] bib:petersen2014reputation
entry-hash: aee3a875d81b
source: doi:10.1073/pnas.1323111111 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Reputation and impact in academic careers', Proceedings of the National Academy of Sciences 111(43) 15316-15321; doi: missing from bib; registrar DOI 10.1073/pnas.1323111111

## [2026-11-05T16:07Z] bib:aksnes2003macro
entry-hash: f2cbfae73b63
source: doi:10.1023/a:1021919228368 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'A macro study of self-citation', Scientometrics 56(2) 235-246; doi: missing from bib; registrar DOI 10.1023/a:1021919228368

## [2026-11-05T16:07Z] bib:zitt2005relativity
entry-hash: 0d1348c6dc2e
source: doi:10.1007/s11192-005-0218-y — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Relativity of citation performance and excellence measures: From cross-field to cross-scale effects of field-normalisation', Scientometrics 63(2) 373-401; doi: missing from bib; registrar DOI 10.1007/s11192-005-0218-y

## [2026-11-05T16:07Z] bib:waltman2015field
entry-hash: 776f693eae0d
source: doi:10.1016/j.joi.2015.08.001 — matched by search; proposed addition
registrar: Crossref
updates: none
verdict: discrepant
evidence: registrar record: 'Field-normalized citation impact indicators and the choice of an appropriate counting method', Journal of Informetrics 9(4) 872-894; doi: missing from bib; registrar DOI 10.1016/j.joi.2015.08.001

## [2026-11-05T16:07Z] bib:dora2012
entry-hash: 49842c4f304a
source: https://sfdora.org/read/ (accessed 2026-11-05)
registrar: none (URL-backed)
verdict: confirmed
evidence: official declaration page https://sfdora.org/read/ (accessed 2026-11-05) carries the title 'San Francisco Declaration on Research Assessment' and dates it to the December 2012 ASCB meeting; matches the entry


## [2026-11-05T16:07Z] novelty — "This paper proposes PiP, short for \emph{percentile in percentile}, which places a scholar's papers in percentiles of their publication year and then places the scholar in percentiles of their career cohort."
searched: Crossref: percentile rank scholar career cohort citation percentile publication year -> 8; Crossref: individual researcher percentile rank classes citation impact profile -> 8; Crossref: stochastic dominance citation distributions researchers -> 8 (no bibliometric hits); Crossref: career age normalized percentile indicator researcher citations cohort -> 8; Crossref: number of papers in top percentile aggregated over multiple thresholds researcher indicator -> 8; Crossref: percentile-based indicator for individual scientists compared with peers same academic age -> failed (non-JSON); Crossref: stochastic dominance citation distributions research units comparison -> 7; Crossref: comparing citation distributions of scientists with cohort percentile rank -> 7; Crossref: citation percentiles of individual scientists relative to academic age peers -> 7; Crossref: top-cited papers share scientist percentile cohort publication year normalized career -> 8; Crossref: quantile based indicator scholar impact area under percentile curve -> 7 (irrelevant); Crossref: h-index percentile rank among researchers with same career start year -> 7; Crossref: Percentile Rank and Author Superiority Indexes -> 7; Crossref: relative author superiority index research evaluation citation-based performance -> 6; Crossref: percentile rank scientists seniority-adjusted ranking citation distribution of an author's papers -> 6; Crossref: first-order stochastic dominance citation curves comparison of scientists -> 6 (no bibliometric hits); Crossref: percentile ranks of papers for individual scientists distribution comparison with reference set researchers same career length -> 6; Crossref: relative position of researcher among cohort by year of first publication percentile h-index citations -> 7; Crossref: citation rank percentile area under curve author-level indicator -> 6; Crossref: stochastic dominance citation distributions scientometrics Ruiz-Castillo -> 6; Crossref: comparison of citation distributions research groups stochastic dominance percentile -> 6; Crossref: percentile rank classes individual researcher evaluation proportion of papers in top 1% top 10% top 50% -> 6; Crossref: Ptop 10% quotient researcher papers per year active -> 6 (irrelevant); Crossref: career-stage percentile researcher benchmarking citations years since first publication -> 6; arXiv: citation percentile academic age researchers -> 8; arXiv: stochastic dominance citation distribution researchers -> 8 (no on-point hits); arXiv: three advanced phrase queries (percentile/career/cohort; stochastic dominance/citation; percentile/academic age) -> 0 results each (query syntax returned empty); WebSearch: Pudovkin Garfield Percentile Rank and Author Superiority Indexes pdf -> 10; WebSearch: researcher indicator percentile of papers within publication year then rank among researchers same career start year; stochastic dominance -> 10; Crossref DOI records read for 10.1007/s11192-026-05608-y, 10.1080/09737766.2009.10700871, 10.1101/2020.09.23.310052, 10.1590/2318-08892017000200006, 10.12688/f1000research.7070.1 (Belikov and Belikov; read in full and set aside, not percentile-based) | channel failures: OpenAlex: every query returned 'Rate limit exceeded ... free daily budget ... used up' (HTTP 429-style JSON error), so OpenAlex was not searched at all; Semantic Scholar Graph API: all 6 queries returned 'Too Many Requests' (429), so Semantic Scholar was not searched; arXiv: quoted-phrase queries with AND returned empty feeds; only plain all:<terms> queries worked; Crossref: one query returned non-JSON (rate/format error); tandfonline PDF for Pudovkin & Garfield 2009 (Collnet journal version): HTTP 403; Unpaywall reports it closed access. The author's conference version of the same paper was read instead; scielo.br PDF of Ronda-Pupo 2017: HTTP 403; redalyc page: HTTP 403 (only the Crossref abstract was read); biorxiv full text of Seppanen et al.: HTTP 429; only Crossref abstract read; No search found any paper on stochastic dominance comparisons of citation distributions of individual researchers; this part of the question was searched on Crossref and arXiv only (OpenAlex and Semantic Scholar unavailable), so it is not a complete negative
lead: https://doi.org/10.1080/09737766.2009.10700871 — Pudovkin, A.I. and Garfield, E. (2009), Percentile Rank and Author Superiority Indexes for Evaluating Individual Journal Articles and the Author's Overall Citation Performance, Collnet Journal of Scientometrics and Information Management 3(2), 3-10 (text read: the authors' version presented at the 5th WIS / 10th COLLNET Meeting, Dalian, 13-16 Sept 2009)
  version-read: conference version presented 2009; Crossref record for the journal version saved at cr_10.1080_09737766.2009.10700871.json in same dir
  archived: gs://scholar-version2-literature/sources/pudovkin2009--10.1080_09737766.2009.10700871-7ff98c79.pdf sha256:2cc2e40029a2de1aceca1bc0f54f6491838ae0901b1878b2e48b8a74f077c2a7
  license: unknown (treated as restricted; private store)
  evidence: "The Percentile Rank Index (PRI) indicates the citation rank of the author's individual papers among the papers published in the same year and source (journal or multi-authored monograph or book.) PRI is independent of the paper's age, specialty, or source journal size. The Author's Superiority Index (ASI) is determined by the number of the author's papers with a PRI at or above a specified value (99, 95, or 75)." (Abstract, p.1); "We propose a procedure involving a two step process. It requires that we first obtain a Percentile Rank Index (PRI) for each of the individual papers an author has published, and then calculate the Author Superiority Index (ASI), which is based on PRI values for all the author's papers." (p.2); "PRI = (N - R + 1)/N*100, where N is the number of papers in the year set of the journal, R is the descending citation rank of the paper" (p.3); "ASI allows for the evaluation and comparison of authors across different specialties, across different time periods, taking into account quite recent publications." (Summary, p.5) — overlap: Step (a) and the threshold count of (b): papers are percentile-ranked within their publication year (here within journal and year), then the author's number of papers at or above a percentile threshold (ASI at 99, 95, 75; later work adds 50) is reported, which is the same threshold-count quantity the manuscript aggregates. It uses several thresholds side by side. Differences: the percentile is within journal-year, not field-year or all papers of the year; the ASI counts are NOT ranked against a peer group defined by career start year (they are raw counts; the text claims comparability 'across time periods' only through the PRI normalization); and the thresholds are not aggregated or integrated into one score.
lead: https://doi.org/10.1007/s11192-026-05608-y — Gyorffy, B., Weltz, B. and Szabo, I. (2026), A global interdisciplinary platform for academic age-normalized researcher evaluation, Scientometrics (published 23 Mar 2026; CC BY 4.0)
  version-read: publisher open-access version of record (CC BY 4.0 per Crossref)
  archived: gs://scholar-version2-literature/sources/gyorffy2026--10.1007_s11192-026-05608-y-b26e1173.pdf sha256:9d6f075e62a0d7e720fcd59e7835277873c98eda1b60b066b1fa0a28d00bc2db
  license: CC-BY-4.0 (per publisher)
  evidence: "The benchmarks utilized were stratified by career stage, assessed at each year after a researcher's first publication. A composite score was developed to rank publication performance into deciles (D1-D10), where D1 represents the highest level of achievement." (Abstract); "Each of the three analyzed features (H-index, annual citation count, and the count of publications) was calculated for every publication age, enabling comparison of a particular researcher with others who had equal length of scholarly career." (Methods, 'Computing percentile tables'); "Because the majority of authors do not have a high-impact paper, it was not possible to establish a percentile ranking using the number of high-impact publications. Therefore, we have set up the system so that each high-impact publication increases the total score by a fixed value." (Methods, composite score); "total score = (percentile[H-index] + percentile[citation count for all preceding publications in the most recent year] + 2*percentile[scholarly papers published in the last five years])/4 + (sum of high impact articles * 4)" (Methods, composite score); "we first determined a citation threshold by using the highly-cited thresholds from Web of Science which correspond to publications cited in the top 1% within their field and within their publication year." (Methods, high-impact publications) — overlap: Part (b), career-age-normalized percentile of the scholar's metrics against a peer group at the same number of years since first publication, and a composite of percentiles. Differences: percentiles are of scholar-level aggregates (h-index, yearly citations, 5-year publication count) within field and publication age, not of paper-level citation percentiles within publication year; the count of high-impact papers (top 1% within field and publication year, a single threshold) is a fixed-increment add-on, not percentile-ranked against the cohort; there is no step (a) paper percentile and no multi-threshold aggregation.
lead: https://doi.org/10.1371/journal.pone.0280480 — Gyorffy, B., Weltz, B. and Szabo, I. (2023), Supporting grant reviewers through the scientometric ranking of applicants, PLOS ONE (PMC9858403; published 20 Jan 2023)
  version-read: PMC HTML page of the published article
  archived: gs://scholar-version2-literature/sources/gyorffy2023--10.1371_journal.pone.0280480-f4fbe588.html sha256:44ad0abf80dbe2350d58c0490c3cd7432064d71decfabebb941e6c340342b9e1
  license: unknown (treated as restricted; private store)
  evidence: "Finally, researchers are ranked across researchers with the same publication age within the same scientific section into percentiles." (Methods, 'Publication and citation data'); "To increase simplicity, the average of the three percentiles is computed (in this, the number of publications has a double weight), and this final score is transformed into deciles between D1 and D10, where D1 is the best." (Methods, 'Publication and citation data'); "Publication-age matched researchers are then ranked and the results are provided to grant reviewers." (Abstract) — overlap: Earlier version of the same platform; a scholar is percentile-ranked against peers with the same publication age (years since first publication) and field, then percentiles are averaged into a score. Same difference as the 2026 paper: percentiles are of scholar-level aggregates (h-index, yearly citations, recent publication count, plus a count of highly cited papers), not of paper-level within-year citation percentiles, and no multi-threshold aggregation.
leads-found: 3; Closest prior work found: Pudovkin & Garfield's PRI/ASI (paper percentile within journal-year, then counts of papers at 99/95/75 thresholds, with no peer-cohort ranking and no aggregation) and Gyorffy et al.'s scientometrics.org ranking (scholar-level metrics ranked against peers of the same publication age, with no paper-level percentiles). Nothing on point was found for the combination (a)+(b)+(c) or for stochastic-dominance comparisons of citation distributions of individual researchers, but OpenAlex and Semantic Scholar were unreachable (rate-limited), so that is not a complete negative. Unread candidates: Ronda-Pupo, G.A. (2017), Research evaluation of author's citation-based performance through the relative author superiority index, Transinformacao 29(2), doi:10.1590/2318-08892017000200006 (Full text 403 from scielo.br and redalyc; only the Crossref abstract was read. The abstract (a Relative Author Superiority Index built on PRI and ASI, said to overcome 'the age of publications, length of the author's career') suggests a follow-up on ASI that may add a relative or cohort comparison, so it bears on (b).); Seppanen, J.-T., Varri, H. and Ylonen, I. (2022), Co-citation Percentile Rank and JYUcite, Scientometrics, doi:10.1007/s11192-022-04393-8 (bioRxiv 10.1101/2020.09.23.310052) (bioRxiv full text returned 429; only the Crossref abstract was read. It is an output-level percentile rank within a co-citation set, not an author-level cohort indicator, so likely off point.); Yamamoto, K. and Yasunaga, H. (2022), A percentile rank score of group productivity, Scientometrics, doi:10.1007/s11192-022-04278-w (Only the Crossref abstract was read. It is a group (university) percentile rank score of publication productivity in rank classes, not an individual career-cohort indicator.); Bornmann, L. (2013), How to analyze percentile citation impact data meaningfully in bibliometrics: the statistical analysis of distributions, percentile rank classes, and top-cited papers, JASIST, doi:10.1002/asi.22792 (Appeared in search, not opened; likely close to the already cited bornmann2013use and about percentile rank classes of units, not a career-cohort indicator.); Albarran, Ortuno, Ruiz-Castillo and related Ruiz-Castillo papers on comparing citation distributions (e.g. Ruiz-Castillo 2011, SERIEs, doi:10.1007/s13209-011-0074-3) (Surfaced only as Crossref titles in a stochastic-dominance query; not opened. Unclear whether any treats individual researchers or percentile profiles.)
status: search incomplete — OpenAlex and Semantic Scholar were rate-limited for the whole run (Crossref, arXiv and web search ran); nonreusable

## [2026-11-05T16:07Z] novelty — "PiP is bounded, never decreases when a paper is added or gains citations, gives nothing to papers below the cutoff, and reads as the probability that the scholar has more papers than a random peer above a random impact threshold (Section~\ref{sec:pip})."
searched: OpenAlex: 6 queries (probability superiority / Mann-Whitney / AUC / stochastic dominance / percentile probability) -> 0 (rate-limited, no results); Crossref: probability randomly chosen paper more citations Mann-Whitney; stochastic dominance citation distributions; AUC ROC citation indicator; common language effect size citations; percentile rank probability; ordinal dominance citation -> ~8 each, about 48 looked at; Crossref (bibliographic): 7 scientometrics-targeted queries (stochastic dominance, probabilistic percentile, Mann-Whitney effect size, ROC/AUC, random paper more cited, dominance ranking, Albarran Ruiz-Castillo) -> ~8 each, about 56 looked at; Semantic Scholar: 5 queries -> 0 (HTTP 429 rate-limited); arXiv: 6 queries (stochastic dominance/citation; randomly chosen/research units; Mann-Whitney/bibliometric; ROC/bibliometric; percentile ranking; author-level percentile career stage) -> 0-1 each; arXiv id_list lookups for 2404.05861, 1401.2254, 1607.03179; WebSearch: Pudovkin Garfield Author Superiority Index -> 10; probability randomly chosen paper institution Mann-Whitney AUC -> 10; area-under-curve scientist indicator thresholds -> 10; stochastic dominance citation distributions universities -> 10; Unpaywall for 10.1080/09737766.2009.10700871 -> closed access | channel failures: OpenAlex: HTTP error 'Rate limit exceeded' (shared free daily budget exhausted); no OpenAlex results obtained.; Semantic Scholar API: HTTP 429 Too Many Requests on all queries.; tandfonline.com (Pudovkin & Garfield 2009): Cloudflare bot check ('Just a moment...'); not bypassed. Unpaywall shows no OA copy.
lead: 10.1016/j.joi.2016.12.006 ; arXiv:1607.03179v3 — Milojevic, Radicchi & Bar-Ilan (2017), Citation success index - An intuitive pair-wise journal comparison metric, Journal of Informetrics 11(1) [DOI/year from Crossref record: 10.1016/j.joi.2016.12.006, issued 2017-02]; author list from arXiv record
  version-read: arXiv v3 preprint (not the publisher VoR)
  archived: gs://scholar-version2-literature/sources/milojevic2017--10.1016_j.joi.2016.12.006-ca385ee7.pdf sha256:f9b43ad3aa37b5d375986560759e5b03b0b558729664051b909fe0464f914d39
  license: unknown (treated as restricted; private store)
  evidence: "Citation success index is the probability that a random paper in one journal has more citations than a random paper in another journal (50% means the two journals do equally well)." (Abstract); "This probability is equivalent to the fraction of all "wins" if every article from target journal had been compared with every article from the reference journal. The calculation of the citation success index is carried out using the rank sum." (Sec. 2 (Data and methods), after Eq. 1) — overlap: Reads a citation-based indicator as a probability-of-superiority (Mann-Whitney/rank-sum) over a random peer, with ties counted half. Differs: compares two journals' paper distributions pairwise, not a scholar against a cohort of peers; no productivity (paper count) component, no threshold averaging, no boundedness or monotonicity properties of an author indicator.
  notes: dropped a re-typed equation that was not a verbatim quote
lead: arXiv:2404.05861v2 (http://arxiv.org/abs/2404.05861) — Gates, Gao & Mane (2025, arXiv v2 10 Jun 2025; first posted 2024-04-08), The increasing fragmentation of global science limits the diffusion of ideas, arXiv preprint (cs.SI); no journal DOI found in the arXiv record
  version-read: arXiv v2 preprint
  archived: gs://scholar-version2-literature/sources/gates2025--arxiv_2404.05861-72cc0a0d.pdf sha256:baa97fb2573dac27b7ef8c5be79fcea2581f52578882cf55a7c4eb5d6abfce22
  license: unknown (treated as restricted; private store)
  evidence: "This yields a measure equivalent to the Area Under the Curve (AUC) in a two-sample rank test. It represents the probability that a randomly selected paper from the target country is cited more frequently by the source country than a randomly selected paper from the rest of the world." (Results, 'A pairwise measure of national citation preference'); "This AUC-based interpretation is also mathematically equivalent to the Mann-Whitney U statistic (also known as the Wilcoxon rank-sum statistic)" (Results, same section) — overlap: Explicit AUC / Mann-Whitney probability-of-superiority reading of a citation-based measure, comparing a unit (country) with the rest of the world. Differs: countries, not scholars; measures citing preference, not scholar impact; no threshold averaging or productivity percentile.
lead: http://ideas.repec.org/p/grt/wpegrt/2011-22.html ; https://cahiersdugretha.u-bordeaux.fr/2011/2011-22.pdf — Carayol & Lahatte (2011), Dominance relations when both quantity and quality matter, and applications to the comparison of US research universities and worldwide top departments in economics, Cahiers du GREThA n. 2011-22 (working paper)
  version-read: GREThA working paper
  archived: gs://scholar-version2-literature/sources/carayol2011--cahiersdugretha.u-bordeaux.fr_2011_2011-22.pdf-c91cb0a5--2026-11-05-75505b6b.pdf sha256:75505b6b641360267af79329ad3c10db3e20f7cf429a8cfd9cbd55ce8957f69a
  license: unknown (treated as restricted; private store)
  evidence: "we propose an extension of the concept of stochastic dominance intensively used in economics for the comparison of composite outcomes both the quality and the quantity of which do matter." (Abstract); "the value function of any article is positive and non-decreasing with its quality." (Sec. on assumptions (around line 655 of extracted text; verify section number)) — overlap: Axiomatic, monotonicity-based (non-decreasing in quality, quantity) comparison of units combining volume and citation impact, via a dominance concept. Relevant to the manuscript's monotonicity-type properties. Does not, in what was read, give a random-peer probability interpretation and does not define a single bounded score; I did not read the full technical sections.
leads-found: 3; Found prior work reading citation comparisons as a Mann-Whitney/AUC probability that a random paper of one unit beats a random paper of another (journals: Milojevic et al.; countries: Gates et al.), plus an axiomatic quantity-and-quality dominance paper (Carayol & Lahatte); none of what I read averages over random citation thresholds or applies it to a scholar versus peers by paper count. Search was incomplete: OpenAlex and Semantic Scholar were rate-limited and Pudovkin & Garfield was blocked, so the scan is not exhaustive. Unread candidates: Pudovkin & Garfield (2009), Percentile Rank and Author Superiority Indexes for Evaluating Individual Journal Articles and the Author's Overall Citation Performance, Collnet J. Scientometrics and Information Management 3(2), doi 10.1080/09737766.2009.10700871 (Publisher page behind a Cloudflare bot check (not bypassed), Unpaywall lists no OA copy. A web-search summary says ASI counts an author's papers with percentile rank at or above thresholds (99/95/75); no probability interpretation was found, but this is unverified. A Garfield-site copy (garfield.library.upenn.edu/papers/aipegdalianchina2009.pdf) was suggested but not fetched.); Yamamoto & Yasunaga (2022), A percentile rank score of group productivity: an evaluation of publication productivity for researchers from various fields, Scientometrics, 10.1007/s11192-022-04278-w (Found in Crossref only; not opened. Title suggests percentile-based productivity scoring, potentially close to the PiP productivity percentile.); Albarran, Ortuno & Ruiz-Castillo (2011), Average-based versus high- and low-impact indicators for the evaluation of scientific distributions, Research Evaluation, 10.3152/095820211x13164389670310 (Found in Crossref only; not opened. Possible stochastic-dominance/threshold-based citation indicators.); Bornmann & Williams (2020), An evaluation of percentile measures of citation impact, and a proposal for making them better, Scientometrics, 10.1007/s11192-020-03512-7 (Found in Crossref only; may be the already-cited bornmann2020evaluation, not opened.); Perianes-Rodriguez & Ruiz-Castillo (2016), University citation distributions, JASIST 67(11):2790-2804 (Seen only in a web-search summary; not opened.)
status: search incomplete — OpenAlex and Semantic Scholar were rate-limited for the whole run (Crossref, arXiv and web search ran); nonreusable

## [2026-11-05T16:07Z] novelty — "PiP predicts a scholar's future top papers better than the $h$-index in every setting and field we test,"
searched: OpenAlex: 4 queries (h-index predict future vs percentile/top 10%) -> 0 (rate-limited, HTTP error body); Semantic Scholar: 2 queries -> 0 (HTTP 429); Crossref: 'predictive power of h-index versus percentile indicators future performance of scientists' -> 8 looked at, none relevant; Crossref: 'predicting future citation impact of researchers h-index top 10% papers' -> 8 looked at, none relevant; arXiv: 4 queries (h-index + predict + future + percentile / top 10% / author-level) -> ~24 looked at; 1306.0114, 1606.05905, 2207.09655 are Penner/Sinatra/h-prediction works (cited or not about percentile indicators); WebSearch: predictive validity of h-index vs top 10% publications count -> 9 looked at; WebSearch: percentile-based author indicators predict future citations vs h-index Ptop 10% -> 9 looked at (2103.16025 is tian2021predictability, already cited); WebSearch: Danell 2011 track record / highly cited papers -> 9 looked at; WebSearch: Lehmann Jackson Lautrup indicators of scientific performance -> 9 looked at; WebSearch: which bibliometric indicator best predicts future top-cited papers of an author -> 9 looked at; WebSearch: Abramo D'Angelo past productivity predicts future performance -> 9 looked at (nothing comparing percentile indicators with h) | channel failures: OpenAlex: HTTP error 'Rate limit exceeded' (shared free daily budget exhausted); no OpenAlex results obtained; Semantic Scholar API: HTTP 429 Too Many Requests; no results obtained; Wiley (Danell 2011, DOI 10.1002/asi.21443): publisher page returned HTTP 403, no open copy found; Crossref returned a wrong record for that DOI. Not read.
lead: https://doi.org/10.1007/s11192-014-1476-3 (arXiv:1404.3084v1) — Havemann F., Larsen B. (2014/2015), Bibliometric Indicators of Young Authors in Astrophysics: Can Later Stars be Predicted?, Scientometrics (arXiv:1404.3084)
  version-read: arXiv v1 preprint, 11 Apr 2014
  archived: gs://scholar-version2-literature/sources/havemann2014--10.1007_s11192-014-1476-3-34112344.pdf sha256:6848a17756f67ace4c39d2fc314cc464c7945432da9364b957eab4cd83b1c5e9
  license: unknown (treated as restricted; private store)
  evidence: "We find that field and citation-window normalisation substantially improves the predicting power of citation indicators. The two indicators of total influence based on citation numbers normalised with expected citation numbers are the only indicators which show differences between later stars and random authors significant on a 1 % level. Indicators of paper output are not very useful to predict later stars. The famous h-index makes no difference at all between later stars and the random control group." (Abstract); "We have to leave a test of the percentile method with our samples to further work due to a lack of citation data of control samples." (Sec. 2 (indicators, percentile discussion)); "We did not consider any indicator based on the number of highly cited papers because this contradicts our sampling procedure: we selected later stars who have no highly cited paper in their first five years of publishing." (Sec. 2 (indicators)) — overlap: A prior comparison of 16 indicators, including the h-index, on how well they pick out authors who later co-author highly cited papers (a future top-paper outcome). Field- and window-normalised citation indicators beat the h-index, which did not discriminate at all. It is a prediction test of the kind the manuscript runs, but with a small sample (331 authors, astrophysics only, 5 years after first paper). It uses normalised citation counts, not percentiles, and explicitly did not test percentile or top-cited-count indicators.
lead: https://doi.org/10.1007/s11192-007-1868-8 (arXiv:physics/0701311v1) — Lehmann S., Jackson A. D., Lautrup B. E. (2007/2008), A quantitative analysis of measures of quality in science (later: A quantitative analysis of indicators of scientific performance), Scientometrics (arXiv:physics/0701311)
  version-read: arXiv v1 preprint, 27 Jan 2007
  archived: gs://scholar-version2-literature/sources/lehmann2007--10.1007_s11192-007-1868-8-82c25743.pdf sha256:a48848e70d95a21019ecc9a34d358909f479b7db68e117d8ff7960d9391195d6
  license: unknown (treated as restricted; private store)
  evidence: "Using scaling arguments, we demonstrate that the best of these measures require approximately 50 papers to draw conclusions regarding long term scientific performance with usefully small statistical uncertainties." (Abstract); "Initial assignments of author A based on mean, median, 65th percentile, and maximum citations all appear to provide an accurate reflection of his full citation record with a satisfactorily small uncertainty. Hirsch's measures falls somewhere between the best and worst choice of measures." (Sec. on Bayesian bin assignments (Figs 3-6)) — overlap: An older head-to-head comparison of author-level measures (h-index, h normalised by age or papers, mean, median, 65th percentile of the author's own citation distribution, max) for how well early records predict later position. Its 'percentile' is a percentile of the author's own paper-citation distribution, not a cohort percentile of papers or authors. It finds mean/median/65th percentile discriminate better than h, with h in the middle. The measure of success is consistency of an author's bin over papers, not future top papers.
leads-found: 2; No prior study found that compares percentile-based author indicators (top-10% counts, sums or means of paper percentiles, cohort percentiles) against the h-index for predicting future output; the searches turned up only two partial precedents (Havemann & Larsen on normalised-citation indicators, Lehmann et al. on within-author percentile measures), both beating or matching h without testing cohort-percentile indicators. OpenAlex and Semantic Scholar were unavailable (rate limits), so the search relied on Crossref, arXiv and web search and is incomplete. Unread candidates: Danell R. (2011), Can the quality of scientific work be predicted using information on the author's track record?, JASIST 62(1):50-60 (DOI believed 10.1002/asi.21443, unverified) (Publisher page returned HTTP 403, no open copy found; only secondhand summaries seen (past performance predicts future impact, strongest at the high end of the citation distribution). Not verified.); Bornmann L., Leydesdorff L., Wang J. (2013), Which percentile-based approach should be preferred for calculating normalized citation impact values?, J. Informetrics (Paywalled (ScienceDirect); seen in search snippets only. Paper-level comparison of percentile approaches (one snippet says SCImago indicator predicts long-term impact from early citations), not author-level.); Lehmann S., Jackson A. D., Lautrup B. E. (2006), Measures for measures, Nature 444:1003 (Nature commentary, paywalled; seen in snippets only.); van Leeuwen T. (2008), Testing the validity of the Hirsch index for research assessment purposes, Research Evaluation (Named in a read paper's introduction; not retrieved.)
status: search incomplete — OpenAlex and Semantic Scholar were rate-limited for the whole run (Crossref, arXiv and web search ran); nonreusable

## [2026-11-05T16:07Z] novelty — "Measured at the same career age, PiP identifies future high performers at least as accurately as the other indicators we test, and it reaches the accuracy of the $h$-index at career age 10 1.6 to 1.9 years sooner"
searched: Crossref: rising stars / early-career / future high performing / top scientists indicator queries (10 queries) -> ~70 records scanned, mostly noise; surfaced Lee 2019; arXiv: rising stars + h-index; predicting future h-index early career; early identification top researchers; top performers indicators (10 queries) -> ~35 records scanned; WebSearch: early-career bibliometric indicators predict future top researchers; Abramo/D'Angelo persistence of top performers; rising stars percentile career age; Daud/Mazloumian (4 queries); Crossref DOI lookups for Jensen 2008, Lee 2019, Havemann & Larsen 2014, Shirouyeh 2025 | channel failures: OpenAlex: HTTP rate limit exceeded (shared daily budget used up), no results obtained; Semantic Scholar API: HTTP 429 on all queries, no results obtained; Unpaywall: no OA copy for Lee 2019; ResearchGate not used
lead: https://doi.org/10.1007/s11192-014-1476-3 (arXiv:1404.3084) — Havemann & Larsen (2014), Bibliometric indicators of young authors in astrophysics: Can later stars be predicted?, Scientometrics
  version-read: arXiv v1 preprint
  archived: gs://scholar-version2-literature/sources/havemann2014--10.1007_s11192-014-1476-3-34112344.pdf sha256:6848a17756f67ace4c39d2fc314cc464c7945432da9364b957eab4cd83b1c5e9
  license: unknown (treated as restricted; private store)
  evidence: "We test 16 bibliometric indicators with respect to their validity at the level of the individual researcher by estimating their power to predict later successful researchers." (abstract); "field and citation-window normalisation substantially improves the predicting power of citation indicators. ... The famous h-index makes no difference at all between later stars and the random control group." (abstract) — overlap: Compares indicator accuracy at identifying future highly cited authors from first 5 publishing years, including a percentile-based paper approach and h-type indices; differs: one field, 29 later stars vs 74 random authors, tests significance of group differences, no peer-cohort career-age percentile of volume and no years-sooner measure.
lead: https://doi.org/10.1007/s11192-007-2014-3 (arXiv:0811.0237) — Jensen, Rouquier & Croissant (2008/2009), Testing bibliometric indicators by their prediction of scientists promotions, Scientometrics
  version-read: arXiv v1 preprint
  archived: gs://scholar-version2-literature/sources/jensen2008--10.1007_s11192-007-2014-3-7cab7b64.pdf sha256:76c4f4a4368f0bb58cddf3b294620885d0726f3c444bde3649c897fa8a7b3bd2
  license: unknown (treated as restricted; private store)
  evidence: "Overall, however, the Hirsch index h provides the least bad correlations, followed by the number of papers published. It is important to realize however that even h is able to recover only half of the actual promotions." (abstract) — overlap: Compares several indicators (h, papers, citations, mean citations) on how well they identify later promoted CNRS scientists, a performance-outcome accuracy comparison; differs: promotion outcome rather than future bibliometric performance, no career-age percentiles, no years-sooner metric.
lead: https://doi.org/10.1007/s11192-016-2097-9 (PMC5124050) — Lindahl & Danell (2016), The information value of early career productivity in mathematics: a ROC analysis of prediction errors in bibliometricly informed decision making, Scientometrics
  version-read: PMC author manuscript/open version
  archived: gs://scholar-version2-literature/sources/lindahl2016--10.1007_s11192-016-2097-9-eb7cebc4.html sha256:5ea3c5ba2db2522facbc81d9e046618640d82f425d49b85e5ae486f2f5403d35
  license: unknown (treated as restricted; private store)
  evidence: "We used ROC analysis to evaluate a bibliometric indicator as a tool for binary decision making. The dataset consisted of 451 early career researchers in the mathematical sub-field of number theory. We investigated the effect of three different definitions of top performance groups—top 10, top 25, and top 50 %" (abstract); "future performance is more predictable if the definition of a high performance group is more exclusive." (abstract) — overlap: ROC-style accuracy of an early-career indicator (publication rate) at identifying future top 10/25/50% performers, i.e. same type of accuracy evaluation; differs: a single indicator, one subfield, no indicator comparison and no years-sooner measure.
lead: https://doi.org/10.1038/s41598-023-46050-x (PMC10620415) — Krauss, Danus & Sales-Pardo (2023), Early-career factors largely determine the future impact of prominent researchers: evidence across eight scientific fields, Scientific Reports
  version-read: PMC open version
  archived: gs://scholar-version2-literature/sources/krauss2023--10.1038_s41598-023-46050-x-87cb08df.html sha256:453d044194aae3c03d85080fea189229a3e7133faaa69347d21e6ef0838967ed
  license: unknown (treated as restricted; private store)
  evidence: "Our results show that the early-career factors we study can explain h-index quartiles as early as 5 years after the first publication" (Results); "as early as 5 years after the first publication, we can already make accurate predictions of whether a prominent researcher is going to be within the top quartile of leading researchers later on or not." (Results) — overlap: Speaks to how early (years after first publication) top-quartile future performers can be predicted; differs: uses categorical early-career factors on only the 100 most prominent researchers per field, not a comparison of bibliometric indicators and no years-sooner statement.
lead: arXiv:1606.05752 — Zhang, Liu, Yu, Zhang & Zhou (2016), Identifying the Academic Rising Stars, arXiv preprint
  version-read: arXiv v1 preprint
  archived: gs://scholar-version2-literature/sources/zhang2016--arxiv_1606.05752-2150c77e.pdf sha256:4614cec3f8b6ef22395a7c2710004caf11e2f94edc72e11b8005c3315718b073
  license: unknown (treated as restricted; private store)
  evidence: "we solve the problem of how to effectively predict the top k% researchers who achieve the highest citation increment in Δt years" (abstract); "Base-1 which predicts the rising stars according to current author's citation value and Base-2 which ranks the authors by using their average citation increments in previous two years." (Methods, Benchmark methods) — overlap: Top-k% future performer prediction among young researchers with baseline indicators; differs: machine-learning ranking of citation increment, not a percentile indicator comparison at matched career age, no years-sooner measure.
lead: https://doi.org/10.3390/metrics2040022 (arXiv:2407.14559) — Shirouyeh, Schiffauerova & Ebadi (2025), Predicting Star Scientists in the Field of Artificial Intelligence: A Machine Learning Approach, Metrics
  version-read: arXiv v1 preprint
  archived: gs://scholar-version2-literature/sources/shirouyeh2025--10.3390_metrics2040022-8a0c99a0.pdf sha256:26ea2be12a336223b52905ddbd9b7eb9d57c3e611926a92a444a5e1f9396a108
  license: unknown (treated as restricted; private store)
  evidence: "Identifying potential star scientists before their performance becomes outstanding is important for recruitment, collaboration, networking, or research funding decisions." (abstract) — overlap: Early-career (first 5 years) features predict later h-index growth stars; differs: ML feature importance, not indicator-vs-indicator accuracy, no years-sooner measure.
leads-found: 6; No source found that measures how many years sooner one indicator reaches another's accuracy at identifying future top performers; the closest leads are indicator-comparison and early-career prediction studies (Havemann & Larsen 2014, Jensen et al. 2008, Lindahl & Danell 2016). OpenAlex and Semantic Scholar were unavailable (rate limits), so coverage comes from Crossref, arXiv and WebSearch only. Unread candidates: Lee (2019), Predicting the research performance of early career scientists, Scientometrics, doi:10.1007/s11192-019-03232-7 (Only the Springer abstract page was read (abstract page read, not archived); it is a regression on 13 early-career factors, not an indicator-accuracy comparison; no OA full text); Daud et al. (2020), Finding rising stars in bibliometric networks (survey), Scientometrics 124, 633-661 (Seen only in search results, not opened); Carbon_h-factor (PubMed 22194909) predicting individuals' research impact at early career (PubMed page returned no usable metadata; not read); Abramo, D'Angelo & Soldatenkova (2017) persistence of top performers; Abramo, D'Angelo & Viel promotion-prediction work (Seen only via search snippets, not opened)
status: search incomplete — OpenAlex and Semantic Scholar were rate-limited for the whole run (Crossref, arXiv and web search ran); nonreusable

## [2026-11-05T16:07Z] novelty — "Measured early in a career, PiP anticipates later academy memberships, fellowships and prizes better than the $h$-index among all scholars;"
searched: Crossref: h-index predicts election National Academy of Sciences membership -> 8; Crossref: bibliometric indicators Nobel laureates citation prediction -> 8; Crossref: early career citation indicators predict later awards honors scientists -> 8; Crossref: predicting scientific awards from citation data -> 8; Crossref: h-index validation awards honors membership academy -> 8; Crossref: bibliographic lookup of Haslam & Baes title -> 3; arXiv API (5 boolean queries on h-index/Nobel/NAS/awards/prize) -> 0 results each (empty responses, see channel_failures); WebSearch: h-index predicts election to NAS, Royal Society fellows, Nobel laureates validation -> 9; WebSearch: Ayaz Masood comparison of researcher impact indices award winners -> 10; WebSearch: early career citation metrics predict later election NAS / Royal Society / ACM Fellow -> 9; WebSearch: predicting Nobel Prize winners from bibliometric indicators ... -> 9; WebSearch: Kosmulski 2020 h-index prizes Nobel laureates -> 9 | channel failures: OpenAlex: HTTP error 'Rate limit exceeded' (shared free daily budget used up), no results retrieved; Semantic Scholar API: HTTP 429 Too Many Requests on all queries, no results retrieved; arXiv API: returned empty bodies for every query tried (no results; not a confirmed empty result); Crossref: one query (h-index Fellows Royal Society validity prizes) returned a non-JSON response; Crossref results otherwise noisy (mostly award notices); No source specific to Royal Society or ACM Fellow election was surfaced by any channel; this is incompletely searched, not confirmed absent
lead: https://doi.org/10.1371/journal.pone.0253397 — Koltun V, Hafner D (2021), The h-index is no longer an effective correlate of scientific reputation, PLoS ONE 16(6): e0253397
  version-read: publisher open version (CC BY)
  archived: gs://scholar-version2-literature/sources/koltun2021--10.1371_journal.pone.0253397-09778d3a.pdf sha256:7c653ff28cf1c7d66a0a6698a825317480c8bdc06d03ce744ccb5c156d97b9bb
  license: CC-BY)-7 (per publisher)
  evidence: "In contrast, we study the ability of an indicator to predict a scientist's future reputation as evidenced by scientific awards. (Hirsch recognized this as a meaningful goal when he wrote "how likely is each candidate to become a member of the National Academy of Sciences 20 years down the line?", but did not operationalize this [7].)" (Results, 'Predictive power and other measures'); "We measure the correlation of rankings induced by scientometric indicators in a given year (e.g. 2010) with rankings induced by awards in a future year (e.g. 2015)." (Results, 'Predictive power and other measures'); "The predictive power of the h-index has declined since its introduction (average τ = 0.32 in 2004 versus 0.24 in 2014). ... h-frac has the highest predictive power across datasets and its predictive power is stable over time" (Results, Fig 2A); "cross-referenced the scientists in our datasets against lists of recipients of scientific awards ...: Nobel Prizes, Breakthrough Prizes, membership in the National Academies, fellowship of the American Physical Society, Turing Award, fellowship of the Econometric Society, and other distinctions" (Introduction) — overlap: Closest prior work found. Same design as the claim: an indicator measured in year t is rank-correlated with awards (NAS/National Academies, APS fellowship, Nobel, Turing and others) received later (5 years ahead), comparing the h-index with many other indicators (fractional h-frac, hI, hm, etc.). Differences: sample is 4,000 prominent scientists in four fields (not 'all scholars'); not restricted to early-career measurement, no percentile/age-cohort-based indicator; finding is that fractional h variants beat h.
lead: https://doi.org/10.1007/s11192-023-04870-8 — Haslam N, Baes N (2023), Scientific eminence and scientific hierarchy: bibliometric prediction of fellowship in the Australian Academy of Science, Scientometrics 128:6659-6674
  version-read: publisher open access version (CC BY)
  archived: gs://scholar-version2-literature/sources/haslam2023--10.1007_s11192-023-04870-8-d74f890c.pdf sha256:e429dc9de416344edf45a618a9b6f91cff22e81b23cf2fca9eb4d50cc9834b95
  license: CC-BY) (per publisher)
  evidence: "Research metrics are known to predict many markers of scientific eminence, but fellowship in learned academies has not been examined in this context." (Abstract); "As yet, there has been little published research on bibliometric prediction of fellowship, although Hirsh (2008) demonstrated that NAS fellows tended to have very high h-index scores." (Introduction); "the composite index and its components all strongly differentiated Australian researchers who were elected as AAS fellows from those who were not." (Abstract); "Hirsch h-index 0.70 0.63 19.26 514.06 < .001 0.94" (Table 3 (fellow vs non-fellow, effect sizes) (the c-index effect size d = 1.13 is in the same table; worker's annotation removed from the quote)) — overlap: Tests whether citation indices (h, hm, total citations and Ioannidis composite c) discriminate learned-academy fellows (AAS) from non-fellows and compares the h-index with other indicators. Differences: career-long Scopus snapshot (Sept 2022) among top global researchers, so concurrent discrimination rather than early-career measurement, and the sample is restricted to top scientists.
lead: https://doi.org/10.1007/s11192-020-03378-9 — Kosmulski M (2020), Nobel laureates are not hot, Scientometrics 123:487-495
  version-read: publisher open access version
  archived: gs://scholar-version2-literature/sources/kosmulski2020--10.1007_s11192-020-03378-9-6a5fb61b.pdf sha256:ec9aae9527ace3e8faa014dba90260ff162a3f2288ce0e5d835e5493eddec608
  license: unknown (treated as restricted; private store)
  evidence: "Ninety recent Nobel laureates were in top 100,000 scientists in terms of c, and 45 recent Nobel laureates were in top 6000 scientists in terms of c. Only 32 recent Nobel laureates were in top 6000 scientists in terms of ch (Hirsch-type index with self-citations excluded)" (Abstract) — overlap: Compares the h-type index with a composite and other indicators at ranking Nobel laureates (2010-2019) among all scientists. Differences: contemporaneous (not early-career) measurement; Nobel only; no percentile or cohort indicator.
lead: https://arxiv.org/abs/1910.02369 — Kinouchi O, Holanda AJ, Cardoso GC (2019), Citation network centrality: a scientific awards predictor?, arXiv:1910.02369v2 [cs.DL] (Elsevier preprint)
  version-read: arXiv v2 (preprint)
  archived: gs://scholar-version2-literature/sources/kinouchi2019--arxiv_1910.02369-b1456307.pdf sha256:f5b9c1792dca35a1235b7867caffd961017cbea9cfd64a577b7423cef963a7ec
  license: unknown (treated as restricted; private store)
  evidence: "The K-index ... has several advantages over Hirsh's h-index and, in previous studies, has shown better correlation with Nobel prizes than any other index given by the Web of Science, including the h-index." (Abstract); "we perform an experiment using the K-index, producing a shortlist of twelve candidates for major scientific prizes, including the Physics Nobel award, in the near future." (Abstract) — overlap: Prospective test of an alternative indicator versus h at predicting Nobel prizes. Differences: short list from Clarivate Highly Cited Researchers, physics only, no early-career design, small and informal test.
lead: https://arxiv.org/abs/0808.2517 — Gingras Y, Wallace ML (2008/2010), Why it has become more difficult to predict Nobel Prize winners: a bibliometric analysis of nominees and winners of the chemistry and physics prizes (1901-2007), arXiv:0808.2517 (published in Scientometrics 82)
  version-read: arXiv preprint
  archived: gs://scholar-version2-literature/sources/gingras2008--arxiv_0808.2517-f3cf3826.pdf sha256:48ad9edac5a22b17236bb2d13af85d265e15c718947f339c31218f84d9f05a24
  license: unknown (treated as restricted; private store)
  evidence: "Changes in the size and organization of the two fields result in a rapid decline of predictive power of bibliometric data over the century." (Abstract) — overlap: Historical test of citation rankings for anticipating Nobel winners; finds weakening predictive power. Differs: citation counts and centrality, no h-index comparison with percentile indicators, no early-career measurement.
lead: https://arxiv.org/abs/2110.14115 — Kormendy J (2021), Metrics of research impact in astronomy: Predicting later impact from metrics measured 10-15 years after the PhD, arXiv:2110.14115 (accepted PNAS 2021)
  version-read: arXiv v2
  archived: gs://scholar-version2-literature/sources/kormendy2021--arxiv_2110.14115-f37dd906.pdf sha256:5981bd1899b6bf2f39796918f5fc5bf3d416c7219bf988e8b8dd2bc10660dc8f
  license: unknown (treated as restricted; private store)
  evidence: "Average prediction machines are constructed for different cohorts of 1990-2007 PhDs and used to postdict 2017 impact from metrics measured 10, 12, and 15 years after the PhD." (Abstract); "Calibration is based on perceived 2017 impact as voted by 22 experienced astronomers for 510 faculty members" (Abstract) — overlap: Early-career (10-15 years post-PhD) metrics used to predict later impact, by PhD cohort. Differs: target is peer-voted impact (an impact scale anchored partly on prizes and academy membership), not honors themselves; astronomy only; raw citation metrics, not percentile-based or compared with h.
lead: https://doi.org/10.1371/journal.pone.0233765 — Ayaz S, Masood N (2020), Comparison of researchers' impact indices, PLoS ONE 15(5): e0233765
  version-read: publisher open version (CC BY)
  archived: gs://scholar-version2-literature/sources/ayaz2020--10.1371_journal.pone.0233765-b0330451.pdf sha256:117ad69f98aecec2e8c2a22c14186a4476fc280aee760232761a723d5dba6a94
  license: CC-BY)-117 (per publisher)
  evidence: "completing-h and K-index succeeded in bringing highest number of award winners in top ranks. Thus, having found this, we posit our conclusion that these indices perform comparatively better than h-index." (Discussion/Conclusion) — overlap: Compares h with h variants at ranking computer-science award winners. Differs: cross-sectional, one field, no early-career or percentile design.
leads-found: 7; Prior work does test whether citation indicators, h-index included, rank or anticipate later NAS/National Academy membership, APS fellowship, Nobel and other prizes (closest: Koltun & Hafner 2021, 5-year-ahead award prediction; also Haslam & Baes 2023, Kosmulski 2020), but none found uses early-career measurement with percentile/cohort indicators; OpenAlex and Semantic Scholar were unavailable and no Royal Society or ACM Fellow study surfaced, so coverage is incomplete. Unread candidates: Krauss A, Danus L, Sales-Pardo M (2023), Early-career factors largely determine the future impact of prominent researchers, Sci Rep (doi 10.1038/s41598-023-46050-x) (Full PDF staged (earlycareer_sr2023.pdf, sha256 df2035abb5b2ca2464b0b2092fb4828b1cdbdc5c45930230a542fb68f61e2da6), abstract read; it predicts later citations/h-index of prominent researchers from early-career factors, not honors, so not reported as a lead.); Clarivate Citation Laureates (Nobel prediction list, no peer-reviewed source read) (Only secondary web summaries seen; no primary record fetched.); Sinatra et al. 2016 / Ioannidis et al. 2019 (already cited) (Already in the manuscript's bibliography; not re-read here (Koltun & Hafner describe both as award-correlation studies).)
status: search incomplete — OpenAlex and Semantic Scholar were rate-limited for the whole run (Crossref, arXiv and web search ran); nonreusable

## [2026-11-05T16:07Z] uncited — paper/manuscript_qss/main.tex:83
claim: "Percentiles do not depend on the skewed tail of the citation distribution, they need no assumption about how citations grow with age, and their meaning does not change with the calendar."
claim-hash: 8a99fbe09720
searched: OpenAlex: percentile rank classes citation impact (and 3 similar) -> 0 (rate limit exceeded); Crossref: 5 queries (percentile rank classes outliers skewed; citation percentiles field/publication year normalization; crown indicator percentile robust; time-independent percentile publication age; universality of citation distributions) -> ~40 records scanned; Semantic Scholar: percentile robust skewed outliers -> 6 results; 4 further queries -> HTTP 429; arXiv API: percentile+citation -> 0 parsed results; WebSearch: percentile-based citation indicators robust skewed / comparable across years -> 10 results; Bornmann/Leydesdorff/Mutz percentile rank classes -> 10 results; Full text read: arXiv:1211.0381 and arXiv:2001.04290 | channel failures: OpenAlex: daily free budget exhausted (rate limit); Semantic Scholar: HTTP 429 after first query; arXiv API: returned nothing parseable (PDFs fetched directly instead)
candidate-source: arXiv:1211.0381 / https://arxiv.org/pdf/1211.0381 — Bornmann, Leydesdorff & Mutz (2013), The use of percentiles and percentile rank classes in the analysis of bibliometric data: Opportunities and limits, Journal of Informetrics (accepted version) [this is already-cited key bornmann2013use]
  version-read: arXiv accepted manuscript
  archived: gs://scholar-version2-literature/sources/bornmann2013--arxiv_1211.0381-e3cc8e1a.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
  license: unknown (treated as restricted; private store)
  evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value (Waltman, et al., 2012, p. 10); (2) they do not depend on the choice of a specific probability density function in comparison to the arithmetic mean, which requires normally distributed data. (3) Percentiles can be calculated, even if the data are skewed, as it is usual for bibliometric data." (Section 2, The calculation of percentiles); "The reference set was made up of publications which were published in the same subject area, the same publication year and with the same document type as the publications in question." (Section 1 (describes reference-set construction by publication year)) — overlap: Directly supports the first two properties: robustness to the skewed tail/extreme values, and no assumption about the distribution's form. Cross-year comparability is only implicit (reference set = same publication year); the text does not say percentiles keep their meaning across calendar years, and says nothing about citation growth with age. Already cited in the manuscript.
candidate-source: https://doi.org/10.48550/arXiv.2001.04290 — Bornmann & Williams (2020), An Evaluation of Percentile Measures of Citation Impact, and a Proposal for Making Them Better, arXiv:2001.04290 (journal version not verified in this session)
  version-read: arXiv preprint
  archived: gs://scholar-version2-literature/sources/bornmann2020--arxiv_2001.04290-2b0dfbf1.pdf sha256:888752b8e8455231aec1bcd55afb7cd5c000240d9d8975066717149bddf3fd88
  license: unknown (treated as restricted; private store)
  evidence: "The advantage of PRs (and PPs) is that they are not strongly affected by outliers (highly cited papers) and their interpretation is simple and clear: if a focal paper has a PR of 90, then 90% of the papers in the publication year and subject category have a citation impact which is (at or) below the impact of the focal paper." (Introduction); "This approach of generating normalized citation impact values which can be used for cross-time and cross-field comparisons has been frequently criticized, since it is based on arithmetic averages of citations and citations as a rule are skewed distributed." (Introduction (describes mean-based normalization; percentiles proposed as the alternative)) — overlap: Supports robustness to outliers and a fixed, year-relative interpretation of a percentile. It frames percentiles as the alternative to mean-based normalization for cross-time comparison, but does not explicitly say percentiles need no age-growth model.
status: candidate offered to the author; nothing inserted; search incomplete — OpenAlex and Semantic Scholar were rate-limited for the whole run (Crossref, arXiv and web search ran); nonreusable

## [2026-11-05T16:07Z] uncited — paper/manuscript_qss/main.tex:69
claim: "Hiring and tenure committees, fellowship panels and early-career grants judge scholars three to eight years into their careers, when most of their papers are recent and few have had time to collect many citations."
claim-hash: bda4fa5ec21b
searched: curl: AAUP 1940 Statement page -> 1 read; curl: ERC Starting Grant page -> 1 read; curl: NIH NOT-OD-17-101 (ESI policy) -> 1 read; NOT-OD-09-013 (rescinded) -> seen, not archived; curl: NSF CAREER program page and CAREER FAQ -> 2 read; WebSearch: NSF CAREER eligibility untenured assistant professor -> ~20 results, summary only; WebSearch: median time to tenure years after PhD -> 10 results, none opened; Crossref: tenure probationary period query -> 5 results, none relevant | channel failures: OpenAlex: rate limit exceeded (free daily budget used up); Semantic Scholar API: HTTP 429; Peer-reviewed empirical studies of career stage at hiring/tenure were therefore not searched through the scholarly APIs; only official policy sources were read.
candidate-source: https://www.aaup.org/report/1940-statement-principles-academic-freedom-and-tenure — AAUP (1940, with 1970 interpretive comments), 1940 Statement of Principles on Academic Freedom and Tenure
  version-read: publisher HTML, re-fetched by coordinator 2026-11-05
  archived: gs://scholar-version2-literature/sources/aaup1940--www.aaup.org_report_1940-statement-principles-academic-freed-0320fec2--2026-11-05-2ae7b58a.html sha256:2ae7b58ad0fd56d601375b008df0a00b2a48740150255c8749babbfa341e311f
  license: unknown (treated as restricted; private store)
  evidence: "the probationary period should not exceed seven years, including within this period full-time service in all institutions of higher education" (Statement, Academic Tenure, item 2) — overlap: Supports the upper bound of the tenure clock (seven years of probationary service), i.e. tenure decisions at roughly years 6-7 of faculty service. It does not say how many years after PhD, and does not mention hiring.
  notes: worker's saved file contained an NIH 'Page not found' page; coordinator re-fetched the AAUP page and confirmed the quote
candidate-source: https://erc.europa.eu/apply-grant/starting-grant — European Research Council, Starting Grant (program page)
  version-read: publisher HTML, fetched 2026-11-05
  archived: gs://scholar-version2-literature/sources/european--erc.europa.eu_apply-grant_starting-grant-e8b5a58d--2026-11-05-02ae62a2.html sha256:02ae62a2529e43b76c629c9372e60f32e9cd41d0c2b62d330ebb569f29641ee3
  license: unknown (treated as restricted; private store)
  evidence: "Researchers of any nationality with 0-10 years of experience since completion of PhD, a scientific track record showing great promise and an excellent research proposal can apply." (Who can apply?) — overlap: Early-career fellowship window is 0-10 years after PhD, wider than the manuscript's three to eight years; it does not pin the window to that range. I did not retrieve the older 2-7 year rule.
candidate-source: https://grants.nih.gov/grants/guide/notice-files/NOT-OD-17-101.html — NIH (2017), Notice NOT-OD-17-101 (Early Stage Investigator policy; rescinds NOT-OD-09-013)
  version-read: NIH Guide HTML, fetched 2026-11-05
  archived: gs://scholar-version2-literature/sources/nih2017--grants.nih.gov_grants_guide_notice-files_not-od-17-101.html-f3df2b92--2026-11-05-7a82c949.html sha256:7a82c949964e3d7f556f9e94eac57cc6a850c37dc932a2e9d2432e0d635bb840
  license: unknown (treated as restricted; private store)
  evidence: "An ESI is a Program Director / Principal Investigator (PD/PI) who has completed their terminal research degree or end of post-graduate clinical training, whichever date is later, within the past 10 years and who has not previously competed successfully as PD/PI for a substantial NIH independent research award." (ESI definition) — overlap: Early-career grant window is up to 10 years after terminal degree; this is the outer edge, not the 3-8 year range.
candidate-source: https://www.nsf.gov/funding/information/faq-frequently-asked-questions-faqs-faculty-early-career — NSF, Faculty Early Career Development (CAREER) Program FAQ
  version-read: publisher HTML, fetched 2026-11-05
  archived: gs://scholar-version2-literature/sources/nsf--www.nsf.gov_funding_information_faq-frequently-asked-questio-c806977f--2026-11-05-a10e04fe.html sha256:a10e04feef0149539f8854b0aa9cd3abfab971a45eb0b14d61fbc614b3c0c2b7
  license: unknown (treated as restricted; private store)
  evidence: "CAREER eligibility is not limited by time from degree or years in a tenure track appointment." (FAQ answer on untenured assistant professors) — overlap: NSF CAREER targets untenured tenure-track assistant professors, with no years-since-PhD limit; it ties eligibility to career stage, not a year range. Does not support specific years.
status: candidate offered to the author; nothing inserted; search incomplete — OpenAlex and Semantic Scholar were rate-limited for the whole run (Crossref, arXiv and web search ran); nonreusable

## [2026-11-05T16:07Z] uncited — paper/manuscript_qss/main.tex:199
claim: "Semantic Scholar assigns author identifiers through automatic disambiguation."
claim-hash: d4367fb78a3c
searched: official S2 docs fetched: api.semanticscholar.org/graph/v1/swagger.json -> 1 (fields-of-study list); official S2 Datasets API: api.semanticscholar.org/datasets/v1/release/latest (release 2026-10-27) -> 1 release, 11 dataset READMEs scanned for LICENSE; web: semanticscholar.org/product/api/license -> 1 (API License Agreement); web: api.semanticscholar.org/api-docs/ , /api-docs/datasets, /api-docs/graph -> 3 (JS ReDoc shells, no content); web: semanticscholar.org/product/api/tutorial -> 1 (no license/23-field statement); staged text kinney2023semantic: grep disambig/ODC -> 'disambiguat' hits at lines 57, 98, 230-239; no license; arXiv API: id_list=2103.07534 (S2AND) -> 1 | channel failures: api.semanticscholar.org/api-docs/* pages return JS-only ReDoc shells (no text); content read instead from the underlying swagger.json and datasets API. Graph API search call returned HTTP 429 (not needed).
candidate-source: arXiv:2301.10140 — Kinney et al. (2023), The Semantic Scholar Open Data Platform, arXiv:2301.10140v2 (already cited as kinney2023semantic)
  version-read: v2 staged text
  archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
  license: unknown (treated as restricted; private store)
  evidence: "In Author Disambiguation, we use a system named S2AND, introduced in Subramanian et al. (2021), to assign an ID to each author mention (a name of an author appearing in a particular paper)." (Sec. on Author Disambiguation (staged text lines 230-239)); "S2AG is a disambiguated, high-quality, bibliographic" (abstract, staged text line 57) — overlap: Directly supports (a): author IDs come from automatic disambiguation by S2AND. It is already cited in the manuscript, so line 199 could cite it and Subramanian et al. 2021.
candidate-source: arXiv:2103.07534 — Subramanian, King, Downey, Feldman (2021), S2AND: A Benchmark and Evaluation System for Author Name Disambiguation, arXiv:2103.07534v3
  version-read: abstract-only; v3 arXiv API record
  archived: gs://scholar-version2-literature/sources/subramanian2021--arxiv_2103.07534-eaccefdd.xml sha256:2460c13baac9f78b476fa0b6170130d800ef06f33b97ea31aba6fe742340d0bd
  license: unknown (treated as restricted; private store)
  evidence: "Author Name Disambiguation (AND) is the task of resolving which author mentions in a bibliographic database refer to the same real-world person ... we present S2AND, a unified benchmark dataset for AND on scholarly papers, as well as an open-source reference model implementation." (abstract) — overlap: Primary source for the S2AND system that kinney2023semantic says assigns the author IDs. Authors and title taken from the archived arXiv record.
status: candidate offered to the author; nothing inserted

## [2026-11-05T16:07Z] uncited — paper/manuscript_qss/main.tex:196
claim: "Each article carries zero or more of Semantic Scholar's 23 fields of study."
claim-hash: 35d306c65420
searched: official S2 docs fetched: api.semanticscholar.org/graph/v1/swagger.json -> 1 (fields-of-study list); official S2 Datasets API: api.semanticscholar.org/datasets/v1/release/latest (release 2026-10-27) -> 1 release, 11 dataset READMEs scanned for LICENSE; web: semanticscholar.org/product/api/license -> 1 (API License Agreement); web: api.semanticscholar.org/api-docs/ , /api-docs/datasets, /api-docs/graph -> 3 (JS ReDoc shells, no content); web: semanticscholar.org/product/api/tutorial -> 1 (no license/23-field statement); staged text kinney2023semantic: grep disambig/ODC -> 'disambiguat' hits at lines 57, 98, 230-239; no license; arXiv API: id_list=2103.07534 (S2AND) -> 1 | channel failures: api.semanticscholar.org/api-docs/* pages return JS-only ReDoc shells (no text); content read instead from the underlying swagger.json and datasets API. Graph API search call returned HTTP 429 (not needed).
candidate-source: https://api.semanticscholar.org/graph/v1/swagger.json — Allen Institute for AI, Semantic Scholar Academic Graph API specification (Paper object, fieldsOfStudy)
  version-read: live spec fetched 2026-11-05
  archived: gs://scholar-version2-literature/sources/allen--api.semanticscholar.org_graph_v1_swagger.json-eb4c3a82--2026-11-05-09663a55.json sha256:09663a55920f46d4ab2b9febdac51bef7b3c854ebddaa5d97b5d7ed7d244890e
  license: unknown (treated as restricted; private store)
  evidence: "A list of the paper’s high-level academic categories from external sources. The possible fields are: Computer Science, Medicine, Chemistry, Biology, Materials Science, Physics, Geology, Psychology, Art, History, Geography, Sociology, Business, Political Science, Economics, Philosophy, Mathematics, Engineering, Environmental Science, Agricultural and Food Sciences, Education, Law, and Linguistics." (definitions, Paper.fieldsOfStudy (s2FieldsOfStudy says 'The possible fields are the same as in fieldsOfStudy')) — overlap: The listed possible fields number 23 (counted by me), matching the manuscript's '23 fields of study'; the field is an array, so a paper may have zero or more. The doc itself does not state the number 23 or 'zero or more' explicitly.
status: candidate offered to the author; nothing inserted

## [2026-11-05T16:07Z] uncited — paper/manuscript_qss/main.tex:186
claim: "We use the Semantic Scholar Academic Graph \citep{kinney2023semantic}, release of 27 October 2026, which is distributed under the ODC-BY license."
claim-hash: 4dae5e8290d8
searched: official S2 docs fetched: api.semanticscholar.org/graph/v1/swagger.json -> 1 (fields-of-study list); official S2 Datasets API: api.semanticscholar.org/datasets/v1/release/latest (release 2026-10-27) -> 1 release, 11 dataset READMEs scanned for LICENSE; web: semanticscholar.org/product/api/license -> 1 (API License Agreement); web: api.semanticscholar.org/api-docs/ , /api-docs/datasets, /api-docs/graph -> 3 (JS ReDoc shells, no content); web: semanticscholar.org/product/api/tutorial -> 1 (no license/23-field statement); staged text kinney2023semantic: grep disambig/ODC -> 'disambiguat' hits at lines 57, 98, 230-239; no license; arXiv API: id_list=2103.07534 (S2AND) -> 1 | channel failures: api.semanticscholar.org/api-docs/* pages return JS-only ReDoc shells (no text); content read instead from the underlying swagger.json and datasets API. Graph API search call returned HTTP 429 (not needed).
candidate-source: https://api.semanticscholar.org/datasets/v1/release/latest — Allen Institute for AI, Semantic Scholar Academic Graph Datasets API, release 2026-10-27, per-dataset README (abstracts, authors, citations, paper-ids, papers, publication-venues, s2orc, s2orc_v2, tldrs)
  version-read: release 2026-10-27, fetched 2026-11-05
  archived: gs://scholar-version2-literature/sources/allen2026--api.semanticscholar.org_datasets_v1_release_latest-605f8eba--2026-11-05-385e6b41.json sha256:385e6b41f1ccaa0199439dd8b7cf1a0c3cfb112ec2f1e784741d749ca5e869f0
  license: unknown (treated as restricted; private store)
  evidence: "LICENSE This collection is licensed under ODC-BY. (https://opendatacommons.org/licenses/by/1.0/) By downloading this data you acknowledge that you have read and agreed to all the terms in this license." (README of each of authors, papers, citations, abstracts, paper-ids, publication-venues, s2orc, s2orc_v2(partly), tldrs datasets); "This software is released under the Apache 2.0 license." (README of embeddings-specter_v1 and embeddings-specter_v2 (exceptions)) — overlap: Supports (c) for the core S2AG datasets (papers, authors, citations, etc.): ODC-BY. Not all datasets: SPECTER embeddings are Apache 2.0, and s2orc_v2 carries per-paper Unpaywall licences, so 'S2AG is distributed under ODC-BY' is accurate for the core tables, not for every dataset.
candidate-source: https://www.semanticscholar.org/product/api/license — Allen Institute for AI, Semantic Scholar API License Agreement (last updated May 17, 2023)
  version-read: page dated 'Last updated: May 17, 2023', fetched 2026-11-05
  archived: gs://scholar-version2-literature/sources/allen2023--www.semanticscholar.org_product_api_license-4432f22d--2026-11-05-f8928577.html sha256:f8928577a2cd80f8236e523b2ce0a30b507e729bb0148827342d1e67d2cac663
  license: unknown (treated as restricted; private store)
  evidence: "Licensee's use of S2 Data accessed via the API are separately governed by the licenses that accompany such S2 Data, such as CC BY-NC or ODC-BY" (S2 Data clause of the API License Agreement) — overlap: Confirms ODC-BY as a data licence but does not itself assign it to S2AG; the dataset READMEs above are the more direct source.
status: candidate offered to the author; nothing inserted; candidate replacement for the license clause, which the cited kinney2023semantic does not support (see cite:kinney2023semantic, verdict unsupported)

## [2026-11-05T16:07Z] uncited — paper/manuscript_qss/main.tex:216
claim: "Unlike Semantic Scholar's author records, these profiles are curated by the scholars themselves, so they are a check on automatic disambiguation (Section~\ref{sec:robustness})."
claim-hash: f040d55d4266
searched: WebFetch/curl: scholar.google.com/intl/en/scholar/citations.html (official Google Scholar Profiles help) -> 1 page, read in full; curl: scholar.google.com/citations/intl/en/help.html -> 404; curl: arXiv 2103.07534 (S2AND, Subramanian et al.) -> 1 PDF read; local: staged kinney2023semantic text, grep disambig/claim -> 2 passages read; curl: semanticscholar.org/faq -> fetched (JS shell, not read for content) | channel failures: Semantic Scholar Graph API search returned 429 (rate limited); arXiv export API returned an empty body. Both were replaced by direct fetches, so nothing was missed.
candidate-source: https://scholar.google.com/intl/en/scholar/citations.html — Google Scholar (undated, accessed 2026-11-05), Google Scholar Profiles help page, Google
  version-read: live page as fetched 2026-11-05
  archived: gs://scholar-version2-literature/sources/google2026--scholar.google.com_intl_en_scholar_citations.html-5a1f02e3--2026-11-05-ee36f4c6.html sha256:ee36f4c64761878fab990a35b7ca3413ea762bd9d37e6685ebf1b6a7efe41615
  license: unknown (treated as restricted; private store)
  evidence: "Google Scholar Profiles provide a simple way for authors to showcase their academic publications." (intro); "How do I create my author profile? Start here. It's quick and free. First, sign in to your Google account" (Setting up your profile); "Add all articles that you have written; keep in mind your articles may be in several different groups, and some groups may occasionally include articles by several different authors." (Setting up your profile); "You can choose to have your list of articles updated automatically or review the updates yourself, or to manually update your articles at any time." (intro); "We use a statistical model to try to tell different authors apart but such automatic processes are not always accurate. The best way to fix this is to look through the articles in your profile and remove the ones that were written by others." (FAQ: articles in my profile aren't mine); "open the Scholar profile sign up form" (Setting up your profile) — overlap: Directly supports 'profiles are created and curated by the scholars themselves': authors sign up, select their own articles, and can remove wrong ones. Nuance: the page says Google's statistical model groups candidate articles and, with automatic updates on, may add articles, so curation is by the scholar but not purely manual.
candidate-source: arXiv:2301.10140 (already cited as kinney2023semantic) — Kinney et al. (2023), The Semantic Scholar Open Data Platform, arXiv:2301.10140v2
  version-read: v2
  archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
  license: unknown (treated as restricted; private store)
  evidence: "In Author Disambiguation, we use a system named" (Sec. 3 (column-split)); "S2AND, introduced in Subramanian et al. (2021)," (Sec. 3, continuation); "Users of the web site can claim the iden" (Sec. 2 (column-split)); "tity of an author in our corpus and manually curate the papers associated with that author" (Sec. 2, continuation) — overlap: Supports that S2 author records come from automatic disambiguation (S2AND). Caveat the manuscript's wording ignores: S2 also lets authors claim and manually curate their records, so 'unlike S2, curated by the scholars themselves' is a difference of degree (claimed profiles in S2 are a minority, not quantified here).
candidate-source: arXiv:2103.07534 — Subramanian, King, Downey & Feldman (2021, v3 2022), S2AND: A Benchmark and Evaluation System for Author Name Disambiguation, arXiv (JCDL 2021)
  version-read: arXiv v3, 21 Feb 2022
  archived: gs://scholar-version2-literature/sources/subramanian2021--arxiv_2103.07534-eaccefdd.pdf sha256:fb8528da8901302e924cba7b3db4bfd23973f6200980464821a706ea92d0d8af
  license: unknown (treated as restricted; private store)
  evidence: "Author Name Disambiguation (AND) is the task of resolving which author mentions in a bibliographic database refer to the same real-world person" (Abstract); "production on S2 today, reducing error by more than 50%" (Intro (line ~91 of extracted text; surrounding sentence not quoted in full)) — overlap: Describes the algorithmic author clustering used for S2 author records and an evaluation of the S2 production system. Supports the 'automatic disambiguation' half of the claim.
status: candidate offered to the author; nothing inserted


## [2026-11-05T16:29Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:82
claim: "At the level of single papers, percentiles have become an established normalization, which the Leiden Manifesto calls the most robust \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}: a paper is compared with papers of the same publication year, and possibly the same field, by the share of them it out-cites."
claim-hash: 7fadc987d1ee
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Percentiles and PR have been established in bibliometrics as an important alternative" (sec. 5 Discussion)
notes: Supports 'established'. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:waltman2016review — paper/manuscript_qss/main.tex:82
claim: "At the level of single papers, percentiles have become an established normalization, which the Leiden Manifesto calls the most robust \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}: a paper is compared with papers of the same publication year, and possibly the same field, by the share of them it out-cites."
claim-hash: 7fadc987d1ee
source: doi:10.1016/j.joi.2016.02.007
version-read: preprint (arXiv:1507.02099v3)
archived: gs://scholar-version2-literature/sources/waltman2016review--10.1016_j.joi.2016.02.007-f65cd1b4.pdf sha256:dbfc29bc5a786a45fd14c9a7ab47d7b40949adff0dce04f0a3c540631491f907
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Nowadays, the idea of calculating the proportion of publications that belong to the top 10% most highly cited in their field plays an important role both in the CWTS Leiden Ranking and in the SCImago Institutions Rankings" (sec. 6.2, p. 25)
notes: Percentile-based indicators are in standard use in the major rankings; the review does not call them preferred, which the revised sentence no longer claims. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:82
claim: "At the level of single papers, percentiles have become an established normalization, which the Leiden Manifesto calls the most robust \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}: a paper is compared with papers of the same publication year, and possibly the same field, by the share of them it out-cites."
claim-hash: 7fadc987d1ee
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the most robust normalization method is based on percentiles" (p. 431, principle 6)
notes: The Leiden Manifesto's own wording. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:84
claim: "Percentiles do not depend on the skewed tail of the citation distribution \citep{bornmann2013use}, they need no assumption about how citations grow with age, and their meaning does not change with the calendar."
claim-hash: cb51c03bec86
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value" (sec. 2, The calculation of percentiles); "(3) Percentiles can be calculated, even if the data are skewed, as it is usual for bibliometric data." (sec. 2)
notes: Cited for the first clause only; the age and calendar clauses are the manuscript's own argument. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:144
claim: "The $h$-index \citep{hirsch2005index} is a single number that depends on both the number of papers and their citations, and it has been studied, criticized and modified at length."
claim-hash: 07a434ef9dcc
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "I propose the index h, defined as the number of papers with citation number higher or equal to h" (abstract); "to propose a single number, the" (p. 1)
notes: A single number defined by both paper count and citations. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates \citep{schubert1986relative,Radicchi2008} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: 6140009d112b
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value" (sec. 2, The calculation of percentiles)
notes: Cited for 'less sensitive to outliers'. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:bornmann2020evaluation — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates \citep{schubert1986relative,Radicchi2008} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: 6140009d112b
source: doi:10.1007/s11192-020-03512-7
version-read: version of record
archived: gs://scholar-version2-literature/sources/bornmann2020evaluation--10.1007_s11192-020-03512-7-1f24fb42.pdf sha256:3ea8c359d01836ba0507727a744a394ae2b0778cfc91682e8bafe7b5676bc29e
license: CC-BY-4.0 (Springer open access)
updates: none
verdict: supported
evidence: "The advantage of PRs (and PPs) is that they are not strongly affected by outliers (highly cited papers)" (p. 1458, Introduction)
notes: Version of record. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:schubert1986relative — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates \citep{schubert1986relative,Radicchi2008} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: 6140009d112b
source: doi:10.1007/bf02017249
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/schubert1986relative--10.1007_bf02017249-0d8903d0--abstract--2026-11-05-26c0d74a.html sha256:26c0d74a3353e9680a72faffcf7efd140021c30bfc6d95ac61256fdc19e6c693
license: unknown (treated as restricted; private store)
updates: none
verdict: unverifiable
evidence: none — Cited as an example of normalization by mean citation rates. The abstract describes indicators relative to 'field-specific reference standards' but does not say mean; full text needed (paywalled).
notes: Cited as an example of normalization by mean citation rates. The abstract describes indicators relative to 'field-specific reference standards' but does not say mean; full text needed (paywalled). Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:Radicchi2008 — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates \citep{schubert1986relative,Radicchi2008} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: 6140009d112b
source: doi:10.1073/pnas.0806977105
version-read: preprint (arXiv:0806.0974v2)
archived: gs://scholar-version2-literature/sources/radicchi2008--10.1073_pnas.0806977105-fa509e28.pdf sha256:6dde217e74a426a0d1f6d900d74f0fce3452ab8c500260979751d86d06d7c5a0
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we have considered the average number c0 of citations" (p. 2); "as an unbiased relative indicator of scientific impact" (p. 1)
notes: Cited as an example of normalization by mean citation rates (cf = c/c0). Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:156
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates \citep{schubert1986relative,Radicchi2008} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: 6140009d112b
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the most robust normalization method is based on percentiles" (p. 431, principle 6)
notes: Cited for the Leiden Manifesto recommendation. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:bornmann2014individual — paper/manuscript_qss/main.tex:160
claim: "At the level of a scholar, \citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 39b426b6f244
source: doi:10.1007/s11192-013-1161-y
version-read: preprint (arXiv:1302.3697v2)
archived: gs://scholar-version2-literature/sources/bornmann2014individual--10.1007_s11192-013-1161-y-22e3c955.pdf sha256:f53354eb37b3efb9676ae2f64101071c8aa1d7ffe88e12df5d581f88ea210334
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we would like to propose using the number of years as an active researcher (Ptop 10% quotient) to normalise Ptop 10% for age" (sec. 3.2)
notes: Matches the revised wording (top 10%, divided by academic age). Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:160
claim: "At the level of a scholar, \citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 39b426b6f244
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "One can also consider hundred percentiles as a continuous random variable and sum these values" (pp. 5-6); "The citations are weighted in accordance with the percentile" (p. 7)
notes: I3 sums percentiles over a set of papers. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:rousseau2012basic — paper/manuscript_qss/main.tex:160
claim: "At the level of a scholar, \citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 39b426b6f244
source: doi:10.1002/asi.21684
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/rousseau2012basic--10.1002_asi.21684-660d9303--abstract--2026-11-05-c17f011c.json sha256:c17f011c6bfe3e3e0bd0544abf7df6ba45c0d6aab62927dedf6ecd5754fb2bf4
license: unknown (treated as restricted; private store)
updates: none
verdict: unverifiable
evidence: none — Abstract only; it discusses I3 but does not state that it sums percentiles. Full text needed (paywalled).
notes: Abstract only; it discusses I3 but does not state that it sums percentiles. Full text needed (paywalled). Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:186
claim: "We use the Semantic Scholar Academic Graph \citep{kinney2023semantic}, release of 27 October 2026, which is distributed under the ODC-BY license \citep{s2datasets2026}."
claim-hash: c360130c7554
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "S2AG is a disambiguated, high-quality, bibliographic" (abstract)
notes: Cited as the description of S2AG; the license is now cited to s2datasets2026, and the release date is the authors' own. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:s2datasets2026 — paper/manuscript_qss/main.tex:186
claim: "We use the Semantic Scholar Academic Graph \citep{kinney2023semantic}, release of 27 October 2026, which is distributed under the ODC-BY license \citep{s2datasets2026}."
claim-hash: c360130c7554
source: https://api.semanticscholar.org/datasets/v1/release/2026-10-27
version-read: release 2026-10-27 dataset READMEs (JSON), fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/s2datasets2026--api.semanticscholar.org_datasets_v1_release_2026-10-27-1ddd5a1e--2026-11-05-385e6b41.json sha256:385e6b41f1ccaa0199439dd8b7cf1a0c3cfb112ec2f1e784741d749ca5e869f0
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "This collection is licensed under ODC-BY." (README of the papers, authors and citations datasets, release 2026-10-27)
notes: Core datasets (papers, authors, citations) are ODC-BY; SPECTER embeddings are Apache 2.0 and s2orc_v2 carries per-paper licenses, none of which the paper uses. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:s2graphapi — paper/manuscript_qss/main.tex:197
claim: "Each article carries zero or more of Semantic Scholar's 23 fields of study \citep{s2graphapi}."
claim-hash: c3493ebebd1b
source: https://api.semanticscholar.org/api-docs/graph (content read from https://api.semanticscholar.org/graph/v1/swagger.json)
version-read: API specification fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/allen--api.semanticscholar.org_graph_v1_swagger.json-eb4c3a82--2026-11-05-09663a55.json sha256:09663a55920f46d4ab2b9febdac51bef7b3c854ebddaa5d97b5d7ed7d244890e
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "The possible fields are: Computer Science, Medicine, Chemistry, Biology, Materials Science, Physics, Geology, Psychology, Art, History, Geography, Sociology, Business, Political Science, Economics, Philosophy, Mathematics, Engineering, Environmental Science, Agricultural and Food Sciences, Education, Law, and Linguistics." (Paper.fieldsOfStudy)
notes: The list has 23 entries (counted); fieldsOfStudy is an array, so zero or more per paper. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:200
claim: "Semantic Scholar assigns author identifiers through automatic disambiguation \citep{kinney2023semantic}."
claim-hash: 08f0f68bb1b2
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "In Author Disambiguation, we use a system named" (sec. 3 (column-split)); "S2AND, introduced in Subramanian et al. (2021)," (sec. 3, continuation)
notes: S2AND assigns an ID to each author mention. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:218
claim: "Semantic Scholar's author records are built by automatic disambiguation, although authors can claim and correct them \citep{kinney2023semantic}; Google Scholar profiles are created and curated by the scholars themselves \citep{googlescholarprofiles}, so they are a check on automatic disambiguation (Section~\ref{sec:robustness})."
claim-hash: bfeae30d63bd
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "In Author Disambiguation, we use a system named" (sec. 3 (column-split)); "S2AND, introduced in Subramanian et al. (2021)," (sec. 3, continuation); "Users of the web site can claim the iden" (sec. 2 (column-split)); "tity of an author in our corpus and manually curate the papers associated with that author" (sec. 2, continuation)
notes: Automatic disambiguation plus author claims and manual curation. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:googlescholarprofiles — paper/manuscript_qss/main.tex:218
claim: "Semantic Scholar's author records are built by automatic disambiguation, although authors can claim and correct them \citep{kinney2023semantic}; Google Scholar profiles are created and curated by the scholars themselves \citep{googlescholarprofiles}, so they are a check on automatic disambiguation (Section~\ref{sec:robustness})."
claim-hash: bfeae30d63bd
source: https://scholar.google.com/intl/en/scholar/citations.html
version-read: help page fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/google2026--scholar.google.com_intl_en_scholar_citations.html-5a1f02e3--2026-11-05-ee36f4c6.html sha256:ee36f4c64761878fab990a35b7ca3413ea762bd9d37e6685ebf1b6a7efe41615
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "Google Scholar Profiles provide a simple way for authors to showcase their academic publications." (intro); "Add all articles that you have written" (Setting up your profile); "The best way to fix this is to look through the articles in your profile and remove the ones that were written by others." (FAQ)
notes: Profiles are created and curated by the scholars; Google's statistical model proposes articles and can update them automatically if the author allows. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:331
claim: "Mid-ranks, which count half of the tied peers, are a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, but here they would give credit to authors with nothing above a threshold: at high thresholds most peers have no article, and an author tied with them at zero would stand at half their share."
claim-hash: e0e4e6500ce3
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "The compromise ((i-0.5)/n*100) derived by (Hazen, 1914) is used very frequently" (sec. 2)
notes: Hazen's (i-0.5)/n is the mid-rank; 'used very frequently' supports 'a common choice'. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:331
claim: "Mid-ranks, which count half of the tied peers, are a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, but here they would give credit to authors with nothing above a threshold: at high thresholds most peers have no article, and an author tied with them at zero would stand at half their share."
claim-hash: e0e4e6500ce3
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Pudovkin and Garfield put the tied publications in a random order and calculate their average percentile." (sec. 3)
notes: Describes average-percentile tie handling as one approach; 'common' now rests on bornmann2013use. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:zitt2005relativity — paper/manuscript_qss/main.tex:1008
claim: "Field normalization brings its own choices: the field scheme and the assignment of multi-field papers \citep{zitt2005relativity,waltman2015field,waltman2019field}, and, for a scholar, the assignment of a main field."
claim-hash: 458b1cd21099
source: doi:10.1007/s11192-005-0218-y
version-read: abstract-only
archived: gs://scholar-version2-literature/sources/zitt2005relativity--10.1007_s11192-005-0218-y-bf0707e5--abstract--2026-11-05-ce9c72cb.html sha256:ce9c72cbeba9eee1d4c552b5f9ffd9d3754059dec1c034a9abad75b2c54304b1
license: unknown (treated as restricted; private store)
updates: none
verdict: unverifiable
evidence: none — Abstract only: it supports the field-scheme part ('fields may be defined at various levels ... normalisation values are expected to vary') but is silent on multi-field papers. Full text needed (paywalled).
notes: Abstract only: it supports the field-scheme part ('fields may be defined at various levels ... normalisation values are expected to vary') but is silent on multi-field papers. Full text needed (paywalled). Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:waltman2015field — paper/manuscript_qss/main.tex:1008
claim: "Field normalization brings its own choices: the field scheme and the assignment of multi-field papers \citep{zitt2005relativity,waltman2015field,waltman2019field}, and, for a scholar, the assignment of a main field."
claim-hash: 458b1cd21099
source: doi:10.1016/j.joi.2015.08.001
version-read: preprint (arXiv:1501.04431v1)
archived: gs://scholar-version2-literature/sources/waltman2015field--10.1016_j.joi.2015.08.001-8988ca16.pdf sha256:acc87a4b752fbf52994b18aa703b1561c84124c780e9de46a66ff8a0341fc2a6
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "fields are defined by the WoS journal subject categories." (sec. 4, p. 19); "A fractional counting approach is taken to handle publications belonging to multiple subject categories." (footnote 6, p. 19)
notes: Both choices appear as choices the analysis makes; the author main-field clause, which it did not support, is no longer attributed. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:waltman2019field — paper/manuscript_qss/main.tex:1008
claim: "Field normalization brings its own choices: the field scheme and the assignment of multi-field papers \citep{zitt2005relativity,waltman2015field,waltman2019field}, and, for a scholar, the assignment of a main field."
claim-hash: 458b1cd21099
source: doi:10.1007/978-3-030-02511-3_11
version-read: preprint (arXiv:1801.09985v1)
archived: gs://scholar-version2-literature/sources/waltman2019field--10.1007_978-3-030-02511-3_11-357ae0ed.pdf sha256:af5d1381889500d4cb5876fa62907d470b34613ed4b7db6a647e6a3d252aa77a
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "normalized indicators are quite sensitive to methodological choices, such as the choice of a field classification system" (sec. 7.2); "Another issue in the calculation of the normalized citation score of a publication is the way in which publications belonging to multiple fields are handled." (sec. 5.1)
notes: Field scheme and multi-field assignment as choices. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] cite:s2datasets2026 — paper/manuscript_qss/main.tex:1064
claim: "Semantic Scholar data are available under the ODC-BY license from the Semantic Scholar Datasets API \citep{s2datasets2026}."
claim-hash: cb120cdd61bf
source: https://api.semanticscholar.org/datasets/v1/release/2026-10-27
version-read: release 2026-10-27 dataset READMEs (JSON), fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/s2datasets2026--api.semanticscholar.org_datasets_v1_release_2026-10-27-1ddd5a1e--2026-11-05-385e6b41.json sha256:385e6b41f1ccaa0199439dd8b7cf1a0c3cfb112ec2f1e784741d749ca5e869f0
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "This collection is licensed under ODC-BY." (README of the papers, authors and citations datasets, release 2026-10-27)
notes: Same source as line 186. Sentence revised by the author-approved fixes of 2026-11-05; verified by the coordinator against the archived text.

## [2026-11-05T16:29Z] bib:hirsch2005index
entry-hash: a73810278bc1
source: doi:10.1073/pnas.0507655102
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'An index to quantify an individual's scientific research output', Proceedings of the National Academy of Sciences 102(46) 16569-16572, doi:10.1073/pnas.0507655102; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:bornmann2013use
entry-hash: cf4c5158b40a
source: doi:10.1016/j.joi.2012.10.001
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'The use of percentiles and percentile rank classes in the analysis of bibliometric data: Opportunities and limits', Journal of Informetrics 7(1) 158-165, doi:10.1016/j.joi.2012.10.001; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:waltman2016review
entry-hash: 72bad80ce78c
source: doi:10.1016/j.joi.2016.02.007
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'A review of the literature on citation impact indicators', Journal of Informetrics 10(2) 365-391, doi:10.1016/j.joi.2016.02.007; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:hicks2015bibliometrics
entry-hash: d29d879491de
source: doi:10.1038/520429a
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Bibliometrics: The Leiden Manifesto for research metrics', Nature 520(7548) 429-431, doi:10.1038/520429a; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:waltman2012leiden
entry-hash: 59e9f2af6f39
source: doi:10.1002/asi.22708
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'The Leiden ranking 2011/2012: Data collection, indicators, and interpretation', Journal of the American Society for Information Science and Technology 63(12) 2419-2432, doi:10.1002/asi.22708; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:bornmann2014individual
entry-hash: 16dd6f7bb470
source: doi:10.1007/s11192-013-1161-y
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'How to evaluate individual researchers working in the natural and life sciences meaningfully? A proposal of methods based on percentiles of citations', Scientometrics 98(1) 487-509, doi:10.1007/s11192-013-1161-y; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:kinney2023semantic
entry-hash: a578a5ee5f35
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
registrar: DataCite (arXiv)
verdict: confirmed
evidence: arXiv API and DataCite records match the entry's title, authors and year; the entry now carries the DataCite DOI

## [2026-11-05T16:29Z] bib:tian2021predictability
entry-hash: 85710d2387fc
source: arXiv:2103.16025 (DataCite doi:10.48550/arXiv.2103.16025)
registrar: DataCite (arXiv)
verdict: confirmed
evidence: arXiv API and DataCite records match the entry's title, authors and year; the entry now carries the DataCite DOI

## [2026-11-05T16:29Z] bib:ipeirotis2024pip
entry-hash: 7641ae68b846
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html (accessed 2026-11-05)
registrar: none (URL-backed)
verdict: confirmed
evidence: the entry now carries the page's full title 'The PiP-AUC score for research productivity: A somewhat new metric for paper citations and number of papers'

## [2026-11-05T16:29Z] bib:egghe2006theory
entry-hash: 30b481fc5ef9
source: doi:10.1007/s11192-006-0144-7
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Theory and practise of the g-index', Scientometrics 69(1) 131-152, doi:10.1007/s11192-006-0144-7; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:sidiropoulos2007generalized
entry-hash: 302d13d327f9
source: doi:10.1007/s11192-007-1722-z
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Generalized Hirsch h-index for disclosing latent facts in citation networks', Scientometrics 72(2) 253-280, doi:10.1007/s11192-007-1722-z; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:harzing2014hia
entry-hash: db6eadeea8d7
source: doi:10.1007/s11192-013-1208-0
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'hIa: an individual annual h-index to accommodate disciplinary and career length differences', Scientometrics 99(3) 811-821, doi:10.1007/s11192-013-1208-0; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:kaur2013universality
entry-hash: a2b647927631
source: doi:10.1016/j.joi.2013.09.002
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Universality of scholarly impact metrics', Journal of Informetrics 7(4) 924-932, doi:10.1016/j.joi.2013.09.002; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:bornmann2011metaanalysis
entry-hash: 1f035a8421d7
source: doi:10.1016/j.joi.2011.01.006
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'A multilevel meta-analysis of studies reporting correlations between the h index and 37 different h index variants', Journal of Informetrics 5(3) 346-359, doi:10.1016/j.joi.2011.01.006; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:leydesdorff2011turning
entry-hash: 83e1ac1cbf1e
source: doi:10.1002/asi.21534
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Turning the tables on citation analysis one more time: Principles for comparing sets of documents', Journal of the American Society for Information Science and Technology 62(7) 1370-1381, doi:10.1002/asi.21534; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:waltman2013percentile
entry-hash: 17b7013b3861
source: doi:10.1002/asi.22775
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'On the calculation of percentile‐based bibliometric indicators', Journal of the American Society for Information Science and Technology 64(2) 372-379, doi:10.1002/asi.22775; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:bornmann2020evaluation
entry-hash: a4fc0ab0d657
source: doi:10.1007/s11192-020-03512-7
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'An evaluation of percentile measures of citation impact, and a proposal for making them better', Scientometrics 124(2) 1457-1478, doi:10.1007/s11192-020-03512-7; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:schubert1986relative
entry-hash: 69a599b266dd
source: doi:10.1007/bf02017249
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Relative indicators and relational charts for comparative assessment of publication output and citation impact', Scientometrics 9(5-6) 281-291, doi:10.1007/bf02017249; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:Radicchi2008
entry-hash: 4e9f4271122e
source: doi:10.1073/pnas.0806977105
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Universality of citation distributions: Toward an objective measure of scientific impact', Proceedings of the National Academy of Sciences 105(45) 17268-17272, doi:10.1073/pnas.0806977105; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:hazen1914storage
entry-hash: fe23f41b1300
source: doi:10.1061/taceat.0002563
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Storage to be Provided in Impounding Municipal Water Supply', Transactions of the American Society of Civil Engineers 77(1) 1539-1640, doi:10.1061/taceat.0002563; the entry's title now follows the paper's own title page ('Storage to be Provided in Impounding Reservoirs for Municipal Water Supply', Trans. ASCE Paper No. 1308, archive.org scan); the Crossref record omits 'reservoirs for', so the difference is a registrar error, not an entry correction

## [2026-11-05T16:29Z] bib:rousseau2012basic
entry-hash: 202b1dae8dc5
source: doi:10.1002/asi.21684
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Basic properties of both percentile rank scores and the I3 indicator', Journal of the American Society for Information Science and Technology 63(2) 416-420, doi:10.1002/asi.21684; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:ioannidis2019standardized
entry-hash: 5a1ad68ccfb0
source: doi:10.1371/journal.pbio.3000384
registrar: Crossref
updates: new_version doi:10.1371/journal.pbio.3000384 (2019-08-19, publisher)
verdict: confirmed
evidence: registrar record: 'A standardized citation metrics author database annotated for scientific field', PLOS Biology 17(8) e3000384, doi:10.1371/journal.pbio.3000384; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:abramo2014productivity
entry-hash: 8c00a8378f4f
source: doi:10.1007/s11192-014-1269-8
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'How do you define and measure research productivity?', Scientometrics 101(2) 1129-1144, doi:10.1007/s11192-014-1269-8; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:hirsch2007does
entry-hash: 8fa99f7b52d6
source: doi:10.1073/pnas.0707962104
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Does the h index have predictive power?', Proceedings of the National Academy of Sciences 104(49) 19193-19198, doi:10.1073/pnas.0707962104; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:acuna2012future
entry-hash: 12c24421dd1d
source: doi:10.1038/489201a
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Predicting scientific success', Nature 489(7415) 201-202, doi:10.1038/489201a; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:penner2013predictability
entry-hash: c9c28e8d3ced
source: doi:10.1038/srep03052
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'On the Predictability of Future Impact in Science', Scientific Reports 3(1) 3052, doi:10.1038/srep03052; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:sinatra2016quantifying
entry-hash: c6e31a8191f5
source: doi:10.1126/science.aaf5239
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Quantifying the evolution of individual scientific impact', Science 354(6312) aaf5239, doi:10.1126/science.aaf5239; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:clauset2017datadriven
entry-hash: de2db683f7e5
source: doi:10.1126/science.aal4217
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Data-driven predictions in the science of science', Science 355(6324) 477-480, doi:10.1126/science.aal4217; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:wang2013window
entry-hash: de6e2d8fd17d
source: doi:10.1007/s11192-012-0775-9
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Citation time window choice for research impact evaluation', Scientometrics 94(3) 851-872, doi:10.1007/s11192-012-0775-9; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:abramo2011window
entry-hash: 9276f557914c
source: doi:10.1016/j.joi.2011.06.004
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Assessing the varying level of impact measurement accuracy as a function of the citation window length', Journal of Informetrics 5(4) 659-667, doi:10.1016/j.joi.2011.06.004; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:merton1968matthew
entry-hash: e7a7c5a5efc2
source: doi:10.1126/science.159.3810.56
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'The Matthew Effect in Science', Science 159(3810) 56-63, doi:10.1126/science.159.3810.56; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:petersen2014reputation
entry-hash: bc8670877214
source: doi:10.1073/pnas.1323111111
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Reputation and impact in academic careers', Proceedings of the National Academy of Sciences 111(43) 15316-15321, doi:10.1073/pnas.1323111111; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:s2datasets2026
entry-hash: fe99d82c622b
source: https://api.semanticscholar.org/datasets/v1/release/2026-10-27 (accessed 2026-11-05)
registrar: none (URL-backed)
verdict: confirmed
evidence: the cited page is the work itself, published by its operator (archived as s2datasets2026--api.semanticscholar.org_datasets_v1_release_2026-10-27-1ddd5a1e--2026-11-05-385e6b41.json); title and URL match the entry

## [2026-11-05T16:29Z] bib:s2graphapi
entry-hash: 4a7d8baaeb7f
source: https://api.semanticscholar.org/api-docs/graph (content read from https://api.semanticscholar.org/graph/v1/swagger.json) (accessed 2026-11-05)
registrar: none (URL-backed)
verdict: confirmed
evidence: the cited page is the work itself, published by its operator (archived as allen--api.semanticscholar.org_graph_v1_swagger.json-eb4c3a82--2026-11-05-09663a55.json); title and URL match the entry

## [2026-11-05T16:29Z] bib:googlescholarprofiles
entry-hash: d2cd5eba05de
source: https://scholar.google.com/intl/en/scholar/citations.html (accessed 2026-11-05)
registrar: none (URL-backed)
verdict: confirmed
evidence: the cited page is the work itself, published by its operator (archived as google2026--scholar.google.com_intl_en_scholar_citations.html-5a1f02e3--2026-11-05-ee36f4c6.html); title and URL match the entry

## [2026-11-05T16:29Z] bib:aksnes2003macro
entry-hash: 42551c7bd51f
source: doi:10.1023/a:1021919228368
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'A macro study of self-citation', Scientometrics 56(2) 235-246, doi:10.1023/a:1021919228368; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:zitt2005relativity
entry-hash: 3103b833442b
source: doi:10.1007/s11192-005-0218-y
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Relativity of citation performance and excellence measures: From cross-field to cross-scale effects of field-normalisation', Scientometrics 63(2) 373-401, doi:10.1007/s11192-005-0218-y; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:waltman2015field
entry-hash: 97c7512efde2
source: doi:10.1016/j.joi.2015.08.001
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Field-normalized citation impact indicators and the choice of an appropriate counting method', Journal of Informetrics 9(4) 872-894, doi:10.1016/j.joi.2015.08.001; title, volume, issue, pages and DOI match the entry

## [2026-11-05T16:29Z] bib:waltman2019field
entry-hash: 8a0ba37fa992
source: doi:10.1007/978-3-030-02511-3_11
registrar: Crossref
updates: none
verdict: confirmed
evidence: registrar record: 'Field Normalization of Scientometric Indicators', Springer Handbooks None(None) 281-300, doi:10.1007/978-3-030-02511-3_11; title, volume, issue, pages and DOI match the entry


## [2026-11-05T17:00Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:138
claim: "A first, asymmetric form of the score was described by \citet{ipeirotis2024pip} and is used by the website \url{https://www.pip-score.org} (formerly \url{https://scholar.ipeirotis.org}); Section~\ref{sec:pip} explains why we replace it."
claim-hash: 706bc54d8173
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: supported
evidence: "we have a nice new score that we have given the thoroughly boring name "Percentile in Percentile Area Under the Curve" score, or PiP-AUC for short" (post body); "we created a small app at https://scholar.ipeirotis.org/" (post body, second paragraph)
notes: The post describes the first form of the score and links scholar.ipeirotis.org, which the revised sentence now names as the site's former address; on 2026-11-05 https://scholar.ipeirotis.org/ answered 301 to https://www.scholar-analytics.org/, which ends at https://www.pip-score.org/ (200), consistent with the author's statement that it redirects to pip-score.org. 'Asymmetric' is the manuscript's own characterization (Section sec:pip), not attributed to the post. Supersedes the 'partially supported' entry for the earlier wording.

## [2026-11-05T17:31Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:66
claim: "The most common are citation counts and the $h$-index \citep{hirsch2005index}, and both are hard to read in this setting: their values depend on the field, grow with seniority and drift with the calendar."
claim-hash: 879dc1a2c943
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "I propose the index h, defined as the number of papers with citation number higher or equal to h, as a useful index to characterize the scientific output of a researcher." (abstract)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:85
claim: "Percentiles have become an established normalization, which the Leiden Manifesto calls the most robust \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}."
claim-hash: 4871a1a5309d
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Percentiles and PR have been established in bibliometrics as an important alternative" (sec. 5 Discussion)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:waltman2016review — paper/manuscript_qss/main.tex:85
claim: "Percentiles have become an established normalization, which the Leiden Manifesto calls the most robust \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}."
claim-hash: 4871a1a5309d
source: doi:10.1016/j.joi.2016.02.007
version-read: preprint (arXiv:1507.02099v3)
archived: gs://scholar-version2-literature/sources/waltman2016review--10.1016_j.joi.2016.02.007-f65cd1b4.pdf sha256:dbfc29bc5a786a45fd14c9a7ab47d7b40949adff0dce04f0a3c540631491f907
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Nowadays, the idea of calculating the proportion of publications that belong to the top 10% most highly cited in their field plays an important role both in the CWTS Leiden Ranking and in the SCImago Institutions Rankings" (sec. 6.2, p. 25)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:85
claim: "Percentiles have become an established normalization, which the Leiden Manifesto calls the most robust \citep{bornmann2013use,waltman2016review,hicks2015bibliometrics}."
claim-hash: 4871a1a5309d
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the most robust normalization method is based on percentiles" (p. 431, principle 6)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:86
claim: "They do not depend on the skewed tail of the citation distribution \citep{bornmann2013use}, they need no assumption about how citations grow with age, and their meaning does not change with the calendar."
claim-hash: db3ed4a4fa1e
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value" (sec. 2, The calculation of percentiles); "(3) Percentiles can be calculated, even if the data are skewed, as it is usual for bibliometric data." (sec. 2)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:108
claim: "We compute PiP from the Semantic Scholar Academic Graph \citep{kinney2023semantic} for 11.1 million authors at every career age from 1 to 30, where career age is the number of years since an author's first paper."
claim-hash: 12b8c1ccd770
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "form is to build and distribute the Semantic Scholar" (sec. 2, Platform Overview (continues 'Academic Graph, or S2AG')); "Download links to monthly snapshots of our knowl" (sec. 4.2, Datasets)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:egghe2006theory — paper/manuscript_qss/main.tex:154
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by academic age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 71e92edb885f
source: doi:10.1007/s11192-006-0144-7
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/egghe2006theory--10.1007_s11192-006-0144-7-c5ffc342--author-supplied.pdf sha256:6cbee1f21ba0279b2b76d5d8692a0c7a29e6a9be859436fceaa88adbc747b72d
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the g-index is the (unique) largest number such that the top g articles received (together) at least g2 citations" (abstract); "it is insensitive to one or several outstandingly highly cited papers" (sec. 1, on the h-index)
notes: The g-index counts the citations of the top g articles together, so citations above h's threshold earn credit; Egghe notes the h-index is insensitive to outstandingly highly cited papers. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:bornmann2011metaanalysis — paper/manuscript_qss/main.tex:158
claim: "Most variants correlate highly with the original \citep{bornmann2011metaanalysis}."
claim-hash: cb0ee89f8235
source: doi:10.1016/j.joi.2011.01.006
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/bornmann2011metaanalysis--10.1016_j.joi.2011.01.006-23b88c51--author-supplied.pdf sha256:3ca617ae04265d08b3b6d60dbdb3f652386370815ccc3e999b84141b3dd1e5ca
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "show a high correlation between the h index and its variants" (abstract); "This means that there is redundancy between most of the h index variants and the h index." (abstract)
notes: Meta-analysis of 37 variants: most correlate highly with h. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:schubert1986relative — paper/manuscript_qss/main.tex:165
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates \citep{schubert1986relative,Radicchi2008} and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: 6140009d112b
source: doi:10.1007/bf02017249
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/schubert1986relative--10.1007_bf02017249-0d8903d0--author-supplied.pdf sha256:608a7ca180944a650b810b6317e708c6785aab50bd6953918c1411afd98ca2f0
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the average citation rate of the journal in which a paper was published is a valid standard to which its citation rate can be matched" (sec. 2); "corresponds precisely to the world average" (sec. 2, activity and attractivity indexes)
notes: Relative indicators divide a citation rate by an average (journal or world) citation rate: normalization by mean citation rates, as the revised sentence cites it. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:167
claim: "Ties and the definition of the percentile need care \citep{waltman2013percentile}."
claim-hash: 3938bfb9c80f
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "A difficulty in the calculation of percentile-based indicators is caused by the discrete nature of citation distributions combined with the presence of many publications with the same number of citations." (abstract); "Usually, there are multiple publications in a field that have the same number of citations. The presence of such ‘ties’ may aggravate the above problem." (sec. 1, Introduction)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:hazen1914storage — paper/manuscript_qss/main.tex:169
claim: "For scoring we use the share of the reference set with strictly fewer citations, so that uncited papers have percentile zero; for correlations we use Hazen's mid-rank \citep{hazen1914storage} (Section~\ref{sec:properties} discusses the choice)."
claim-hash: 8ec2466bb162
source: doi:10.1061/taceat.0002563
version-read: version of record (scan of Trans. ASCE vol. 77, 1914; pages 1629-1740 of the archive.org volume scan, extracted)
archived: gs://scholar-version2-literature/sources/hazen1914storage--10.1061_taceat.0002563-1fbb5981.pdf sha256:e80f3c2b37f161d8ac843c6c0e1a78db86343bb4e08fff8db8f9324db8dfc5de
license: public domain (1914 US publication)
updates: none
verdict: supported
evidence: "the figure for each term, arranged in its order of magnitude, is plotted at the percentage which corresponds to the middle of its strip" (printed p. 1549, 'Probability Paper'); "The first term will be plotted at the middle of the first 2% strip; that is, on the 1% line" (printed p. 1549)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:bornmann2014individual — paper/manuscript_qss/main.tex:171
claim: "\citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: fbeedac9749c
source: doi:10.1007/s11192-013-1161-y
version-read: preprint (arXiv:1302.3697v2)
archived: gs://scholar-version2-literature/sources/bornmann2014individual--10.1007_s11192-013-1161-y-22e3c955.pdf sha256:f53354eb37b3efb9676ae2f64101071c8aa1d7ffe88e12df5d581f88ea210334
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we would like to propose using the number of years as an active researcher (Ptop 10% quotient) to normalise Ptop 10% for age" (sec. 3.2)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:171
claim: "\citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: fbeedac9749c
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "One can also consider hundred percentiles as a continuous random variable and sum these values" (pp. 5-6); "The citations are weighted in accordance with the percentile" (p. 7)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:rousseau2012basic — paper/manuscript_qss/main.tex:171
claim: "\citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by academic age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: fbeedac9749c
source: doi:10.1002/asi.21684
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/rousseau2012basic--10.1002_asi.21684-660d9303--author-supplied.pdf sha256:1a433fb4d0d616a7ddda5131a3f78980e18f46857eb154451f833f2941c2f8e3
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "The I3 score of a Set A is defined as" (Definition 4 (formula: sum over classes k = 1..K of x_k n_A(k))); "the Integrated Impact Indicator (I3), introduced by Leydesdorff and Bornmann (2011), is a strictly congruous indicator of absolute performance" (abstract)
notes: I3 sums the percentile-class scores x_k of a set's papers (formula garbled by text extraction; read from the PDF). Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:rodriguez2018double — paper/manuscript_qss/main.tex:177
claim: "For countries and institutions, double rank analysis gives each of a unit's papers a rank within the unit and a rank among all papers in the world, and reads the unit's output at very high percentiles from the relation between the two \citep{rodriguez2018double,brito2018percentile}."
claim-hash: de03b22ec35b
source: doi:10.1016/j.joi.2017.11.004
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/rodriguez2018double--10.1016_j.joi.2017.11.004-a349ac75--author-supplied.pdf sha256:b6b046520aa6f09e3fc45ef69b73699277e103b12d7c4db425a716a7c06efcfe
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the internal ranking number can be expressed as a function of the world ranking number" (abstract); "extrapolation allows estimating the likelihood of publishing the highest cited publication" (abstract); "which includes a low number of publications in most countries and institutions" (abstract)
notes: Each paper's rank within the unit as a function of its world rank; extrapolation reads the very top of the distribution; applied to countries and institutions. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:sinatra2016quantifying — paper/manuscript_qss/main.tex:184
claim: "Later work modeled impact as a stable individual parameter applied to papers of random quality \citep{sinatra2016quantifying}, and \citet{clauset2017datadriven} review the area."
claim-hash: 3d9649427947
source: doi:10.1126/science.aaf5239
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/sinatra2016quantifying--10.1126_science.aaf5239-d62f25d9--author-supplied.pdf sha256:9bff5e1e8523223f47f74fa00895ec376ae244d498f73e241bf668c517c8a9f9
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "The model assumes that each scientist selects a project with a random potential p and improves on it with a factor Qi, resulting in a publication of impact Qip." (research article summary); "Together, these measurements indicate that the Q parameter is generally stable throughout a career" (results)
notes: Impact = Q_i x p with random p and a career-stable Q_i. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:clauset2017datadriven — paper/manuscript_qss/main.tex:184
claim: "Later work modeled impact as a stable individual parameter applied to papers of random quality \citep{sinatra2016quantifying}, and \citet{clauset2017datadriven} review the area."
claim-hash: 3d9649427947
source: doi:10.1126/science.aal4217
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/clauset2017datadriven--10.1126_science.aal4217-d398d626--author-supplied.pdf sha256:cbdacc0c763f62aee73c1d82bc442cd0e2dec46690802cf883004ef8d374f430
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "we survey the emerging and interdisciplinary field of the" (abstract); "Four areas exemplify these achievements: citations of past discoveries, who gets hired into career researcher positions, and both the scientific productivity and the timing of major discoveries over a career." (introduction)
notes: An Essay surveying prediction in the science of science, including citations, careers and productivity. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:acuna2012future — paper/manuscript_qss/main.tex:186
claim: "Other work predicted future $h$-index values from features of a career \citep{acuna2012future} and questioned how much of that predictability is mechanical, because the future $h$-index contains the current one \citep{penner2013predictability}."
claim-hash: 0e658014568d
source: doi:10.1038/489201a
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/acuna2012future--10.1038_489201a-fecad52c--author-supplied.pdf sha256:6cee4984a49915129d5de245ffbee91a948378792120f2e1a329424a0bef4c6b
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "present a formula to estimate the future h-index of life scientists" (standfirst); "n, number of articles written; h, current h-index; y, years since publishing first article; j, number of distinct journals published in" (figure key)
notes: The formula predicts the future h-index from career features (articles, current h, career length, distinct journals, articles in top journals). Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:penner2013predictability — paper/manuscript_qss/main.tex:186
claim: "Other work predicted future $h$-index values from features of a career \citep{acuna2012future} and questioned how much of that predictability is mechanical, because the future $h$-index contains the current one \citep{penner2013predictability}."
claim-hash: 0e658014568d
source: doi:10.1038/srep03052
version-read: version of record
archived: gs://scholar-version2-literature/sources/penner2013predictability--10.1038_srep03052-5af70d94.xml sha256:0ec5949cf8c9968d00edb312c484c4008ff9fb57f89ce2fa9fb1a3d89800c0a4
license: CC-BY-NC-SA-3.0 (Scientific Reports)
updates: none
verdict: supported
evidence: "cumulative non-decreasing measures like the h -index contain intrinsic autocorrelation, resulting in significant overestimation of their “predictive power”" (Abstract); "linear regression models are not so much predicting future impact as they are picking up on a correlation intrinsic to cumulative measures" (Results, 'Modeling non-cumulative measures')
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:merton1968matthew — paper/manuscript_qss/main.tex:192
claim: "Citations also feed on themselves: well-known scholars receive more attention for the same work \citep{merton1968matthew,petersen2014reputation}."
claim-hash: cfffe0ec91af
source: doi:10.1126/science.159.3810.56
version-read: version of record (scan of Science 159(3810):56-63, 1968; OCR text)
archived: gs://scholar-version2-literature/sources/merton1968matthew--10.1126_science.159.3810.56-0577b48e.pdf sha256:95fc1cb4a84cf1c563b8085c0c8adca9f44c0162ae343368c688c1f5b64e5209
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "entist may have greater stimuius vaiue" (p. 61 (OCR of 'cognitive material presented by an outstanding scientist may have greater stimulus value')); "than roughly the same kind of mater" (p. 61, next line); "ial presented by an obscure one-a" (p. 61)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:petersen2014reputation — paper/manuscript_qss/main.tex:192
claim: "Citations also feed on themselves: well-known scholars receive more attention for the same work \citep{merton1968matthew,petersen2014reputation}."
claim-hash: cfffe0ec91af
source: doi:10.1073/pnas.1323111111
version-read: preprint (arXiv:1303.7274v4)
archived: gs://scholar-version2-literature/sources/petersen2014reputation--10.1073_pnas.1323111111-aab695c0.pdf sha256:e9a2fedc3721a39f5ec0a686fb3d15f9ebaf99579c5bc1b844dce252f36e2205
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "a new publication may gain a significant early advantage corresponding to roughly a 66% increase in the citation rate for each tenfold increase in Ci" (Abstract)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:200
claim: "We use the 27 October 2026 release of the Semantic Scholar Academic Graph \citep{kinney2023semantic}, which is distributed under the ODC-BY license \citep{s2datasets2026}."
claim-hash: a4bd383b1d63
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "S2AG is a disambiguated, high-quality, bibliographic" (abstract)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:s2datasets2026 — paper/manuscript_qss/main.tex:200
claim: "We use the 27 October 2026 release of the Semantic Scholar Academic Graph \citep{kinney2023semantic}, which is distributed under the ODC-BY license \citep{s2datasets2026}."
claim-hash: a4bd383b1d63
source: https://api.semanticscholar.org/datasets/v1/release/2026-10-27
version-read: release 2026-10-27 dataset READMEs (JSON), fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/s2datasets2026--api.semanticscholar.org_datasets_v1_release_2026-10-27-1ddd5a1e--2026-11-05-385e6b41.json sha256:385e6b41f1ccaa0199439dd8b7cf1a0c3cfb112ec2f1e784741d749ca5e869f0
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "This collection is licensed under ODC-BY." (README of the papers, authors and citations datasets, release 2026-10-27)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:232
claim: "Semantic Scholar's author records are built by automatic disambiguation, although authors can claim and correct them \citep{kinney2023semantic}."
claim-hash: 50abf8487eb6
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "In Author Disambiguation, we use a system named" (sec. 3 (column-split)); "S2AND, introduced in Subramanian et al. (2021)," (sec. 3, continuation)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:googlescholarprofiles — paper/manuscript_qss/main.tex:233
claim: "Google Scholar profiles are created and curated by the scholars themselves \citep{googlescholarprofiles}, so they are a check on automatic disambiguation (Section~\ref{sec:robustness})."
claim-hash: aa17b7390437
source: https://scholar.google.com/intl/en/scholar/citations.html
version-read: help page fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/google2026--scholar.google.com_intl_en_scholar_citations.html-5a1f02e3--2026-11-05-ee36f4c6.html sha256:ee36f4c64761878fab990a35b7ca3413ea762bd9d37e6685ebf1b6a7efe41615
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "Google Scholar Profiles provide a simple way for authors to showcase their academic publications." (intro); "Add all articles that you have written" (Setting up your profile); "The best way to fix this is to look through the articles in your profile and remove the ones that were written by others." (FAQ)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:354
claim: "They are a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, but in the standing they would give credit to authors with nothing above a threshold: at high thresholds most peers have no article, and an author tied with them at zero would stand at half their share."
claim-hash: 22f090d147a6
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "The compromise ((i-0.5)/n*100) derived by (Hazen, 1914) is used very frequently" (sec. 2)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:354
claim: "They are a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, but in the standing they would give credit to authors with nothing above a threshold: at high thresholds most peers have no article, and an author tied with them at zero would stand at half their share."
claim-hash: 22f090d147a6
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Pudovkin and Garfield put the tied publications in a random order and calculate their average percentile." (sec. 3)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:392
claim: "The first form of the score \citep{ipeirotis2024pip} is asymmetric; the website \url{https://www.pip-score.org} uses it."
claim-hash: 6da5ed1d1e69
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: supported
evidence: "we created a small app at https://scholar.ipeirotis.org/ that allows anyone to search for a Google Scholar profile" (post body, second paragraph)
notes: The post describes the first form of the score; that the website pip-score.org uses it is the authors' statement about their own site, consistent with the author's confirmation and the 2026-11-05 redirect check (scholar.ipeirotis.org -> pip-score.org). 'Asymmetric' is the manuscript's own characterization.

## [2026-11-05T17:31Z] cite:hazen1914storage — paper/manuscript_qss/main.tex:458
claim: "These percentiles are not the strict shares used in the score but Hazen mid-ranks \citep{hazen1914storage}, which also count half of the tied papers."
claim-hash: 34f33a9ec934
source: doi:10.1061/taceat.0002563
version-read: version of record (scan of Trans. ASCE vol. 77, 1914; pages 1629-1740 of the archive.org volume scan, extracted)
archived: gs://scholar-version2-literature/sources/hazen1914storage--10.1061_taceat.0002563-1fbb5981.pdf sha256:e80f3c2b37f161d8ac843c6c0e1a78db86343bb4e08fff8db8f9324db8dfc5de
license: public domain (1914 US publication)
updates: none
verdict: partially supported
evidence: "if there are 50 terms in the series, each is taken to represent 2% of the whole space" (printed p. 1549); "the figure for each term, arranged in its order of magnitude, is plotted at the percentage which corresponds to the middle of its strip" (printed p. 1549)
notes: Hazen supports the mid-rank (middle-of-strip) plotting position; the added clause 'which also count half of the tied papers' describes tie handling, which Hazen 1914 does not discuss (it is the mid-rank convention as the manuscript uses it in the Ties paragraph). Clause added in the blue-pencil revision; flagged to the author.

## [2026-11-05T17:31Z] cite:aksnes2003macro — paper/manuscript_qss/main.tex:995
claim: "These are 8.1\% of all citations; such rates depend strongly on the definition and the citation window \citep{aksnes2003macro}."
claim-hash: 928cce965a4f
source: doi:10.1023/a:1021919228368
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/aksnes2003macro--10.1023_a_1021919228368-e0aafd03--author-supplied.pdf sha256:92570f9adaa3f4694f532bdfd9d5ab74268dd6ce73a01d4e6a95fd1cbb664756
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "Using a three-year citation window we find that 36% of all citations represent author self-citations." (abstract); "In contrast, the self-citation share would be 29% when using a 5-year window." (results); "Methodologically, self-citations can be counted in two different ways, diachronously or synchronously." (methods)
notes: The share depends on the window (36% vs 29%) and on how self-citations are counted. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:zitt2005relativity — paper/manuscript_qss/main.tex:1076
claim: "Field normalization brings its own choices: the field scheme and the assignment of multi-field papers \citep{zitt2005relativity,waltman2015field,waltman2019field}, and, for a scholar, the assignment of a main field."
claim-hash: 458b1cd21099
source: doi:10.1007/s11192-005-0218-y
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/zitt2005relativity--10.1007_s11192-005-0218-y-bf0707e5--author-supplied.pdf sha256:6e9640a55a90b60b373274f2171207e2633146e5c84f790cb75db0bf6b43becf
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the contents of the" (abstract); "set is completely dependent on the level of observation" (abstract); "Overlapping classes result in several normalised impact measures for each multi-assigned journal" (sec. 2, data)
notes: The field scheme (level of observation) changes which articles are top-cited; multi-assignment to overlapping classes is a choice the study sets aside. Supersedes the earlier 'unverifiable' entry (full text supplied by the author on 2026-11-05).

## [2026-11-05T17:31Z] cite:waltman2015field — paper/manuscript_qss/main.tex:1092
claim: "A fractional version would weight each paper's contribution to the author's count $n_a(\theta)$ at each threshold by the author's share \citep{waltman2015field}; we have not tested it."
claim-hash: b10b31611133
source: doi:10.1016/j.joi.2015.08.001
version-read: preprint (arXiv:1501.04431v1)
archived: gs://scholar-version2-literature/sources/waltman2015field--10.1016_j.joi.2015.08.001-8988ca16.pdf sha256:acc87a4b752fbf52994b18aa703b1561c84124c780e9de46a66ff8a0341fc2a6
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "In the fractional counting case, a publication is fractionally assigned to each co-author." (sec. 2.1, pp. 1-2); "publication is assigned to a co-author indicates the share of the publication allocated to that co-author." (sec. 2.1, pp. 1-2)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T17:31Z] cite:waltman2012inconsistency — paper/manuscript_qss/main.tex:1106
claim: "\emph{Consistency.} PiP is not consistent in the sense of \citet{waltman2012inconsistency}: adding the same article to two authors can reverse their order, although such reversals are rare in simulation."
claim-hash: a4c7b76c8c0d
source: doi:10.1002/asi.21678
version-read: preprint (arXiv:1108.3901v1)
archived: gs://scholar-version2-literature/sources/waltman2012inconsistency--10.1002_asi.21678-42823f3e.pdf sha256:8afcd8681c22f8267eb5442a4ca26783f88c7206b05a96133294a97a369198ca
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "If two scientists achieve the same absolute performance improvement, their ranking relative to each other should remain unchanged." (sec. 3.2, Example 2, p. 7); "the h-index fails to provide consistent rankings of individual scientists and research groups" (sec. 3.3, Example 3, p. 8)
notes: Re-verified against the same archived passage after the blue-pencil revision of 2026-11-05: the wording changed, the attribution to this source did not.


## [2026-11-05T18:40Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:88
claim: "They are less influenced than means by the skewed tail of the citation distribution \citep{bornmann2013use}, they need no assumption about how citations grow with age, and their meaning does not change with the calendar."
claim-hash: 8cbe48529cc8
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value" (sec. 2, The calculation of percentiles); "(3) Percentiles can be calculated, even if the data are skewed, as it is usual for bibliometric data." (sec. 2)
notes: Intro sentence now says percentiles are 'less influenced than means' by the skewed tail, the source's own comparative claim (it replaced the absolute 'do not depend on'). In Related work the attribution is unchanged; the added gloss of mean normalization is attributed to the co-cited Schubert & Braun and Radicchi et al.

## [2026-11-05T18:40Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:151
claim: "An earlier, asymmetric form of the score was described by \citet{ipeirotis2024pip} and is used by the website \url{https://www.pip-score.org} (formerly \url{https://scholar.ipeirotis.org}); Section~\ref{sec:pip} explains why we replace it."
claim-hash: b02cc949bcc4
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: supported
evidence: "we have a nice new score that we have given the thoroughly boring name "Percentile in Percentile Area Under the Curve" score, or PiP-AUC for short" (post body); "we created a small app at https://scholar.ipeirotis.org/" (post body, second paragraph)
notes: 'A first'/'The first' form became 'an earlier'/'the earlier' form; the post still describes it (PiP-AUC) and the website link is unchanged. Whether the post's area is a step or a trapezoid is not stated in the post; the manuscript now ties the step formula to the website ('as the website computes it'), not to the post.

## [2026-11-05T18:40Z] cite:egghe2006theory — paper/manuscript_qss/main.tex:160
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 01bad35a5c43
source: doi:10.1007/s11192-006-0144-7
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/egghe2006theory--10.1007_s11192-006-0144-7-c5ffc342--author-supplied.pdf sha256:6cbee1f21ba0279b2b76d5d8692a0c7a29e6a9be859436fceaa88adbc747b72d
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the g-index is the (unique) largest number such that the top g articles received (together) at least g2 citations" (abstract); "it is insensitive to one or several outstandingly highly cited papers" (sec. 1, on the h-index)
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:160
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 01bad35a5c43
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "of h versus n, the parameter m, should provide a useful yardstick to compare scientists of different seniority." (p. 3, after Eq. (6))
notes: 'Academic age' became 'career age' (years since the first publication, as the manuscript defines it). Hirsch's m divides h by the years since the first paper. Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:sidiropoulos2007generalized — paper/manuscript_qss/main.tex:160
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 01bad35a5c43
source: doi:10.1007/s11192-007-1722-z
version-read: preprint (arXiv:cs/0607066v1)
archived: gs://scholar-version2-literature/sources/sidiropoulos2007generalized--10.1007_s11192-007-1722-z-93dc68b1.pdf sha256:926776aa9e2f6d8a4f93cf91421b1c18070eba8be4192c44955d37ef2b6de9dc
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "This way, an old article gradually loses its “value”, even if it still gets citations." (p. 4, The contemporary h-index (after Eq. 2)); "If we set δ=1, then S c (i) is the number of citations that the article i has received, divided by the “age” of the article." (p. 4, The contemporary h-index (after Eq. 2))
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:harzing2014hia — paper/manuscript_qss/main.tex:160
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 01bad35a5c43
source: doi:10.1007/s11192-013-1208-0
version-read: accepted manuscript (author version, December 2013)
archived: gs://scholar-version2-literature/sources/harzing2014hia--10.1007_s11192-013-1208-0-17a20b4c.pdf sha256:c9490f3f542009fd086dc526c1de10836280b53afbf58252d29aafc5cdd44f1d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "We therefore follow Hirsch’s original suggestion to correct for career length by dividing the h‐index by the number of years an academic has been publishing." (p. 5); "the hIa‐index is thus calculated by dividing Harzing’s Publish or Perish hI‐norm (Harzing, 2007) by the number of years that an academic has been publishing" (p. 5)
notes: 'Academic age' became 'career age'; the source divides by 'the number of years that an academic has been publishing', which is career age as the manuscript defines it.

## [2026-11-05T18:40Z] cite:kaur2013universality — paper/manuscript_qss/main.tex:160
claim: "The $g$-index \citep{egghe2006theory} gives credit to the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: 01bad35a5c43
source: doi:10.1016/j.joi.2013.09.002
version-read: preprint (arXiv:1305.6339v2)
archived: gs://scholar-version2-literature/sources/kaur2013universality--10.1016_j.joi.2013.09.002-74fa8e45.pdf sha256:784a714cf7633ff9623c1b53b56491fb53fa5b7d0ab70b1592b7b50a29317267
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "hs index is proposed here as a normalization of the h index by the average h of the authors in the same discipline." (sec. 2, metric definitions (hs index))
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:170
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}, and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d7a782b7120e
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value" (sec. 2, The calculation of percentiles)
notes: Intro sentence now says percentiles are 'less influenced than means' by the skewed tail, the source's own comparative claim (it replaced the absolute 'do not depend on'). In Related work the attribution is unchanged; the added gloss of mean normalization is attributed to the co-cited Schubert & Braun and Radicchi et al.

## [2026-11-05T18:40Z] cite:bornmann2020evaluation — paper/manuscript_qss/main.tex:170
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}, and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d7a782b7120e
source: doi:10.1007/s11192-020-03512-7
version-read: version of record
archived: gs://scholar-version2-literature/sources/bornmann2020evaluation--10.1007_s11192-020-03512-7-1f24fb42.pdf sha256:3ea8c359d01836ba0507727a744a394ae2b0778cfc91682e8bafe7b5676bc29e
license: CC-BY-4.0 (Springer open access)
updates: none
verdict: supported
evidence: "The advantage of PRs (and PPs) is that they are not strongly affected by outliers (highly cited papers)" (p. 1458, Introduction)
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:schubert1986relative — paper/manuscript_qss/main.tex:170
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}, and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d7a782b7120e
source: doi:10.1007/bf02017249
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/schubert1986relative--10.1007_bf02017249-0d8903d0--author-supplied.pdf sha256:608a7ca180944a650b810b6317e708c6785aab50bd6953918c1411afd98ca2f0
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the average citation rate of the journal in which a paper was published is a valid standard to which its citation rate can be matched" (sec. 2); "corresponds precisely to the world average" (sec. 2, activity and attractivity indexes)
notes: Sentence now glosses mean normalization as comparing 'a paper's citations with the average of comparable papers'; the source matches a paper's citation rate against the journal's average citation rate.

## [2026-11-05T18:40Z] cite:Radicchi2008 — paper/manuscript_qss/main.tex:170
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}, and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d7a782b7120e
source: doi:10.1073/pnas.0806977105
version-read: preprint (arXiv:0806.0974v2)
archived: gs://scholar-version2-literature/sources/radicchi2008--10.1073_pnas.0806977105-fa509e28.pdf sha256:6dde217e74a426a0d1f6d900d74f0fce3452ab8c500260979751d86d06d7c5a0
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we have considered the average number c0 of citations" (p. 2); "as an unbiased relative indicator of scientific impact" (p. 1)
notes: Sentence now glosses mean normalization as comparing 'a paper's citations with the average of comparable papers'; the source divides by the average number c0 of citations of comparable papers.

## [2026-11-05T18:40Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:170
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than normalization by mean citation rates, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}, and are recommended in the Leiden Manifesto \citep{hicks2015bibliometrics}."
claim-hash: d7a782b7120e
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the most robust normalization method is based on percentiles" (p. 431, principle 6)
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:bornmann2014individual — paper/manuscript_qss/main.tex:177
claim: "\citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by career age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 04323bd4b03c
source: doi:10.1007/s11192-013-1161-y
version-read: preprint (arXiv:1302.3697v2)
archived: gs://scholar-version2-literature/sources/bornmann2014individual--10.1007_s11192-013-1161-y-22e3c955.pdf sha256:f53354eb37b3efb9676ae2f64101071c8aa1d7ffe88e12df5d581f88ea210334
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we would like to propose using the number of years as an active researcher (Ptop 10% quotient) to normalise Ptop 10% for age" (sec. 3.2)
notes: 'Academic age' became 'career age'; the source normalizes by 'the number of years as an active researcher', which is career age as the manuscript defines it.

## [2026-11-05T18:40Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:177
claim: "\citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by career age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 04323bd4b03c
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "One can also consider hundred percentiles as a continuous random variable and sum these values" (pp. 5-6); "The citations are weighted in accordance with the percentile" (p. 7)
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:rousseau2012basic — paper/manuscript_qss/main.tex:177
claim: "\citet{bornmann2014individual} propose counting a scholar's papers in the top 10\% and dividing by career age, and I3 \citep{leydesdorff2011integrated,rousseau2012basic} sums percentiles over a set of papers."
claim-hash: 04323bd4b03c
source: doi:10.1002/asi.21684
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/rousseau2012basic--10.1002_asi.21684-660d9303--author-supplied.pdf sha256:1a433fb4d0d616a7ddda5131a3f78980e18f46857eb154451f833f2941c2f8e3
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "The I3 score of a Set A is defined as" (Definition 4 (formula: sum over classes k = 1..K of x_k n_A(k))); "the Integrated Impact Indicator (I3), introduced by Leydesdorff and Bornmann (2011), is a strictly congruous indicator of absolute performance" (abstract)
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:ioannidis2019standardized — paper/manuscript_qss/main.tex:179
claim: "Composite indicators such as the one of \citet{ioannidis2019standardized} combine several citation counts, and \citet{abramo2014productivity} relate field-normalized citations to the cost of labor."
claim-hash: 3f30593cad52
source: doi:10.1371/journal.pbio.3000384
version-read: version of record
archived: gs://scholar-version2-literature/sources/ioannidis2019standardized--10.1371_journal.pbio.3000384-89de78a2.xml sha256:68b36d5eddd9616ed488af0c2dd001e879b5a380d2d6059023cd6c338ff901a8
license: CC-BY-4.0 (PLOS)
updates: new_version doi:10.1371/journal.pbio.3000384 (2019-08-19, publisher)
verdict: supported
evidence: "composite indicator that considers six citation metrics (total citations; Hirsch h-index; coauthorship-adjusted Schreiber hm-index; number of citations to papers as single author; number of citations to papers as single or first author; and number of citations to papers as single, first, or last author)" (Main text, database construction paragraph)
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:abramo2014productivity — paper/manuscript_qss/main.tex:179
claim: "Composite indicators such as the one of \citet{ioannidis2019standardized} combine several citation counts, and \citet{abramo2014productivity} relate field-normalized citations to the cost of labor."
claim-hash: 3f30593cad52
source: doi:10.1007/s11192-014-1269-8
version-read: preprint (arXiv:1810.12830v1)
archived: gs://scholar-version2-literature/sources/abramo2014productivity--10.1007_s11192-014-1269-8-95b9efa5.pdf sha256:04b35f0064c50e7c47e8a5d72e4caff628f477c4fc8658039293823fb32e5f37
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we measure FSSR, a proxy of the average yearly productivity over a period of time, accounting for the cost of labor." (sec. Labor productivity at the individual level, p. 7); "To measure the cost efficiency or research units, one should normalize its output by the cost of labor." (sec. Labor productivity in research activity and the FSS, p. 5-6); "average of the distribution of citations received for all cited publications" (definition of c-bar in Eq. [1], p. 7)
notes: 'Weight field-normalized citations by inputs' became 'relate field-normalized citations to the cost of labor', the source's own wording ('accounting for the cost of labor').

## [2026-11-05T18:40Z] cite:rodriguez2018double — paper/manuscript_qss/main.tex:183
claim: "For countries and institutions, double rank analysis expresses the rank of each of a unit's papers within the unit as a function of its rank among all papers in the world; a power law fits this relation and estimates how likely the unit is to produce a very highly cited paper \citep{rodriguez2018double,brito2018percentile}."
claim-hash: 05a65e8d5a00
source: doi:10.1016/j.joi.2017.11.004
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/rodriguez2018double--10.1016_j.joi.2017.11.004-a349ac75--author-supplied.pdf sha256:b6b046520aa6f09e3fc45ef69b73699277e103b12d7c4db425a716a7c06efcfe
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the internal ranking number can be expressed as a function of the world ranking number" (abstract); "extrapolation allows estimating the likelihood of publishing the highest cited publication" (abstract); "which includes a low number of publications in most countries and institutions" (abstract)
notes: Double rank analysis is now described as expressing a paper's rank within the unit as a function of its world rank, fitted by a power law to estimate the chance of a very highly cited paper; the abstract says both.

## [2026-11-05T18:40Z] cite:brito2018percentile — paper/manuscript_qss/main.tex:183
claim: "For countries and institutions, double rank analysis expresses the rank of each of a unit's papers within the unit as a function of its rank among all papers in the world; a power law fits this relation and estimates how likely the unit is to produce a very highly cited paper \citep{rodriguez2018double,brito2018percentile}."
claim-hash: 05a65e8d5a00
source: doi:10.1016/j.joi.2018.01.011
version-read: preprint (arXiv:1712.00399v2)
archived: gs://scholar-version2-literature/sources/brito2018percentile--10.1016_j.joi.2018.01.011-2027d857.pdf sha256:7ebab94bcec8cf9f7d71333a6783dadd0107ff16314c4b89a211681f1d8508ba
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "In the double rank analysis of research publications, the local rank position of a country or institution publication is expressed as a function of the world rank position." (Abstract); "the ranking number of a publication in a country or institution is expressed as a function of its world ranking number. The resulting function can be well fitted by a power law, which allows estimating the likelihood of any country or institution producing a very highly cited paper" (Sec. 1, Introduction)
notes: Double rank analysis is now described as expressing a paper's rank within the unit as a function of its world rank, fitted by a power law to estimate the chance of a very highly cited paper; the introduction says both.

## [2026-11-05T18:40Z] cite:wikidata2026 — paper/manuscript_qss/main.tex:248
claim: "For the test of later recognition (Section~\ref{sec:honors}) we use Wikidata \citep{wikidata2026}, a free, collaborative knowledge base whose structured data are released under CC0."
claim-hash: eba2a68f3b55
source: https://www.wikidata.org/wiki/Wikidata:Licensing; https://www.wikidata.org/wiki/Wikidata:Introduction
version-read: web pages fetched 2026-11-05 (Licensing last edited 24 September 2026; Introduction last edited 21 October 2026)
archived: gs://scholar-version2-literature/sources/wikidata2026--www.wikidata.org_wiki_wikidata_licensing-46a07189--2026-11-05-cf11c407.html sha256:cf11c4071d85b2fdcfd796c5dacb563a71e852e8e398e7d1dd876123ad893832; gs://scholar-version2-literature/sources/wikidata2026--www.wikidata.org_wiki_wikidata_introduction-4bc51fa4--2026-11-05-dcd25538.html sha256:dcd25538bdfe6fb04270fc41d884dd618f24e33fcaa74a52192fb66110b0dd0b
license: CC BY-SA 4.0 page text (private store)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: "All structured data (i.e. the main, Property, Lexeme, and EntitySchema namespaces) is released into the public domain under Creative Commons Zero." (Wikidata:Licensing, Official policy); "Wikidata is a free, collaborative, multilingual, secondary knowledge base, collecting structured data to provide support for Wikipedia, Wikimedia Commons, the other wikis of the Wikimedia movement, and to anyone in the world." (Wikidata:Introduction, first paragraph)
notes: New source added on 2026-11-05 for the Wikidata honors and Wikipedia links of Section 4.7; the query date is the fetch date recorded in paper/honors/fetch_wikidata_honors.py. What items record (honors, dates, identifiers) is the manuscript's own description of the query, not attributed to these pages.

## [2026-11-05T18:40Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:416
claim: "The earlier form of the score \citep{ipeirotis2024pip} is asymmetric; the website \url{https://www.pip-score.org} uses it."
claim-hash: 69c1e5fd3f33
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: supported
evidence: "we created a small app at https://scholar.ipeirotis.org/ that allows anyone to search for a Google Scholar profile" (post body, second paragraph)
notes: 'A first'/'The first' form became 'an earlier'/'the earlier' form; the post still describes it (PiP-AUC) and the website link is unchanged. Whether the post's area is a step or a trapezoid is not stated in the post; the manuscript now ties the step formula to the website ('as the website computes it'), not to the post.

## [2026-11-05T18:40Z] cite:hazen1914storage — paper/manuscript_qss/main.tex:483
claim: "These percentiles are not the strict shares used in the score but Hazen mid-ranks \citep{hazen1914storage}."
claim-hash: 60f7c21e5d37
source: doi:10.1061/taceat.0002563
version-read: version of record (scan of Trans. ASCE vol. 77, 1914; pages 1629-1740 of the archive.org volume scan, extracted)
archived: gs://scholar-version2-literature/sources/hazen1914storage--10.1061_taceat.0002563-1fbb5981.pdf sha256:e80f3c2b37f161d8ac843c6c0e1a78db86343bb4e08fff8db8f9324db8dfc5de
license: public domain (1914 US publication)
updates: none
verdict: supported
evidence: "if there are 50 terms in the series, each is taken to represent 2% of the whole space" (printed p. 1549); "the figure for each term, arranged in its order of magnitude, is plotted at the percentage which corresponds to the middle of its strip" (printed p. 1549)
notes: The clause 'which also count half of the tied papers', which Hazen 1914 does not discuss, was removed at the author's request on 2026-11-05. The sentence now attributes only the mid-rank (middle-of-strip) plotting position to Hazen, which the source supports. Supersedes the 'partially supported' entry for the earlier wording.

## [2026-11-05T18:40Z] cite:s2datasets2026 — paper/manuscript_qss/main.tex:1205
claim: "Semantic Scholar data are available under the ODC-BY license from the Semantic Scholar Datasets API \citep{s2datasets2026}; Wikidata is available under CC0 \citep{wikidata2026}."
claim-hash: fa66eaf992e3
source: https://api.semanticscholar.org/datasets/v1/release/2026-10-27
version-read: release 2026-10-27 dataset READMEs (JSON), fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/s2datasets2026--api.semanticscholar.org_datasets_v1_release_2026-10-27-1ddd5a1e--2026-11-05-385e6b41.json sha256:385e6b41f1ccaa0199439dd8b7cf1a0c3cfb112ec2f1e784741d749ca5e869f0
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "This collection is licensed under ODC-BY." (README of the papers, authors and citations datasets, release 2026-10-27)
notes: Re-verified against the same archived passage after the fixes of 2026-11-05: the wording changed, the attribution to this source did not.

## [2026-11-05T18:40Z] cite:wikidata2026 — paper/manuscript_qss/main.tex:1205
claim: "Semantic Scholar data are available under the ODC-BY license from the Semantic Scholar Datasets API \citep{s2datasets2026}; Wikidata is available under CC0 \citep{wikidata2026}."
claim-hash: fa66eaf992e3
source: https://www.wikidata.org/wiki/Wikidata:Licensing; https://www.wikidata.org/wiki/Wikidata:Introduction
version-read: web pages fetched 2026-11-05 (Licensing last edited 24 September 2026; Introduction last edited 21 October 2026)
archived: gs://scholar-version2-literature/sources/wikidata2026--www.wikidata.org_wiki_wikidata_licensing-46a07189--2026-11-05-cf11c407.html sha256:cf11c4071d85b2fdcfd796c5dacb563a71e852e8e398e7d1dd876123ad893832; gs://scholar-version2-literature/sources/wikidata2026--www.wikidata.org_wiki_wikidata_introduction-4bc51fa4--2026-11-05-dcd25538.html sha256:dcd25538bdfe6fb04270fc41d884dd618f24e33fcaa74a52192fb66110b0dd0b
license: CC BY-SA 4.0 page text (private store)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: "All structured data (i.e. the main, Property, Lexeme, and EntitySchema namespaces) is released into the public domain under Creative Commons Zero." (Wikidata:Licensing, Official policy)
notes: New source added on 2026-11-05 for the Wikidata honors and Wikipedia links of Section 4.7; the query date is the fetch date recorded in paper/honors/fetch_wikidata_honors.py. What items record (honors, dates, identifiers) is the manuscript's own description of the query, not attributed to these pages.

## [2026-11-05T19:05Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1204
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-05
archived: not copied; the deposited file is pip-replication-package-cc071a3.zip (md5:98fa984979c62214e96fc2579208ebf4, sha256:033f1d8c4784099827d198bd7d9a6113f0a6c80e05d0716760a2429dd3078c19), built by paper/make_zenodo_package.sh from commit cc071a3
license: CC BY 4.0 (record metadata)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at cc071a3, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication.

## [2026-11-05T19:20Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1204
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-05
archived: not copied; the deposited file is pip-replication-package-9b84e65.zip (md5:c788194f02aac0e1ba572f0fda9e39f7, sha256:5cead90a5ed47c72a92e8565098c0da98549e6740b358dfafa966b1ebe4c8b96), built by paper/make_zenodo_package.sh from commit 9b84e65
license: CC BY 4.0 (record metadata)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at 9b84e65, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for cc071a3: the file was replaced after the CRediT statement was added.

## [2026-11-05T20:10Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1204
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-05
archived: not copied; the deposited file is pip-replication-package-05c00d3.zip (md5:c49e8ec09bd7c53e4bd4eac11c2b4e96, sha256:c8c155d076cf9af58f7df72a751cdba999838e24a41258a361beb278efb25b31), built by paper/make_zenodo_package.sh from commit 05c00d3
license: CC BY 4.0 (record metadata)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at 05c00d3, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for 9b84e65: the file was replaced after the review edits and the new abstract, and paper/v3_code/ is now left out.

## [2026-11-05T22:05Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1204
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-05
archived: not copied; the deposited file is pip-replication-package-dc9ff2b.zip (md5:33de642531272581fdbbb66401b11e7f, sha256:db58edbfc11984005c0b2315e6a05d75d315642579601f9fcd23f80f2f0bf0a8), built by paper/make_zenodo_package.sh from commit dc9ff2b
license: CC BY 4.0 (record metadata)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at dc9ff2b, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for 05c00d3: the file was replaced after the exposition pass (new table, figure annotations).

## [2026-11-06T00:10Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1204
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-05
archived: not copied; the deposited file is pip-replication-package-3191c7c.zip (md5:a2de2722de7ed81247b87d8171563cab, sha256:d65bae154e8d917be2b82ecda9ab1e1f3e16088a59a3a4246bfc8edf3b07023d), built by paper/make_zenodo_package.sh from commit 3191c7c
license: CC BY 4.0 (record metadata)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at 3191c7c, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for dc9ff2b: the file was replaced after the sawtooth wording fix.

## [2026-11-06T01:10Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:384
claim: "Mid-ranks, a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, would give credit to authors with nothing above a threshold; the strict rule prevents this, at the price that the mean score of a peer group depends on how common ties are (Eq.~\ref{eq:mean}; Supplementary Section~\ref{S-supp:ties})."
claim-hash: 2936f5fea2d6
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "The compromise ((i-0.5)/n*100) derived by (Hazen, 1914) is used very frequently" (sec. 2)
notes: The Ties paragraph was shortened when its detail moved to the Supplementary Information (supplement.tex, Section S2, which keeps the earlier sentence verbatim, claim-hash 22f090d147a6). The attribution to this source is unchanged: mid-ranks as a common choice for single-paper percentiles.

## [2026-11-06T01:10Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:384
claim: "Mid-ranks, a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, would give credit to authors with nothing above a threshold; the strict rule prevents this, at the price that the mean score of a peer group depends on how common ties are (Eq.~\ref{eq:mean}; Supplementary Section~\ref{S-supp:ties})."
claim-hash: 2936f5fea2d6
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Pudovkin and Garfield put the tied publications in a random order and calculate their average percentile." (sec. 3)
notes: The Ties paragraph was shortened when its detail moved to the Supplementary Information (supplement.tex, Section S2, which keeps the earlier sentence verbatim, claim-hash 22f090d147a6). The attribution to this source is unchanged: mid-ranks as a common choice for single-paper percentiles. Co-cited Bornmann et al. (2013) covers the 'common choice' part, as before.

## [2026-11-06T01:20Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1204
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-05
archived: not copied; the deposited file is pip-replication-package-07777bf.zip (md5:c6527171d19b832ea0ec628d7d943347, sha256:72cf5a9ddf5228693ffdd0e0326ade453931793c1b276e30283c0315cd3ca930), built by paper/make_zenodo_package.sh from commit 07777bf
license: CC BY 4.0 (record metadata)
accessed: 2026-11-05
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at 07777bf, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for 3191c7c: the file was replaced after the move of ten checks into supplement.tex.

## [2026-11-06T02:43Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:58
claim: "The most common are citation counts and the $h$-index \citep{hirsch2005index}, whose values depend on the field, grow with seniority and drift with the calendar."
claim-hash: 98b19e36f794
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "I propose the index h, defined as the number of papers with citation number higher or equal to h, as a useful index to characterize the scientific output of a researcher." (abstract)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:78
claim: "They are less influenced than means by the skewed tail of the citation distribution \citep{bornmann2013use}, need no assumption about how citations grow with age, and do not change meaning with the calendar."
claim-hash: 0a508e615b2c
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value" (sec. 2, The calculation of percentiles); "(3) Percentiles can be calculated, even if the data are skewed, as it is usual for bibliometric data." (sec. 2)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:82
claim: "Sums such as I3 \citep{leydesdorff2011integrated} grow without bound with the number of papers, so many weak papers can outweigh one strong paper."
claim-hash: f59b01c96a73
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Using the sum total of the frequencies (f) in each percentile, however, accounts for both the size and shape of the distribution." (p. 6-7, section on percentile rank classes (I3 definition)); "number of publications with less impact matters for the overall impact of two otherwise comparable documents sets." (p. 16-17, results (comparison of I3 with h-index-like top segment))
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not. Earlier note, still applicable: The source defines I3 as a sum over percentile values and says it accounts for size, so that the number of lower-impact publications matters. It does not state that the sum grows without bound or that many weak papers can outweigh one strong paper; that is the manuscript's own inference from the summation.

## [2026-11-06T02:43Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:99
claim: "We compute PiP from the Semantic Scholar Academic Graph \citep{kinney2023semantic} for 11.1 million authors at career ages 1 to 30 (years since their first paper)."
claim-hash: e3b85637e2e4
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "form is to build and distribute the Semantic Scholar" (sec. 2, Platform Overview (continues 'Academic Graph, or S2AG')); "Download links to monthly snapshots of our knowl" (sec. 4.2, Datasets)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:tian2021predictability — paper/manuscript_qss/main.tex:132
claim: "An earlier version of this work \citep{tian2021predictability} studied the predictability of paper and scholar percentiles on a smaller dataset of faculty at top US universities and rated scholars by summing their paper percentiles."
claim-hash: 486463bf0851
source: arXiv:2103.16025 (DataCite doi:10.48550/arXiv.2103.16025)
version-read: preprint (arXiv:2103.16025v3)
archived: gs://scholar-version2-literature/sources/tian2021predictability--on_the_predictability_of_utilizing_rank_percentile_to_evalua-681c8251.pdf sha256:b937c4c4ca0316809d0d9133cf4a82700a26d04511b1788436d7edee67ad64ed
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "The dataset utilized for this study is from Google Scholar and includes active faculty members (assistant, associate, and full professors) in multiple disciplines from the top 10 universities in the United States, which totals 14, 358 scholars." (p. 2, Dataset); "The publication percentile is highly stable over time, while the scholar percentile exhibits short-term stability and can be predicted via a simple linear regression model." (p. 1, Abstract); "The evaluation metric for scholar i is determined by aggregating the performance of all N(t)" (p. 3, The Rank Percentile Indicator for Scholars); "that is ∑ Pcj (5)" (p. 3, The Rank Percentile Indicator for Scholars)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:142
claim: "The $h$-index \citep{hirsch2005index} combines the number of papers and their citations into a single number and has been studied, criticized and modified at length."
claim-hash: 2ab04f756dba
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "I propose the index h, defined as the number of papers with citation number higher or equal to h" (abstract); "to propose a single number, the" (p. 1)
notes: 'Is a single number that depends on both the number of papers and their citations' became 'combines the number of papers and their citations into a single number'; the source defines $h$ from a scholar's papers and their citation counts. Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:egghe2006theory — paper/manuscript_qss/main.tex:144
claim: "The $g$-index \citep{egghe2006theory} credits the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: d3fe93ade82a
source: doi:10.1007/s11192-006-0144-7
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/egghe2006theory--10.1007_s11192-006-0144-7-c5ffc342--author-supplied.pdf sha256:6cbee1f21ba0279b2b76d5d8692a0c7a29e6a9be859436fceaa88adbc747b72d
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the g-index is the (unique) largest number such that the top g articles received (together) at least g2 citations" (abstract); "it is insensitive to one or several outstandingly highly cited papers" (sec. 1, on the h-index)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:hirsch2005index — paper/manuscript_qss/main.tex:144
claim: "The $g$-index \citep{egghe2006theory} credits the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: d3fe93ade82a
source: doi:10.1073/pnas.0507655102
version-read: preprint (arXiv:physics/0508025v5)
archived: gs://scholar-version2-literature/sources/hirsch2005index--10.1073_pnas.0507655102-f28ddc65.pdf sha256:ad20b4b792bde68eba06d00b944b47532aa26adefdabbabf6a649fa984481b96
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "of h versus n, the parameter m, should provide a useful yardstick to compare scientists of different seniority." (p. 3, after Eq. (6))
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:sidiropoulos2007generalized — paper/manuscript_qss/main.tex:144
claim: "The $g$-index \citep{egghe2006theory} credits the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: d3fe93ade82a
source: doi:10.1007/s11192-007-1722-z
version-read: preprint (arXiv:cs/0607066v1)
archived: gs://scholar-version2-literature/sources/sidiropoulos2007generalized--10.1007_s11192-007-1722-z-93dc68b1.pdf sha256:926776aa9e2f6d8a4f93cf91421b1c18070eba8be4192c44955d37ef2b6de9dc
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "This way, an old article gradually loses its “value”, even if it still gets citations." (p. 4, The contemporary h-index (after Eq. 2)); "If we set δ=1, then S c (i) is the number of citations that the article i has received, divided by the “age” of the article." (p. 4, The contemporary h-index (after Eq. 2))
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:harzing2014hia — paper/manuscript_qss/main.tex:144
claim: "The $g$-index \citep{egghe2006theory} credits the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: d3fe93ade82a
source: doi:10.1007/s11192-013-1208-0
version-read: accepted manuscript (author version, December 2013)
archived: gs://scholar-version2-literature/sources/harzing2014hia--10.1007_s11192-013-1208-0-17a20b4c.pdf sha256:c9490f3f542009fd086dc526c1de10836280b53afbf58252d29aafc5cdd44f1d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "We therefore follow Hirsch’s original suggestion to correct for career length by dividing the h‐index by the number of years an academic has been publishing." (p. 5); "the hIa‐index is thus calculated by dividing Harzing’s Publish or Perish hI‐norm (Harzing, 2007) by the number of years that an academic has been publishing" (p. 5)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:kaur2013universality — paper/manuscript_qss/main.tex:144
claim: "The $g$-index \citep{egghe2006theory} credits the citations above the corner; the $m$-quotient \citep{hirsch2005index}, the contemporary $h$-index \citep{sidiropoulos2007generalized} and $hI_{a}$ \citep{harzing2014hia} correct for career length by dividing by career age or discounting old papers; \citet{kaur2013universality} rescale the $h$-index by the field average."
claim-hash: d3fe93ade82a
source: doi:10.1016/j.joi.2013.09.002
version-read: preprint (arXiv:1305.6339v2)
archived: gs://scholar-version2-literature/sources/kaur2013universality--10.1016_j.joi.2013.09.002-74fa8e45.pdf sha256:784a714cf7633ff9623c1b53b56491fb53fa5b7d0ab70b1592b7b50a29317267
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "hs index is proposed here as a normalization of the h index by the average h of the authors in the same discipline." (sec. 2, metric definitions (hs index))
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:154
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than mean normalization, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}."
claim-hash: 866335ba2b42
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "(1) percentiles are not as strongly influenced by extreme values of the distribution as the mean value" (sec. 2, The calculation of percentiles)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:bornmann2020evaluation — paper/manuscript_qss/main.tex:154
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than mean normalization, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}."
claim-hash: 866335ba2b42
source: doi:10.1007/s11192-020-03512-7
version-read: version of record
archived: gs://scholar-version2-literature/sources/bornmann2020evaluation--10.1007_s11192-020-03512-7-1f24fb42.pdf sha256:3ea8c359d01836ba0507727a744a394ae2b0778cfc91682e8bafe7b5676bc29e
license: CC-BY-4.0 (Springer open access)
updates: none
verdict: supported
evidence: "The advantage of PRs (and PPs) is that they are not strongly affected by outliers (highly cited papers)" (p. 1458, Introduction)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:schubert1986relative — paper/manuscript_qss/main.tex:154
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than mean normalization, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}."
claim-hash: 866335ba2b42
source: doi:10.1007/bf02017249
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/schubert1986relative--10.1007_bf02017249-0d8903d0--author-supplied.pdf sha256:608a7ca180944a650b810b6317e708c6785aab50bd6953918c1411afd98ca2f0
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the average citation rate of the journal in which a paper was published is a valid standard to which its citation rate can be matched" (sec. 2); "corresponds precisely to the world average" (sec. 2, activity and attractivity indexes)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:Radicchi2008 — paper/manuscript_qss/main.tex:154
claim: "They are less sensitive to outliers \citep{bornmann2013use,bornmann2020evaluation} than mean normalization, which compares a paper's citations with the average of comparable papers \citep{schubert1986relative,Radicchi2008}."
claim-hash: 866335ba2b42
source: doi:10.1073/pnas.0806977105
version-read: preprint (arXiv:0806.0974v2)
archived: gs://scholar-version2-literature/sources/radicchi2008--10.1073_pnas.0806977105-fa509e28.pdf sha256:6dde217e74a426a0d1f6d900d74f0fce3452ab8c500260979751d86d06d7c5a0
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we have considered the average number c0 of citations" (p. 2); "as an unbiased relative indicator of scientific impact" (p. 1)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:hicks2015bibliometrics — paper/manuscript_qss/main.tex:156
claim: "The Leiden Manifesto recommends them \citep{hicks2015bibliometrics}."
claim-hash: 4eb493a165e8
source: doi:10.1038/520429a
version-read: version of record (Nature 520:429-431 typeset PDF, deposited in DIGITAL.CSIC)
archived: gs://scholar-version2-literature/sources/hicks2015bibliometrics--10.1038_520429a-a1c33bfa.pdf sha256:f141b19cb5d5f7db1aba13ad02decde290b83f059b52724126f8ae5ec563395d
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "the most robust normalization method is based on percentiles" (p. 431, principle 6)
notes: The Leiden Manifesto clause became its own sentence ('The Leiden Manifesto recommends them'); the attribution, the Manifesto's recommendation of percentile-based normalization, is unchanged. Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:hazen1914storage — paper/manuscript_qss/main.tex:158
claim: "For scoring we use the share of the reference set with strictly fewer citations, so that uncited papers have percentile zero; for correlations, Hazen's mid-rank \citep{hazen1914storage} (Section~\ref{sec:properties})."
claim-hash: 94981dad6cca
source: doi:10.1061/taceat.0002563
version-read: version of record (scan of Trans. ASCE vol. 77, 1914; pages 1629-1740 of the archive.org volume scan, extracted)
archived: gs://scholar-version2-literature/sources/hazen1914storage--10.1061_taceat.0002563-1fbb5981.pdf sha256:e80f3c2b37f161d8ac843c6c0e1a78db86343bb4e08fff8db8f9324db8dfc5de
license: public domain (1914 US publication)
updates: none
verdict: supported
evidence: "the figure for each term, arranged in its order of magnitude, is plotted at the percentage which corresponds to the middle of its strip" (printed p. 1549, 'Probability Paper'); "The first term will be plotted at the middle of the first 2% strip; that is, on the 1% line" (printed p. 1549)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:ioannidis2019standardized — paper/manuscript_qss/main.tex:163
claim: "Composite indicators such as that of \citet{ioannidis2019standardized} combine several citation counts, and \citet{abramo2014productivity} relate field-normalized citations to the cost of labor."
claim-hash: de20a729d9a9
source: doi:10.1371/journal.pbio.3000384
version-read: version of record
archived: gs://scholar-version2-literature/sources/ioannidis2019standardized--10.1371_journal.pbio.3000384-89de78a2.xml sha256:68b36d5eddd9616ed488af0c2dd001e879b5a380d2d6059023cd6c338ff901a8
license: CC-BY-4.0 (PLOS)
updates: new_version doi:10.1371/journal.pbio.3000384 (2019-08-19, publisher)
verdict: supported
evidence: "composite indicator that considers six citation metrics (total citations; Hirsch h-index; coauthorship-adjusted Schreiber hm-index; number of citations to papers as single author; number of citations to papers as single or first author; and number of citations to papers as single, first, or last author)" (Main text, database construction paragraph)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:abramo2014productivity — paper/manuscript_qss/main.tex:163
claim: "Composite indicators such as that of \citet{ioannidis2019standardized} combine several citation counts, and \citet{abramo2014productivity} relate field-normalized citations to the cost of labor."
claim-hash: de20a729d9a9
source: doi:10.1007/s11192-014-1269-8
version-read: preprint (arXiv:1810.12830v1)
archived: gs://scholar-version2-literature/sources/abramo2014productivity--10.1007_s11192-014-1269-8-95b9efa5.pdf sha256:04b35f0064c50e7c47e8a5d72e4caff628f477c4fc8658039293823fb32e5f37
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we measure FSSR, a proxy of the average yearly productivity over a period of time, accounting for the cost of labor." (sec. Labor productivity at the individual level, p. 7); "To measure the cost efficiency or research units, one should normalize its output by the cost of labor." (sec. Labor productivity in research activity and the FSS, p. 5-6); "average of the distribution of citations received for all cited publications" (definition of c-bar in Eq. [1], p. 7)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:rodriguez2018double — paper/manuscript_qss/main.tex:167
claim: "For countries and institutions, double rank analysis expresses each paper's rank within its unit as a function of its rank among all papers in the world; a power law fitted to this relation estimates the unit's chance of producing a very highly cited paper \citep{rodriguez2018double,brito2018percentile}."
claim-hash: f6f9cf82df94
source: doi:10.1016/j.joi.2017.11.004
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/rodriguez2018double--10.1016_j.joi.2017.11.004-a349ac75--author-supplied.pdf sha256:b6b046520aa6f09e3fc45ef69b73699277e103b12d7c4db425a716a7c06efcfe
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the internal ranking number can be expressed as a function of the world ranking number" (abstract); "extrapolation allows estimating the likelihood of publishing the highest cited publication" (abstract); "which includes a low number of publications in most countries and institutions" (abstract)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:brito2018percentile — paper/manuscript_qss/main.tex:167
claim: "For countries and institutions, double rank analysis expresses each paper's rank within its unit as a function of its rank among all papers in the world; a power law fitted to this relation estimates the unit's chance of producing a very highly cited paper \citep{rodriguez2018double,brito2018percentile}."
claim-hash: f6f9cf82df94
source: doi:10.1016/j.joi.2018.01.011
version-read: preprint (arXiv:1712.00399v2)
archived: gs://scholar-version2-literature/sources/brito2018percentile--10.1016_j.joi.2018.01.011-2027d857.pdf sha256:7ebab94bcec8cf9f7d71333a6783dadd0107ff16314c4b89a211681f1d8508ba
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "In the double rank analysis of research publications, the local rank position of a country or institution publication is expressed as a function of the world rank position." (Abstract); "the ranking number of a publication in a country or institution is expressed as a function of its world ranking number. The resulting function can be well fitted by a power law, which allows estimating the likelihood of any country or institution producing a very highly cited paper" (Sec. 1, Introduction)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:hirsch2007does — paper/manuscript_qss/main.tex:171
claim: "\citet{hirsch2007does} found that the $h$-index predicts a scholar's later $h$-index and citations better than paper or citation counts do."
claim-hash: bba4e8cfdfbc
source: doi:10.1073/pnas.0707962104
version-read: preprint (arXiv:0708.0646v2)
archived: gs://scholar-version2-literature/sources/hirsch2007does--10.1073_pnas.0707962104-c362526c.pdf sha256:a56674327d7e8c6fb528c1518969af379c2deb402e21fd217d4e120b74428285
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Our findings indicate that the h-index is better than other indicators considered (total citation count, citations per paper, and total paper count) in predicting future scientific achievement." (Abstract)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:acuna2012future — paper/manuscript_qss/main.tex:174
claim: "Other work predicted future $h$-index values from career features \citep{acuna2012future} and questioned how much of that predictability is mechanical, because the future $h$-index contains the current one \citep{penner2013predictability}."
claim-hash: 858fc8fc26b8
source: doi:10.1038/489201a
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/acuna2012future--10.1038_489201a-fecad52c--author-supplied.pdf sha256:6cee4984a49915129d5de245ffbee91a948378792120f2e1a329424a0bef4c6b
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "present a formula to estimate the future h-index of life scientists" (standfirst); "n, number of articles written; h, current h-index; y, years since publishing first article; j, number of distinct journals published in" (figure key)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:penner2013predictability — paper/manuscript_qss/main.tex:174
claim: "Other work predicted future $h$-index values from career features \citep{acuna2012future} and questioned how much of that predictability is mechanical, because the future $h$-index contains the current one \citep{penner2013predictability}."
claim-hash: 858fc8fc26b8
source: doi:10.1038/srep03052
version-read: version of record
archived: gs://scholar-version2-literature/sources/penner2013predictability--10.1038_srep03052-5af70d94.xml sha256:0ec5949cf8c9968d00edb312c484c4008ff9fb57f89ce2fa9fb1a3d89800c0a4
license: CC-BY-NC-SA-3.0 (Scientific Reports)
updates: none
verdict: supported
evidence: "cumulative non-decreasing measures like the h -index contain intrinsic autocorrelation, resulting in significant overestimation of their “predictive power”" (Abstract); "linear regression models are not so much predicting future impact as they are picking up on a correlation intrinsic to cumulative measures" (Results, 'Modeling non-cumulative measures')
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:190
claim: "We use the 27 October 2026 release of the Semantic Scholar Academic Graph \citep{kinney2023semantic}, distributed under the ODC-BY license \citep{s2datasets2026}."
claim-hash: ed1725ca6e1c
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "S2AG is a disambiguated, high-quality, bibliographic" (abstract)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:s2datasets2026 — paper/manuscript_qss/main.tex:190
claim: "We use the 27 October 2026 release of the Semantic Scholar Academic Graph \citep{kinney2023semantic}, distributed under the ODC-BY license \citep{s2datasets2026}."
claim-hash: ed1725ca6e1c
source: https://api.semanticscholar.org/datasets/v1/release/2026-10-27
version-read: release 2026-10-27 dataset READMEs (JSON), fetched 2026-11-05
archived: gs://scholar-version2-literature/sources/s2datasets2026--api.semanticscholar.org_datasets_v1_release_2026-10-27-1ddd5a1e--2026-11-05-385e6b41.json sha256:385e6b41f1ccaa0199439dd8b7cf1a0c3cfb112ec2f1e784741d749ca5e869f0
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "This collection is licensed under ODC-BY." (README of the papers, authors and citations datasets, release 2026-10-27)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:kinney2023semantic — paper/manuscript_qss/main.tex:226
claim: "Semantic Scholar's author records are built automatically, although authors can claim and correct them \citep{kinney2023semantic}."
claim-hash: a1ac65843029
source: arXiv:2301.10140 (DataCite doi:10.48550/arXiv.2301.10140)
version-read: preprint (arXiv:2301.10140v2)
archived: gs://scholar-version2-literature/sources/kinney2023semantic--the_semantic_scholar_open_data_platform-95c53807.pdf sha256:6f4c9082252e8b420ff2cbb811379f989f087bf4e8b46ff9ec5f9565b2b43e6f
license: unknown (treated as restricted; private store)
accessed: 2026-11-05
verdict: supported
evidence: "In Author Disambiguation, we use a system named" (sec. 3 (column-split)); "S2AND, introduced in Subramanian et al. (2021)," (sec. 3, continuation)
notes: 'Built by automatic disambiguation' became 'built automatically'; the source describes S2AG as built by automated pipelines and states that users can claim and correct author records. Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:364
claim: "Mid-ranks, a common choice for single-paper percentiles \citep{bornmann2013use,waltman2013percentile}, would credit authors with nothing above a threshold; the strict rule prevents this, at the price that a peer group's mean score depends on how common ties are (Eq.~\ref{eq:mean}; Supplementary Section~\ref{S-supp:ties})."
claim-hash: 0f0911cee5f3
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "The compromise ((i-0.5)/n*100) derived by (Hazen, 1914) is used very frequently" (sec. 2)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:364
claim: "Mid-ranks, a common choice for single-paper percentiles \citep{bornmann2013use,waltman2013percentile}, would credit authors with nothing above a threshold; the strict rule prevents this, at the price that a peer group's mean score depends on how common ties are (Eq.~\ref{eq:mean}; Supplementary Section~\ref{S-supp:ties})."
claim-hash: 0f0911cee5f3
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Pudovkin and Garfield put the tied publications in a random order and calculate their average percentile." (sec. 3)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not. Earlier note, still applicable: The Ties paragraph was shortened when its detail moved to the Supplementary Information (supplement.tex, Section S2, which keeps the earlier sentence verbatim, claim-hash 22f090d147a6). The attribution to this source is unchanged: mid-ranks as a common choice for single-paper percentiles. Co-cited Bornmann et al. (2013) covers the 'common choice' part, as before.

## [2026-11-06T02:43Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:404
claim: "The website \url{https://www.pip-score.org} uses the earlier, asymmetric form of the score \citep{ipeirotis2024pip}."
claim-hash: e57187c4868a
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: supported
evidence: "we created a small app at https://scholar.ipeirotis.org/ that allows anyone to search for a Google Scholar profile" (post body, second paragraph)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:hazen1914storage — paper/manuscript_qss/main.tex:446
claim: "These percentiles are not the score's strict shares but Hazen mid-ranks \citep{hazen1914storage}, so the correlation is the Spearman correlation of the citation counts."
claim-hash: c62e6eb2d850
source: doi:10.1061/taceat.0002563
version-read: version of record (scan of Trans. ASCE vol. 77, 1914; pages 1629-1740 of the archive.org volume scan, extracted)
archived: gs://scholar-version2-literature/sources/hazen1914storage--10.1061_taceat.0002563-1fbb5981.pdf sha256:e80f3c2b37f161d8ac843c6c0e1a78db86343bb4e08fff8db8f9324db8dfc5de
license: public domain (1914 US publication)
updates: none
verdict: supported
evidence: "if there are 50 terms in the series, each is taken to represent 2% of the whole space" (printed p. 1549); "the figure for each term, arranged in its order of magnitude, is plotted at the percentage which corresponds to the middle of its strip" (printed p. 1549)
notes: The sentence now adds that the correlation of these mid-ranks is the Spearman correlation of the citation counts; that clause follows the citation and is the authors' own statement, not attributed to Hazen. The attribution to Hazen, the mid-rank plotting position, is unchanged.

## [2026-11-06T02:43Z] cite:waltman2012leiden — paper/manuscript_qss/main.tex:719
claim: "The closest alternatives to PiP combine the same paper percentiles differently: the number of papers in the top 10\% \citep{waltman2012leiden}, also divided by career age \citep{bornmann2014individual}; the number in the top 1\%; I3, the sum of the percentiles \citep{leydesdorff2011integrated}; and their mean."
claim-hash: 9a7e522bb3ea
source: doi:10.1002/asi.22708
version-read: preprint (arXiv:1202.3941v1)
archived: gs://scholar-version2-literature/sources/waltman2012leiden--10.1002_asi.22708-420e6186.pdf sha256:463b811dfb698626177a2533bda112c3cc0def73dd694eb2ba606c068d4c54f9
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Proportion top 10% publications (PPtop 10%). The proportion of the publications of a university that, compared with other similar publications, belong to the top 10% most frequently cited" (sec. 3, indicator list)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:bornmann2014individual — paper/manuscript_qss/main.tex:719
claim: "The closest alternatives to PiP combine the same paper percentiles differently: the number of papers in the top 10\% \citep{waltman2012leiden}, also divided by career age \citep{bornmann2014individual}; the number in the top 1\%; I3, the sum of the percentiles \citep{leydesdorff2011integrated}; and their mean."
claim-hash: 9a7e522bb3ea
source: doi:10.1007/s11192-013-1161-y
version-read: preprint (arXiv:1302.3697v2)
archived: gs://scholar-version2-literature/sources/bornmann2014individual--10.1007_s11192-013-1161-y-22e3c955.pdf sha256:f53354eb37b3efb9676ae2f64101071c8aa1d7ffe88e12df5d581f88ea210334
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "we would like to propose using the number of years as an active researcher (Ptop 10% quotient) to normalise Ptop 10% for age" (sec. 3.2, Ptop 10% paragraphs); "With a value of 2.8, Researcher 2 published around twice as many Ptop 10% as Researcher 3" (sec. 3.2, end)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:719
claim: "The closest alternatives to PiP combine the same paper percentiles differently: the number of papers in the top 10\% \citep{waltman2012leiden}, also divided by career age \citep{bornmann2014individual}; the number in the top 1\%; I3, the sum of the percentiles \citep{leydesdorff2011integrated}; and their mean."
claim-hash: 9a7e522bb3ea
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Because this indicator is based on a summation, the value for the EU-27" (p. 25, nations decomposition); "The integrals in this stepwise" (p. 5, percentile rank section)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:aksnes2003macro — paper/manuscript_qss/main.tex:953
claim: "Self-citations are 8.1\% of all citations; such rates depend strongly on the definition and the citation window \citep{aksnes2003macro}."
claim-hash: d3669455d31e
source: doi:10.1023/a:1021919228368
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/aksnes2003macro--10.1023_a_1021919228368-e0aafd03--author-supplied.pdf sha256:92570f9adaa3f4694f532bdfd9d5ab74268dd6ce73a01d4e6a95fd1cbb664756
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "Using a three-year citation window we find that 36% of all citations represent author self-citations." (abstract); "In contrast, the self-citation share would be 29% when using a 5-year window." (results); "Methodologically, self-citations can be counted in two different ways, diachronously or synchronously." (methods)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:zitt2005relativity — paper/manuscript_qss/main.tex:1017
claim: "Field normalization brings its own choices: the field scheme and the assignment of multi-field papers \citep{zitt2005relativity,waltman2015field,waltman2019field} and, for a scholar, of a main field."
claim-hash: f72dd8a809d3
source: doi:10.1007/s11192-005-0218-y
version-read: version of record (author-supplied copy, identity checked against the registrar record)
archived: gs://scholar-version2-literature/sources/zitt2005relativity--10.1007_s11192-005-0218-y-bf0707e5--author-supplied.pdf sha256:6e9640a55a90b60b373274f2171207e2633146e5c84f790cb75db0bf6b43becf
license: restricted — author-supplied copy, private store
updates: none
verdict: supported
evidence: "the contents of the" (abstract); "set is completely dependent on the level of observation" (abstract); "Overlapping classes result in several normalised impact measures for each multi-assigned journal" (sec. 2, data)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:waltman2015field — paper/manuscript_qss/main.tex:1017
claim: "Field normalization brings its own choices: the field scheme and the assignment of multi-field papers \citep{zitt2005relativity,waltman2015field,waltman2019field} and, for a scholar, of a main field."
claim-hash: f72dd8a809d3
source: doi:10.1016/j.joi.2015.08.001
version-read: preprint (arXiv:1501.04431v1)
archived: gs://scholar-version2-literature/sources/waltman2015field--10.1016_j.joi.2015.08.001-8988ca16.pdf sha256:acc87a4b752fbf52994b18aa703b1561c84124c780e9de46a66ff8a0341fc2a6
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "fields are defined by the WoS journal subject categories." (sec. 4, p. 19); "A fractional counting approach is taken to handle publications belonging to multiple subject categories." (footnote 6, p. 19)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:waltman2019field — paper/manuscript_qss/main.tex:1017
claim: "Field normalization brings its own choices: the field scheme and the assignment of multi-field papers \citep{zitt2005relativity,waltman2015field,waltman2019field} and, for a scholar, of a main field."
claim-hash: f72dd8a809d3
source: doi:10.1007/978-3-030-02511-3_11
version-read: preprint (arXiv:1801.09985v1)
archived: gs://scholar-version2-literature/sources/waltman2019field--10.1007_978-3-030-02511-3_11-357ae0ed.pdf sha256:af5d1381889500d4cb5876fa62907d470b34613ed4b7db6a647e6a3d252aa77a
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "normalized indicators are quite sensitive to methodological choices, such as the choice of a field classification system" (sec. 7.2); "Another issue in the calculation of the normalized citation score of a publication is the way in which publications belonging to multiple fields are handled." (sec. 5.1)
notes: Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:43Z] cite:waltman2012inconsistency — paper/manuscript_qss/main.tex:1047
claim: "\emph{Consistency.} Like the $h$-index, PiP is not consistent in the sense of \citet{waltman2012inconsistency}: adding the same article to two authors can reverse their order, though rarely in simulation."
claim-hash: e5a76857c3c7
source: doi:10.1002/asi.21678
version-read: preprint (arXiv:1108.3901v1)
archived: gs://scholar-version2-literature/sources/waltman2012inconsistency--10.1002_asi.21678-42823f3e.pdf sha256:8afcd8681c22f8267eb5442a4ca26783f88c7206b05a96133294a97a369198ca
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "If two scientists achieve the same absolute performance improvement, their ranking relative to each other should remain unchanged." (sec. 3.2, Example 2, p. 7); "the h-index fails to provide consistent rankings of individual scientists and research groups" (sec. 3.3, Example 3, p. 8)
notes: The item now opens 'Like the $h$-index'; the source shows that the $h$-index is inconsistent in this sense, which is what the sentence attributes to it. The simulation result is the authors' own. Re-verified on 2026-11-06 against the same archived passage after the sentence was tightened to bring the main text under 9,000 words: the wording changed, the attribution to this source did not.

## [2026-11-06T02:46Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1087
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-06
archived: not copied; the deposited file is pip-replication-package-d7ab533.zip (md5:ed49af7b01928f42688043a0cbcb51c3, sha256:e511af7c461ae03bc9e36ed64a06473f6b990f087e6899b029f759166ce85d0c), built by paper/make_zenodo_package.sh from commit d7ab533
license: CC BY 4.0 (record metadata)
accessed: 2026-11-06
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at d7ab533, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for 07777bf: the file was replaced after the cut of the main text below 9,000 words and the typography audit (the archive of fb579f3 was never uploaded).

## [2026-11-06T03:00Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1091
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-06
archived: not copied; the deposited file is pip-replication-package-f0a69b9.zip (md5:4035cff9f9faa12dc3c1dab811cb94a6, sha256:3503e74f44c23f48396c4f694ae583fb880189bc49e948a4ee6b8ac0a0f9ce36), built by paper/make_zenodo_package.sh from commit f0a69b9
license: CC BY 4.0 (record metadata)
accessed: 2026-11-06
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at f0a69b9, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for d7ab533: the file was replaced after the tenure example and the later-recognition summary were restored.

## [2026-11-06T03:36Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1191
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-06
archived: not copied; the deposited file is pip-replication-package-2fc4023.zip (md5:03c33ac66e69d01039ac70009cf689ef, sha256:c513cecc436aa06e216aebd19ba338da933ee2ab89f3b899e1d457fc5f10afa8), built by paper/make_zenodo_package.sh from commit 2fc4023
license: CC BY 4.0 (record metadata)
accessed: 2026-11-06
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at 2fc4023, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for f0a69b9: the file was replaced after the Supplementary Information was folded back into main.tex (supplement.tex removed). The cited sentences that sat in supplement.tex (Ties, claim-hash 22f090d147a6; the asymmetric form, 69c1e5fd3f33) are back in main.tex word for word, under their earlier entries.

## [2026-11-06T04:31Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:137
claim: "An earlier, asymmetric form of the score was described by \citet{ipeirotis2024pip} and is used by the website \url{https://www.pip-score.org} (formerly \url{https://scholar.ipeirotis.org}); Section~\ref{sec:auc} explains why we replace it."
claim-hash: bab3cd6dcb1f
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: supported
evidence: "we have a nice new score that we have given the thoroughly boring name "Percentile in Percentile Area Under the Curve" score, or PiP-AUC for short" (post body); "we created a small app at https://scholar.ipeirotis.org/" (post body, second paragraph)
notes: Only the cross-reference changed (Section~\ref{sec:pip} became Section~\ref{sec:auc}, the new subsection that defines PiP-AUC) when Section 4 was reordered on 2026-11-06 at the author's request; the attribution to this source is unchanged.

## [2026-11-06T04:31Z] cite:ipeirotis2024pip — paper/manuscript_qss/main.tex:266
claim: "The website \url{https://www.pip-score.org} uses the earlier form of the score, PiP-AUC \citep{ipeirotis2024pip}."
claim-hash: c8f654870b85
source: https://www.behind-the-enemy-lines.com/2024/01/the-pip-auc-score-for-research.html
version-read: version of record (blog post, snapshot 2026-11-05)
archived: gs://scholar-version2-literature/sources/ipeirotis2024pip--the_pip-auc_score_for_research_productivity-05d4fa50--2026-11-05-63eaa116.html sha256:63eaa11614270464d18349834da6403392da00961e5077b08a6139e1f55e95f3
license: CC-BY-NC-3.0 (site footer)
accessed: 2026-11-05
verdict: supported
evidence: "we created a small app at https://scholar.ipeirotis.org/ that allows anyone to search for a Google Scholar profile" (post body, second paragraph)
notes: Section 4 was reordered on 2026-11-06 at the author's request: the earlier form is now introduced by name, PiP-AUC, before PiP. The name is the source's own (its title is 'The PiP-AUC score for research productivity'); the attribution, an earlier form of the score used by the website, is unchanged. The asymmetry and its problems are stated by the authors in the sentences that follow, not attributed to the post.

## [2026-11-06T04:33Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1176
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-06
archived: not copied; the deposited file is pip-replication-package-90674b3.zip (md5:bcf371a3b96bca1e79a350b7728e1312, sha256:fd9f3e3969bd8c216fcd788b183315ddb184a2c2a25d98ad21bbb93c9956ab4a), built by paper/make_zenodo_package.sh from commit 90674b3
license: CC BY 4.0 (record metadata)
accessed: 2026-11-06
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at 90674b3, which contains results/ (28 result tables) and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for 2fc4023: the file was replaced after Section 4 was reordered and the Computation subsection removed.

## [2026-11-06T04:40Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1168
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-06
archived: not copied; the deposited file is pip-replication-package-74000c5.zip (md5:377f82bfcba72896bc60a4b3549f9cd3, sha256:4316a3deed19eeb5a9439fa754e32247242bfabee971d4dea465327800fa38a9), built by paper/make_zenodo_package.sh from commit 74000c5
license: CC BY 4.0 (record metadata)
accessed: 2026-11-06
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at 74000c5, which contains results/ (28 result tables), COMPUTATION.md and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for 90674b3: the file was replaced after the computation details moved from the paper into paper/COMPUTATION.md.

## [2026-11-06T12:58Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1175
claim: "The result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: e5950df8c0b7
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-06
archived: not copied; the deposited file is pip-replication-package-ac0eae2.zip (md5:9be541db21bc39357e78a6dea2d7dbe7, sha256:8f1d6eaf41c23f834c888cb80903f609e1dcb5eade64a42ccd73739a34119ed0), built by paper/make_zenodo_package.sh from commit ac0eae2
license: CC BY 4.0 (record metadata)
accessed: 2026-11-06
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at ac0eae2, which contains results/ (28 result tables), COMPUTATION.md and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: The authors' own deposit. The DOI is reserved and resolves only after the authors publish the record; re-check then, and replace the file if the manuscript changes before publication. Supersedes the entry for 74000c5: the file was replaced after the Numerical evaluation paragraph was added to Section 4.

## [2026-11-06T20:20Z] cite:waltman2012inconsistency — paper/manuscript_qss/main.tex:459
claim: "PiP is not consistent in the sense of \citet{waltman2012inconsistency}: adding the same article to two authors can reverse their order, because peer standings are not linear in article counts."
claim-hash: cc58eb29fdbf
source: doi:10.1002/asi.21678
version-read: preprint (arXiv:1108.3901v1)
archived: gs://scholar-version2-literature/sources/waltman2012inconsistency--10.1002_asi.21678-42823f3e.pdf sha256:8afcd8681c22f8267eb5442a4ca26783f88c7206b05a96133294a97a369198ca
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "If two scientists achieve the same absolute performance improvement, their ranking relative to each other should remain unchanged." (sec. 3.2, Example 2, p. 7); "Scientists X and Y have both achieved exactly the same performance improvement (i.e., two publications with six citations each), but despite of this their ranking has reversed." (sec. 3.2, Example 2, p. 7)
notes: Re-verified on 2026-11-06 after replacing the internal symbol $G_\theta$ with 'peer standings are not linear in article counts'; the attribution to this source is unchanged.

## [2026-11-06T20:35Z] cite:leydesdorff2011integrated — paper/manuscript_qss/main.tex:272
claim: "The area under this step curve is the sum of the scholar's paper percentiles ($4.40$ in Figure~\ref{fig:construction}a), which is the I3 indicator of \citet{leydesdorff2011integrated}."
claim-hash: d89ea5bdc57e
source: doi:10.1002/asi.21609
version-read: preprint (arXiv:1103.5241v2)
archived: gs://scholar-version2-literature/sources/leydesdorff2011integrated--10.1002_asi.21609-cbbd6e66.pdf sha256:b50d95eee11710072a36fbee7e64ebdb3ba5b3dcb4c2f1e3fee5e7d630892d9b
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "Using the sum total of the frequencies (f) in each percentile, however, accounts for both the size and shape of the distribution." (p. 6-7, section on percentile rank classes (I3 definition)); "The integrals in this stepwise" (p. 5, percentile rank section)
notes: Added in Section 4.2 when illustrating the step curve of paper percentiles against raw paper rank; the attribution to this source (defining I3 as the sum of paper percentiles) is identical to the existing entry at line 769.

## [2026-11-06T20:35Z] cite:bornmann2013use — paper/manuscript_qss/main.tex:443
claim: "They are a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, but in $g(\theta)$ they would give credit to authors with nothing above a threshold: at high thresholds most peers have no article, and an author tied with them at zero would stand at half their share."
claim-hash: 444f3426f82b
source: doi:10.1016/j.joi.2012.10.001
version-read: preprint (arXiv:1211.0381v1)
archived: gs://scholar-version2-literature/sources/bornmann2013use--10.1016_j.joi.2012.10.001-681945bb.pdf sha256:3638ced7af5747f7fcc1e34f278c7ebcee3bea58c100951823931a3eab77e27c
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "In the case of publications with the same number of citations (tied ranks), the average rank is assigned." (sec. 2)
notes: Re-verified on 2026-11-06 after replacing 'in the standing' with 'in $g(\theta)$' for line-break tightening; the attribution to this source is unchanged.

## [2026-11-06T20:35Z] cite:waltman2013percentile — paper/manuscript_qss/main.tex:443
claim: "They are a common choice for the percentile of a single paper \citep{bornmann2013use,waltman2013percentile}, but in $g(\theta)$ they would give credit to authors with nothing above a threshold: at high thresholds most peers have no article, and an author tied with them at zero would stand at half their share."
claim-hash: 444f3426f82b
source: doi:10.1002/asi.22775
version-read: preprint (arXiv:1205.0646v1)
archived: gs://scholar-version2-literature/sources/waltman2013percentile--10.1002_asi.22775-039024e8.pdf sha256:89e4c0756f18084062345454a5ad2dc5649cf29dec145615f2f19e2844310ac7
license: unknown (treated as restricted; private store)
updates: none
verdict: partially supported
evidence: "Pudovkin and Garfield put the tied publications in a random order and calculate their average percentile." (sec. 3)
notes: Re-verified on 2026-11-06 after replacing 'in the standing' with 'in $g(\theta)$' for line-break tightening; the attribution to this source is unchanged.

## [2026-11-06T20:35Z] cite:waltman2015field — paper/manuscript_qss/main.tex:1143
claim: "A fractional version would weight each paper by the author's share when counting papers above each threshold \citep{waltman2015field}; we have not tested it."
claim-hash: 9d228cddd00b
source: doi:10.1016/j.joi.2015.08.001
version-read: preprint (arXiv:1501.04431v1)
archived: gs://scholar-version2-literature/sources/waltman2015field--10.1016_j.joi.2015.08.001-8988ca16.pdf sha256:acc87a4b752fbf52994b18aa703b1561c84124c780e9de46a66ff8a0341fc2a6
license: unknown (treated as restricted; private store)
updates: none
verdict: supported
evidence: "In the case of fractional counting, a publication co-authored by n researchers is assigned a weight of 1/n for each researcher." (sec. 2.2)
notes: Re-verified on 2026-11-06 after replacing the removed notation $n_a(\theta)$ with 'when counting papers above each threshold'; the attribution to this source is unchanged.

## [2026-11-06T20:35Z] cite:ipeirotis2026replication — paper/manuscript_qss/main.tex:1194
claim: "Result tables and a snapshot of the replication package are archived on Zenodo \citep{ipeirotis2026replication}."
claim-hash: f0adc4ad5444
source: doi:10.5281/zenodo.23987119
version-read: unpublished Zenodo draft (deposition 23987119), read through the deposit API on 2026-11-06
archived: not copied; the deposited file is pip-replication-package-ac0eae2.zip (md5:9be541db21bc39357e78a6dea2d7dbe7, sha256:8f1d6eaf41c23f834c888cb80903f609e1dcb5eade64a42ccd73739a34119ed0), built by paper/make_zenodo_package.sh from commit ac0eae2
license: CC BY 4.0 (record metadata)
accessed: 2026-11-06
updates: none
verdict: supported
evidence: The draft holds one file, the archive of paper/ at ac0eae2, which contains results/ (28 result tables), COMPUTATION.md and the SQL, figure data, checks and honors scripts; the record description names the result tables and the BigQuery datasets.
notes: Re-verified on 2026-11-06 after dropping the leading 'The' for paragraph-ending line-break tightening; the attribution to this source is unchanged.

