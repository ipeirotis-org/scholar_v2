# Product Recommendations: Better UX + More Meaningful Benchmarking (beyond Semantic Scholar quality)

## 1) Reframe the core promise away from “data completeness” to “decision confidence”
If users feel S2 data quality is uneven, the product should explicitly communicate confidence and uncertainty.

### What to change in UX
- Show a **Confidence Meter** per profile (e.g., High / Medium / Low confidence).
- Add a brief **“Why this confidence?”** explainer (coverage %, likely missing venues, name ambiguity risk).
- Replace binary “good/bad profile” perception with “how much trust should I place in this output?”

### Why this helps
Users tolerate imperfect data if uncertainty is visible and actionable.

---

## 2) Introduce a “Profile Claim & Correction” user loop
Even if your backend data source is imperfect, users can help fix identity and publication issues.

### UX ideas
- “Is this your profile?” claim flow.
- Quick actions:
  - Remove wrong publication
  - Mark missing key publication
  - Merge/split author identities
- Badge corrected profiles as **“User-verified.”**

### Why this helps
- Converts disappointment into contribution.
- Creates perceived product quality even before upstream data quality improves.

---

## 3) Shift from absolute leaderboard mentality to “peer cohort storytelling”
Users dislike rankings when they feel inputs are noisy or unfair.

### UX ideas
- Show 3 comparison modes prominently:
  1. **Career age cohort** (years since first publication)
  2. **Field cohort** (discipline-normalized)
  3. **Institution type cohort** (R1, teaching-focused, industry)
- Let users choose “Who am I compared to?” before showing scores.
- Label all benchmark outputs as “relative to selected cohort.”

### Why this helps
People accept relative comparisons they perceive as fair.

---

## 4) Add “trajectory over snapshot” as the default view
When data quality is mixed, trends are often more trustworthy than single-point values.

### UX ideas
- Default profile view: **3-year and 5-year trend cards**
  - momentum percentile
  - consistency percentile
  - volatility indicator
- Add “career phase lens” (early, mid, late) to avoid seniority bias.

### Why this helps
Users care about direction and sustainability, not just current rank.

---

## 5) Build a benchmark that rewards both impact quality and sustained contribution
Your PiP-AUC idea is strong; make it more interpretable with a composite narrative.

## Recommended benchmark framework
Use a 4-part scorecard instead of one scalar score:

1. **Impact Quality**
   - citation percentile quality of papers (field-year normalized)
2. **Productivity Efficiency**
   - output adjusted by career age and field norms
3. **Consistency**
   - fraction of years with above-median impact
4. **Momentum**
   - recent slope vs prior baseline

Then derive:
- **Overall Benchmark Tier** (e.g., Top 10%, Top 25%, etc.)
- **Style label** (e.g., “High-peak selective,” “Consistent builder,” “Rapid riser”)

### Why this helps
A multidimensional benchmark is harder to game and feels more “human-true.”

---

## 6) Make benchmark fairness explicit and inspectable
If users can’t inspect fairness assumptions, they will distrust results.

### UX ideas
- “How this score is computed” panel with plain-language bullets.
- “What changes your score most?” sensitivity panel.
- “Data caveats” section per author (missing venues, uncertain identity, sparse years).

### Why this helps
Transparency increases trust more than opaque sophistication.

---

## 7) Create “decision-oriented outputs” for real user jobs
People don’t want scores; they want help with decisions.

### Candidate jobs-to-be-done
- Hiring committee screening
- Grant panel shortlisting
- Department benchmarking
- Individual career self-assessment

### UX ideas
- **Use-case mode selector** (Hiring / Grants / Self / Department).
- Each mode emphasizes different dimensions (e.g., momentum for hiring, consistency for grants).
- Provide summary cards suitable for exporting to committee packets.

---

## 8) Add confidence-aware ranking behavior
Do not rank profiles with weak data as if they are equally reliable.

### UX ideas
- Rank with confidence tiers:
  - Tier A: high-confidence profiles fully rankable
  - Tier B: rank range (e.g., 30–45th percentile)
  - Tier C: “insufficient confidence for precise rank”
- Separate “estimated” vs “verified” badges.

### Why this helps
Prevents false precision and reduces user frustration.

---

## 9) Include a “benchmark alternatives” panel (without changing engineering immediately)
Even before ingesting new sources, explain how users should triangulate.

### UX ideas
- “Cross-check with” panel: Google Scholar / ORCID / personal website.
- Encourage user-reported discrepancies as part of profile quality score.

### Why this helps
Acknowledges limitations honestly and frames your tool as decision support, not truth oracle.

---

## 10) Move from “top-N table” to guided narratives
Raw tables amplify data quality complaints.

### UX ideas
For each author, show:
- **Strengths**: where they outperform cohort
- **Watch-outs**: where interpretation is uncertain
- **Most similar researchers**: contextual peers
- **Next-step suggestion**: what to examine before a decision

### Why this helps
Narratives are more useful and more tolerant of imperfect data.

---

## 11) Product experiments to run next (non-engineering framing)
Prioritize low-risk UX tests that directly address trust.

1. **Confidence Meter A/B test**
   - Metric: reduction in “data looks wrong” feedback
2. **Cohort selector default test**
   - Metric: increase in session depth and profile saves
3. **Trajectory-first profile test**
   - Metric: increase in return usage by evaluators
4. **4-part benchmark card vs single score**
   - Metric: perceived fairness and recommendation likelihood

---

## 12) Practical messaging changes for your homepage
- Current impression likely: “we rank researchers.”
- Better impression: **“we provide fair, confidence-aware research impact comparisons.”**

Suggested copy direction:
- “Benchmark researchers against comparable peers, with transparent uncertainty and career-stage normalization.”

---

## Recommended immediate priority order
1. Confidence + uncertainty UX layer
2. Cohort-first comparison workflow
3. Multi-dimensional benchmark card (quality, efficiency, consistency, momentum)
4. User verification/correction loop
5. Decision-mode summaries for hiring/grants/self-assessment

These five changes will improve perceived product quality and meaningfulness even before major data-source changes.
