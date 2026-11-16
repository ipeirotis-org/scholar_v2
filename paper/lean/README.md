# Machine-Checked Proofs for the PiP Score (Lean 4 / Mathlib)

This directory contains a pinned Lean 4 and Mathlib formalization of the mathematical properties of
the PiP (percentile-in-percentile) indicator stated in Section 4 and Appendix C of the manuscript,
as well as the exact-integration identity used in `COMPUTATION.md` and
`sql/25_pip_exact.sql`.

## Toolchain and Reproducibility

- **Lean 4 toolchain:** `leanprover/lean4:v4.35.0` (pinned in [`lean-toolchain`](lean-toolchain))
- **Mathlib version:** `v4.35.0` (pinned in [`lakefile.toml`](lakefile.toml) and [`lake-manifest.json`](lake-manifest.json))

To build and verify all proofs from a fresh checkout:

```bash
cd paper/lean
lake exe cache get
lake build
```

Building the root module [`PiP.lean`](PiP.lean) compiles all four proof modules and runs
`#print axioms` on every principal theorem, confirming that the formalization uses no `sorry`
placeholders and depends only on Lean's three standard foundational axioms
(`[propext, Classical.choice, Quot.sound]`).

## Module Overview and Theorem Mapping

The standing uses the mid-rank rule: at threshold `θ`, a peer with strictly fewer qualifying articles
than the focal author counts one and a peer with exactly as many counts one half (the focal author
is a member of the peer group and so always ties with themselves),
`g_a(θ) = (#{b : n_b(θ) < n_a(θ)} + (1/2) #{b : n_b(θ) = n_a(θ)}) / N`
(`PiP.standing`, `PiP.standingAt`; equivalently `(L(n_a) + L(n_a + 1)) / 2` with `L(m)` the share of
peers with fewer than `m`, `PiP.standing_eq_half_belowShare_add`).

| Manuscript / Appendix Reference | Claim | Lean Module | Lean Theorem Name |
|---|---|---|---|
| Section 4.4, Prop. C.1(i) | Bounds: `1/(2N) ≤ g_a(θ) ≤ 1 - 1/(2N)` and the same for the grid score, hence `0 < S(a) < 1` (`a` ties with themselves, and that tie counts only one half) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.self_mem_tiedWith`, `PiP.self_not_mem_strictlyBelow`, `PiP.standing_ge_inv_two_card`, `PiP.standing_le_one_sub_inv_two_card`, `PiP.gridScore_ge_inv_two_card`, `PiP.gridScore_le_one_sub_inv_two_card`, `PiP.gridScore_pos`, `PiP.gridScore_nonneg`, `PiP.gridScore_lt_one` |
| Prop. C.1(ii) | Below the cutoff: an author with no qualifying article at `θ` has standing equal to half the share of peers with none, the lowest standing in the cohort (standing is monotone in the count across authors); an author whose papers all lie strictly below `τ` has grid score equal to the average of those half zero-shares, and no peer scores lower | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.standing_of_count_eq_zero`, `PiP.standing_mono_count`, `PiP.standing_le_of_count_eq_zero`, `PiP.gridScore_eq_of_counts_eq_zero`, `PiP.gridScore_mono_count`, `PiP.gridScore_le_of_counts_eq_zero`, `PiP.gridScore_eq_of_all_below_cutoff`, `PiP.gridScore_le_of_all_below_cutoff` |
| Prop. C.1(iii) | Standing curve is non-decreasing on intervals where the focal author's qualifying count is constant (sawtooth property) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.standing_mono_threshold_of_const_count`, `PiP.standing_mono_threshold_on_const_count` |
| Prop. C.2 | Conditional monotonicity at the count level (updating the focal author's record inside the peer group never lowers their standing or score) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.standing_mono_focal`, `PiP.gridScore_mono_focal` |
| Prop. C.2 | Adding a paper, raising an assigned percentile, and invariance to papers strictly below `τ` | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.gridScore_add_paper_le`, `PiP.gridScore_raise_paper_le`, `PiP.gridScore_add_paper_below_cutoff_eq` |
| Section 4.4, Remark | Citation-monotonicity counterexample (gaining raw citations can lower a tied paper's percentile and reduce PiP: standing `3/4 → 1/2`, four-class grid score `11/16 → 5/8`) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.citation_monotonicity_counterexample` |
| Prop. C.3 | Cohort mean is exactly `1/2`: at every threshold, for the grid score at every cutoff, and for the continuous score `PiP_τ` (`τ < 1`); the standing curve is integrable on every interval | [`PiP/CohortMean.lean`](PiP/CohortMean.lean) | `PiP.sum_midWeight_pairs_eq_half_card_sq`, `PiP.sum_standing_eq_half_card`, `PiP.cohort_mean_standing`, `PiP.cohort_mean_gridScore`, `PiP.intervalIntegrable_standingAt`, `PiP.cohort_mean_pip` |
| Prop. C.4 | Strict upward rounding `R_M(y) = (⌊M y⌋ + 1)/M`: cell correspondence, grid-to-integral equivalence, and cutoff boundary effect | [`PiP/GridIntegral.lean`](PiP/GridIntegral.lean) | `PiP.roundUp_ge_iff_ge_grid`, `PiP.standingAt_roundUp_eq_on_cell`, `PiP.integral_standingAt_roundUp_cell`, `PiP.grid_eq_integral_roundUp`, `PiP.roundUp_at_cutoff_qualifies` |
| Prop. C.5 | Order-statistics exact integration (`25_pip_exact.sql`): where the focal count is the constant `k ≥ 0`, `∫ g = ((H_k(x₁) - H_k(x₀)) + (H_{k+1}(x₁) - H_{k+1}(x₀)))/2` with `H_0 = 0`; sum over the constant-count intervals (including the top one, `k = 0`); `asof` running-count/sum formula | [`PiP/ExactIntegral.lean`](PiP/ExactIntegral.lean) | `PiP.countAboveList_lt_iff_kthPercentile_lt`, `PiP.countAboveList_le_iff_kthPercentile_succ_lt`, `PiP.integral_ite_lt_eq_max_sub_max`, `PiP.integral_belowShare_eq_H_sub_H`, `PiP.integral_standingAt_const_eq_H_sub_H`, `PiP.integral_standingAt_zero_count_eq_half_H_one`, `PiP.integral_standingAt_partition_eq_sum_H_sub_H`, `PiP.pip_eq_sum_H_sub_H`, `PiP.H_eq_asof_formula` |
