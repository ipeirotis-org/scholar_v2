# Machine-Checked Proofs for the PiP Score (Lean 4 / Mathlib)

This directory contains a pinned Lean 4 and Mathlib formalization of the mathematical properties of
the PiP (percentile-in-percentile) indicator stated in Section 4 and Appendix C of the manuscript,
as well as the exact-integration identity used in `paper/COMPUTATION.md` and
`paper/sql/25_pip_exact.sql`.

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

| Manuscript / Appendix Reference | Claim | Lean Module | Lean Theorem Name |
|---|---|---|---|
| Section 4.3, Prop. C.1 | Standing curve is non-decreasing on intervals where the focal author's qualifying count is constant (sawtooth property) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.standing_mono_threshold_on_const_count` |
| Section 4.5, Prop. C.2 | Bounds (`0 ≤ S(a) ≤ (N - 1)/N < 1`) and self-exclusion (`a` never strictly beats `a`) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.self_not_mem_strictlyBelow`, `PiP.gridScore_nonneg`, `PiP.gridScore_le_pred_div_card`, `PiP.gridScore_lt_one` |
| Section 4.5, Prop. C.2 | Zero score when all papers lie strictly below the cutoff `τ` | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.gridScore_eq_zero_of_counts_eq_zero`, `PiP.gridScore_eq_zero_of_all_below_cutoff` |
| Section 4.5, Prop. C.3 | Conditional monotonicity at the count level (updating the focal author's record inside the peer group) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.standing_mono_focal`, `PiP.gridScore_mono_focal` |
| Section 4.5, Cor. C.4 | Adding a paper, raising an assigned percentile, and invariance to papers strictly below `τ` | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.gridScore_add_paper_le`, `PiP.gridScore_raise_paper_le`, `PiP.gridScore_add_paper_below_cutoff_eq` |
| Section 4.5, Rem. C.5 | Citation-monotonicity counterexample (gaining raw citations can lower a tied paper's percentile under Eq. 1 and reduce PiP) | [`PiP/Basic.lean`](PiP/Basic.lean) | `PiP.citation_monotonicity_counterexample` |
| Section 4.4 (Eq. 6), Prop. C.6 | Cohort-mean identity as a function of ties (finite grid and continuous integral) and `< 1/2` bound | [`PiP/CohortMean.lean`](PiP/CohortMean.lean) | `PiP.two_mul_sum_strict_lt_add_sum_sq_eq_card_sq`, `PiP.cohort_mean_standing`, `PiP.cohort_mean_gridScore`, `PiP.cohort_mean_gridScore_lt_half`, `PiP.cohort_mean_pip` |
| Section 4.6, Prop. C.7 | Strict upward rounding `R_M(y) = (⌊M y⌋ + 1)/M`: cell correspondence, grid-to-integral equivalence, and cutoff boundary effect | [`PiP/GridIntegral.lean`](PiP/GridIntegral.lean) | `PiP.roundUp_ge_iff_ge_grid`, `PiP.standingAt_roundUp_eq_on_cell`, `PiP.integral_standingAt_roundUp_cell`, `PiP.grid_eq_integral_roundUp`, `PiP.roundUp_at_cutoff_qualifies` |
| `COMPUTATION.md`, Prop. C.8 | Order-statistics exact integration (`25_pip_exact.sql`) and `asof` running-count/sum formula | [`PiP/ExactIntegral.lean`](PiP/ExactIntegral.lean) | `PiP.countAboveList_lt_iff_kthPercentile_lt`, `PiP.integral_ite_lt_eq_max_sub_max`, `PiP.integral_standingAt_const_eq_H_sub_H`, `PiP.integral_standingAt_partition_eq_sum_H_sub_H`, `PiP.H_eq_asof_formula` |
