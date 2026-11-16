module

public import PiP.Basic
public import PiP.CohortMean
public import PiP.GridIntegral
public import PiP.ExactIntegral

@[expose] public section

/-!
# PiP Formal Verification Root Module

This file imports all four modules of the PiP formalization and prints the axiom dependencies
of each principal theorem to verify that no `sorry` or non-standard axioms are used.
-/

-- 1. Bounds, below-cutoff standing, conditional monotonicity, sawtooth lemma, and counterexample
#print axioms PiP.standing_eq_half_belowShare_add
#print axioms PiP.standing_ge_inv_two_card
#print axioms PiP.standing_le_one_sub_inv_two_card
#print axioms PiP.gridScore_ge_inv_two_card
#print axioms PiP.gridScore_le_one_sub_inv_two_card
#print axioms PiP.gridScore_pos
#print axioms PiP.gridScore_nonneg
#print axioms PiP.gridScore_lt_one
#print axioms PiP.standing_of_count_eq_zero
#print axioms PiP.standing_mono_count
#print axioms PiP.standing_le_of_count_eq_zero
#print axioms PiP.gridScore_eq_of_counts_eq_zero
#print axioms PiP.gridScore_le_of_counts_eq_zero
#print axioms PiP.gridScore_eq_of_all_below_cutoff
#print axioms PiP.gridScore_le_of_all_below_cutoff
#print axioms PiP.standing_mono_focal
#print axioms PiP.gridScore_mono_focal
#print axioms PiP.gridScore_add_paper_le
#print axioms PiP.gridScore_raise_paper_le
#print axioms PiP.gridScore_add_paper_below_cutoff_eq
#print axioms PiP.standing_mono_threshold_of_const_count
#print axioms PiP.standing_mono_threshold_on_const_count
#print axioms PiP.citation_monotonicity_counterexample

-- 2. Cohort mean of the mid-rank standing is exactly one half
#print axioms PiP.sum_midWeight_pairs_eq_half_card_sq
#print axioms PiP.sum_standing_eq_half_card
#print axioms PiP.cohort_mean_standing
#print axioms PiP.cohort_mean_gridScore
#print axioms PiP.intervalIntegrable_standingAt
#print axioms PiP.cohort_mean_pip

-- 3. Strict upward rounding and grid-to-integral equivalence
#print axioms PiP.roundUp_ge_iff_ge_grid
#print axioms PiP.standingAt_roundUp_eq_on_cell
#print axioms PiP.integral_standingAt_roundUp_cell
#print axioms PiP.grid_eq_integral_roundUp
#print axioms PiP.roundUp_at_cutoff_qualifies

-- 4. Order-statistics exact-integration algorithm (`25_pip_exact.sql`)
#print axioms PiP.countAboveList_lt_iff_kthPercentile_lt
#print axioms PiP.countAboveList_le_iff_kthPercentile_succ_lt
#print axioms PiP.integral_ite_lt_eq_max_sub_max
#print axioms PiP.integral_belowShare_eq_H_sub_H
#print axioms PiP.integral_standingAt_const_eq_H_sub_H
#print axioms PiP.integral_standingAt_zero_count_eq_half_H_one
#print axioms PiP.integral_standingAt_partition_eq_sum_H_sub_H
#print axioms PiP.pip_eq_sum_H_sub_H
#print axioms PiP.H_eq_asof_formula
