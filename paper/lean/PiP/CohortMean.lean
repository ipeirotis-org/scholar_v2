module

public import PiP.Basic
public import Mathlib.MeasureTheory.Integral.IntervalIntegral.Basic

@[expose] public section

set_option linter.unusedSectionVars false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# Cohort-Mean Identity: the Mid-Rank Standing Averages to One Half

This module formalizes the cohort-mean identity (Equation 6 of the paper and its finite-grid
counterpart). Under the mid-rank rule a tie counts one half, so the cohort mean of the standing
is exactly `1 / 2` at every threshold, whatever the number of ties:

1. **Ordered-pair counting (`sum_midWeight_pairs_eq_half_card_sq`)**:
   Every ordered pair `(a, b) ∈ α × α` contributes the weight `1{n_b < n_a} + (1/2) 1{n_b = n_a}`.
   The contributions of `(a, b)` and `(b, a)` sum to `1` (`midWeight_add_swap`): a strict win
   pairs with a strict loss, and a tie gives one half to each side (the diagonal contributes
   `1 / 2`). Summing over all `N^2` ordered pairs gives `N^2 / 2`.
2. **Single-threshold cohort-mean standing (`sum_standing_eq_half_card`, `cohort_mean_standing`)**:
   `∑_a standing(n, a, k) = N / 2`, so `(1 / N) ∑_a standing(n, a, k) = 1 / 2`.
3. **Finite-grid cohort-mean identity (`cohort_mean_gridScore`)**:
   `(1 / N) ∑_a S(a) = 1 / 2` for every grid and every cutoff.
4. **Integrability of the standing curve (`intervalIntegrable_standingAt`)**:
   The standing curve `θ ↦ standingAt(papers, a, θ)` is measurable and bounded, hence
   interval-integrable on every interval.
5. **Continuous-integral cohort-mean identity (`cohort_mean_pip`, Equation 6)**:
   `(1 / N) ∑_a PiP_τ(a) = 1 / 2` for every cutoff `τ < 1`.
-/

namespace PiP

open Finset BigOperators MeasureTheory

section FiniteCohortMean

variable {α κ : Type*} [Fintype α] [DecidableEq α]

/-- **Ordered-pair counting identity**:
The mid-rank weights of all ordered pairs of cohort members sum to `N^2 / 2`, because each pair
`(a, b)` and its mirror image `(b, a)` share exactly one unit of weight. -/
theorem sum_midWeight_pairs_eq_half_card_sq (f : α → ℕ) :
    (∑ a : α, ∑ b : α, midWeight (f b) (f a)) = (Fintype.card α : ℝ) ^ 2 / 2 := by
  have hswap : (∑ a : α, ∑ b : α, midWeight (f a) (f b)) =
      ∑ a : α, ∑ b : α, midWeight (f b) (f a) :=
    Finset.sum_comm
  have htotal :
      (∑ a : α, ∑ b : α, (midWeight (f b) (f a) + midWeight (f a) (f b))) =
        (Fintype.card α : ℝ) ^ 2 := by
    simp_rw [midWeight_add_swap]
    simp [sq]
  simp only [Finset.sum_add_distrib] at htotal
  linarith

/-- **The standings of a cohort sum to `N / 2` at every threshold.** -/
theorem sum_standing_eq_half_card (n : α → κ → ℕ) (k : κ) :
    ∑ a : α, standing n a k = (Fintype.card α : ℝ) / 2 := by
  simp_rw [standing_eq_sum_midWeight]
  rw [← Finset.sum_div, sum_midWeight_pairs_eq_half_card_sq (fun a => n a k)]
  by_cases hN : (Fintype.card α : ℝ) = 0
  · simp [hN]
  · field_simp

/-- **Single-threshold cohort-mean standing identity**:
At any threshold `k`, the cohort average of the mid-rank `standing n a k` is exactly `1 / 2`. -/
theorem cohort_mean_standing [Nonempty α] (n : α → κ → ℕ) (k : κ) :
    (∑ a : α, standing n a k) / (Fintype.card α : ℝ) = 1 / 2 := by
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  have hNne : (Fintype.card α : ℝ) ≠ 0 := ne_of_gt hN
  rw [sum_standing_eq_half_card]
  field_simp

/-- **Finite-grid cohort-mean identity**:
`(1 / N) ∑_a S(a) = 1 / 2` for every threshold grid, hence for every cutoff. -/
theorem cohort_mean_gridScore [Fintype κ] [Nonempty α] [Nonempty κ] (n : α → κ → ℕ) :
    (∑ a : α, gridScore n a) / (Fintype.card α : ℝ) = 1 / 2 := by
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  have hNne : (Fintype.card α : ℝ) ≠ 0 := ne_of_gt hN
  have hK : (0 : ℝ) < Fintype.card κ := Nat.cast_pos.mpr Fintype.card_pos
  have hKne : (Fintype.card κ : ℝ) ≠ 0 := ne_of_gt hK
  unfold gridScore
  rw [← Finset.sum_div, Finset.sum_comm]
  simp_rw [sum_standing_eq_half_card]
  rw [Finset.sum_const, Finset.card_univ, nsmul_eq_mul]
  field_simp

end FiniteCohortMean

/-! ### Continuous-Integral Cohort-Mean Identity (Equation 6) -/

section ContinuousCohortMean

variable {α : Type*} [Fintype α] [DecidableEq α]

/-- Continuous PiP score `PiP_τ(a) = (1 / (1 - τ)) ∫_τ^1 standingAt(papers, a, θ) dθ` (Eq. 4). -/
noncomputable def pip (papers : α → List ℝ) (τ : ℝ) (a : α) : ℝ :=
  (∫ θ in τ..1, standingAt papers a θ) / (1 - τ)

/-- Pointwise cohort-mean standing identity at any continuous threshold `θ ∈ ℝ`. -/
theorem cohort_mean_standingAt [Nonempty α] (papers : α → List ℝ) (θ : ℝ) :
    (∑ a : α, standingAt papers a θ) / (Fintype.card α : ℝ) = 1 / 2 :=
  cohort_mean_standing (countAboveAt papers) θ

/-- Each author's qualifying-paper count is a measurable function of the threshold. -/
theorem measurable_countAboveAt (papers : α → List ℝ) (b : α) :
    Measurable (countAboveAt papers b) :=
  Antitone.measurable (fun _ _ h => countAboveList_antitone (papers b) h)

/-- The standing curve is a measurable function of the threshold. -/
theorem measurable_standingAt (papers : α → List ℝ) (a : α) :
    Measurable (standingAt papers a) := by
  have h : standingAt papers a = fun t =>
      (∑ b : α, midWeight (countAboveAt papers b t) (countAboveAt papers a t)) /
        (Fintype.card α : ℝ) := by
    funext t
    rw [standingAt_eq_standing, standing_eq_sum_midWeight]
  rw [h]
  refine Measurable.div_const (Finset.measurable_sum _ (fun b _ => ?_)) _
  exact (Measurable.of_discrete (f := fun p : ℕ × ℕ => midWeight p.1 p.2)).comp
    ((measurable_countAboveAt papers b).prodMk (measurable_countAboveAt papers a))

/-- **The standing curve is interval-integrable on every interval**: it is measurable and takes
values in `[0, 1]`. -/
theorem intervalIntegrable_standingAt (papers : α → List ℝ) (a : α) (x₀ x₁ : ℝ) :
    IntervalIntegrable (standingAt papers a) volume x₀ x₁ := by
  rw [intervalIntegrable_iff]
  refine MeasureTheory.Measure.integrableOn_of_bounded (M := 1) ?_
    (measurable_standingAt papers a).aestronglyMeasurable ?_
  · exact measure_Ioc_lt_top.ne
  · refine Filter.Eventually.of_forall (fun t => ?_)
    rw [Real.norm_eq_abs, abs_le]
    have h0 : 0 ≤ standingAt papers a t := standing_nonneg (countAboveAt papers) a t
    have h1 : standingAt papers a t < 1 := standing_lt_one (countAboveAt papers) a t
    constructor <;> linarith

/-- **Continuous cohort-mean identity (Equation 6 of the paper)**:
`(1 / N) ∑_a PiP_τ(a) = 1 / 2` for every cutoff `τ < 1`. -/
theorem cohort_mean_pip [Nonempty α] (papers : α → List ℝ) {τ : ℝ} (hτ : τ < 1) :
    (∑ a : α, pip papers τ a) / (Fintype.card α : ℝ) = 1 / 2 := by
  have hτne : (1 : ℝ) - τ ≠ 0 := by linarith
  unfold pip
  rw [← Finset.sum_div, div_right_comm]
  rw [← intervalIntegral.integral_finset_sum (fun a _ => intervalIntegrable_standingAt papers a τ 1)]
  rw [← intervalIntegral.integral_div]
  have hcongr :
      (∫ θ in τ..1, (∑ a : α, standingAt papers a θ) / (Fintype.card α : ℝ)) =
        ∫ _θ in τ..1, (1 / 2 : ℝ) := by
    refine intervalIntegral.integral_congr (fun θ _ => cohort_mean_standingAt papers θ)
  rw [hcongr, intervalIntegral.integral_const, smul_eq_mul]
  field_simp

end ContinuousCohortMean

end PiP
