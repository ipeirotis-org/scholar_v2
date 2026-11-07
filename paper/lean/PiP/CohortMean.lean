module

public import PiP.Basic
public import Mathlib.MeasureTheory.Integral.IntervalIntegral.Basic

@[expose] public section

set_option linter.unusedSectionVars false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# Cohort-Mean Tie Identity

This module formalizes the cohort-mean identity (Equation 6 of the paper and its finite-grid
counterpart):

1. **Ordered-pair trichotomy and swap symmetry (`two_mul_sum_strict_lt_add_sum_sq_eq_card_sq`)**:
   Every ordered pair `(a, b) ∈ α × α` either satisfies `n b k < n a k`, `n a k < n b k`, or
   `n a k = n b k`. Swapping `a` and `b` pairs each strict win with a strict loss, while the number
   of ties is `∑ m, #{a : n a k = m}^2`.
2. **Single-threshold cohort-mean standing (`cohort_mean_standing`)**:
   `(1 / N) ∑_a standing(n, a, k) = (1 / 2) * (1 - ∑_m p_{k,m}^2)`.
3. **Finite-grid cohort-mean identity (`cohort_mean_gridScore`)**:
   `(1 / N) ∑_a S(a) = (1 / (2 K)) ∑_k (1 - ∑_m p_{k,m}^2)`.
4. **Continuous-integral cohort-mean identity (`cohort_mean_pip`, Equation 6)**:
   `(1 / N) ∑_a PiP_τ(a) = (1 / (2 (1 - τ))) ∫_τ^1 (1 - ∑_m p_{θ,m}^2) dθ`.
5. **Cohort mean strictly below one half (`cohort_mean_standing_lt_half`, `cohort_mean_gridScore_lt_half`)**:
   Because every author at least ties with themselves (`∑_m p_{k,m}^2 ≥ 1 / N > 0`), the cohort mean
   is at most `(N - 1) / (2 N) < 1 / 2`.
-/

namespace PiP

open Finset BigOperators MeasureTheory

section FiniteCohortMean

variable {α κ : Type*} [Fintype α] [DecidableEq α] [Fintype κ] [DecidableEq κ]

/-- The share `p_{k,m} = #{a : n a k = m} / N` of peers in cohort `α` who have
exactly `m` qualifying papers at threshold `k`. -/
noncomputable def countShare (n : α → κ → ℕ) (k : κ) (m : ℕ) : ℝ :=
  ((Finset.univ.filter (fun a : α => n a k = m)).card : ℝ) / (Fintype.card α : ℝ)

/-- Pointwise trichotomy decomposition for indicator functions on `ℕ`. -/
theorem trichotomy_indicator_nat (x y : ℕ) :
    (if y < x then (1 : ℝ) else 0) +
      (if x < y then (1 : ℝ) else 0) +
      (if x = y then (1 : ℝ) else 0) = 1 := by
  rcases lt_trichotomy x y with hlt | rfl | hgt
  · have h1 : ¬ y < x := not_lt.mpr (le_of_lt hlt)
    have h2 : x ≠ y := ne_of_lt hlt
    simp [hlt, h1, h2]
  · simp
  · have h1 : ¬ x < y := not_lt.mpr (le_of_lt hgt)
    have h2 : x ≠ y := ne_of_gt hgt
    simp [hgt, h1, h2]

/-- Expressing the cardinality of `strictlyBelow n a k` as a sum of indicator functions. -/
theorem card_strictlyBelow_eq_sum_ite (n : α → κ → ℕ) (a : α) (k : κ) :
    ((strictlyBelow n a k).card : ℝ) =
      ∑ b : α, (if n b k < n a k then (1 : ℝ) else 0) := by
  unfold strictlyBelow
  rw [Finset.card_filter, Nat.cast_sum]
  refine Finset.sum_congr rfl (fun b _ => ?_)
  split_ifs <;> simp

/-- Expressing the cardinality of `{a : f a = m}` as a sum of indicator functions. -/
theorem card_filter_eq_sum_ite (f : α → ℕ) (m : ℕ) :
    ((Finset.univ.filter (fun a : α => f a = m)).card : ℝ) =
      ∑ a : α, (if f a = m then (1 : ℝ) else 0) := by
  rw [Finset.card_filter, Nat.cast_sum]
  refine Finset.sum_congr rfl (fun a _ => ?_)
  split_ifs <;> simp

/-- Partitioning the tie pairs `(a, b)` with `f a = f b` by their common value `m ∈ S`,
where `S` is any finite set of natural numbers containing all values of `f`. -/
theorem sum_ties_eq_sum_sq (f : α → ℕ) (S : Finset ℕ) (hS : ∀ a : α, f a ∈ S) :
    (∑ a : α, ∑ b : α, (if f a = f b then (1 : ℝ) else 0)) =
      ∑ m ∈ S, (((Finset.univ.filter (fun a : α => f a = m)).card : ℝ) ^ 2) := by
  have hpoint : ∀ a b : α,
      (if f a = f b then (1 : ℝ) else 0) =
        ∑ m ∈ S, (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0) := by
    intro a b
    have hmem : f a ∈ S := hS a
    have hcollapse :
        ∑ m ∈ S, (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0) =
          (if f b = f a then (1 : ℝ) else 0) := by
      rw [← Finset.sum_subset (Finset.singleton_subset_iff.mpr hmem)]
      · simp [eq_comm]
      · intro m _ hm
        rw [Finset.mem_singleton] at hm
        have hne : f a ≠ m := Ne.symm hm
        simp [hne]
    rw [hcollapse]
    by_cases h : f a = f b <;> simp [h, eq_comm]
  have h1 : (∑ a : α, ∑ b : α, (if f a = f b then (1 : ℝ) else 0)) =
      ∑ a : α, ∑ b : α, ∑ m ∈ S,
        (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0) :=
    Finset.sum_congr rfl (fun a _ => Finset.sum_congr rfl (fun b _ => hpoint a b))
  have h2 : (∑ a : α, ∑ b : α, ∑ m ∈ S,
        (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0)) =
      ∑ a : α, ∑ m ∈ S, ∑ b : α,
        (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0) :=
    Finset.sum_congr rfl (fun a _ => Finset.sum_comm)
  have h3 : (∑ a : α, ∑ m ∈ S, ∑ b : α,
        (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0)) =
      ∑ m ∈ S, ∑ a : α, ∑ b : α,
        (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0) :=
    Finset.sum_comm
  have h4 : (∑ m ∈ S, ∑ a : α, ∑ b : α,
        (if f a = m then (1 : ℝ) else 0) * (if f b = m then (1 : ℝ) else 0)) =
      ∑ m ∈ S, (((Finset.univ.filter (fun a : α => f a = m)).card : ℝ) ^ 2) := by
    refine Finset.sum_congr rfl (fun m _ => ?_)
    rw [card_filter_eq_sum_ite f m, sq, Finset.sum_mul]
    refine Finset.sum_congr rfl (fun a _ => ?_)
    rw [← Finset.mul_sum]
  exact h1.trans (h2.trans (h3.trans h4))

/-- **Ordered-pair counting identity**:
Twice the total number of strict pairwise wins across the cohort plus the sum of squared tie-group
sizes equals `N^2`. -/
theorem two_mul_sum_strict_lt_add_sum_sq_eq_card_sq
    (n : α → κ → ℕ) (k : κ) (S : Finset ℕ) (hS : ∀ a : α, n a k ∈ S) :
    2 * (∑ a : α, ((strictlyBelow n a k).card : ℝ)) +
      (∑ m ∈ S, (((Finset.univ.filter (fun a : α => n a k = m)).card : ℝ) ^ 2)) =
      (Fintype.card α : ℝ) ^ 2 := by
  have h1 : (∑ a : α, ((strictlyBelow n a k).card : ℝ)) =
      ∑ a : α, ∑ b : α, (if n b k < n a k then (1 : ℝ) else 0) :=
    Finset.sum_congr rfl (fun a _ => card_strictlyBelow_eq_sum_ite n a k)
  have hswap : (∑ a : α, ∑ b : α, (if n a k < n b k then (1 : ℝ) else 0)) =
      ∑ a : α, ∑ b : α, (if n b k < n a k then (1 : ℝ) else 0) := by
    rw [Finset.sum_comm]
  have hties := sum_ties_eq_sum_sq (fun a => n a k) S hS
  have htotal :
      (∑ a : α, ∑ b : α,
        ((if n b k < n a k then (1 : ℝ) else 0) +
         (if n a k < n b k then (1 : ℝ) else 0) +
         (if n a k = n b k then (1 : ℝ) else 0))) =
      (Fintype.card α : ℝ) ^ 2 := by
    simp_rw [trichotomy_indicator_nat]
    simp [sq]
  simp only [Finset.sum_add_distrib] at htotal
  linarith

/-- **Single-threshold cohort-mean standing identity**:
At any threshold `k`, the cohort average of `standing n a k` equals `(1 / 2) * (1 - ∑_{m ∈ S} p_{k,m}^2)`. -/
theorem cohort_mean_standing [Nonempty α] (n : α → κ → ℕ) (k : κ)
    (S : Finset ℕ) (hS : ∀ a : α, n a k ∈ S) :
    (∑ a : α, standing n a k) / (Fintype.card α : ℝ) =
      (1 - ∑ m ∈ S, (countShare n k m) ^ 2) / 2 := by
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  have hNne : (Fintype.card α : ℝ) ≠ 0 := ne_of_gt hN
  have hpair := two_mul_sum_strict_lt_add_sum_sq_eq_card_sq n k S hS
  unfold standing countShare
  rw [← Finset.sum_div]
  have hsq : (∑ m ∈ S, (((Finset.univ.filter (fun a : α => n a k = m)).card : ℝ) / (Fintype.card α : ℝ)) ^ 2) =
      (∑ m ∈ S, (((Finset.univ.filter (fun a : α => n a k = m)).card : ℝ) ^ 2)) / ((Fintype.card α : ℝ) ^ 2) := by
    rw [Finset.sum_div]
    refine Finset.sum_congr rfl (fun m _ => div_pow _ _ 2)
  rw [hsq]
  field_simp [hNne]
  linarith

/-- **Finite-grid cohort-mean identity**:
`(1 / N) ∑_a S(a) = (1 / (2 K)) ∑_k (1 - ∑_{m ∈ S k} p_{k,m}^2)`. -/
theorem cohort_mean_gridScore [Nonempty α] (n : α → κ → ℕ)
    (S : κ → Finset ℕ) (hS : ∀ k : κ, ∀ a : α, n a k ∈ S k) :
    (∑ a : α, gridScore n a) / (Fintype.card α : ℝ) =
      (∑ k : κ, (1 - ∑ m ∈ S k, (countShare n k m) ^ 2)) / (2 * (Fintype.card κ : ℝ)) := by
  unfold gridScore
  rw [← Finset.sum_div, Finset.sum_comm, div_right_comm, Finset.sum_div]
  have hsum :
      ∑ x : κ, (∑ y : α, standing n y x) / (Fintype.card α : ℝ) =
        ∑ k : κ, (1 - ∑ m ∈ S k, (countShare n k m) ^ 2) / 2 := by
    refine Finset.sum_congr rfl (fun k _ => cohort_mean_standing n k (S k) (hS k))
  rw [hsum, ← Finset.sum_div, div_div]

/-- **The cohort mean of `standing` at any threshold is strictly less than `1 / 2`.** -/
theorem cohort_mean_standing_lt_half [Nonempty α] (n : α → κ → ℕ) (k : κ) :
    (∑ a : α, standing n a k) / (Fintype.card α : ℝ) < 1 / 2 := by
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  have hS : ∀ a : α, n a k ∈ Finset.univ.image (fun b : α => n b k) :=
    fun a => Finset.mem_image_of_mem _ (Finset.mem_univ a)
  rw [cohort_mean_standing n k (Finset.univ.image (fun b : α => n b k)) hS]
  have ⟨a₀⟩ := ‹Nonempty α›
  have hpos_card : 0 < (Finset.univ.filter (fun a : α => n a k = n a₀ k)).card := by
    rw [Finset.card_pos]
    exact ⟨a₀, Finset.mem_filter.mpr ⟨Finset.mem_univ a₀, rfl⟩⟩
  have hpos_share : 0 < countShare n k (n a₀ k) := by
    unfold countShare
    exact div_pos (Nat.cast_pos.mpr hpos_card) hN
  have hpos_sq : 0 < (countShare n k (n a₀ k)) ^ 2 := sq_pos_of_pos hpos_share
  have hpos_sum : 0 < ∑ m ∈ Finset.univ.image (fun b : α => n b k), (countShare n k m) ^ 2 := by
    refine lt_of_lt_of_le hpos_sq ?_
    exact Finset.single_le_sum (fun m _ => sq_nonneg (countShare n k m)) (hS a₀)
  linarith

/-- **The cohort mean of `gridScore` is strictly less than `1 / 2`.** -/
theorem cohort_mean_gridScore_lt_half [Nonempty α] [Nonempty κ] (n : α → κ → ℕ) :
    (∑ a : α, gridScore n a) / (Fintype.card α : ℝ) < 1 / 2 := by
  have hK : (0 : ℝ) < Fintype.card κ := Nat.cast_pos.mpr Fintype.card_pos
  unfold gridScore
  rw [← Finset.sum_div, Finset.sum_comm, div_right_comm, Finset.sum_div]
  rw [div_lt_iff₀ hK]
  calc ∑ k : κ, (∑ a : α, standing n a k) / (Fintype.card α : ℝ)
      < ∑ _k : κ, (1 / 2 : ℝ) :=
        Finset.sum_lt_sum_of_nonempty Finset.univ_nonempty (fun k _ => cohort_mean_standing_lt_half n k)
    _ = 1 / 2 * (Fintype.card κ : ℝ) := by simp [mul_comm]

end FiniteCohortMean

/-! ### Continuous-Integral Cohort-Mean Identity (Equation 6) -/

section ContinuousCohortMean

variable {α : Type*} [Fintype α] [DecidableEq α]

/-- The continuous-threshold share `p_{θ,m}` of peers with `m` articles at or above `θ`. -/
noncomputable def countShareAt (papers : α → List ℝ) (θ : ℝ) (m : ℕ) : ℝ :=
  ((Finset.univ.filter (fun a : α => countAboveAt papers a θ = m)).card : ℝ) /
    (Fintype.card α : ℝ)

/-- Continuous PiP score `PiP_τ(a) = (1 / (1 - τ)) ∫_τ^1 standingAt(papers, a, θ) dθ` (Eq. 4). -/
noncomputable def pip (papers : α → List ℝ) (τ : ℝ) (a : α) : ℝ :=
  (∫ θ in τ..1, standingAt papers a θ) / (1 - τ)

/-- Uniform upper bound `M + 1` on any peer's qualifying paper count, so `Finset.range (M + 1)`
contains `countAboveAt papers a θ` for every author `a` and every threshold `θ`. -/
def maxPaperCount (papers : α → List ℝ) : ℕ :=
  Finset.univ.sup (fun a : α => (papers a).length)

theorem countAboveAt_mem_range_maxPaperCount (papers : α → List ℝ) (a : α) (θ : ℝ) :
    countAboveAt papers a θ ∈ Finset.range (maxPaperCount papers + 1) := by
  rw [Finset.mem_range, Nat.lt_succ_iff]
  unfold countAboveAt countAboveList maxPaperCount
  exact le_trans (List.length_filter_le _ _)
    (Finset.le_sup (f := fun b : α => (papers b).length) (Finset.mem_univ a))

/-- Pointwise cohort-mean standing identity at any continuous threshold `θ ∈ ℝ`. -/
theorem cohort_mean_standingAt [Nonempty α] (papers : α → List ℝ) (θ : ℝ) :
    (∑ a : α, standingAt papers a θ) / (Fintype.card α : ℝ) =
      (1 - ∑ m ∈ Finset.range (maxPaperCount papers + 1), (countShareAt papers θ m) ^ 2) / 2 := by
  let n : α → Unit → ℕ := fun a _ => countAboveAt papers a θ
  have h := cohort_mean_standing n () (Finset.range (maxPaperCount papers + 1))
    (fun a => countAboveAt_mem_range_maxPaperCount papers a θ)
  exact h

/-- **Continuous cohort-mean identity (Equation 6 of the paper)**:
`(1 / N) ∑_a PiP_τ(a) = (1 / (2 (1 - τ))) ∫_τ^1 (1 - ∑_m p_{θ,m}^2) dθ`. -/
theorem cohort_mean_pip [Nonempty α] (papers : α → List ℝ) (τ : ℝ)
    (hint : ∀ a : α, IntervalIntegrable (standingAt papers a) volume τ 1) :
    (∑ a : α, pip papers τ a) / (Fintype.card α : ℝ) =
      (∫ θ in τ..1,
        (1 - ∑ m ∈ Finset.range (maxPaperCount papers + 1), (countShareAt papers θ m) ^ 2)) /
        (2 * (1 - τ)) := by
  unfold pip
  rw [← Finset.sum_div, div_right_comm]
  rw [← intervalIntegral.integral_finset_sum (fun a _ => hint a)]
  rw [← intervalIntegral.integral_div]
  have hcongr :
      (∫ θ in τ..1, (∑ a : α, standingAt papers a θ) / (Fintype.card α : ℝ)) =
        ∫ θ in τ..1,
          (1 - ∑ m ∈ Finset.range (maxPaperCount papers + 1), (countShareAt papers θ m) ^ 2) / 2 := by
    refine intervalIntegral.integral_congr (fun θ _ => cohort_mean_standingAt papers θ)
  rw [hcongr, intervalIntegral.integral_div, div_div]

end ContinuousCohortMean

end PiP
