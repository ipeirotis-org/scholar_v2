module

public import PiP.GridIntegral

@[expose] public section

set_option linter.unusedSectionVars false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# Order-Statistics Exact-Integration Algorithm (`25_pip_exact.sql`)

This module formalizes the exact-integration algorithm from `paper/COMPUTATION.md` and
`paper/sql/25_pip_exact.sql`:

Given each author's paper percentiles sorted in non-increasing order `y_{(1)} ≥ y_{(2)} ≥ ⋯ ≥ 0`,
let `Y_k^b` (`kthPercentile (papers b) k`) denote peer `b`'s `k`-th best percentile (and `0` when
`b` has fewer than `k` papers), and let `L_t(m)` (`belowShare (countAboveAt papers) t m`) denote
the share of peers with fewer than `m` papers at or above `t`, so that `L_t(0) = 0`.

1. **Order-statistic threshold equivalences (`countAboveList_lt_iff_kthPercentile_lt`,
   `countAboveList_le_iff_kthPercentile_succ_lt`)**:
   For `k ≥ 1` and threshold `t > 0`, peer `b` has strictly fewer than `k` papers at or above `t`
   (`countAboveList (papers b) t < k`) if and only if `Y_k^b < t`; for `k ≥ 0`, peer `b` has at
   most `k` papers at or above `t` if and only if `Y_{k+1}^b < t`.
2. **Single-peer indicator integral (`integral_ite_lt_eq_max_sub_max`)**:
   For `x₀ ≤ x₁`, `∫ t in x₀..x₁, 1{Y_k^b < t} dt = max(x₁ - Y_k^b, 0) - max(x₀ - Y_k^b, 0)`.
3. **Antiderivative `H_m` of the "fewer than `m`" share (`integral_belowShare_eq_H_sub_H`)**:
   Defining `H_m(x) = (1 / N) ∑_b max(x - Y_m^b, 0)` for `m ≥ 1` and `H_0 = 0`, on any interval
   `(x₀, x₁]` with `0 ≤ x₀ ≤ x₁`, `∫ t in x₀..x₁, L_t(m) dt = H_m(x₁) - H_m(x₀)`.
4. **Mid-rank standing on constant-count intervals (`integral_standingAt_const_eq_H_sub_H`)**:
   Where focal author `a` has exactly `k ≥ 0` qualifying papers, the mid-rank standing is
   `(L_t(k) + L_t(k + 1)) / 2`, so
   `∫ t in x₀..x₁, standingAt papers a t dt
      = ((H_k(x₁) - H_k(x₀)) + (H_{k+1}(x₁) - H_{k+1}(x₀))) / 2`.
   For `k = 0`, above the focal author's best article, only the `H_1` term appears
   (`integral_standingAt_zero_count_eq_half_H_one`).
5. **Whole-range sum (`integral_standingAt_partition_eq_sum_H_sub_H`, `pip_eq_sum_H_sub_H`)**:
   Summing over the constant-count intervals, including the top interval where the count is `0`,
   gives the integral of the standing curve and hence `PiP_τ(a)`.
6. **SQL `asof` running-count and running-sum identity (`H_eq_asof_formula`)**:
   For `k ≥ 1` and `x ≥ 0`, separating peers with at least `k` papers (`n_k` peers) from those
   with fewer than `k` papers (`N - n_k` peers, for whom `Y_k^b = 0`),
   `H_k(x) = (x * (c_le(x) + N - n_k) - s_le(x)) / N`,
   matching the `asof` step of `paper/sql/25_pip_exact.sql`.
-/

namespace PiP

open Finset BigOperators MeasureTheory

variable {α : Type*} [Fintype α] [DecidableEq α]

/-- The 1-indexed `k`-th paper percentile `Y_k` from a sorted descending list `ps`,
defaulting to `0` when `ps` has fewer than `k` papers. -/
def kthPercentile (ps : List ℝ) (k : ℕ) : ℝ :=
  ps.getD (k - 1) 0

/-- If `ps = y :: ys` is sorted in non-increasing order (`ps.Pairwise (· ≥ ·)`) and `y < t`,
then no element of `y :: ys` is at or above `t`. -/
theorem countAboveList_cons_eq_zero_of_lt {y t : ℝ} {ys : List ℝ}
    (hsorted : (y :: ys).Pairwise (· ≥ ·)) (hy : y < t) :
    countAboveList (y :: ys) t = 0 := by
  unfold countAboveList
  rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
  intro z hz
  simp only [decide_eq_true_eq, not_le]
  rcases List.mem_cons.mp hz with rfl | hmem
  · exact hy
  · have hzy : z ≤ y := (List.pairwise_cons.mp hsorted).1 z hmem
    exact lt_of_le_of_lt hzy hy

/-- Helper lemma (0-indexed): for a sorted non-increasing list `ps` of non-negative percentiles
and any threshold `t > 0`, `j + 1 ≤ countAboveList ps t ↔ t ≤ ps.getD j 0`. -/
theorem countAboveList_ge_succ_iff_getD_ge (ps : List ℝ)
    (hsorted : ps.Pairwise (· ≥ ·)) (hnonneg : ∀ y ∈ ps, 0 ≤ y)
    (j : ℕ) {t : ℝ} (ht : 0 < t) :
    j + 1 ≤ countAboveList ps t ↔ t ≤ ps.getD j 0 := by
  induction ps generalizing j with
  | nil =>
    simp only [countAboveList, List.filter_nil, List.length_nil, List.getD_nil]
    constructor
    · intro h; omega
    · intro h; linarith
  | cons y ys ih =>
    have hys_sorted : ys.Pairwise (· ≥ ·) := (List.pairwise_cons.mp hsorted).2
    have hys_nonneg : ∀ z ∈ ys, 0 ≤ z := fun z hz => hnonneg z (List.mem_cons_of_mem y hz)
    cases j with
    | zero =>
      simp only [Nat.zero_add, List.getD_cons_zero]
      constructor
      · intro hpos
        by_contra hlt
        push Not at hlt
        have hzero := countAboveList_cons_eq_zero_of_lt hsorted hlt
        omega
      · intro hty
        rw [countAboveList_cons, if_pos hty]
        omega
    | succ j' =>
      simp only [List.getD_cons_succ]
      have ih' := ih hys_sorted hys_nonneg j'
      constructor
      · intro hge
        have hty : t ≤ y := by
          by_contra hlt
          push Not at hlt
          have hzero := countAboveList_cons_eq_zero_of_lt hsorted hlt
          omega
        rw [countAboveList_cons, if_pos hty] at hge
        exact ih'.mp (by omega)
      · intro hget
        have hge_ys : j' + 1 ≤ countAboveList ys t := ih'.mpr hget
        have hty : t ≤ y := by
          by_contra hlt
          push Not at hlt
          have hzero : countAboveList ys t = 0 := by
            unfold countAboveList
            rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
            intro z hz
            simp only [decide_eq_true_eq, not_le]
            have hzy : z ≤ y := (List.pairwise_cons.mp hsorted).1 z hz
            exact lt_of_le_of_lt hzy hlt
          omega
        rw [countAboveList_cons, if_pos hty]
        omega

/-- **Order-statistic threshold equivalence (`≥`)**:
For `k ≥ 1` and `t > 0`, a sorted non-increasing list `ps` of non-negative percentiles has at
least `k` papers at or above `t` if and only if its `k`-th percentile is at least `t`. -/
theorem countAboveList_ge_iff_kthPercentile_ge (ps : List ℝ)
    (hsorted : ps.Pairwise (· ≥ ·)) (hnonneg : ∀ y ∈ ps, 0 ≤ y)
    {k : ℕ} (hk : 1 ≤ k) {t : ℝ} (ht : 0 < t) :
    k ≤ countAboveList ps t ↔ t ≤ kthPercentile ps k := by
  unfold kthPercentile
  have hk' : (k - 1) + 1 = k := Nat.sub_add_cancel hk
  rw [← hk']
  exact countAboveList_ge_succ_iff_getD_ge ps hsorted hnonneg (k - 1) ht

/-- **Order-statistic threshold equivalence (`<`)**:
For `k ≥ 1` and `t > 0`, a sorted non-increasing list `ps` of non-negative percentiles has
strictly fewer than `k` papers at or above `t` if and only if its `k`-th percentile is strictly
below `t`. -/
theorem countAboveList_lt_iff_kthPercentile_lt (ps : List ℝ)
    (hsorted : ps.Pairwise (· ≥ ·)) (hnonneg : ∀ y ∈ ps, 0 ≤ y)
    {k : ℕ} (hk : 1 ≤ k) {t : ℝ} (ht : 0 < t) :
    countAboveList ps t < k ↔ kthPercentile ps k < t := by
  rw [← not_le, countAboveList_ge_iff_kthPercentile_ge ps hsorted hnonneg hk ht, not_le]

/-- **Order-statistic threshold equivalence (`≤`)**:
For `k ≥ 0` and `t > 0`, a sorted non-increasing list `ps` of non-negative percentiles has at
most `k` papers at or above `t` if and only if its `(k + 1)`-th percentile is strictly below `t`. -/
theorem countAboveList_le_iff_kthPercentile_succ_lt (ps : List ℝ)
    (hsorted : ps.Pairwise (· ≥ ·)) (hnonneg : ∀ y ∈ ps, 0 ≤ y)
    (k : ℕ) {t : ℝ} (ht : 0 < t) :
    countAboveList ps t ≤ k ↔ kthPercentile ps (k + 1) < t := by
  rw [← Nat.lt_add_one_iff]
  exact countAboveList_lt_iff_kthPercentile_lt ps hsorted hnonneg (Nat.le_add_left 1 k) ht

/-- Integrability of the single-peer strict-inequality indicator `t ↦ if Y < t then 1 else 0`
on any interval `[x₀, x₁]` with `x₀ ≤ x₁`. -/
theorem intervalIntegrable_ite_lt (Y : ℝ) {x₀ x₁ : ℝ} (hx : x₀ ≤ x₁) :
    IntervalIntegrable (fun t : ℝ => if Y < t then (1 : ℝ) else 0) volume x₀ x₁ := by
  by_cases h1 : x₁ ≤ Y
  · have hconst : IntervalIntegrable (fun _ : ℝ => (0 : ℝ)) volume x₀ x₁ := intervalIntegrable_const
    rw [intervalIntegrable_iff_integrableOn_Ioc_of_le hx] at hconst ⊢
    refine hconst.congr_fun (fun t ht => ?_) measurableSet_Ioc
    have hnot : ¬ (Y < t) := not_lt.mpr (le_trans ht.2 h1)
    simp [hnot]
  · push Not at h1
    by_cases h0 : Y ≤ x₀
    · have hconst : IntervalIntegrable (fun _ : ℝ => (1 : ℝ)) volume x₀ x₁ := intervalIntegrable_const
      rw [intervalIntegrable_iff_integrableOn_Ioc_of_le hx] at hconst ⊢
      refine hconst.congr_fun (fun t ht => ?_) measurableSet_Ioc
      have hlt : Y < t := lt_of_le_of_lt h0 ht.1
      simp [hlt]
    · push Not at h0
      have hleft : IntervalIntegrable (fun t : ℝ => if Y < t then (1 : ℝ) else 0) volume x₀ Y := by
        have hconst : IntervalIntegrable (fun _ : ℝ => (0 : ℝ)) volume x₀ Y := intervalIntegrable_const
        rw [intervalIntegrable_iff_integrableOn_Ioc_of_le (le_of_lt h0)] at hconst ⊢
        refine hconst.congr_fun (fun t ht => ?_) measurableSet_Ioc
        have hnot : ¬ (Y < t) := not_lt.mpr ht.2
        simp [hnot]
      have hright : IntervalIntegrable (fun t : ℝ => if Y < t then (1 : ℝ) else 0) volume Y x₁ := by
        have hconst : IntervalIntegrable (fun _ : ℝ => (1 : ℝ)) volume Y x₁ := intervalIntegrable_const
        rw [intervalIntegrable_iff_integrableOn_Ioc_of_le (le_of_lt h1)] at hconst ⊢
        refine hconst.congr_fun (fun t ht => ?_) measurableSet_Ioc
        simp [ht.1]
      exact hleft.trans hright

/-- **Single-peer indicator integral**:
For `x₀ ≤ x₁`, `∫ t in x₀..x₁, (if Y < t then 1 else 0) dt = max (x₁ - Y) 0 - max (x₀ - Y) 0`. -/
theorem integral_ite_lt_eq_max_sub_max (Y : ℝ) {x₀ x₁ : ℝ} (hx : x₀ ≤ x₁) :
    (∫ t in x₀..x₁, (if Y < t then (1 : ℝ) else 0)) =
      max (x₁ - Y) 0 - max (x₀ - Y) 0 := by
  by_cases h1 : x₁ ≤ Y
  · rw [intervalIntegral.integral_of_le hx]
    have hzero : ∫ t in Set.Ioc x₀ x₁, (if Y < t then (1 : ℝ) else 0) = ∫ _t in Set.Ioc x₀ x₁, (0 : ℝ) := by
      refine MeasureTheory.setIntegral_congr_fun measurableSet_Ioc (fun t ht => ?_)
      have hnot : ¬ (Y < t) := not_lt.mpr (le_trans ht.2 h1)
      simp [hnot]
    rw [hzero, MeasureTheory.setIntegral_const, smul_zero]
    have hm1 : max (x₁ - Y) 0 = 0 := max_eq_right (by linarith)
    have hm0 : max (x₀ - Y) 0 = 0 := max_eq_right (by linarith)
    linarith
  · push Not at h1
    by_cases h0 : Y ≤ x₀
    · rw [intervalIntegral.integral_of_le hx]
      have hone : ∫ t in Set.Ioc x₀ x₁, (if Y < t then (1 : ℝ) else 0) = ∫ _t in Set.Ioc x₀ x₁, (1 : ℝ) := by
        refine MeasureTheory.setIntegral_congr_fun measurableSet_Ioc (fun t ht => ?_)
        have hlt : Y < t := lt_of_le_of_lt h0 ht.1
        simp [hlt]
      rw [hone, ← intervalIntegral.integral_of_le hx, intervalIntegral.integral_const, smul_eq_mul]
      have hm1 : max (x₁ - Y) 0 = x₁ - Y := max_eq_left (by linarith)
      have hm0 : max (x₀ - Y) 0 = x₀ - Y := max_eq_left (by linarith)
      linarith
    · push Not at h0
      have hleft_int := intervalIntegrable_ite_lt Y (le_of_lt h0)
      have hright_int := intervalIntegrable_ite_lt Y (le_of_lt h1)
      rw [← intervalIntegral.integral_add_adjacent_intervals hleft_int hright_int]
      have hleft_val : (∫ t in x₀..Y, (if Y < t then (1 : ℝ) else 0)) = 0 := by
        rw [intervalIntegral.integral_of_le (le_of_lt h0)]
        have hzero : ∫ t in Set.Ioc x₀ Y, (if Y < t then (1 : ℝ) else 0) = ∫ _t in Set.Ioc x₀ Y, (0 : ℝ) := by
          refine MeasureTheory.setIntegral_congr_fun measurableSet_Ioc (fun t ht => ?_)
          have hnot : ¬ (Y < t) := not_lt.mpr ht.2
          simp [hnot]
        rw [hzero, MeasureTheory.setIntegral_const, smul_zero]
      have hright_val : (∫ t in Y..x₁, (if Y < t then (1 : ℝ) else 0)) = x₁ - Y := by
        rw [intervalIntegral.integral_of_le (le_of_lt h1)]
        have hone : ∫ t in Set.Ioc Y x₁, (if Y < t then (1 : ℝ) else 0) = ∫ _t in Set.Ioc Y x₁, (1 : ℝ) := by
          refine MeasureTheory.setIntegral_congr_fun measurableSet_Ioc (fun t ht => ?_)
          simp [ht.1]
        rw [hone, ← intervalIntegral.integral_of_le (le_of_lt h1), intervalIntegral.integral_const, smul_eq_mul]
        ring
      have hm1 : max (x₁ - Y) 0 = x₁ - Y := max_eq_left (by linarith)
      have hm0 : max (x₀ - Y) 0 = 0 := max_eq_right (by linarith)
      linarith

/-- The cohort antiderivative `H_k(x) = (1 / N) ∑_b max(x - Y_k^b, 0)` from `paper/COMPUTATION.md`
and `paper/sql/25_pip_exact.sql`, with the convention `H_0 = 0` (no peer has fewer than zero
articles). -/
noncomputable def H (papers : α → List ℝ) (k : ℕ) (x : ℝ) : ℝ :=
  if k = 0 then 0
  else (∑ b : α, max (x - kthPercentile (papers b) k) 0) / (Fintype.card α : ℝ)

/-- The convention `H_0 = 0`. -/
theorem H_zero (papers : α → List ℝ) (x : ℝ) : H papers 0 x = 0 := by
  simp [H]

/-- For `k ≥ 1`, `H_k(x) = (1 / N) ∑_b max(x - Y_k^b, 0)`. -/
theorem H_of_pos (papers : α → List ℝ) {k : ℕ} (hk : 1 ≤ k) (x : ℝ) :
    H papers k x = (∑ b : α, max (x - kthPercentile (papers b) k) 0) / (Fintype.card α : ℝ) := by
  unfold H
  rw [if_neg (by omega)]

/-- At any threshold `t > 0` and for `m ≥ 1`, the share `L_t(m)` of peers with fewer than `m`
articles at or above `t` equals `(1 / N) ∑_b 1{Y_m^b < t}`. -/
theorem belowShare_eq_sum_kthPercentile_lt (papers : α → List ℝ)
    (hsorted : ∀ b : α, (papers b).Pairwise (· ≥ ·))
    (hnonneg : ∀ b : α, ∀ y ∈ papers b, 0 ≤ y)
    {m : ℕ} (hm : 1 ≤ m) {t : ℝ} (ht : 0 < t) :
    belowShare (countAboveAt papers) t m =
      (∑ b : α, (if kthPercentile (papers b) m < t then (1 : ℝ) else 0)) / (Fintype.card α : ℝ) := by
  unfold belowShare
  rw [card_filter_univ_eq_sum_ite]
  congr 1
  refine Finset.sum_congr rfl (fun b _ => ?_)
  have hiff := countAboveList_lt_iff_kthPercentile_lt (papers b) (hsorted b) (hnonneg b) hm ht
  unfold countAboveAt
  by_cases h : kthPercentile (papers b) m < t
  · rw [if_pos h, if_pos (hiff.mpr h)]
  · rw [if_neg h, if_neg (fun hc => h (hiff.mp hc))]

/-- Integrability of the "fewer than `m`" share `t ↦ L_t(m)` on any interval `(x₀, x₁]` with
`0 ≤ x₀ ≤ x₁`. -/
theorem intervalIntegrable_belowShare (papers : α → List ℝ)
    (hsorted : ∀ b : α, (papers b).Pairwise (· ≥ ·))
    (hnonneg : ∀ b : α, ∀ y ∈ papers b, 0 ≤ y)
    (m : ℕ) {x₀ x₁ : ℝ} (hx₀ : 0 ≤ x₀) (hx : x₀ ≤ x₁) :
    IntervalIntegrable (fun t : ℝ => belowShare (countAboveAt papers) t m) volume x₀ x₁ := by
  rcases Nat.eq_zero_or_pos m with rfl | hm
  · simp_rw [belowShare_zero]
    exact intervalIntegrable_const
  · have hsum : IntervalIntegrable
        (fun t : ℝ => (∑ b : α, (if kthPercentile (papers b) m < t then (1 : ℝ) else 0)) / (Fintype.card α : ℝ))
        volume x₀ x₁ := by
      simpa using
        (IntervalIntegrable.sum Finset.univ
          (fun b _ => intervalIntegrable_ite_lt (kthPercentile (papers b) m) hx)).div_const (Fintype.card α : ℝ)
    rw [intervalIntegrable_iff_integrableOn_Ioc_of_le hx] at hsum ⊢
    refine hsum.congr_fun (fun t ht => ?_) measurableSet_Ioc
    exact (belowShare_eq_sum_kthPercentile_lt papers hsorted hnonneg hm (lt_of_le_of_lt hx₀ ht.1)).symm

/-- **`H_m` is the antiderivative of the "fewer than `m`" share**:
On any interval with `0 ≤ x₀ ≤ x₁` and for every `m ≥ 0`,
`∫ t in x₀..x₁, L_t(m) dt = H_m(x₁) - H_m(x₀)` (both sides are zero for `m = 0`). -/
theorem integral_belowShare_eq_H_sub_H (papers : α → List ℝ)
    (hsorted : ∀ b : α, (papers b).Pairwise (· ≥ ·))
    (hnonneg : ∀ b : α, ∀ y ∈ papers b, 0 ≤ y)
    (m : ℕ) {x₀ x₁ : ℝ} (hx₀ : 0 ≤ x₀) (hx : x₀ ≤ x₁) :
    (∫ t in x₀..x₁, belowShare (countAboveAt papers) t m) = H papers m x₁ - H papers m x₀ := by
  rcases Nat.eq_zero_or_pos m with rfl | hm
  · simp [belowShare_zero, H_zero]
  · rw [intervalIntegral.integral_of_le hx]
    have hcongr :
        (∫ t in Set.Ioc x₀ x₁, belowShare (countAboveAt papers) t m) =
          ∫ t in Set.Ioc x₀ x₁,
            (∑ b : α, (if kthPercentile (papers b) m < t then (1 : ℝ) else 0)) / (Fintype.card α : ℝ) := by
      refine MeasureTheory.setIntegral_congr_fun measurableSet_Ioc (fun t ht => ?_)
      exact belowShare_eq_sum_kthPercentile_lt papers hsorted hnonneg hm (lt_of_le_of_lt hx₀ ht.1)
    rw [hcongr, ← intervalIntegral.integral_of_le hx, intervalIntegral.integral_div]
    rw [intervalIntegral.integral_finset_sum]
    · simp_rw [integral_ite_lt_eq_max_sub_max _ hx, Finset.sum_sub_distrib]
      rw [H_of_pos papers hm, H_of_pos papers hm]
      ring
    · intro b _
      exact intervalIntegrable_ite_lt (kthPercentile (papers b) m) hx

/-- At any threshold `t` where focal author `a` has `countAboveAt papers a t = k`, the mid-rank
standing is the average `(L_t(k) + L_t(k + 1)) / 2` of the share of peers with fewer than `k`
articles and the share with at most `k` articles at or above `t`. -/
theorem standingAt_eq_half_belowShare_add (papers : α → List ℝ) (a : α) {k : ℕ} {t : ℝ}
    (ha : countAboveAt papers a t = k) :
    standingAt papers a t =
      (belowShare (countAboveAt papers) t k + belowShare (countAboveAt papers) t (k + 1)) / 2 := by
  rw [standingAt_eq_standing, standing_eq_half_belowShare_add, ha]

/-- **Exact cell-integral theorem (`25_pip_exact.sql`)**:
On any interval `(x₀, x₁]` with `0 ≤ x₀ ≤ x₁` where focal author `a` has a constant qualifying
paper count `k ≥ 0`, the integral of the mid-rank `standingAt papers a t` over `[x₀, x₁]` equals
`((H_k(x₁) - H_k(x₀)) + (H_{k+1}(x₁) - H_{k+1}(x₀))) / 2`, with `H_0 = 0`. -/
theorem integral_standingAt_const_eq_H_sub_H (papers : α → List ℝ)
    (hsorted : ∀ b : α, (papers b).Pairwise (· ≥ ·))
    (hnonneg : ∀ b : α, ∀ y ∈ papers b, 0 ≤ y)
    (a : α) {k : ℕ} {x₀ x₁ : ℝ} (hx₀ : 0 ≤ x₀) (hx : x₀ ≤ x₁)
    (hconst : ∀ t ∈ Set.Ioc x₀ x₁, countAboveAt papers a t = k) :
    (∫ t in x₀..x₁, standingAt papers a t) =
      ((H papers k x₁ - H papers k x₀) + (H papers (k + 1) x₁ - H papers (k + 1) x₀)) / 2 := by
  have hcongr :
      (∫ t in x₀..x₁, standingAt papers a t) =
        ∫ t in x₀..x₁,
          (belowShare (countAboveAt papers) t k + belowShare (countAboveAt papers) t (k + 1)) / 2 := by
    rw [intervalIntegral.integral_of_le hx, intervalIntegral.integral_of_le hx]
    exact MeasureTheory.setIntegral_congr_fun measurableSet_Ioc
      (fun t ht => standingAt_eq_half_belowShare_add papers a (hconst t ht))
  rw [hcongr, intervalIntegral.integral_div,
    intervalIntegral.integral_add
      (intervalIntegrable_belowShare papers hsorted hnonneg k hx₀ hx)
      (intervalIntegrable_belowShare papers hsorted hnonneg (k + 1) hx₀ hx),
    integral_belowShare_eq_H_sub_H papers hsorted hnonneg k hx₀ hx,
    integral_belowShare_eq_H_sub_H papers hsorted hnonneg (k + 1) hx₀ hx]

/-- **Top interval (above the focal author's best article)**:
On any interval `(x₀, x₁]` with `0 ≤ x₀ ≤ x₁` where focal author `a` has zero qualifying papers,
the standing is half the share of peers who also have none, and its integral is
`(H_1(x₁) - H_1(x₀)) / 2`. -/
theorem integral_standingAt_zero_count_eq_half_H_one (papers : α → List ℝ)
    (hsorted : ∀ b : α, (papers b).Pairwise (· ≥ ·))
    (hnonneg : ∀ b : α, ∀ y ∈ papers b, 0 ≤ y)
    (a : α) {x₀ x₁ : ℝ} (hx₀ : 0 ≤ x₀) (hx : x₀ ≤ x₁)
    (hzero : ∀ t ∈ Set.Ioc x₀ x₁, countAboveAt papers a t = 0) :
    (∫ t in x₀..x₁, standingAt papers a t) = (H papers 1 x₁ - H papers 1 x₀) / 2 := by
  rw [integral_standingAt_const_eq_H_sub_H papers hsorted hnonneg a hx₀ hx hzero, H_zero, H_zero]
  simp

/-- **Multi-cell exact-integral theorem (`25_pip_exact.sql`)**:
Given a non-decreasing sequence of breakpoints `x : ℕ → ℝ` (`0 ≤ x 0` and `x i ≤ x (i + 1)`)
such that on each cell `(x i, x (i + 1)]` (`i < m`), focal author `a` has constant qualifying
paper count `k i ≥ 0` (the top cell, above the author's best article, has `k i = 0`), the integral
of the mid-rank `standingAt papers a t` over `[x 0, x m]` equals the sum over `i ∈ Finset.range m`
of `((H (k i) (x (i + 1)) - H (k i) (x i)) + (H (k i + 1) (x (i + 1)) - H (k i + 1) (x i))) / 2`. -/
theorem integral_standingAt_partition_eq_sum_H_sub_H (papers : α → List ℝ)
    (hsorted : ∀ b : α, (papers b).Pairwise (· ≥ ·))
    (hnonneg : ∀ b : α, ∀ y ∈ papers b, 0 ≤ y)
    (a : α) (m : ℕ) (x : ℕ → ℝ) (k : ℕ → ℕ)
    (hx₀ : 0 ≤ x 0)
    (hmono : ∀ i < m, x i ≤ x (i + 1))
    (hconst : ∀ i < m, ∀ t ∈ Set.Ioc (x i) (x (i + 1)), countAboveAt papers a t = k i) :
    (∫ t in (x 0)..(x m), standingAt papers a t) =
      ∑ i ∈ Finset.range m,
        ((H papers (k i) (x (i + 1)) - H papers (k i) (x i)) +
          (H papers (k i + 1) (x (i + 1)) - H papers (k i + 1) (x i))) / 2 := by
  have hx_nonneg : ∀ i ≤ m, 0 ≤ x i := by
    intro i hi
    induction i with
    | zero => exact hx₀
    | succ j ih =>
      exact le_trans (ih (by omega)) (hmono j (by omega))
  have hadj := intervalIntegral.sum_integral_adjacent_intervals
    (a := x) (n := m) (fun i _ => intervalIntegrable_standingAt papers a (x i) (x (i + 1)))
  rw [← hadj]
  refine Finset.sum_congr rfl (fun i hi => ?_)
  rw [Finset.mem_range] at hi
  exact integral_standingAt_const_eq_H_sub_H papers hsorted hnonneg a
    (hx_nonneg i (le_of_lt hi)) (hmono i hi) (hconst i hi)

/-- **Exact PiP score (`25_pip_exact.sql`)**:
With breakpoints running from the cutoff `x 0 = τ ≥ 0` to `x m = 1`, the continuous score is
`PiP_τ(a) = (1 / (1 - τ)) ∑_i (1/2) [(H_{k_i}(x_{i+1}) - H_{k_i}(x_i)) +
  (H_{k_i+1}(x_{i+1}) - H_{k_i+1}(x_i))]`. -/
theorem pip_eq_sum_H_sub_H (papers : α → List ℝ)
    (hsorted : ∀ b : α, (papers b).Pairwise (· ≥ ·))
    (hnonneg : ∀ b : α, ∀ y ∈ papers b, 0 ≤ y)
    (a : α) (m : ℕ) (x : ℕ → ℝ) (k : ℕ → ℕ) {τ : ℝ}
    (hτ : 0 ≤ τ) (hstart : x 0 = τ) (hend : x m = 1)
    (hmono : ∀ i < m, x i ≤ x (i + 1))
    (hconst : ∀ i < m, ∀ t ∈ Set.Ioc (x i) (x (i + 1)), countAboveAt papers a t = k i) :
    pip papers τ a =
      (∑ i ∈ Finset.range m,
        ((H papers (k i) (x (i + 1)) - H papers (k i) (x i)) +
          (H papers (k i + 1) (x (i + 1)) - H papers (k i + 1) (x i))) / 2) / (1 - τ) := by
  unfold pip
  rw [← integral_standingAt_partition_eq_sum_H_sub_H papers hsorted hnonneg a m x k
    (by rw [hstart]; exact hτ) hmono hconst, hstart, hend]

/-! ### SQL `asof` Running-Count and Running-Sum Decomposition (`25_pip_exact.sql`) -/

/-- The subset of peers in `α` who have at least `k` papers (`n_k` in `25_pip_exact.sql`). -/
def peersWithAtLeastK (papers : α → List ℝ) (k : ℕ) : Finset α :=
  Finset.univ.filter (fun b : α => k ≤ (papers b).length)

/-- The running count `c_le` in `25_pip_exact.sql`: the number of peers in `peersWithAtLeastK`
whose `k`-th paper percentile is at most `x`. -/
noncomputable def asofCountLe (papers : α → List ℝ) (k : ℕ) (x : ℝ) : ℕ :=
  ((peersWithAtLeastK papers k).filter (fun b : α => kthPercentile (papers b) k ≤ x)).card

/-- The running sum `s_le` in `25_pip_exact.sql`: the sum of `Y_k^b` over peers in
`peersWithAtLeastK` whose `k`-th paper percentile is at most `x`. -/
noncomputable def asofSumLe (papers : α → List ℝ) (k : ℕ) (x : ℝ) : ℝ :=
  ∑ b ∈ (peersWithAtLeastK papers k).filter (fun b : α => kthPercentile (papers b) k ≤ x),
    kthPercentile (papers b) k

/-- When a peer `b` has fewer than `k` papers (`k ≥ 1`), their `k`-th percentile is `0`. -/
theorem kthPercentile_eq_zero_of_length_lt {ps : List ℝ} {k : ℕ} (hk : 1 ≤ k) (hlen : ps.length < k) :
    kthPercentile ps k = 0 := by
  unfold kthPercentile
  have hnone : ps[k - 1]? = none := List.getElem?_eq_none (by omega)
  simp [List.getD, hnone]

/-- **SQL `asof` running-sum identity (`25_pip_exact.sql`)**:
For `k ≥ 1` and any query point `x ≥ 0`,
`H_k(x) = (x * (c_le(x) + N - n_k) - s_le(x)) / N`. -/
theorem H_eq_asof_formula (papers : α → List ℝ) {k : ℕ} (hk : 1 ≤ k) {x : ℝ} (hx : 0 ≤ x) :
    H papers k x =
      (x * ((asofCountLe papers k x : ℝ) + (Fintype.card α : ℝ) -
        ((peersWithAtLeastK papers k).card : ℝ)) - asofSumLe papers k x) /
        (Fintype.card α : ℝ) := by
  rw [H_of_pos papers hk]
  unfold asofCountLe asofSumLe
  congr 1
  let P := peersWithAtLeastK papers k
  let Ple := P.filter (fun b : α => kthPercentile (papers b) k ≤ x)
  have hsplit := (Finset.sum_filter_add_sum_filter_not (s := Finset.univ)
    (p := fun b : α => k ≤ (papers b).length)
    (f := fun b : α => max (x - kthPercentile (papers b) k) 0)).symm
  rw [hsplit]
  have hnotP :
      ∑ b ∈ Finset.univ.filter (fun b : α => ¬ k ≤ (papers b).length),
        max (x - kthPercentile (papers b) k) 0 =
      x * ((Fintype.card α : ℝ) - (P.card : ℝ)) := by
    have hterm : ∀ b ∈ Finset.univ.filter (fun b : α => ¬ k ≤ (papers b).length),
        max (x - kthPercentile (papers b) k) 0 = x := by
      intro b hb
      simp only [Finset.mem_filter, Finset.mem_univ, true_and, not_le] at hb
      rw [kthPercentile_eq_zero_of_length_lt hk hb, sub_zero, max_eq_left hx]
    rw [Finset.sum_congr rfl hterm, Finset.sum_const, nsmul_eq_mul]
    have hcard_sum := Finset.sum_filter_add_sum_filter_not (s := Finset.univ)
      (p := fun b : α => k ≤ (papers b).length) (f := fun _ => (1 : ℝ))
    simp only [Finset.sum_const, nsmul_eq_mul, mul_one, Finset.card_univ] at hcard_sum
    have hcard_real : ((Finset.univ.filter (fun b : α => ¬ k ≤ (papers b).length)).card : ℝ) =
        (Fintype.card α : ℝ) - (P.card : ℝ) := by
      change (P.card : ℝ) + ((Finset.univ.filter (fun b : α => ¬ k ≤ (papers b).length)).card : ℝ) =
        (Fintype.card α : ℝ) at hcard_sum
      linarith
    rw [hcard_real, mul_comm]
  have hP :
      ∑ b ∈ P, max (x - kthPercentile (papers b) k) 0 =
        x * (Ple.card : ℝ) - ∑ b ∈ Ple, kthPercentile (papers b) k := by
    rw [← Finset.sum_filter_add_sum_filter_not (s := P) (p := fun b : α => kthPercentile (papers b) k ≤ x)]
    have hgt : ∑ b ∈ P.filter (fun b : α => ¬ kthPercentile (papers b) k ≤ x),
        max (x - kthPercentile (papers b) k) 0 = 0 := by
      refine Finset.sum_eq_zero (fun b hb => ?_)
      simp only [Finset.mem_filter, not_le] at hb
      exact max_eq_right (by linarith [hb.2])
    have hle : ∑ b ∈ Ple, max (x - kthPercentile (papers b) k) 0 =
        x * (Ple.card : ℝ) - ∑ b ∈ Ple, kthPercentile (papers b) k := by
      have hle_term : ∀ b ∈ Ple, max (x - kthPercentile (papers b) k) 0 = x - kthPercentile (papers b) k := by
        intro b hb
        simp only [Ple, Finset.mem_filter] at hb
        exact max_eq_left (by linarith [hb.2])
      rw [Finset.sum_congr rfl hle_term, Finset.sum_sub_distrib, Finset.sum_const, nsmul_eq_mul, mul_comm]
    linarith
  change (∑ b ∈ P, max (x - kthPercentile (papers b) k) 0) +
      (∑ b ∈ Finset.univ.filter (fun b : α => ¬ k ≤ (papers b).length),
        max (x - kthPercentile (papers b) k) 0) = _
  linarith

end PiP
