module

public import PiP.CohortMean
public import Mathlib.MeasureTheory.Integral.IntervalIntegral.Basic
public import Mathlib.MeasureTheory.Measure.Lebesgue.Basic

@[expose] public section

set_option linter.unusedSectionVars false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# Strict Upward Rounding and Grid-to-Integral Equivalence

This module formalizes the connection between the finite `M`-class grid computation and the
continuous integral definition (Section 4.6 of the paper and `paper/COMPUTATION.md`):

For a grid with `M > 0` classes, define the strict upward rounding transformation
`R_M(y) = (⌊M y⌋ + 1) / M` (`roundUp M y`).

1. **Cell correspondence (`roundUp_ge_iff_ge_grid`)**:
   Throughout the half-open cell `θ ∈ (k / M, (k + 1) / M]`, a rounded paper percentile qualifies
   (`θ ≤ R_M(y)`) if and only if its original percentile satisfies `k / M ≤ y`.
2. **Constancy of counts and standings on each grid cell (`countAboveAt_roundUp_eq_on_cell`, `standingAt_roundUp_eq_on_cell`)**:
   Applying `R_M` to every author's paper percentiles throughout the cohort, each author's count
   and standing on `(k / M, (k + 1) / M]` equal their unrounded count and standing at the left
   endpoint `k / M`.
3. **Cell integral identity (`integral_standingAt_roundUp_cell`)**:
   `∫ θ in (k / M)..((k + 1) / M), g_a^{R_M}(θ) dθ = g_a(k / M) / M`.
4. **Grid-to-integral equivalence (`grid_eq_integral_roundUp`)**:
   For `0 < M` and `q < M` with cutoff `τ = q / M`,
   `(1 / (M - q)) ∑_{k=q}^{M-1} g_a(k / M) = (1 / (1 - τ)) ∫_τ^1 g_a^{R_M}(θ) dθ`.
5. **Cutoff boundary behavior (`roundUp_at_cutoff_qualifies`)**:
   A paper with original percentile `y = τ = q / M` strictly rounds up to `(q + 1) / M` and
   qualifies on the entire cell `(q / M, (q + 1) / M]`, whereas without rounding it qualifies at
   no point of `(q / M, 1]`.
-/

namespace PiP

open Finset BigOperators MeasureTheory

variable {α : Type*} [Fintype α] [DecidableEq α]

/-- Strict upward rounding to the next grid point of `1 / M`:
`R_M(y) = (⌊M y⌋ + 1) / M`. -/
noncomputable def roundUp (M : ℕ) (y : ℝ) : ℝ :=
  ((⌊(M : ℝ) * y⌋ : ℝ) + 1) / (M : ℝ)

/-- Cohort paper profile after applying `roundUp M` to every paper of every author in `α`. -/
noncomputable def roundUpPapers (M : ℕ) (papers : α → List ℝ) (b : α) : List ℝ :=
  (papers b).map (roundUp M)

/-- **Cell correspondence**:
For `M > 0`, integer `k`, and `θ ∈ (k / M, (k + 1) / M]`, a strictly rounded percentile `R_M(y)`
is at least `θ` if and only if the original percentile `y` is at least `k / M`. -/
theorem roundUp_ge_iff_ge_grid {M : ℕ} (hM : 0 < M) (k : ℕ) (y θ : ℝ)
    (hθ_gt : (k : ℝ) / (M : ℝ) < θ) (hθ_le : θ ≤ ((k : ℝ) + 1) / (M : ℝ)) :
    θ ≤ roundUp M y ↔ (k : ℝ) / (M : ℝ) ≤ y := by
  have hMpos : (0 : ℝ) < (M : ℝ) := Nat.cast_pos.mpr hM
  unfold roundUp
  rw [le_div_iff₀ hMpos, div_le_iff₀ hMpos]
  have hk_lt : (k : ℝ) < θ * (M : ℝ) := (div_lt_iff₀ hMpos).mp hθ_gt
  have hθ_le' : θ * (M : ℝ) ≤ (k : ℝ) + 1 := (le_div_iff₀ hMpos).mp hθ_le
  constructor
  · intro h
    have hlt_real : (((k : ℤ) : ℝ)) < (((⌊(M : ℝ) * y⌋ + 1 : ℤ) : ℝ)) := by
      push_cast
      linarith
    have hlt_int : (k : ℤ) < ⌊(M : ℝ) * y⌋ + 1 := Int.cast_lt.mp hlt_real
    have hle_int : (k : ℤ) ≤ ⌊(M : ℝ) * y⌋ := by omega
    have hle_real : ((k : ℤ) : ℝ) ≤ (M : ℝ) * y := Int.le_floor.mp hle_int
    push_cast at hle_real
    linarith
  · intro h
    have hle_real : ((k : ℤ) : ℝ) ≤ (M : ℝ) * y := by
      push_cast
      linarith
    have hle_int : (k : ℤ) ≤ ⌊(M : ℝ) * y⌋ := Int.le_floor.mpr hle_real
    have hcast : (k : ℝ) ≤ (⌊(M : ℝ) * y⌋ : ℝ) := by exact_mod_cast hle_int
    linarith

/-- On the cell `θ ∈ (k / M, (k + 1) / M]`, `countAboveList` on rounded percentiles equals
`countAboveList` on the original percentiles at the left endpoint `k / M`. -/
theorem countAboveList_map_roundUp_eq {M : ℕ} (hM : 0 < M) (k : ℕ) (ps : List ℝ)
    {θ : ℝ} (hθ : θ ∈ Set.Ioc ((k : ℝ) / (M : ℝ)) (((k : ℝ) + 1) / (M : ℝ))) :
    countAboveList (ps.map (roundUp M)) θ = countAboveList ps ((k : ℝ) / (M : ℝ)) := by
  induction ps with
  | nil => simp [countAboveList]
  | cons y ys ih =>
    simp only [List.map_cons, countAboveList_cons, ih]
    have hiff := roundUp_ge_iff_ge_grid hM k y θ hθ.1 hθ.2
    by_cases hy : (k : ℝ) / (M : ℝ) ≤ y
    · have hhy : θ ≤ roundUp M y := hiff.mpr hy
      simp [hy, hhy]
    · have hhy : ¬ θ ≤ roundUp M y := fun h => hy (hiff.mp h)
      simp [hy, hhy]

/-- Every author's qualifying paper count after rounding is constant on `(k / M, (k + 1) / M]`
and equal to their unrounded count at `k / M`. -/
theorem countAboveAt_roundUp_eq_on_cell {M : ℕ} (hM : 0 < M) (papers : α → List ℝ)
    (b : α) (k : ℕ) {θ : ℝ}
    (hθ : θ ∈ Set.Ioc ((k : ℝ) / (M : ℝ)) (((k : ℝ) + 1) / (M : ℝ))) :
    countAboveAt (roundUpPapers M papers) b θ = countAboveAt papers b ((k : ℝ) / (M : ℝ)) :=
  countAboveList_map_roundUp_eq hM k (papers b) hθ

/-- Every author's standing after cohort-wide upward rounding is constant on `(k / M, (k + 1) / M]`
and equal to their unrounded standing at `k / M`. -/
theorem standingAt_roundUp_eq_on_cell {M : ℕ} (hM : 0 < M) (papers : α → List ℝ)
    (a : α) (k : ℕ) {θ : ℝ}
    (hθ : θ ∈ Set.Ioc ((k : ℝ) / (M : ℝ)) (((k : ℝ) + 1) / (M : ℝ))) :
    standingAt (roundUpPapers M papers) a θ = standingAt papers a ((k : ℝ) / (M : ℝ)) := by
  unfold standingAt
  simp_rw [countAboveAt_roundUp_eq_on_cell hM papers _ k hθ]

/-- The rounded standing curve is interval-integrable on every grid cell `[k / M, (k + 1) / M]`. -/
theorem intervalIntegrable_standingAt_roundUp_cell {M : ℕ} (hM : 0 < M)
    (papers : α → List ℝ) (a : α) (k : ℕ) :
    IntervalIntegrable (standingAt (roundUpPapers M papers) a) volume
      ((k : ℝ) / (M : ℝ)) (((k : ℝ) + 1) / (M : ℝ)) := by
  have hMpos : (0 : ℝ) < (M : ℝ) := Nat.cast_pos.mpr hM
  have hle : (k : ℝ) / (M : ℝ) ≤ ((k : ℝ) + 1) / (M : ℝ) := by
    rw [div_le_div_iff_of_pos_right hMpos]
    linarith
  have hconst : IntervalIntegrable (fun _ : ℝ => standingAt papers a ((k : ℝ) / (M : ℝ))) volume
      ((k : ℝ) / (M : ℝ)) (((k : ℝ) + 1) / (M : ℝ)) := intervalIntegrable_const
  rw [intervalIntegrable_iff_integrableOn_Ioc_of_le hle] at hconst ⊢
  refine hconst.congr_fun (fun θ hθ => ?_) measurableSet_Ioc
  exact (standingAt_roundUp_eq_on_cell hM papers a k hθ).symm

/-- **Cell integral identity**:
The integral of the rounded standing curve over `[k / M, (k + 1) / M]` is the area of the
rectangle of width `1 / M` and height `g_a(k / M)`. -/
theorem integral_standingAt_roundUp_cell {M : ℕ} (hM : 0 < M)
    (papers : α → List ℝ) (a : α) (k : ℕ) :
    (∫ θ in ((k : ℝ) / (M : ℝ))..(((k : ℝ) + 1) / (M : ℝ)),
      standingAt (roundUpPapers M papers) a θ) =
      standingAt papers a ((k : ℝ) / (M : ℝ)) / (M : ℝ) := by
  have hMpos : (0 : ℝ) < (M : ℝ) := Nat.cast_pos.mpr hM
  have hle : (k : ℝ) / (M : ℝ) ≤ ((k : ℝ) + 1) / (M : ℝ) := by
    rw [div_le_div_iff_of_pos_right hMpos]
    linarith
  rw [intervalIntegral.integral_of_le hle]
  rw [MeasureTheory.setIntegral_congr_fun measurableSet_Ioc
    (fun θ hθ => standingAt_roundUp_eq_on_cell hM papers a k hθ)]
  rw [← intervalIntegral.integral_of_le hle, intervalIntegral.integral_const, smul_eq_mul]
  have hlen : ((k : ℝ) + 1) / (M : ℝ) - (k : ℝ) / (M : ℝ) = 1 / (M : ℝ) := by ring
  rw [hlen]
  ring

/-- **Grid-to-integral equivalence theorem**:
For a grid of `M > 0` classes and cutoff `τ = q / M` with `q < M`, the finite-grid average
`(1 / (M - q)) ∑_{k ∈ Ico q M} g_a(k / M)` equals the continuous PiP integral
`(1 / (1 - τ)) ∫_τ^1 g_a^{R_M}(θ) dθ` computed after applying `R_M` to every author's paper percentiles. -/
theorem grid_eq_integral_roundUp {M q : ℕ} (hM : 0 < M) (hq : q < M)
    (papers : α → List ℝ) (a : α) :
    (∑ k ∈ Finset.Ico q M, standingAt papers a ((k : ℝ) / (M : ℝ))) / ((M : ℝ) - (q : ℝ)) =
      pip (roundUpPapers M papers) ((q : ℝ) / (M : ℝ)) a := by
  have hMpos : (0 : ℝ) < (M : ℝ) := Nat.cast_pos.mpr hM
  have hMne : (M : ℝ) ≠ 0 := ne_of_gt hMpos
  have hqM : (q : ℝ) < (M : ℝ) := Nat.cast_lt.mpr hq
  unfold pip
  have h1 : (((M : ℕ) : ℝ) / (M : ℝ)) = 1 := div_self hMne
  rw [← h1]
  have hadj := intervalIntegral.sum_integral_adjacent_intervals_Ico (a := fun k : ℕ => (k : ℝ) / (M : ℝ))
    (m := q) (n := M) (le_of_lt hq) (fun k _ => by
      simpa [Nat.cast_add, Nat.cast_one] using
        intervalIntegrable_standingAt_roundUp_cell hM papers a k)
  rw [← hadj]
  have hcells :
      (∑ k ∈ Finset.Ico q M,
        ∫ θ in ((k : ℝ) / (M : ℝ))..(((k + 1 : ℕ) : ℝ) / (M : ℝ)),
          standingAt (roundUpPapers M papers) a θ) =
        (∑ k ∈ Finset.Ico q M, standingAt papers a ((k : ℝ) / (M : ℝ))) / (M : ℝ) := by
    rw [Finset.sum_div]
    refine Finset.sum_congr rfl (fun k _ => ?_)
    simpa [Nat.cast_add, Nat.cast_one] using integral_standingAt_roundUp_cell hM papers a k
  rw [hcells]
  have hden : 1 - (q : ℝ) / (M : ℝ) = ((M : ℝ) - (q : ℝ)) / (M : ℝ) := by
    rw [one_sub_div hMne]
  rw [h1, hden, div_div_div_cancel_right₀ hMne]

/-- **Cutoff boundary effect**:
A paper lying exactly on the grid cutoff `y = q / M` rounds up to `(q + 1) / M`, so it qualifies
at every threshold in the first cell `(q / M, (q + 1) / M]`, whereas the unrounded percentile
`y = q / M` qualifies at no threshold strictly above `q / M`. -/
theorem roundUp_at_cutoff_qualifies {M : ℕ} (hM : 0 < M) (q : ℕ) {θ : ℝ}
    (hθ : θ ∈ Set.Ioc ((q : ℝ) / (M : ℝ)) (((q : ℝ) + 1) / (M : ℝ))) :
    θ ≤ roundUp M ((q : ℝ) / (M : ℝ)) ∧ ¬ (θ ≤ (q : ℝ) / (M : ℝ)) := by
  refine ⟨?_, not_le.mpr hθ.1⟩
  exact (roundUp_ge_iff_ge_grid hM q ((q : ℝ) / (M : ℝ)) θ hθ.1 hθ.2).mpr (le_refl _)

end PiP
