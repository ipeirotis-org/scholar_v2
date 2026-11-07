module

public import Mathlib.Algebra.BigOperators.Group.Finset.Basic
public import Mathlib.Algebra.Order.BigOperators.Group.Finset
public import Mathlib.Algebra.Order.Field.Basic
public import Mathlib.Data.Fintype.Card
public import Mathlib.Basic.Real.Basic
public import Mathlib.Tactic

@[expose] public section

set_option linter.unusedSectionVars false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# Finite PiP Score: Bounds, Conditional Monotonicity, Sawtooth Lemma, and Citation Counterexample

This module formalizes the core definitions and finite-sample properties of the PiP
(percentile-in-percentile) indicator from Section 4 of the paper:

1. **Strict peer standing (`standing`) and finite-grid score (`gridScore`)**:
   The focal author `a` belongs to the peer cohort `α`, and comparisons are strict (`n b k < n a k`).
2. **Range and zero-score conditions**:
   `0 ≤ S(a) ≤ (N - 1) / N < 1`, because `a` is in the peer group and never strictly beats themselves;
   `S(a) = 0` when all qualifying counts above the cutoff are zero.
3. **Conditional monotonicity (`standing_mono_focal`, `gridScore_mono_focal`)**:
   Updating the focal author's record inside the peer group (without assuming the standing function
   `G_θ` is externally fixed) weakly increases the focal author's standing at every threshold and
   therefore weakly increases `S(a)`.
4. **Paper-level consequences**:
   Adding a paper (`gridScore_add_paper_le`), raising a paper's percentile (`gridScore_raise_paper_le`),
   and invariance to papers strictly below the cutoff `τ` (`gridScore_add_paper_below_cutoff_eq`).
5. **Sawtooth constant-count lemma (`standing_mono_threshold_on_const_count`)**:
   On any threshold interval where the focal author's qualifying-paper count remains constant,
   their standing cannot decrease as the threshold rises.
6. **Citation-monotonicity counterexample (`citation_monotonicity_counterexample`)**:
   Increasing one paper's raw citation count and recomputing publication-year percentiles (Eq. 1)
   can lower another of the focal author's paper percentiles through a tie and strictly decrease PiP.
-/

namespace PiP

open Finset BigOperators

section CountLevel

variable {α κ : Type*} [Fintype α] [DecidableEq α] [Fintype κ] [DecidableEq κ]

/-- The set of peers in cohort `α` with strictly fewer qualifying papers than `a` at threshold `k`. -/
def strictlyBelow (n : α → κ → ℕ) (a : α) (k : κ) : Finset α :=
  Finset.univ.filter (fun b => n b k < n a k)

/-- Author `a`'s standing at threshold `k`: the share of peers in `α` (with `a` included)
who have strictly fewer qualifying papers than `a` at `k`. -/
noncomputable def standing (n : α → κ → ℕ) (a : α) (k : κ) : ℝ :=
  ((strictlyBelow n a k).card : ℝ) / (Fintype.card α : ℝ)

/-- The finite-grid PiP score over threshold index set `κ`:
`S(a) = (1 / K) ∑_k standing(n, a, k) = (1 / (N K)) ∑_k ∑_b 1{n_{b,k} < n_{a,k}}`. -/
noncomputable def gridScore (n : α → κ → ℕ) (a : α) : ℝ :=
  (∑ k : κ, standing n a k) / (Fintype.card κ : ℝ)

/-- Expressing `gridScore` in the double-sum form `(1 / (N K)) ∑_k ∑_b 1{n_{b,k} < n_{a,k}}`. -/
theorem gridScore_eq_double_sum (n : α → κ → ℕ) (a : α) :
    gridScore n a =
      (∑ k : κ, ∑ b : α, (if n b k < n a k then (1 : ℝ) else 0)) /
        ((Fintype.card α : ℝ) * (Fintype.card κ : ℝ)) := by
  unfold gridScore standing strictlyBelow
  have hsum : ∀ k : κ,
      ((Finset.univ.filter (fun b => n b k < n a k)).card : ℝ) =
        ∑ b : α, (if n b k < n a k then (1 : ℝ) else 0) := by
    intro k
    rw [Finset.card_filter, Nat.cast_sum]
    refine Finset.sum_congr rfl (fun b _ => ?_)
    split_ifs <;> simp
  simp_rw [hsum, ← Finset.sum_div, div_div]

/-- An author never strictly beats themselves at any threshold. -/
theorem self_not_mem_strictlyBelow (n : α → κ → ℕ) (a : α) (k : κ) :
    a ∉ strictlyBelow n a k := by
  simp [strictlyBelow]

/-- At most `N - 1` peers can have strictly fewer qualifying papers than `a`. -/
theorem card_strictlyBelow_le_pred (n : α → κ → ℕ) (a : α) (k : κ) :
    (strictlyBelow n a k).card ≤ Fintype.card α - 1 := by
  have hsub : strictlyBelow n a k ⊆ Finset.univ.erase a := by
    intro b hb
    rw [Finset.mem_erase]
    refine ⟨?_, Finset.mem_univ b⟩
    rintro rfl
    exact self_not_mem_strictlyBelow n b k hb
  have hcard := Finset.card_le_card hsub
  simpa [Finset.card_erase_of_mem (Finset.mem_univ a)] using hcard

/-- Standing at any threshold is non-negative. -/
theorem standing_nonneg (n : α → κ → ℕ) (a : α) (k : κ) :
    0 ≤ standing n a k := by
  unfold standing
  positivity

/-- Standing is bounded above by `(N - 1) / N`. -/
theorem standing_le_pred_div_card [Nonempty α] (n : α → κ → ℕ) (a : α) (k : κ) :
    standing n a k ≤ ((Fintype.card α : ℝ) - 1) / (Fintype.card α : ℝ) := by
  unfold standing
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  rw [div_le_div_iff_of_pos_right hN]
  have hle := card_strictlyBelow_le_pred n a k
  have h1 : 1 ≤ Fintype.card α := Fintype.card_pos
  have hcast : ((strictlyBelow n a k).card : ℝ) ≤ ((Fintype.card α - 1 : ℕ) : ℝ) :=
    Nat.cast_le.mpr hle
  rwa [Nat.cast_sub h1, Nat.cast_one] at hcast

/-- Standing is strictly less than `1`. -/
theorem standing_lt_one [Nonempty α] (n : α → κ → ℕ) (a : α) (k : κ) :
    standing n a k < 1 := by
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  calc standing n a k
      ≤ ((Fintype.card α : ℝ) - 1) / (Fintype.card α : ℝ) := standing_le_pred_div_card n a k
    _ < (Fintype.card α : ℝ) / (Fintype.card α : ℝ) := by
        rw [div_lt_div_iff_of_pos_right hN]
        linarith
    _ = 1 := div_self (ne_of_gt hN)

/-- The finite-grid PiP score is non-negative. -/
theorem gridScore_nonneg (n : α → κ → ℕ) (a : α) :
    0 ≤ gridScore n a := by
  unfold gridScore
  refine div_nonneg (Finset.sum_nonneg (fun k _ => standing_nonneg n a k)) (Nat.cast_nonneg _)

/-- The finite-grid PiP score is at most `(N - 1) / N`. -/
theorem gridScore_le_pred_div_card [Nonempty α] [Nonempty κ] (n : α → κ → ℕ) (a : α) :
    gridScore n a ≤ ((Fintype.card α : ℝ) - 1) / (Fintype.card α : ℝ) := by
  unfold gridScore
  have hK : (0 : ℝ) < Fintype.card κ := Nat.cast_pos.mpr Fintype.card_pos
  rw [div_le_iff₀ hK]
  calc ∑ k : κ, standing n a k
      ≤ ∑ _k : κ, (((Fintype.card α : ℝ) - 1) / (Fintype.card α : ℝ)) :=
        Finset.sum_le_sum (fun k _ => standing_le_pred_div_card n a k)
    _ = ((Fintype.card α : ℝ) - 1) / (Fintype.card α : ℝ) * (Fintype.card κ : ℝ) := by
        simp [mul_comm]

/-- The finite-grid PiP score is strictly less than `1`. -/
theorem gridScore_lt_one [Nonempty α] [Nonempty κ] (n : α → κ → ℕ) (a : α) :
    gridScore n a < 1 := by
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  calc gridScore n a
      ≤ ((Fintype.card α : ℝ) - 1) / (Fintype.card α : ℝ) := gridScore_le_pred_div_card n a
    _ < (Fintype.card α : ℝ) / (Fintype.card α : ℝ) := by
        rw [div_lt_div_iff_of_pos_right hN]
        linarith
    _ = 1 := div_self (ne_of_gt hN)

/-- If author `a` has zero qualifying papers at threshold `k`, their standing at `k` is zero. -/
theorem standing_eq_zero_of_count_eq_zero (n : α → κ → ℕ) (a : α) (k : κ) (h0 : n a k = 0) :
    standing n a k = 0 := by
  unfold standing strictlyBelow
  have hemp : (Finset.univ.filter (fun b => n b k < n a k)) = ∅ := by
    rw [Finset.filter_eq_empty_iff]
    intro b _
    omega
  simp [hemp]

/-- If author `a` has zero qualifying papers at every sampled threshold, their grid score is zero. -/
theorem gridScore_eq_zero_of_counts_eq_zero (n : α → κ → ℕ) (a : α) (h0 : ∀ k : κ, n a k = 0) :
    gridScore n a = 0 := by
  unfold gridScore
  simp [standing_eq_zero_of_count_eq_zero n a _ (h0 _)]

/-- **Conditional monotonicity of standing (with peer-group update)**:
If every other author's count at threshold `k` is unchanged (`∀ b ≠ a, n' b k = n b k`) and
the focal author's count weakly increases (`n a k ≤ n' a k`), then the focal author's standing
at `k` (recomputed over the updated cohort `n'`, which includes `a`) cannot decrease. -/
theorem standing_mono_focal (n n' : α → κ → ℕ) (a : α) (k : κ)
    (hpeers : ∀ b : α, b ≠ a → n' b k = n b k)
    (hfocal : n a k ≤ n' a k) :
    standing n a k ≤ standing n' a k := by
  unfold standing
  refine div_le_div_of_nonneg_right ?_ (Nat.cast_nonneg _)
  exact Nat.cast_le.mpr (Finset.card_le_card (fun b hb => by
    simp only [strictlyBelow, Finset.mem_filter, Finset.mem_univ, true_and] at hb ⊢
    have hba : b ≠ a := by
      rintro rfl
      exact Nat.lt_irrefl _ hb
    rw [hpeers b hba]
    exact lt_of_lt_of_le hb hfocal))

/-- **Conditional monotonicity of the finite-grid PiP score (with peer-group update)**:
If every other author's counts remain unchanged across all thresholds and the focal author's
counts weakly increase at every threshold, the focal author's PiP score cannot decrease. -/
theorem gridScore_mono_focal (n n' : α → κ → ℕ) (a : α)
    (hpeers : ∀ b : α, b ≠ a → ∀ k : κ, n' b k = n b k)
    (hfocal : ∀ k : κ, n a k ≤ n' a k) :
    gridScore n a ≤ gridScore n' a := by
  unfold gridScore
  refine div_le_div_of_nonneg_right ?_ (Nat.cast_nonneg _)
  exact Finset.sum_le_sum (fun k _ => standing_mono_focal n n' a k (fun b hb => hpeers b hb k) (hfocal k))

end CountLevel

/-! ### Paper-Level Definitions and Consequences -/

section PaperLevel

variable {α κ : Type*} [Fintype α] [DecidableEq α] [Fintype κ] [DecidableEq κ]

/-- The number of papers in a list `ps` of percentiles that are at or above threshold `θ`. -/
noncomputable def countAboveList (ps : List ℝ) (θ : ℝ) : ℕ :=
  (ps.filter (fun y => decide (θ ≤ y))).length

/-- Cohort count function generated by paper percentile lists `papers : α → List ℝ` and
sampled thresholds `θ : κ → ℝ`. -/
noncomputable def paperCounts (papers : α → List ℝ) (θ : κ → ℝ) (b : α) (k : κ) : ℕ :=
  countAboveList (papers b) (θ k)

/-- Count of papers at or above a continuous threshold `t ∈ ℝ` for author `b`. -/
noncomputable def countAboveAt (papers : α → List ℝ) (b : α) (t : ℝ) : ℕ :=
  countAboveList (papers b) t

/-- Standing of author `a` at continuous threshold `t ∈ ℝ` for paper profile `papers`. -/
noncomputable def standingAt (papers : α → List ℝ) (a : α) (t : ℝ) : ℝ :=
  ((Finset.univ.filter (fun b => countAboveAt papers b t < countAboveAt papers a t)).card : ℝ) /
    (Fintype.card α : ℝ)

theorem countAboveList_cons (y : ℝ) (ps : List ℝ) (θ : ℝ) :
    countAboveList (y :: ps) θ = (if θ ≤ y then 1 else 0) + countAboveList ps θ := by
  unfold countAboveList
  by_cases h : θ ≤ y <;> simp [List.filter_cons, h, Nat.add_comm]

theorem countAboveList_le_cons (y : ℝ) (ps : List ℝ) (θ : ℝ) :
    countAboveList ps θ ≤ countAboveList (y :: ps) θ := by
  rw [countAboveList_cons]
  omega

theorem countAboveList_cons_of_lt {y θ : ℝ} (h : y < θ) (ps : List ℝ) :
    countAboveList (y :: ps) θ = countAboveList ps θ := by
  rw [countAboveList_cons, if_neg (not_le.mpr h), zero_add]

theorem countAboveList_mono_paper {y₀ y₁ : ℝ} (hy : y₀ ≤ y₁) (ps : List ℝ) (θ : ℝ) :
    countAboveList (y₀ :: ps) θ ≤ countAboveList (y₁ :: ps) θ := by
  rw [countAboveList_cons, countAboveList_cons]
  by_cases h0 : θ ≤ y₀
  · have h1 : θ ≤ y₁ := le_trans h0 hy
    simp [h0, h1]
  · by_cases h1 : θ ≤ y₁ <;> simp [h0, h1]

theorem countAboveList_antitone (ps : List ℝ) {θ₁ θ₂ : ℝ} (hθ : θ₁ ≤ θ₂) :
    countAboveList ps θ₂ ≤ countAboveList ps θ₁ := by
  induction ps with
  | nil => simp [countAboveList]
  | cons y ys ih =>
    rw [countAboveList_cons, countAboveList_cons]
    have hy : (if θ₂ ≤ y then 1 else 0) ≤ (if θ₁ ≤ y then 1 else 0) := by
      by_cases h2 : θ₂ ≤ y
      · have h1 : θ₁ ≤ y := le_trans hθ h2
        simp [h1, h2]
      · simp [h2]
    omega

/-- **Adding a paper with a fixed assigned percentile cannot lower the focal author's score.**
The focal author `a`'s record in `papers` is updated to `y₀ :: papers a`, recomputing peer standings. -/
theorem gridScore_add_paper_le (papers : α → List ℝ) (θ : κ → ℝ) (a : α) (y₀ : ℝ) :
    gridScore (paperCounts papers θ) a ≤
      gridScore (paperCounts (Function.update papers a (y₀ :: papers a)) θ) a := by
  refine gridScore_mono_focal _ _ a (fun b hb k => ?_) (fun k => ?_)
  · simp [paperCounts, Function.update_of_ne hb]
  · simp [paperCounts, countAboveList_le_cons]

/-- **Raising an assigned paper percentile (with other inputs fixed) cannot lower the score.** -/
theorem gridScore_raise_paper_le (papers : α → List ℝ) (θ : κ → ℝ) (a : α)
    {y₀ y₁ : ℝ} (hy : y₀ ≤ y₁) :
    gridScore (paperCounts (Function.update papers a (y₀ :: papers a)) θ) a ≤
      gridScore (paperCounts (Function.update papers a (y₁ :: papers a)) θ) a := by
  refine gridScore_mono_focal _ _ a (fun b hb k => ?_) (fun k => ?_)
  · simp [paperCounts, Function.update_of_ne hb]
  · simp [paperCounts, countAboveList_mono_paper hy]

/-- **Adding a paper strictly below the cutoff leaves the score unchanged.** -/
theorem gridScore_add_paper_below_cutoff_eq (papers : α → List ℝ) (θ : κ → ℝ) (τ : ℝ)
    (hθ : ∀ k : κ, τ ≤ θ k) (a : α) {y₀ : ℝ} (hy : y₀ < τ) :
    gridScore (paperCounts (Function.update papers a (y₀ :: papers a)) θ) a =
      gridScore (paperCounts papers θ) a := by
  have hcounts : paperCounts (Function.update papers a (y₀ :: papers a)) θ = paperCounts papers θ := by
    ext b k
    by_cases hba : b = a
    · subst hba
      simp only [paperCounts, Function.update_self]
      exact countAboveList_cons_of_lt (lt_of_lt_of_le hy (hθ k)) (papers b)
    · simp [paperCounts, Function.update_of_ne hba]
  rw [hcounts]

/-- **Zero contribution below the cutoff (no reward for volume alone)**:
If every paper of author `a` has percentile strictly below `τ` and all sampled thresholds satisfy `τ ≤ θ k`,
then `gridScore (paperCounts papers θ) a = 0`. -/
theorem gridScore_eq_zero_of_all_below_cutoff (papers : α → List ℝ) (θ : κ → ℝ) (τ : ℝ)
    (hθ : ∀ k : κ, τ ≤ θ k) (a : α) (hall : ∀ y ∈ papers a, y < τ) :
    gridScore (paperCounts papers θ) a = 0 := by
  refine gridScore_eq_zero_of_counts_eq_zero _ a (fun k => ?_)
  unfold paperCounts countAboveList
  rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
  intro y hy
  simp only [decide_eq_true_eq, not_le]
  exact lt_of_lt_of_le (hall y hy) (hθ k)

/-- **Sawtooth constant-count lemma**:
On an interval `[θ₁, θ₂]` where the focal author's qualifying-paper count remains constant
(`countAboveAt papers a θ₁ = countAboveAt papers a θ₂`), every peer's count can only fall as the
threshold rises, so the focal author's standing cannot decrease (`standingAt papers a θ₁ ≤ standingAt papers a θ₂`). -/
theorem standing_mono_threshold_on_const_count (papers : α → List ℝ) (a : α)
    {θ₁ θ₂ : ℝ} (hθ : θ₁ ≤ θ₂) (hconst : countAboveAt papers a θ₁ = countAboveAt papers a θ₂) :
    standingAt papers a θ₁ ≤ standingAt papers a θ₂ := by
  unfold standingAt
  refine div_le_div_of_nonneg_right ?_ (Nat.cast_nonneg _)
  exact Nat.cast_le.mpr (Finset.card_le_card (fun b hb => by
    simp only [Finset.mem_filter, Finset.mem_univ, true_and] at hb ⊢
    rw [← hconst]
    exact lt_of_le_of_lt (countAboveList_antitone (papers b) hθ) hb))

end PaperLevel

/-! ### Citation-Monotonicity Counterexample -/

section CitationCounterexample

/-- Equation 1 of the paper: the strict publication-year percentile of paper `j` in a same-year
reference set with citation counts `c : Fin M → ℕ`. -/
abbrev paperPercentile {M : ℕ} (c : Fin M → ℕ) (j : Fin M) : ℚ :=
  (((List.finRange M).filter (fun i : Fin M => decide (c i < c j))).length : ℚ) / (M : ℚ)

/-- Exact rational standing of author `a` at rational threshold `θ` when each author `b` holds
a list of paper indices `authorPapers b` in a publication-year reference set `c : Fin M → ℕ`. -/
abbrev citationStanding {N M : ℕ} (c : Fin M → ℕ) (authorPapers : Fin N → List (Fin M))
    (a : Fin N) (θ : ℚ) : ℚ :=
  let n (b : Fin N) : ℕ := ((authorPapers b).filter (fun j => decide (θ ≤ paperPercentile c j))).length
  (((List.finRange N).filter (fun b : Fin N => decide (n b < n a))).length : ℚ) / (N : ℚ)

/-- Concrete publication-year citation profile before and after focal author `0`'s paper `2`
gains one citation (`1 → 2`), tying focal author `0`'s other paper `3` (which has `2` citations):
- Papers `0` and `1` belong to peer author `1` and have `0` citations each.
- Papers `2` and `3` belong to focal author `0` and have `1` and `2` citations initially.
- Initially, paper `2` has percentile `2/4 = 1/2` and paper `3` has percentile `3/4`.
- After paper `2` gains a citation (`cAfter 2 = 2`), both papers `2` and `3` tie at `2` citations,
  so paper `3`'s percentile drops from `3/4` to `2/4 = 1/2`.
- At threshold `θ = 5/8 ∈ (1/2, 3/4]`, focal author `0`'s qualifying paper count drops from `1` to `0`,
  and their standing drops strictly from `1/2` to `0`. -/
abbrev cBefore : Fin 4 → ℕ
  | 0 => 0
  | 1 => 0
  | 2 => 1
  | 3 => 2

abbrev cAfter : Fin 4 → ℕ
  | 0 => 0
  | 1 => 0
  | 2 => 2
  | 3 => 2

abbrev cohortPapers : Fin 2 → List (Fin 4)
  | 0 => [2, 3]
  | 1 => [0, 1]

/-- **Citation-monotonicity counterexample**:
Increasing a single paper's citation count of the focal author (while holding all other citation
counts and author-paper links fixed) can lower another of the focal author's paper percentiles
when percentiles are recomputed within the publication year, strictly decreasing the author's standing
and score. -/
theorem citation_monotonicity_counterexample :
    (∀ i : Fin 4, cBefore i ≤ cAfter i) ∧
    cBefore 2 < cAfter 2 ∧
    (∀ i : Fin 4, i ≠ 2 → cAfter i = cBefore i) ∧
    paperPercentile cBefore 3 = 3 / 4 ∧
    paperPercentile cAfter 3 = 1 / 2 ∧
    citationStanding cAfter cohortPapers 0 (5 / 8) <
      citationStanding cBefore cohortPapers 0 (5 / 8) := by
  decide +kernel

end CitationCounterexample

end PiP
