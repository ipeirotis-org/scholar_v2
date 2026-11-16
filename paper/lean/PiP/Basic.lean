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

1. **Mid-rank peer standing (`standing`) and finite-grid score (`gridScore`)**:
   The focal author `a` belongs to the peer cohort `α`. A peer with strictly fewer qualifying
   papers (`n b k < n a k`) counts one, a peer with the same number (`n b k = n a k`) counts one
   half, and the tie set always contains `a`:
   `g_a = (#{b : n_b < n_a} + (1/2) #{b : n_b = n_a}) / N`.
   Equivalently `g_a = (L(n_a) + L(n_a + 1)) / 2`, where `L(m)` is the share of peers with fewer
   than `m` qualifying papers (`standing_eq_half_belowShare_add`).
2. **Range (`standing_ge_inv_two_card`, `standing_le_one_sub_inv_two_card` and the `gridScore`
   versions)**:
   `1 / (2 N) ≤ S(a) ≤ 1 - 1 / (2 N)`, hence `0 < S(a) < 1`, because `a` ties with themselves
   and with nobody more than the whole cohort.
3. **Below the cutoff (`standing_of_count_eq_zero`, `gridScore_eq_of_all_below_cutoff`,
   `gridScore_le_of_all_below_cutoff`)**:
   An author with no qualifying paper at a threshold has standing equal to half the share of
   peers with no qualifying paper there, which is the lowest standing in the cohort. The standing
   is monotone in the focal count across authors (`standing_mono_count`).
4. **Conditional monotonicity (`standing_mono_focal`, `gridScore_mono_focal`)**:
   Updating the focal author's record inside the peer group (without assuming the standing function
   `G_θ` is externally fixed) weakly increases the focal author's standing at every threshold and
   therefore weakly increases `S(a)`.
5. **Paper-level consequences**:
   Adding a paper (`gridScore_add_paper_le`), raising a paper's percentile (`gridScore_raise_paper_le`),
   and invariance to papers strictly below the cutoff `τ` (`gridScore_add_paper_below_cutoff_eq`).
6. **Sawtooth constant-count lemma (`standing_mono_threshold_on_const_count`)**:
   On any threshold interval where the focal author's qualifying-paper count remains constant,
   their standing cannot decrease as the threshold rises.
7. **Citation-monotonicity counterexample (`citation_monotonicity_counterexample`)**:
   Increasing one paper's raw citation count and recomputing publication-year percentiles (Eq. 1)
   can lower another of the focal author's paper percentiles through a tie and strictly decrease PiP.
-/

namespace PiP

open Finset BigOperators

/-! ### Mid-Rank Weights -/

section MidWeight

/-- The mid-rank weight of a peer with count `x` against a focal count `m`:
`1` if `x < m`, `1/2` if `x = m`, and `0` if `x > m`. -/
noncomputable def midWeight (x m : ℕ) : ℝ :=
  (if x < m then 1 else 0) + (if x = m then 1 else 0) / 2

theorem midWeight_of_lt {x m : ℕ} (h : x < m) : midWeight x m = 1 := by
  simp [midWeight, h, Nat.ne_of_lt h]

theorem midWeight_self (m : ℕ) : midWeight m m = 1 / 2 := by
  simp [midWeight]

theorem midWeight_of_gt {x m : ℕ} (h : m < x) : midWeight x m = 0 := by
  simp [midWeight, lt_asymm h, Nat.ne_of_gt h]

theorem midWeight_nonneg (x m : ℕ) : 0 ≤ midWeight x m := by
  rcases lt_trichotomy x m with h | rfl | h
  · rw [midWeight_of_lt h]; norm_num
  · rw [midWeight_self]; norm_num
  · rw [midWeight_of_gt h]

theorem midWeight_le_one (x m : ℕ) : midWeight x m ≤ 1 := by
  rcases lt_trichotomy x m with h | rfl | h
  · rw [midWeight_of_lt h]
  · rw [midWeight_self]; norm_num
  · rw [midWeight_of_gt h]; norm_num

/-- The mid-rank weight is non-decreasing in the focal count. -/
theorem midWeight_mono_right (x : ℕ) {m m' : ℕ} (h : m ≤ m') :
    midWeight x m ≤ midWeight x m' := by
  rcases lt_trichotomy x m with h1 | rfl | h1
  · rw [midWeight_of_lt h1, midWeight_of_lt (lt_of_lt_of_le h1 h)]
  · rcases eq_or_lt_of_le h with rfl | h2
    · exact le_refl _
    · rw [midWeight_self, midWeight_of_lt h2]; norm_num
  · rw [midWeight_of_gt h1]; exact midWeight_nonneg _ _

/-- The mid-rank weight is non-increasing in the peer's count. -/
theorem midWeight_anti_left {x x' : ℕ} (h : x' ≤ x) (m : ℕ) :
    midWeight x m ≤ midWeight x' m := by
  rcases lt_trichotomy x m with h1 | rfl | h1
  · rw [midWeight_of_lt h1, midWeight_of_lt (lt_of_le_of_lt h h1)]
  · rcases eq_or_lt_of_le h with rfl | h2
    · exact le_refl _
    · rw [midWeight_self, midWeight_of_lt h2]; norm_num
  · rw [midWeight_of_gt h1]; exact midWeight_nonneg _ _

/-- Each ordered pair and its mirror image share exactly one unit of weight: a strict win
pairs with a strict loss, and a tie gives one half to each side. -/
theorem midWeight_add_swap (x y : ℕ) : midWeight x y + midWeight y x = 1 := by
  rcases lt_trichotomy x y with h | rfl | h
  · rw [midWeight_of_lt h, midWeight_of_gt h]; norm_num
  · rw [midWeight_self]; norm_num
  · rw [midWeight_of_gt h, midWeight_of_lt h]; norm_num

/-- The mid-rank weight is the average of the two indicators `x < m` and `x < m + 1`. -/
theorem midWeight_eq_half_lt_add_lt_succ (x m : ℕ) :
    midWeight x m =
      ((if x < m then (1 : ℝ) else 0) + (if x < m + 1 then (1 : ℝ) else 0)) / 2 := by
  rcases lt_trichotomy x m with h | rfl | h
  · have h' : x < m + 1 := by omega
    rw [midWeight_of_lt h, if_pos h, if_pos h']; norm_num
  · have h' : x < x + 1 := by omega
    rw [midWeight_self, if_neg (lt_irrefl x), if_pos h']; norm_num
  · have h1 : ¬ x < m := by omega
    have h2 : ¬ x < m + 1 := by omega
    rw [midWeight_of_gt h, if_neg h1, if_neg h2]; norm_num

end MidWeight

/-! ### Standing at One Threshold -/

section StandingLevel

variable {α κ : Type*} [Fintype α] [DecidableEq α]

/-- The set of peers in cohort `α` with strictly fewer qualifying papers than `a` at threshold `k`. -/
def strictlyBelow (n : α → κ → ℕ) (a : α) (k : κ) : Finset α :=
  Finset.univ.filter (fun b => n b k < n a k)

/-- The set of peers in cohort `α` with exactly as many qualifying papers as `a` at threshold `k`.
It always contains `a`. -/
def tiedWith (n : α → κ → ℕ) (a : α) (k : κ) : Finset α :=
  Finset.univ.filter (fun b => n b k = n a k)

/-- Author `a`'s mid-rank standing at threshold `k`: the share of peers in `α` (with `a` included)
who have strictly fewer qualifying papers than `a` at `k`, plus half the share who have exactly
as many. -/
noncomputable def standing (n : α → κ → ℕ) (a : α) (k : κ) : ℝ :=
  (((strictlyBelow n a k).card : ℝ) + ((tiedWith n a k).card : ℝ) / 2) / (Fintype.card α : ℝ)

/-- The share `L(m) = #{b : n b k < m} / N` of peers with fewer than `m` qualifying papers at
threshold `k`. -/
noncomputable def belowShare (n : α → κ → ℕ) (k : κ) (m : ℕ) : ℝ :=
  ((Finset.univ.filter (fun b : α => n b k < m)).card : ℝ) / (Fintype.card α : ℝ)

/-- The share `p_{k,m} = #{b : n b k = m} / N` of peers in cohort `α` who have
exactly `m` qualifying papers at threshold `k`. -/
noncomputable def countShare (n : α → κ → ℕ) (k : κ) (m : ℕ) : ℝ :=
  ((Finset.univ.filter (fun b : α => n b k = m)).card : ℝ) / (Fintype.card α : ℝ)

/-- Expressing the cardinality of a filtered cohort as a sum of indicator functions. -/
theorem card_filter_univ_eq_sum_ite (p : α → Prop) [DecidablePred p] :
    ((Finset.univ.filter p).card : ℝ) = ∑ b : α, (if p b then (1 : ℝ) else 0) := by
  rw [Finset.card_filter, Nat.cast_sum]
  refine Finset.sum_congr rfl (fun b _ => ?_)
  split_ifs <;> simp

/-- Standing as a sum of mid-rank weights over the cohort:
`g_a = (1 / N) ∑_b (1{n_b < n_a} + (1/2) 1{n_b = n_a})`. -/
theorem standing_eq_sum_midWeight (n : α → κ → ℕ) (a : α) (k : κ) :
    standing n a k = (∑ b : α, midWeight (n b k) (n a k)) / (Fintype.card α : ℝ) := by
  unfold standing strictlyBelow tiedWith midWeight
  rw [card_filter_univ_eq_sum_ite, card_filter_univ_eq_sum_ite, Finset.sum_add_distrib,
    Finset.sum_div]

/-- An author never strictly beats themselves at any threshold. -/
theorem self_not_mem_strictlyBelow (n : α → κ → ℕ) (a : α) (k : κ) :
    a ∉ strictlyBelow n a k := by
  simp [strictlyBelow]

/-- An author always ties with themselves at every threshold. -/
theorem self_mem_tiedWith (n : α → κ → ℕ) (a : α) (k : κ) :
    a ∈ tiedWith n a k := by
  simp [tiedWith]

/-- No peer has fewer than zero qualifying papers. -/
theorem belowShare_zero (n : α → κ → ℕ) (k : κ) : belowShare n k 0 = 0 := by
  unfold belowShare
  have hemp : (Finset.univ.filter (fun b : α => n b k < 0)) = ∅ := by
    rw [Finset.filter_eq_empty_iff]
    intro b _
    omega
  rw [hemp, Finset.card_empty, Nat.cast_zero, zero_div]

/-- **Mid-rank standing as the average of two "fewer than" shares**:
`g_a = (L(n_a) + L(n_a + 1)) / 2`, the average of the share of peers with strictly fewer
qualifying papers than `a` and the share with at most as many. -/
theorem standing_eq_half_belowShare_add (n : α → κ → ℕ) (a : α) (k : κ) :
    standing n a k = (belowShare n k (n a k) + belowShare n k (n a k + 1)) / 2 := by
  rw [standing_eq_sum_midWeight]
  unfold belowShare
  rw [card_filter_univ_eq_sum_ite, card_filter_univ_eq_sum_ite]
  simp_rw [midWeight_eq_half_lt_add_lt_succ]
  rw [← Finset.sum_div, Finset.sum_add_distrib]
  ring

/-- **Lower bound**: every author ties with themselves, so standing is at least `1 / (2 N)`. -/
theorem standing_ge_inv_two_card (n : α → κ → ℕ) (a : α) (k : κ) :
    1 / (2 * (Fintype.card α : ℝ)) ≤ standing n a k := by
  have : Nonempty α := ⟨a⟩
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  rw [standing_eq_sum_midWeight, ← div_div, div_le_div_iff_of_pos_right hN]
  calc (1 : ℝ) / 2 = midWeight (n a k) (n a k) := (midWeight_self _).symm
    _ ≤ ∑ b : α, midWeight (n b k) (n a k) :=
        Finset.single_le_sum (f := fun b : α => midWeight (n b k) (n a k))
          (fun b _ => midWeight_nonneg _ _) (Finset.mem_univ a)

/-- **Upper bound**: the author's own tie counts only one half, so standing is at most
`1 - 1 / (2 N)`. -/
theorem standing_le_one_sub_inv_two_card (n : α → κ → ℕ) (a : α) (k : κ) :
    standing n a k ≤ 1 - 1 / (2 * (Fintype.card α : ℝ)) := by
  have : Nonempty α := ⟨a⟩
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  have hNne : (Fintype.card α : ℝ) ≠ 0 := ne_of_gt hN
  have hcard : (((Finset.univ.erase a).card : ℕ) : ℝ) + 1 = (Fintype.card α : ℝ) := by
    have h := Finset.card_erase_add_one (Finset.mem_univ a)
    rw [Finset.card_univ] at h
    exact_mod_cast h
  have hothers : ∑ b ∈ Finset.univ.erase a, midWeight (n b k) (n a k) ≤
      ((Finset.univ.erase a).card : ℝ) := by
    calc ∑ b ∈ Finset.univ.erase a, midWeight (n b k) (n a k)
        ≤ ∑ _b ∈ Finset.univ.erase a, (1 : ℝ) :=
          Finset.sum_le_sum (fun b _ => midWeight_le_one _ _)
      _ = ((Finset.univ.erase a).card : ℝ) := by
          rw [Finset.sum_const, nsmul_eq_mul, mul_one]
  have hsplit : midWeight (n a k) (n a k) + ∑ b ∈ Finset.univ.erase a, midWeight (n b k) (n a k) =
      ∑ b : α, midWeight (n b k) (n a k) :=
    Finset.add_sum_erase Finset.univ (fun b : α => midWeight (n b k) (n a k)) (Finset.mem_univ a)
  rw [midWeight_self] at hsplit
  have hsum : ∑ b : α, midWeight (n b k) (n a k) ≤ (Fintype.card α : ℝ) - 1 / 2 := by
    linarith
  have hrhs : 1 - 1 / (2 * (Fintype.card α : ℝ)) =
      ((Fintype.card α : ℝ) - 1 / 2) / (Fintype.card α : ℝ) := by
    field_simp
  rw [standing_eq_sum_midWeight, hrhs, div_le_div_iff_of_pos_right hN]
  exact hsum

/-- Standing is strictly positive. -/
theorem standing_pos (n : α → κ → ℕ) (a : α) (k : κ) :
    0 < standing n a k := by
  have : Nonempty α := ⟨a⟩
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  exact lt_of_lt_of_le (by positivity) (standing_ge_inv_two_card n a k)

/-- Standing at any threshold is non-negative. -/
theorem standing_nonneg (n : α → κ → ℕ) (a : α) (k : κ) :
    0 ≤ standing n a k :=
  le_of_lt (standing_pos n a k)

/-- Standing is strictly less than `1`. -/
theorem standing_lt_one (n : α → κ → ℕ) (a : α) (k : κ) :
    standing n a k < 1 := by
  have : Nonempty α := ⟨a⟩
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  have hpos : (0 : ℝ) < 1 / (2 * (Fintype.card α : ℝ)) := by positivity
  linarith [standing_le_one_sub_inv_two_card n a k]

/-- **Standing below the cutoff**: if author `a` has zero qualifying papers at threshold `k`,
their standing at `k` is half the share of peers who also have zero qualifying papers at `k`. -/
theorem standing_of_count_eq_zero (n : α → κ → ℕ) (a : α) (k : κ) (h0 : n a k = 0) :
    standing n a k = countShare n k 0 / 2 := by
  have hemp : (Finset.univ.filter (fun b => n b k < n a k)) = ∅ := by
    rw [Finset.filter_eq_empty_iff]
    intro b _
    omega
  unfold standing strictlyBelow tiedWith countShare
  rw [hemp, h0, Finset.card_empty, Nat.cast_zero, zero_add]
  ring

/-- **Standing is monotone in the focal count across authors**: within one cohort and at one
threshold, an author with weakly fewer qualifying papers has weakly lower standing. -/
theorem standing_mono_count (n : α → κ → ℕ) (a b : α) (k : κ) (h : n a k ≤ n b k) :
    standing n a k ≤ standing n b k := by
  rw [standing_eq_sum_midWeight, standing_eq_sum_midWeight]
  refine div_le_div_of_nonneg_right ?_ (Nat.cast_nonneg _)
  exact Finset.sum_le_sum (fun c _ => midWeight_mono_right _ h)

/-- **An author with zero qualifying papers has the lowest standing in the cohort.** -/
theorem standing_le_of_count_eq_zero (n : α → κ → ℕ) (a : α) (k : κ) (h0 : n a k = 0) (b : α) :
    standing n a k ≤ standing n b k :=
  standing_mono_count n a b k (by omega)

/-- **Conditional monotonicity of standing (with peer-group update)**:
If every other author's count at threshold `k` is unchanged (`∀ b ≠ a, n' b k = n b k`) and
the focal author's count weakly increases (`n a k ≤ n' a k`), then the focal author's standing
at `k` (recomputed over the updated cohort `n'`, which includes `a`) cannot decrease. -/
theorem standing_mono_focal (n n' : α → κ → ℕ) (a : α) (k : κ)
    (hpeers : ∀ b : α, b ≠ a → n' b k = n b k)
    (hfocal : n a k ≤ n' a k) :
    standing n a k ≤ standing n' a k := by
  rw [standing_eq_sum_midWeight, standing_eq_sum_midWeight]
  refine div_le_div_of_nonneg_right ?_ (Nat.cast_nonneg _)
  refine Finset.sum_le_sum (fun b _ => ?_)
  by_cases hba : b = a
  · rw [hba, midWeight_self, midWeight_self]
  · rw [hpeers b hba]
    exact midWeight_mono_right _ hfocal

/-- **Sawtooth lemma at the count level**:
between two thresholds `k₁` and `k₂` at which the focal author's count is the same and no peer's
count is higher at `k₂` than at `k₁`, the focal author's standing cannot decrease. -/
theorem standing_mono_threshold_of_const_count (n : α → κ → ℕ) (a : α) (k₁ k₂ : κ)
    (hconst : n a k₁ = n a k₂) (hpeers : ∀ b : α, n b k₂ ≤ n b k₁) :
    standing n a k₁ ≤ standing n a k₂ := by
  rw [standing_eq_sum_midWeight, standing_eq_sum_midWeight]
  refine div_le_div_of_nonneg_right ?_ (Nat.cast_nonneg _)
  refine Finset.sum_le_sum (fun b _ => ?_)
  rw [hconst]
  exact midWeight_anti_left (hpeers b) _

end StandingLevel

/-! ### Finite-Grid Score -/

section CountLevel

variable {α κ : Type*} [Fintype α] [DecidableEq α] [Fintype κ]

/-- The finite-grid PiP score over threshold index set `κ`:
`S(a) = (1 / K) ∑_k standing(n, a, k)
      = (1 / (N K)) ∑_k ∑_b (1{n_{b,k} < n_{a,k}} + (1/2) 1{n_{b,k} = n_{a,k}})`. -/
noncomputable def gridScore (n : α → κ → ℕ) (a : α) : ℝ :=
  (∑ k : κ, standing n a k) / (Fintype.card κ : ℝ)

/-- Expressing `gridScore` in the double-sum form
`(1 / (N K)) ∑_k ∑_b (1{n_{b,k} < n_{a,k}} + (1/2) 1{n_{b,k} = n_{a,k}})`. -/
theorem gridScore_eq_double_sum (n : α → κ → ℕ) (a : α) :
    gridScore n a =
      (∑ k : κ, ∑ b : α, midWeight (n b k) (n a k)) /
        ((Fintype.card α : ℝ) * (Fintype.card κ : ℝ)) := by
  unfold gridScore
  simp_rw [standing_eq_sum_midWeight, ← Finset.sum_div, div_div]

/-- If every standing is at least `c`, so is the grid score. -/
theorem gridScore_ge_of_forall_standing_ge [Nonempty κ] (n : α → κ → ℕ) (a : α) (c : ℝ)
    (h : ∀ k : κ, c ≤ standing n a k) : c ≤ gridScore n a := by
  unfold gridScore
  have hK : (0 : ℝ) < Fintype.card κ := Nat.cast_pos.mpr Fintype.card_pos
  rw [le_div_iff₀ hK]
  calc c * (Fintype.card κ : ℝ) = ∑ _k : κ, c := by
        rw [Finset.sum_const, Finset.card_univ, nsmul_eq_mul, mul_comm]
    _ ≤ ∑ k : κ, standing n a k := Finset.sum_le_sum (fun k _ => h k)

/-- If every standing is at most `c`, so is the grid score. -/
theorem gridScore_le_of_forall_standing_le [Nonempty κ] (n : α → κ → ℕ) (a : α) (c : ℝ)
    (h : ∀ k : κ, standing n a k ≤ c) : gridScore n a ≤ c := by
  unfold gridScore
  have hK : (0 : ℝ) < Fintype.card κ := Nat.cast_pos.mpr Fintype.card_pos
  rw [div_le_iff₀ hK]
  calc ∑ k : κ, standing n a k ≤ ∑ _k : κ, c := Finset.sum_le_sum (fun k _ => h k)
    _ = c * (Fintype.card κ : ℝ) := by
        rw [Finset.sum_const, Finset.card_univ, nsmul_eq_mul, mul_comm]

/-- **Lower bound**: the finite-grid PiP score is at least `1 / (2 N)`. -/
theorem gridScore_ge_inv_two_card [Nonempty κ] (n : α → κ → ℕ) (a : α) :
    1 / (2 * (Fintype.card α : ℝ)) ≤ gridScore n a :=
  gridScore_ge_of_forall_standing_ge n a _ (fun k => standing_ge_inv_two_card n a k)

/-- **Upper bound**: the finite-grid PiP score is at most `1 - 1 / (2 N)`. -/
theorem gridScore_le_one_sub_inv_two_card [Nonempty κ] (n : α → κ → ℕ) (a : α) :
    gridScore n a ≤ 1 - 1 / (2 * (Fintype.card α : ℝ)) :=
  gridScore_le_of_forall_standing_le n a _ (fun k => standing_le_one_sub_inv_two_card n a k)

/-- The finite-grid PiP score is strictly positive. -/
theorem gridScore_pos [Nonempty κ] (n : α → κ → ℕ) (a : α) :
    0 < gridScore n a := by
  have : Nonempty α := ⟨a⟩
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  exact lt_of_lt_of_le (by positivity) (gridScore_ge_inv_two_card n a)

/-- The finite-grid PiP score is non-negative. -/
theorem gridScore_nonneg (n : α → κ → ℕ) (a : α) :
    0 ≤ gridScore n a := by
  unfold gridScore
  refine div_nonneg (Finset.sum_nonneg (fun k _ => standing_nonneg n a k)) (Nat.cast_nonneg _)

/-- The finite-grid PiP score is strictly less than `1`. -/
theorem gridScore_lt_one [Nonempty κ] (n : α → κ → ℕ) (a : α) :
    gridScore n a < 1 := by
  have : Nonempty α := ⟨a⟩
  have hN : (0 : ℝ) < Fintype.card α := Nat.cast_pos.mpr Fintype.card_pos
  have hpos : (0 : ℝ) < 1 / (2 * (Fintype.card α : ℝ)) := by positivity
  linarith [gridScore_le_one_sub_inv_two_card n a]

/-- **Score below the cutoff**: if author `a` has zero qualifying papers at every sampled
threshold, their grid score is the average over the thresholds of half the share of peers with
zero qualifying papers. -/
theorem gridScore_eq_of_counts_eq_zero (n : α → κ → ℕ) (a : α) (h0 : ∀ k : κ, n a k = 0) :
    gridScore n a = (∑ k : κ, countShare n k 0 / 2) / (Fintype.card κ : ℝ) := by
  unfold gridScore
  congr 1
  exact Finset.sum_congr rfl (fun k _ => standing_of_count_eq_zero n a k (h0 k))

/-- **The grid score is monotone in the counts across authors**: an author with weakly fewer
qualifying papers than `b` at every sampled threshold has a weakly lower grid score than `b`. -/
theorem gridScore_mono_count (n : α → κ → ℕ) (a b : α) (h : ∀ k : κ, n a k ≤ n b k) :
    gridScore n a ≤ gridScore n b := by
  unfold gridScore
  refine div_le_div_of_nonneg_right ?_ (Nat.cast_nonneg _)
  exact Finset.sum_le_sum (fun k _ => standing_mono_count n a b k (h k))

/-- **An author with zero qualifying papers at every sampled threshold has the lowest grid score
in the cohort.** -/
theorem gridScore_le_of_counts_eq_zero (n : α → κ → ℕ) (a : α) (h0 : ∀ k : κ, n a k = 0)
    (b : α) : gridScore n a ≤ gridScore n b :=
  gridScore_mono_count n a b (fun k => by rw [h0 k]; exact Nat.zero_le _)

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

variable {α κ : Type*} [Fintype α] [DecidableEq α] [Fintype κ]

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

/-- Mid-rank standing of author `a` at continuous threshold `t ∈ ℝ` for paper profile `papers`:
the share of peers with strictly fewer articles at or above `t`, plus half the share with exactly
as many. -/
noncomputable def standingAt (papers : α → List ℝ) (a : α) (t : ℝ) : ℝ :=
  (((Finset.univ.filter (fun b => countAboveAt papers b t < countAboveAt papers a t)).card : ℝ) +
    ((Finset.univ.filter (fun b => countAboveAt papers b t = countAboveAt papers a t)).card : ℝ) / 2) /
    (Fintype.card α : ℝ)

/-- The continuous-threshold standing is the count-level standing for the count function
`countAboveAt papers`, with the threshold itself as the index. -/
theorem standingAt_eq_standing (papers : α → List ℝ) (a : α) (t : ℝ) :
    standingAt papers a t = standing (countAboveAt papers) a t :=
  rfl

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

/-- An author whose papers all lie strictly below the cutoff `τ` has zero qualifying papers at
every sampled threshold `θ k ≥ τ`. -/
theorem paperCounts_eq_zero_of_all_below_cutoff (papers : α → List ℝ) (θ : κ → ℝ) (τ : ℝ)
    (hθ : ∀ k : κ, τ ≤ θ k) (a : α) (hall : ∀ y ∈ papers a, y < τ) (k : κ) :
    paperCounts papers θ a k = 0 := by
  unfold paperCounts countAboveList
  rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
  intro y hy
  simp only [decide_eq_true_eq, not_le]
  exact lt_of_lt_of_le (hall y hy) (hθ k)

/-- **Score when all papers lie below the cutoff (no reward for volume alone)**:
If every paper of author `a` has percentile strictly below `τ` and all sampled thresholds satisfy
`τ ≤ θ k`, then the score of `a` is the average over the thresholds of half the share of peers
with no qualifying paper. It depends only on the peers' records, not on how many papers `a` has. -/
theorem gridScore_eq_of_all_below_cutoff (papers : α → List ℝ) (θ : κ → ℝ) (τ : ℝ)
    (hθ : ∀ k : κ, τ ≤ θ k) (a : α) (hall : ∀ y ∈ papers a, y < τ) :
    gridScore (paperCounts papers θ) a =
      (∑ k : κ, countShare (paperCounts papers θ) k 0 / 2) / (Fintype.card κ : ℝ) :=
  gridScore_eq_of_counts_eq_zero _ a (paperCounts_eq_zero_of_all_below_cutoff papers θ τ hθ a hall)

/-- **The lowest score in the cohort**: an author whose papers all lie strictly below the cutoff
`τ` scores no higher than any peer `b`. -/
theorem gridScore_le_of_all_below_cutoff (papers : α → List ℝ) (θ : κ → ℝ) (τ : ℝ)
    (hθ : ∀ k : κ, τ ≤ θ k) (a : α) (hall : ∀ y ∈ papers a, y < τ) (b : α) :
    gridScore (paperCounts papers θ) a ≤ gridScore (paperCounts papers θ) b :=
  gridScore_le_of_counts_eq_zero _ a (paperCounts_eq_zero_of_all_below_cutoff papers θ τ hθ a hall) b

/-- **Sawtooth constant-count lemma**:
On an interval `[θ₁, θ₂]` where the focal author's qualifying-paper count remains constant
(`countAboveAt papers a θ₁ = countAboveAt papers a θ₂`), every peer's count can only fall as the
threshold rises, so the focal author's mid-rank standing cannot decrease
(`standingAt papers a θ₁ ≤ standingAt papers a θ₂`): a peer can only move from ahead to tied or
from tied to behind. -/
theorem standing_mono_threshold_on_const_count (papers : α → List ℝ) (a : α)
    {θ₁ θ₂ : ℝ} (hθ : θ₁ ≤ θ₂) (hconst : countAboveAt papers a θ₁ = countAboveAt papers a θ₂) :
    standingAt papers a θ₁ ≤ standingAt papers a θ₂ := by
  rw [standingAt_eq_standing, standingAt_eq_standing]
  exact standing_mono_threshold_of_const_count (countAboveAt papers) a θ₁ θ₂ hconst
    (fun b => countAboveList_antitone (papers b) hθ)

end PaperLevel

/-! ### Citation-Monotonicity Counterexample -/

section CitationCounterexample

/-- Equation 1 of the paper: the strict publication-year percentile of paper `j` in a same-year
reference set with citation counts `c : Fin M → ℕ`. -/
abbrev paperPercentile {M : ℕ} (c : Fin M → ℕ) (j : Fin M) : ℚ :=
  (((List.finRange M).filter (fun i : Fin M => decide (c i < c j))).length : ℚ) / (M : ℚ)

/-- Exact rational mid-rank standing of author `a` at rational threshold `θ` when each author `b`
holds a list of paper indices `authorPapers b` in a publication-year reference set
`c : Fin M → ℕ`: peers with strictly fewer qualifying papers count one, ties count one half. -/
abbrev citationStanding {N M : ℕ} (c : Fin M → ℕ) (authorPapers : Fin N → List (Fin M))
    (a : Fin N) (θ : ℚ) : ℚ :=
  let n (b : Fin N) : ℕ := ((authorPapers b).filter (fun j => decide (θ ≤ paperPercentile c j))).length
  ((((List.finRange N).filter (fun b : Fin N => decide (n b < n a))).length : ℚ) +
    (((List.finRange N).filter (fun b : Fin N => decide (n b = n a))).length : ℚ) / 2) / (N : ℚ)

/-- Exact rational grid score of author `a`: the average of `citationStanding` over a list of
thresholds. -/
abbrev citationGridScore {N M : ℕ} (c : Fin M → ℕ) (authorPapers : Fin N → List (Fin M))
    (a : Fin N) (θs : List ℚ) : ℚ :=
  ((θs.map (citationStanding c authorPapers a)).sum) / (θs.length : ℚ)

/-- Concrete publication-year citation profile before and after focal author `0`'s paper `2`
gains one citation (`1 → 2`), tying focal author `0`'s other paper `3` (which has `2` citations):
- Papers `0` and `1` belong to peer author `1` and have `0` citations each.
- Papers `2` and `3` belong to focal author `0` and have `1` and `2` citations initially.
- Initially, paper `2` has percentile `2/4 = 1/2` and paper `3` has percentile `3/4`.
- After paper `2` gains a citation (`cAfter 2 = 2`), both papers `2` and `3` tie at `2` citations,
  so paper `3`'s percentile drops from `3/4` to `2/4 = 1/2`.
- At threshold `θ = 5/8 ∈ (1/2, 3/4]`, focal author `0`'s qualifying paper count drops from `1` to `0`
  while the peer's count stays `0`: before, the focal author beats the peer and ties with
  themselves, standing `(1 + 1/2)/2 = 3/4`; after, both authors tie at zero, standing
  `(0 + 2/2)/2 = 1/2`.
- On the four-class grid `θ ∈ {0, 1/4, 1/2, 3/4}` the standings are `1/2, 3/4, 3/4, 3/4` before
  and `1/2, 3/4, 3/4, 1/2` after, so the grid score drops from `11/16` to `5/8`. -/
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
when percentiles are recomputed within the publication year, strictly decreasing the author's
mid-rank standing (from `3/4` to `1/2` at `θ = 5/8`) and grid score (from `11/16` to `5/8` on the
four-class grid). -/
theorem citation_monotonicity_counterexample :
    (∀ i : Fin 4, cBefore i ≤ cAfter i) ∧
    cBefore 2 < cAfter 2 ∧
    (∀ i : Fin 4, i ≠ 2 → cAfter i = cBefore i) ∧
    paperPercentile cBefore 3 = 3 / 4 ∧
    paperPercentile cAfter 3 = 1 / 2 ∧
    citationStanding cBefore cohortPapers 0 (5 / 8) = 3 / 4 ∧
    citationStanding cAfter cohortPapers 0 (5 / 8) = 1 / 2 ∧
    citationStanding cAfter cohortPapers 0 (5 / 8) <
      citationStanding cBefore cohortPapers 0 (5 / 8) ∧
    citationGridScore cBefore cohortPapers 0 [0, 1 / 4, 1 / 2, 3 / 4] = 11 / 16 ∧
    citationGridScore cAfter cohortPapers 0 [0, 1 / 4, 1 / 2, 3 / 4] = 5 / 8 ∧
    citationGridScore cAfter cohortPapers 0 [0, 1 / 4, 1 / 2, 3 / 4] <
      citationGridScore cBefore cohortPapers 0 [0, 1 / 4, 1 / 2, 3 / 4] := by
  decide +kernel

end CitationCounterexample

end PiP
