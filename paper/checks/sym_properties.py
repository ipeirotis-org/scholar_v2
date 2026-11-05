"""Numerical checks of the symmetric PiP used in the paper (theory section).

PiP_tau(a) = 1/(1-tau) * integral_tau^1 G_t(n_a(t)) dt, where n_a(t) = number of a's papers with percentile >= t and
G_t(m) = share of peers with strictly fewer than m papers at or above t. The peer distribution is held fixed when one
author's record changes (the score is relative to a reference cohort).

Checks: bounds; monotonicity when adding a paper or raising a percentile; papers below tau change nothing;
the factorization PiP = F(n) * Q with Q possibly above 1; consistency in the sense of Waltman & van Eck (2012)
(does adding the same paper to two authors ever reverse their order?), compared with the step-form PiP of the app;
and the synthetic careers of v3 (one hit, one average paper, many weak papers).

Each check runs on two grids of thresholds:
  production  t_j = j/40, j = 0..39, the left Riemann sum computed by the paper's SQL (12, 15). It equals the exact
              integral applied to percentiles rounded up to the next multiple of 1/40 (25_pip_exact.sql), so it is the
              score the paper reports.
  fine        a 400-point midpoint grid, close to the exact integral.

Run: python3 paper/checks/sym_properties.py
"""
import numpy as np

GRIDS = {
    "production (t = j/40)": np.arange(40) / 40,
    "fine (400-point midpoint)": np.linspace(0, 1, 401)[:-1] + 0.00125,
}


def make_cohort(rng, n_peers=2000):
    """Peers with lognormal paper counts (>= 3) and author-specific quality (Beta-distributed paper percentiles)."""
    counts = np.maximum(3, rng.lognormal(2.3, 0.8, n_peers).astype(int))
    quality = rng.beta(2, 2, n_peers)
    return [np.sort(rng.beta(1 + 4 * q, 1 + 4 * (1 - q), c))[::-1] for c, q in zip(counts, quality)]


class Reference:
    def __init__(self, peers, T):
        self.T = T
        self.matrix = np.sort(np.array([self.counts_above(p) for p in peers]), axis=0)  # peers x thresholds, sorted
        self.totals = np.sort(np.array([len(p) for p in peers]))
        self.n = len(peers)

    def counts_above(self, y):
        """n(t) on the grid: number of percentiles >= t."""
        ys = np.sort(y)
        return len(ys) - np.searchsorted(ys, self.T, side="left")

    def G(self, n_t):
        """Share of peers with strictly fewer papers above each t."""
        return np.array([np.searchsorted(self.matrix[:, j], n_t[j], side="left") for j in range(len(self.T))]) / self.n

    def F(self, m):
        """Share of peers with at most m papers in total."""
        return np.searchsorted(self.totals, m, side="right") / self.n

    def sym(self, y, tau=0.5):
        g = self.G(self.counts_above(np.asarray(y)))
        return g[self.T >= tau].mean()

    def step_pip(self, y):
        """The app's current step form: sum_k y_(k) [F(k) - F(k-1)]."""
        y = np.sort(np.asarray(y))[::-1]
        k = np.arange(1, len(y) + 1)
        return float(np.sum(y * (self.F(k) - self.F(k - 1))))


def run(label, T):
    rng = np.random.default_rng(1)   # the same simulated cohort and draws on every grid
    peers = make_cohort(rng)
    ref = Reference(peers, T)
    print(f"--- grid: {label}")
    res = {"out_of_bounds": 0, "add_paper_drops": 0, "raise_paper_drops": 0, "below_tau_changes": 0, "q_above_1": 0}
    trials = 1500
    for _ in range(trials):
        a = peers[rng.integers(len(peers))]
        s = ref.sym(a)
        res["out_of_bounds"] += not (0 <= s <= 1)
        res["add_paper_drops"] += ref.sym(np.append(a, rng.uniform())) < s - 1e-12
        b = a.copy(); i = rng.integers(len(b)); b[i] = rng.uniform(b[i], 1)
        res["raise_paper_drops"] += ref.sym(b) < s - 1e-12
        res["below_tau_changes"] += abs(ref.sym(np.append(a, rng.uniform(0, 0.5))) - s) > 1e-12
        res["q_above_1"] += s / ref.F(len(a)) > 1
    print("over", trials, "random authors (tau = 0.5):", res)

    # Consistency (Waltman & van Eck 2012): A ranked above B; add the same paper to both; does the order reverse?
    flips = {"sym_tau50": 0, "step_pip": 0}
    pairs = 3000
    for _ in range(pairs):
        a, b = peers[rng.integers(len(peers))], peers[rng.integers(len(peers))]
        z = rng.uniform()
        for name, f in (("sym_tau50", ref.sym), ("step_pip", ref.step_pip)):
            fa, fb = f(a), f(b)
            if abs(fa - fb) < 1e-12:
                continue
            ga, gb = f(np.append(a, z)), f(np.append(b, z))
            flips[name] += (fa - fb) * (ga - gb) < 0
    print("order reversals after adding the same paper to both, out of", pairs, "pairs:", flips)

    # v3's synthetic careers, scored against the same cohort; single papers near the top show the grid's resolution
    cases = {
        "one top paper (99th pct)": [0.99],
        "one paper at the 97.5th pct": [0.975],
        "one paper at the 99.9th pct": [0.999],
        "one average paper (50th pct)": [0.50],
        "40 weak papers (10th pct)": [0.10] * 40,
        "median peer": peers[int(np.argsort([len(p) for p in peers])[len(peers) // 2])],
    }
    for name, y in cases.items():
        print(f"{name:32s} sym_tau50 = {ref.sym(y):.3f}   step PiP = {ref.step_pip(y):.3f}   F(n) = {ref.F(len(y)):.3f}")
    return ref, peers


refs = {label: run(label, T) for label, T in GRIDS.items()}

# Agreement between the two grids over the simulated authors
(prod, peers), (fine, _) = refs["production (t = j/40)"], refs["fine (400-point midpoint)"]
sp = np.array([prod.sym(p) for p in peers])
sf = np.array([fine.sym(p) for p in peers])
rank = lambda x: np.argsort(np.argsort(x, kind="mergesort"), kind="mergesort")
top_p, top_f = set(np.argsort(-sp)[:len(sp) // 10]), set(np.argsort(-sf)[:len(sf) // 10])
print(f"--- production against fine, {len(peers)} simulated peers: mean |diff| = {np.abs(sp - sf).mean():.4f}, "
      f"max |diff| = {np.abs(sp - sf).max():.4f}, rank correlation = {np.corrcoef(rank(sp), rank(sf))[0, 1]:.4f}, "
      f"top-10% overlap = {len(top_p & top_f) / len(top_f):.3f}")
