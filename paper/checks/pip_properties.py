# Numerical checks of PiP-AUC properties used in the paper: step form = threshold form, bounds
# y_min*F(n) <= PiP <= y_max*F(n), monotonicity when a paper is added, and the gap between the app's
# trapezoid rule (bigquery/*/stats_author_pip_scores_current.sql) and the step form. Run: python3 checks/pip_properties.py
import numpy as np
rng = np.random.default_rng(0)

def make_F(max_k=400):
    # cohort CDF of paper counts: lognormal-ish counts, F(k) = share of peers with <= k papers
    peers = np.maximum(3, rng.lognormal(mean=rng.uniform(2, 3.5), sigma=rng.uniform(0.6, 1.2), size=20000).astype(int))
    ks = np.arange(0, max_k + 1)
    F = np.searchsorted(np.sort(peers), ks, side="right") / len(peers)
    return lambda k: F[np.minimum(k, max_k)]

def pip_step(y, F):
    y = np.sort(y)[::-1]; k = np.arange(1, len(y) + 1)
    return float(np.sum(y * (F(k) - F(k - 1))))

def pip_trapezoid(y, F):  # mirrors stats_author_pip_scores_current.sql
    y = np.sort(y)[::-1]; x = F(np.arange(1, len(y) + 1))
    xp = np.concatenate([[0.0], x[:-1]]); yp = np.concatenate([[y[0]], y[:-1]])
    return float(np.sum((x - xp) * (y + yp) / 2))

def pip_threshold(y, F, grid=20001):
    t = np.linspace(0, 1, grid); n_t = (np.asarray(y)[None, :] >= t[:, None]).sum(1)
    return float(getattr(np, "trapezoid", getattr(np, "trapz", None))(F(n_t), t))

worst = dict(identity=0, bound=0, step_drop=0, trap_drop=0, trap_vs_step=0)
for _ in range(2000):
    F = make_F(); n = rng.integers(1, 120); y = rng.beta(rng.uniform(0.5, 3), rng.uniform(0.5, 3), size=n)
    s, tr, th = pip_step(y, F), pip_trapezoid(y, F), pip_threshold(y, F)
    worst["identity"] = max(worst["identity"], abs(s - th))
    Fn = F(np.array([n]))[0]
    if not (y.min() * Fn - 1e-12 <= s <= y.max() * Fn + 1e-12): worst["bound"] += 1
    z = rng.uniform(); y2 = np.append(y, z)
    worst["step_drop"] = min(worst["step_drop"], pip_step(y2, F) - s)
    worst["trap_drop"] = min(worst["trap_drop"], pip_trapezoid(y2, F) - tr)
    worst["trap_vs_step"] = max(worst["trap_vs_step"], abs(tr - s))
print({k: round(v, 6) if isinstance(v, float) else v for k, v in worst.items()})

# Illustrative example: A = 50 papers at the 1st percentile, B = 1 paper at the 25th
F = make_F()
print("A:", round(pip_trapezoid(np.full(50, 0.01), F), 4), " B:", round(pip_trapezoid(np.array([0.25]), F), 4),
      " F(1)=", F(np.array([1]))[0], " F(50)=", round(F(np.array([50]))[0], 3))

drops, gaps = [], []
for _ in range(5000):
    F = make_F(); n = rng.integers(1, 60); y = rng.beta(rng.uniform(0.5, 3), rng.uniform(0.5, 3), size=n)
    tr = pip_trapezoid(y, F); z = rng.uniform()
    d = pip_trapezoid(np.append(y, z), F) - tr
    drops.append(d); gaps.append(tr - pip_step(y, F))
drops = np.array(drops); gaps = np.array(gaps)
print("trapezoid: share of additions that lower the score:", (drops < -1e-12).mean(), " worst:", drops.min())
print("trapezoid - step: min", gaps.min(), " median", np.median(gaps), " max", gaps.max())
