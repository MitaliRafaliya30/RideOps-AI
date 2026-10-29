"""
Statistics used by the EDA notebook (kept here so they are tested and reused).

  separation()   how well one feature separates incident windows from normal
                 windows: AUC, Cliff's delta (effect size), KS statistic and
                 the Mann-Whitney U p-value. With ~900k windows every p-value
                 is tiny, so the decision is made on the effect size, not on p.
  psi()          Population Stability Index: has a feature's distribution
                 shifted between two periods? Used to check that a time-based
                 split is safe (< 0.1 stable, 0.1-0.25 watch, > 0.25 shifted).
  rate_ci()      a rate with its Wilson 95 % confidence interval.
"""

import numpy as np
import pandas as pd
from scipy import stats


def separation(positive: pd.Series, negative: pd.Series) -> dict:
    """Effect sizes for 'feature is higher in positives'. NaNs are dropped."""
    pos, neg = positive.dropna().to_numpy(float), negative.dropna().to_numpy(float)
    if len(pos) == 0 or len(neg) == 0:
        return {"auc": np.nan, "cliffs_delta": np.nan, "ks": np.nan, "p_value": np.nan, "n_pos": len(pos)}
    u, p = stats.mannwhitneyu(pos, neg, alternative="two-sided")
    auc = u / (len(pos) * len(neg))                 # P(random positive > random negative), ties count half
    return {
        "auc": auc,
        "cliffs_delta": 2 * auc - 1,
        "ks": stats.ks_2samp(pos, neg).statistic,
        "p_value": p,
        "n_pos": len(pos),
    }


def separation_table(windows: pd.DataFrame, features: list, target: str, negatives: pd.Series) -> pd.DataFrame:
    """separation() of every feature for one incident type against the given negative windows."""
    pos = windows[windows["incident_type"] == target]
    rows = [{"feature": f, **separation(pos[f], windows.loc[negatives, f])} for f in features]
    return pd.DataFrame(rows).set_index("feature").sort_values("auc", ascending=False)


def psi(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    """PSI of `actual` against `expected`, on quantile bins of `expected`."""
    e, a = expected.dropna().to_numpy(float), actual.dropna().to_numpy(float)
    edges = np.unique(np.quantile(e, np.linspace(0, 1, bins + 1)))
    if len(edges) < 2:                               # constant feature: nothing can shift
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    e_share = np.histogram(e, edges)[0] / len(e)
    a_share = np.histogram(a, edges)[0] / len(a)
    e_share, a_share = np.clip(e_share, 1e-6, None), np.clip(a_share, 1e-6, None)
    return float(np.sum((a_share - e_share) * np.log(a_share / e_share)))


def rate_ci(successes: int, total: int, z: float = 1.96) -> tuple:
    """(rate, low, high): Wilson score interval, sound for rare events like a 2 % incident rate."""
    if total == 0:
        return np.nan, np.nan, np.nan
    p = successes / total
    centre = (p + z * z / (2 * total)) / (1 + z * z / total)
    half = z * np.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / (1 + z * z / total)
    return p, centre - half, centre + half
