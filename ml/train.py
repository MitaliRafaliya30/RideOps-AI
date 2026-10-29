"""
Train the rider-safety detectors, compare them, and keep the best one.

  1. Rules (M10 baseline)                    the bar to beat
  2. Isolation Forest                        unsupervised: learns "normal" only, no labels
  3. LightGBM                                supervised, small grid, early stopping on the valid day
  4. Hybrid = deterministic rules + LightGBM the candidate for production

Every detector gets its alert threshold the same way: on the validation day,
the highest recall within the rules' own false-alarm budget. So the
comparison is at equal operational cost, not at each model's best case.

Final verdict on the holdout week (a second simulator run, different seed,
the following week), never touched during development, with bootstrap
confidence intervals. Everything is logged to MLflow.

  python -m ml.train
  python -m ml.train --holdout run_2026-05-11_7d_50000r_seed7
  mlflow ui --backend-store-uri sqlite:///mlflow.db      (to browse the runs)
"""

import argparse
import json
import logging

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score

from geodata.config import REPO_ROOT
from ml.dataset import MODEL_FEATURES, load_windows, matrix
from ml.detector import DETERMINISTIC_RULES, SafetyDetector
from ml.metrics import bootstrap_ci, evaluate, pick_threshold, summary_row, threshold_curve
from ml.rules import apply_rules, rule_flags

logger = logging.getLogger("ml.train")

TRACKING_URI = f"sqlite:///{(REPO_ROOT / 'mlflow.db').as_posix()}"
EXPERIMENT = "rideops-rider-safety"
MODEL_DIR = REPO_ROOT / "models"
DETECTOR_PATH = MODEL_DIR / "safety_detector.joblib"
HOLDOUT_RUN = "run_2026-05-11_7d_50000r_seed7"

LGBM_BASE = dict(objective="binary", metric="average_precision", n_estimators=3000, learning_rate=0.05,
                 min_child_samples=50, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                 random_state=42, verbose=-1)
# scale_pos_weight: how much more an incident window counts (class imbalance); num_leaves: tree complexity
LGBM_GRID = [dict(scale_pos_weight=w, num_leaves=n) for w in (1, 5, 20) for n in (15, 31, 63)]


def fit_lgbm(train, valid, **params):
    model = lgb.LGBMClassifier(**{**LGBM_BASE, **params})
    model.fit(matrix(train), train["is_incident"].astype(int),
              eval_set=[(matrix(valid), valid["is_incident"].astype(int))],
              callbacks=[lgb.early_stopping(100, first_metric_only=True, verbose=False)])
    return model


def _iso_input(windows):
    return np.log1p(matrix(windows).fillna(0).clip(lower=0))       # heavy tails -> log scale


def fit_isolation_forest(train):
    normal = train[~train["is_incident"]]
    return IsolationForest(n_estimators=300, max_samples=4096, random_state=0, n_jobs=-1).fit(_iso_input(normal))


def iso_score(model, windows):
    return pd.Series(-model.score_samples(_iso_input(windows)), index=windows.index)   # higher = more unusual


def choose_threshold(valid, score, labels, budget, forced=None) -> float:
    candidates = np.unique(np.quantile(score, 1 - np.geomspace(5e-2, 1e-6, 400)))
    return pick_threshold(threshold_curve(valid, score, labels, candidates, forced=forced), budget)


def deterministic(windows) -> pd.Series:
    return rule_flags(windows)[DETERMINISTIC_RULES].any(axis=1)


def train(holdout_run: str = HOLDOUT_RUN, register: bool = True):
    windows, labels, _ = load_windows()
    split = windows["split"]
    train_w = windows[(split == "train") & ~windows["after_incident"]]   # ambiguous after-incident windows left out
    valid_w = windows[split == "valid"]
    valid_fit = valid_w[~valid_w["after_incident"]]

    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="safety-detector") as parent:
        mlflow.log_params({"features": len(MODEL_FEATURES), "train_windows": len(train_w),
                           "train_incident_windows": int(train_w["is_incident"].sum()), "holdout_run": holdout_run})

        # Budget: what the rules cost on the validation day
        budget = evaluate(apply_rules(valid_w), labels)["false_alarms_per_1k_trips"]
        mlflow.log_metric("budget_false_alarms_per_1k", budget)

        # LightGBM grid, selected on validation PR-AUC (window level, threshold-free)
        best = None
        for params in LGBM_GRID:
            with mlflow.start_run(run_name=f"lgbm w{params['scale_pos_weight']} l{params['num_leaves']}", nested=True):
                model = fit_lgbm(train_w, valid_fit, **params)
                pr_auc = average_precision_score(valid_fit["is_incident"], model.predict_proba(matrix(valid_fit))[:, 1])
                mlflow.log_params({**params, "best_iteration": model.best_iteration_})
                mlflow.log_metric("valid_pr_auc", pr_auc)
                logger.info("lgbm %s: valid PR-AUC %.4f (%d trees)", params, pr_auc, model.best_iteration_)
                if best is None or pr_auc > best[0]:
                    best = (pr_auc, params, model)
        pr_auc, params, model = best
        mlflow.log_params({f"chosen_{k}": v for k, v in params.items()})
        mlflow.log_metric("valid_pr_auc", pr_auc)

        # Thresholds on the validation day, all at the same false-alarm budget
        lgbm_valid = pd.Series(model.predict_proba(matrix(valid_w))[:, 1], index=valid_w.index)
        threshold_lgbm = choose_threshold(valid_w, lgbm_valid, labels, budget)
        threshold_hybrid = choose_threshold(valid_w, lgbm_valid, labels, budget, forced=deterministic(valid_w))
        iso = fit_isolation_forest(train_w)
        threshold_iso = choose_threshold(valid_w, iso_score(iso, valid_w), labels, budget)
        mlflow.log_metrics({"threshold_lgbm": threshold_lgbm, "threshold_hybrid": threshold_hybrid,
                            "threshold_iso": threshold_iso})

        detector = SafetyDetector(model=model, threshold=threshold_hybrid, version=parent.info.run_id[:8],
                                  metadata={"params": params, "budget_false_alarms_per_1k": budget,
                                            "valid_pr_auc": pr_auc, "trained_on": "run_2026-05-04 days 1-5"})

        def all_alerts(w):
            return {
                "rules": apply_rules(w)["alert"],
                "isolation_forest": iso_score(iso, w) > threshold_iso,
                "lightgbm": pd.Series(model.predict_proba(matrix(w))[:, 1] > threshold_lgbm, index=w.index),
                "hybrid": detector.alerts(w)["alert"],
            }

        # Evaluation: dev test day, then the untouched holdout week
        sets = {"test": (windows[split == "test"], labels)}
        if (REPO_ROOT / "data" / "features" / holdout_run).exists():
            hold_w, hold_labels, _ = load_windows(holdout_run)
            sets["holdout"] = (hold_w, hold_labels)
        rows, cis = [], {}
        for set_name, (w, lab) in sets.items():
            for name, alert in all_alerts(w).items():
                scored = w.assign(alert=alert)
                result = evaluate(scored, lab)
                rows.append({"set": set_name, **summary_row(name, result)})
                mlflow.log_metrics({f"{set_name}_{name}_{k}": result[k] for k in
                                    ("recall", "precision", "false_alarms_per_1k_trips", "median_delay_s")})
                if set_name == "holdout":
                    cis[name] = bootstrap_ci(scored, lab)

        results = pd.DataFrame(rows)
        MODEL_DIR.mkdir(exist_ok=True)
        results.to_csv(MODEL_DIR / "comparison.csv", index=False)
        if cis:
            pd.concat(cis).to_csv(MODEL_DIR / "holdout_ci.csv")
            mlflow.log_artifact(str(MODEL_DIR / "holdout_ci.csv"))
        mlflow.log_artifact(str(MODEL_DIR / "comparison.csv"))

        detector.save(DETECTOR_PATH)
        mlflow.log_artifact(str(DETECTOR_PATH))
        (MODEL_DIR / "detector_card.json").write_text(json.dumps(
            {"version": detector.version, "threshold": detector.threshold, "features": detector.features,
             **detector.metadata}, indent=2, default=str))
        mlflow.lightgbm.log_model(model, name="lightgbm",
                                  registered_model_name="rideops_safety_detector" if register else None,
                                  input_example=matrix(valid_w.head(5)))
        logger.info("results:\n%s", results.set_index(["set", "detector"])
                    [["recall", "precision", "false_alarms_per_1k_trips", "median_delay_s"]].round(3))
    return detector, results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--holdout", default=HOLDOUT_RUN)
    parser.add_argument("--no-register", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    train(args.holdout, register=not args.no_register)


if __name__ == "__main__":
    main()
