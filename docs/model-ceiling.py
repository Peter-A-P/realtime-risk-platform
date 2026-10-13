# ruff: noqa: ANN001, ANN201, D103, E741 - a recorded one-off experiment, not package code
"""Where does the live model's PR-AUC stop: data, tuning, or the features?

Run on the instance, inside the models container, at the lowest priority:
    nice -n 19 python ceiling.py /data/history /data/models/work/ceiling.json

Every fit uses the live candidate's own recipe (weighted XGBoost on the
kept history, the champion's parameters, early stopping on the last 15% of
the training rows by time), and every model is tested on the same later day,
weighted, with a paired bootstrap. Synthetic live track.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
import time
from pathlib import Path

import numpy as np
import xgboost as xgb

from verdict.history.compact import HistoryPaths
from verdict.models import live
from verdict.models.inputs import matrix
from verdict.models.promote import average_precision
from verdict.models.train import EARLY_STOPPING, MAX_ROUNDS, PARAMS, VALIDATION_SHARE

RESAMPLES = 100
SEED = 20271013


def table(paths, days, frauds, legit):
    t, share = live.bounded_table(
        paths, days, columns=live.training_schema().names, targets={True: frauds, False: legit}
    )
    return (
        matrix(t),
        np.asarray(t["is_fraud"].to_numpy(zero_copy_only=False), dtype=bool),
        np.asarray(t["weight"].to_numpy(zero_copy_only=False), dtype=np.float64),
        share,
    )


def fit(x, y, w, params, threads=2):
    split = int(len(y) * (1 - VALIDATION_SHARE))
    p = {**PARAMS, **params, "nthread": threads, "seed": 20270405}
    started = time.perf_counter()
    booster = xgb.train(
        p,
        xgb.DMatrix(x[:split], label=y[:split], weight=w[:split]),
        num_boost_round=MAX_ROUNDS,
        evals=[(xgb.DMatrix(x[split:], label=y[split:], weight=w[split:]), "validation")],
        early_stopping_rounds=EARLY_STOPPING,
        verbose_eval=False,
    )
    return booster, {
        "trees": int(booster.best_iteration) + 1,
        "seconds": round(time.perf_counter() - started),
    }


def score(booster, x):
    return booster.predict(xgb.DMatrix(x), iteration_range=(0, booster.best_iteration + 1))


def precision_at_recall(y, s, w, recall=0.8):
    order = np.argsort(-s, kind="mergesort")
    tp = np.cumsum(np.where(y[order], w[order], 0.0))
    seen = np.cumsum(w[order])
    i = int(np.searchsorted(tp / tp[-1], recall))
    return float(tp[i] / seen[i])


def main() -> None:
    paths = HistoryPaths(Path(sys.argv[1]))
    out = Path(sys.argv[2])
    days = [d for d in live.finalised_days(paths) if d >= dt.date(2026, 9, 29)]
    test_day, train_days = days[-1], days[:-1]
    log = {
        "test_day": test_day.isoformat(),
        "train_days": [d.isoformat() for d in train_days],
        "runs": {},
    }
    print("test", test_day, "train", train_days, flush=True)
    xt, yt, wt, tshare = table(paths, [test_day], 200_000, 400_000)
    log["test"] = {
        "rows": int(len(yt)),
        "frauds": int(yt.sum()),
        "share_kept": tshare,
        "fraud_share_weighted": float(wt[yt].sum() / wt.sum()),
    }

    plan = [
        # name, days, frauds, legit, params
        # A second run (2026-10-12) used this plan instead: the baseline,
        # then 800,000 frauds with 4,000,000 legitimate rows and 1,600,000 with
        # 8,000,000, on the same three days. The last was stopped (see the JSON).
        ("rows_25k", train_days[-3:], 25_000, 125_000, {}),
        ("rows_100k", train_days[-3:], 100_000, 500_000, {}),
        ("baseline_400k_3days", train_days[-3:], 400_000, 2_000_000, {}),
        ("days_1", train_days[-1:], 400_000, 2_000_000, {}),
        ("days_all", train_days, 400_000, 2_000_000, {}),
        ("depth_10", train_days[-3:], 400_000, 2_000_000, {"max_depth": 10}),
        (
            "depth_8_mcw_20_eta_0.1",
            train_days[-3:],
            400_000,
            2_000_000,
            {"max_depth": 8, "min_child_weight": 20.0, "eta": 0.1},
        ),
    ]
    scores = {}
    cache = {}
    for name, ds, f, l, params in plan:
        key = (tuple(ds), f, l)
        if key not in cache:
            cache.clear()
            cache[key] = table(paths, ds, f, l)
        x, y, w, share = cache[key]
        booster, info = fit(x, y, w, params)
        s = score(booster, xt)
        scores[name] = s
        run = {
            "days": [d.isoformat() for d in ds],
            "train_rows": int(len(y)),
            "train_frauds": int(y.sum()),
            "params": params,
            **info,
            "test_pr_auc": average_precision(yt, s, wt),
            "test_precision_at_80_recall": precision_at_recall(yt, s, wt),
        }
        if name in ("baseline_400k_3days", "depth_10"):
            run["in_sample_pr_auc"] = average_precision(y, score(booster, x), w)
        if name == "depth_10" or name == "baseline_400k_3days":
            # Frauds the model cannot lift: weighted share of frauds whose score
            # decile has a fraud rate under twice the stream's.
            base = float(wt[yt].sum() / wt.sum())
            edges = np.quantile(s, np.linspace(0, 1, 21))
            band = np.clip(np.searchsorted(edges, s, side="right") - 1, 0, 19)
            hidden = 0.0
            for b in range(20):
                m = band == b
                if wt[m].sum() and wt[m & yt].sum() / wt[m].sum() < 2 * base:
                    hidden += wt[m & yt].sum()
            run["fraud_share_in_bands_under_2x_base"] = float(hidden / wt[yt].sum())
        log["runs"][name] = run
        print(name, json.dumps(run), flush=True)
        out.write_text(json.dumps(log, indent=1) + "\n")

    rng = np.random.default_rng(SEED)
    n = len(yt)
    names = list(scores)
    boot = {k: [] for k in names}
    diff = {k: [] for k in names}
    for _ in range(RESAMPLES):
        idx = rng.integers(0, n, n)
        base_ap = average_precision(yt[idx], scores["baseline_400k_3days"][idx], wt[idx])
        for k in names:
            ap = average_precision(yt[idx], scores[k][idx], wt[idx])
            boot[k].append(ap)
            diff[k].append(ap - base_ap)
    for k in names:
        lo, hi = np.percentile(boot[k], [2.5, 97.5])
        dlo, dhi = np.percentile(diff[k], [2.5, 97.5])
        log["runs"][k]["test_pr_auc_ci"] = [float(lo), float(hi)]
        log["runs"][k]["minus_baseline_ci"] = [float(dlo), float(dhi)]
    log["resamples"] = RESAMPLES
    out.write_text(json.dumps(log, indent=1) + "\n")
    print("done", flush=True)


if __name__ == "__main__":
    main()
