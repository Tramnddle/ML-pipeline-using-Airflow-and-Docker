import argparse
import json
import pickle
import traceback

import pandas as pd
from deepchecks.tabular import Dataset, Suite
from deepchecks.tabular.checks import RocReport, TrainTestPredictionDrift


def parse_json_list(value):
    return json.loads(value)


def build_dataset(df, predictors, target, cat_features, datetime_name):
    features = [col for col in predictors if col in df.columns]
    categorical = [col for col in cat_features if col in df.columns]
    return Dataset(
        df,
        label=target,
        features=features,
        cat_features=categorical,
        datetime_name=datetime_name,
    )


def run_model_drift(args):
    with open(args.model_path, "rb") as f:
        model = pickle.load(f)

    ref_df = pd.read_csv(args.ref_csv)
    cur_df = pd.read_csv(args.cur_csv)
    predictors = parse_json_list(args.predictors_json)
    cat_features = parse_json_list(args.cat_features_json)

    ref_dataset = build_dataset(ref_df, predictors, args.target, cat_features, args.datetime_name)
    cur_dataset = build_dataset(cur_df, predictors, args.target, cat_features, args.datetime_name)

    suite = Suite(
        "model drift",
        RocReport().add_condition_auc_greater_than(0.7),
        TrainTestPredictionDrift().add_condition_drift_score_less_than(0.1),
    )
    result = suite.run(ref_dataset, cur_dataset, model)
    try:
        result.save_as_html(args.report_path)
    except Exception:
        print(traceback.format_exc())

    payload = {
        "retrain": (len(result.get_not_ran_checks()) > 0) or (len(result.get_not_passed_checks()) > 0),
        "not_passed": len(result.get_not_passed_checks()),
        "not_ran": len(result.get_not_ran_checks()),
    }
    with open(args.output_path, "w") as f:
        json.dump(payload, f)


def run_predict(args):
    with open(args.model_path, "rb") as f:
        model = pickle.load(f)

    df = pd.read_csv(args.dataset_csv)
    predictors = parse_json_list(args.predictors_json)
    predictions = model.predict(df[predictors]).tolist()
    probabilities = None
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(df[predictors])[:, 1].tolist()

    with open(args.output_path, "w") as f:
        json.dump(
            {
                "predictions": predictions,
                "probabilities": probabilities,
            },
            f,
        )


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    model_drift = subparsers.add_parser("model-drift")
    model_drift.add_argument("--ref-csv", required=True)
    model_drift.add_argument("--cur-csv", required=True)
    model_drift.add_argument("--model-path", required=True)
    model_drift.add_argument("--predictors-json", required=True)
    model_drift.add_argument("--target", required=True)
    model_drift.add_argument("--cat-features-json", required=True)
    model_drift.add_argument("--datetime-name", required=True)
    model_drift.add_argument("--report-path", required=True)
    model_drift.add_argument("--output-path", required=True)
    model_drift.set_defaults(func=run_model_drift)

    predict = subparsers.add_parser("predict")
    predict.add_argument("--model-path", required=True)
    predict.add_argument("--dataset-csv", required=True)
    predict.add_argument("--predictors-json", required=True)
    predict.add_argument("--output-path", required=True)
    predict.set_defaults(func=run_predict)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
