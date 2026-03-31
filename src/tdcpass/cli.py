from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Sequence

import pandas as pd

from tdcpass.analysis.accounting import build_accounting_summary, compute_other_component, summary_to_frame
from tdcpass.analysis.treatment_fingerprint import validate_headline_treatment_fingerprint
from tdcpass.analysis.local_projections import run_local_projections
from tdcpass.analysis.shocks import expanding_window_residual
from tdcpass.core.paths import ensure_repo_dirs, repo_root
from tdcpass.core.yaml_utils import load_yaml
from tdcpass.data.fetchers.fiscaldata import fetch_fiscaldata_endpoint
from tdcpass.data.fetchers.fred import fetch_fred_observations
from tdcpass.data.sibling_cache import discover_and_write_manifest
from tdcpass.pipeline.demo import run_demo_pipeline
from tdcpass.pipeline.quarterly import run_quarterly_pipeline


def _doctor(args: argparse.Namespace | None = None) -> int:
    root = repo_root()
    ensure_repo_dirs()
    required = [
        root / "README.md",
        root / "config" / "project.yml",
        root / "config" / "output_contract.yml",
        root / "config" / "regime_specs.yml",
        root / "config" / "series_registry.yml",
        root / "config" / "shock_specs.yml",
        root / "config" / "lp_specs.yml",
        root / "docs" / "output_schema.md",
    ]
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    payload = {
        "repo_root": str(root),
        "missing_files": missing,
        "fred_api_key_present": bool(os.getenv("FRED_API_KEY")),
    }
    print(json.dumps(payload, indent=2))
    return 0 if not missing else 1


def _demo(args: argparse.Namespace | None = None) -> int:
    result = run_demo_pipeline()
    print(json.dumps(result, indent=2))
    return 0


def _discover_cache(args: argparse.Namespace | None = None) -> int:
    path = discover_and_write_manifest()
    print(str(path))
    return 0


def _fetch_fred(args: argparse.Namespace) -> int:
    api_key = os.getenv(args.api_key_env) if args.api_key_env else None
    out_path = Path(args.out)
    df = fetch_fred_observations(
        args.series_id,
        api_key=api_key,
        out_path=out_path,
        observation_start=args.observation_start,
    )
    print(json.dumps({"rows": int(len(df)), "out": str(out_path)}, indent=2))
    return 0


def _fetch_fiscaldata(args: argparse.Namespace) -> int:
    out_path = Path(args.out)
    df = fetch_fiscaldata_endpoint(args.endpoint, out_path=out_path)
    print(json.dumps({"rows": int(len(df)), "out": str(out_path)}, indent=2))
    return 0


def _decompose(args: argparse.Namespace) -> int:
    df = pd.read_csv(args.input)
    df = compute_other_component(df, total_col=args.total_col, tdc_col=args.tdc_col, out_col=args.out_col)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    summary = build_accounting_summary(df, total_col=args.total_col, tdc_col=args.tdc_col, other_col=args.out_col)
    summary_path = out_path.with_name(out_path.stem + "_summary.csv")
    summary_to_frame(summary).to_csv(summary_path, index=False)
    print(json.dumps({"out": str(out_path), "summary": str(summary_path)}, indent=2))
    return 0


def _shock(args: argparse.Namespace) -> int:
    df = pd.read_csv(args.input)
    predictors = [item.strip() for item in args.predictors.split(",") if item.strip()]
    out = expanding_window_residual(
        df,
        target=args.target,
        predictors=predictors,
        min_train_obs=args.min_train_obs,
        standardize=not args.no_standardize,
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(json.dumps({"out": str(out_path), "rows": int(len(out))}, indent=2))
    return 0


def _lp(args: argparse.Namespace) -> int:
    df = pd.read_csv(args.input)
    outcomes = [item.strip() for item in args.outcomes.split(",") if item.strip()]
    controls = [item.strip() for item in args.controls.split(",") if item.strip()]
    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]
    out = run_local_projections(
        df,
        shock_col=args.shock_col,
        outcome_cols=outcomes,
        controls=controls,
        horizons=horizons,
        nw_lags=args.nw_lags,
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(json.dumps({"out": str(out_path), "rows": int(len(out))}, indent=2))
    return 0


def _pipeline_run(args: argparse.Namespace) -> int:
    result = run_quarterly_pipeline(
        base_dir=Path(args.root) if args.root else None,
        source_root=Path(args.source_root) if args.source_root else None,
        contract_path=Path(args.contract) if args.contract else None,
        reuse_mode=args.reuse_mode,
        raw_fixture_root=Path(args.raw_fixture_root) if args.raw_fixture_root else None,
    )
    print(json.dumps(result, indent=2))
    return 0


def _pipeline_closeout(args: argparse.Namespace) -> int:
    root = Path(args.root) if args.root else repo_root()
    summary_path = root / "output" / "models" / "backend_closeout_summary.json"
    report_path = root / "output" / "reports" / "backend_closeout.md"
    packet_path = root / "output" / "models" / "backend_evidence_packet_summary.json"
    bundle_path = root / "output" / "models" / "backend_decision_bundle_summary.json"
    identity_path = root / "output" / "models" / "lp_irf_identity_baseline.csv"
    fingerprint_path = root / "output" / "models" / "headline_treatment_fingerprint.json"
    readiness_path = root / "output" / "models" / "result_readiness_summary.json"
    direct_identification_path = root / "output" / "models" / "direct_identification_summary.json"

    missing = [
        str(path)
        for path in [
            summary_path,
            report_path,
            packet_path,
            bundle_path,
            identity_path,
            fingerprint_path,
            readiness_path,
            direct_identification_path,
        ]
        if not path.exists()
    ]
    if missing:
        print(json.dumps({"status": "missing_artifacts", "root": str(root), "missing": missing}, indent=2))
        return 1

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    direct_identification = json.loads(direct_identification_path.read_text(encoding="utf-8"))
    fingerprint = json.loads(fingerprint_path.read_text(encoding="utf-8"))
    shock_specs = load_yaml(repo_root() / "config" / "shock_specs.yml")["shocks"]
    fingerprint_failures = validate_headline_treatment_fingerprint(
        fingerprint,
        shock_spec=shock_specs["unexpected_tdc_default"],
    )
    closeout_failures: list[str] = []
    identity_frame = pd.read_csv(identity_path)
    if identity_frame.empty:
        closeout_failures.append("Exact identity baseline artifact is empty.")
    elif "decomposition_mode" not in identity_frame.columns or not identity_frame["decomposition_mode"].eq(
        "exact_identity_baseline"
    ).all():
        closeout_failures.append("Exact identity baseline artifact is not labeled exact_identity_baseline.")
    closeout_failures.extend(fingerprint_failures)
    if (
        direct_identification.get("estimation_path", {}).get("primary_decomposition_mode")
        != "exact_identity_baseline"
    ):
        closeout_failures.append("Direct identification summary is not using the exact identity baseline as primary.")
    if readiness.get("estimation_path", {}).get("primary_decomposition_mode") != "exact_identity_baseline":
        closeout_failures.append("Result readiness summary is not using the exact identity baseline as primary.")
    stale_phrase_checks = {
        repo_root() / "README.md": [
            "current provisional proxy implementation",
            "provisional government-deposits-at-banks proxy",
        ],
        repo_root() / "docs" / "claims_and_limits.md": [
            "current provisional proxy implementation",
            "treatment migration is complete",
        ],
        repo_root() / "docs" / "glossary.md": [
            "current provisional treatment",
            "government-deposits-at-banks stock series",
        ],
        repo_root() / "docs" / "project_brief.md": [
            "current checked-in `tdcpass` treatment implementation has drifted onto a provisional",
        ],
        repo_root() / "docs" / "task_backlog.md": [
            "Rename the current provisional treatment",
            "Rebuild unexpected-treatment residualization on canonical TDC",
        ],
    }
    for path, phrases in stale_phrase_checks.items():
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for phrase in phrases:
            if phrase in text:
                closeout_failures.append(f"Stale proxy-era wording remains in {path.relative_to(repo_root())}: {phrase!r}")

    payload = {
        "status": "closeout_failed" if closeout_failures else str(summary.get("status", "unknown")),
        "recommended_action": str(summary.get("recommended_action", "unknown")),
        "headline_question": str(summary.get("headline_question", "")),
        "closeout_summary_path": str(summary_path),
        "closeout_report_path": str(report_path),
        "decision_bundle_path": str(bundle_path),
        "evidence_packet_path": str(packet_path),
        "identity_baseline_path": str(identity_path),
        "headline_treatment_fingerprint_path": str(fingerprint_path),
        "closeout_failures": closeout_failures,
    }
    print(json.dumps(payload, indent=2))
    return 1 if closeout_failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tdcpass", description="TDC pass-through repo scaffold CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor", help="Validate the seed repo structure")
    doctor.set_defaults(func=_doctor)

    demo = subparsers.add_parser("demo", help="Run the synthetic demo pipeline")
    demo.set_defaults(func=_demo)

    pipeline = subparsers.add_parser("pipeline", help="Run the real quarterly export pipeline")
    pipeline_subparsers = pipeline.add_subparsers(dest="pipeline_command", required=True)
    pipeline_run = pipeline_subparsers.add_parser("run", help="Materialize the quarterly contract bundle")
    pipeline_run.add_argument("--root", default=None, help="Destination repository root")
    pipeline_run.add_argument("--source-root", default=None, help="Source bundle root containing quarterly inputs")
    pipeline_run.add_argument("--contract", default=None, help="Override the output contract path")
    pipeline_run.add_argument("--reuse-mode", default=None, help="Sibling cache reuse mode: discover, rebuild, copy, or symlink")
    pipeline_run.add_argument("--raw-fixture-root", default=None, help="Offline frozen raw-data fixture root for a reproducible rebuild")
    pipeline_run.set_defaults(func=_pipeline_run)
    pipeline_closeout = pipeline_subparsers.add_parser("closeout", help="Print the final backend closeout artifact paths from an existing run root")
    pipeline_closeout.add_argument("--root", default=None, help="Existing run root containing backend closeout artifacts")
    pipeline_closeout.set_defaults(func=_pipeline_closeout)

    discover = subparsers.add_parser("discover-cache", help="Search local caches for reusable artifacts")
    discover.set_defaults(func=_discover_cache)

    fred = subparsers.add_parser("fetch-fred", help="Fetch a FRED series via the official API")
    fred.add_argument("series_id")
    fred.add_argument("--out", required=True)
    fred.add_argument("--observation-start", dest="observation_start", default=None)
    fred.add_argument("--api-key-env", dest="api_key_env", default="FRED_API_KEY")
    fred.set_defaults(func=_fetch_fred)

    fiscal = subparsers.add_parser("fetch-fiscaldata", help="Fetch a FiscalData endpoint")
    fiscal.add_argument("endpoint")
    fiscal.add_argument("--out", required=True)
    fiscal.set_defaults(func=_fetch_fiscaldata)

    decompose = subparsers.add_parser("decompose", help="Compute the non-TDC residual and summary")
    decompose.add_argument("--input", required=True)
    decompose.add_argument("--out", required=True)
    decompose.add_argument("--total-col", default="total_deposits_bank_qoq")
    decompose.add_argument("--tdc-col", default="tdc_bank_only_qoq")
    decompose.add_argument("--out-col", default="other_component_qoq")
    decompose.set_defaults(func=_decompose)

    shock = subparsers.add_parser("shock", help="Build out-of-sample residual shocks")
    shock.add_argument("--input", required=True)
    shock.add_argument("--out", required=True)
    shock.add_argument("--target", required=True)
    shock.add_argument("--predictors", required=True)
    shock.add_argument("--min-train-obs", type=int, default=24)
    shock.add_argument("--no-standardize", action="store_true")
    shock.set_defaults(func=_shock)

    lp = subparsers.add_parser("lp", help="Run local projections")
    lp.add_argument("--input", required=True)
    lp.add_argument("--out", required=True)
    lp.add_argument("--shock-col", required=True)
    lp.add_argument("--outcomes", required=True)
    lp.add_argument("--controls", default="")
    lp.add_argument("--horizons", default="0,1,2,3,4,5,6,7,8")
    lp.add_argument("--nw-lags", type=int, default=4)
    lp.set_defaults(func=_lp)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
