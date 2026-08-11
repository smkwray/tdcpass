import json
from pathlib import Path

from tdcpass.pipeline.demo import run_demo_pipeline


def test_demo_pipeline_writes_demo_outputs_without_touching_public_mirror(tmp_path: Path) -> None:
    root = tmp_path
    for rel in [
        "data/raw",
        "data/cache",
        "data/derived",
        "data/examples",
        "output",
        "site/data",
    ]:
        (root / rel).mkdir(parents=True, exist_ok=True)
    public_overview = root / "site" / "data" / "overview.json"
    public_overview.write_text('{"sentinel":"public"}\n', encoding="utf-8")

    result = run_demo_pipeline(base_dir=root)

    assert Path(result["example_path"]).exists()
    assert Path(result["accounting_path"]).exists()
    assert Path(result["shocks_path"]).exists()
    assert Path(result["irf_path"]).exists()
    assert Path(result["overview_path"]) == root / "output" / "demo" / "overview.json"
    assert Path(result["overview_path"]).exists()
    assert public_overview.read_text(encoding="utf-8") == '{"sentinel":"public"}\n'

    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    manifest_outputs = {Path(item["path"]) for item in manifest["outputs"]}
    assert root / "output" / "demo" / "overview.json" in manifest_outputs
    assert public_overview not in manifest_outputs
