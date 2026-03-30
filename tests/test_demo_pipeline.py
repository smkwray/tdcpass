from pathlib import Path
import tempfile

from tdcpass.pipeline.demo import run_demo_pipeline


def test_demo_pipeline_writes_outputs():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # mimic repo structure needed by the pipeline
        for rel in [
            "data/raw", "data/cache", "data/derived", "data/examples", "output", "site/data"
        ]:
            (root / rel).mkdir(parents=True, exist_ok=True)

        result = run_demo_pipeline(base_dir=root)
        assert Path(result["example_path"]).exists()
        assert Path(result["accounting_path"]).exists()
        assert Path(result["shocks_path"]).exists()
        assert Path(result["irf_path"]).exists()
        assert Path(result["overview_path"]).exists()
