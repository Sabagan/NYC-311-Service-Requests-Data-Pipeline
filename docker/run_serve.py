"""Docker entry point for Part 3: export batch results, load, then query.

compose.yaml runs this in the serve container:
    docker compose run --rm --build serve
"""

import runpy
from pathlib import Path

root = Path("/app")
for script in ("batch/export_batch_results.py",
               "serve/load_results.py",
               "serve/query_results.py"):
    print(f"\n===== Running {script} =====", flush=True)
    runpy.run_path(str(root / "src" / script), run_name="__main__")
