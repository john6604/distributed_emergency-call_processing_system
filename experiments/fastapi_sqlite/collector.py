# collector.py
import json

import requests

from emergency_processing.config import env_path, require_env

ORCH_URL = require_env("ORCH_URL")
OUTPUT_JSONL = env_path("MICROSERVICES_RESULTS_JSONL", "outputs/fastapi_sqlite_results.jsonl")
resp = requests.get(f"{ORCH_URL}/results", timeout=300)
resp.raise_for_status()
results = resp.json()
# Save JSONL ordered already by conv_id
with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("Saved", len(results))
