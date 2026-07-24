# collector.py
import json
import os

import requests

try:
    from config import require_env
except ImportError:
    from .config import require_env

ORCH_URL = require_env("ORCH_URL")
OUTPUT_JSONL = os.getenv("MICROSERVICES_RESULTS_JSONL", "resultados_microservicios.jsonl")
resp = requests.get(f"{ORCH_URL}/results", timeout=300)
resp.raise_for_status()
results = resp.json()
# Save JSONL ordered already by conv_id
with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("Saved", len(results))
