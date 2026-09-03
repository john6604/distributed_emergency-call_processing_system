import json

import requests

from emergency_processing.config import env_path, require_env

ORCH_URL = require_env("ORCH_URL")
OUTPUT_JSONL = env_path("MICROSERVICES_RESULTS_JSONL", "outputs/fastapi_sqlite_results.jsonl")
response = requests.get(f"{ORCH_URL}/results", timeout=300)
response.raise_for_status()
results = response.json()

# The results endpoint returns records ordered by conversation ID.
with open(OUTPUT_JSONL, "w", encoding="utf-8") as output_file:
    for result in results:
        output_file.write(json.dumps(result, ensure_ascii=False) + "\n")

print(f"Saved {len(results)} results to {OUTPUT_JSONL}.")
