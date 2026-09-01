# ingest.py

import requests

from emergency_processing.config import env_path, require_env

ORCH_URL = require_env("ORCH_URL")
JSONL_PATH = env_path("MICROSERVICES_JSONL_PATH", "data/conversaciones1.jsonl")

with open(JSONL_PATH, "rb") as f:
    resp = requests.post(f"{ORCH_URL}/ingest", files={"file": ("conversaciones1.jsonl", f, "application/json")}, timeout=300)
    print(resp.status_code, resp.text)
