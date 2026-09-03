import requests

from emergency_processing.config import env_path, require_env

ORCH_URL = require_env("ORCH_URL")
JSONL_PATH = env_path("MICROSERVICES_JSONL_PATH", "data/conversaciones1.jsonl")

with open(JSONL_PATH, "rb") as input_file:
    response = requests.post(
        f"{ORCH_URL}/ingest",
        files={
            "file": ("conversaciones1.jsonl", input_file, "application/json")
        },
        timeout=300,
    )
    print(response.status_code, response.text)
