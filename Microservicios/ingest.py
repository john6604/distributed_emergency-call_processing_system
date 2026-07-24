# ingest.py
import requests
ORCH_URL="http://25.50.175.180:8000"
with open("../dataset/conversaciones1.jsonl","rb") as f:
    resp = requests.post(f"{ORCH_URL}/ingest", files={"file":("conversaciones1.jsonl", f, "application/json")}, timeout=300)
    print(resp.status_code, resp.text)
