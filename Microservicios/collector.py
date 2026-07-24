# collector.py
import requests, json
ORCH_URL="http://25.50.175.180:8000"
resp = requests.get(f"{ORCH_URL}/results", timeout=300)
resp.raise_for_status()
results = resp.json()
# Save JSONL ordered already by conv_id
with open("resultados_microservicios.jsonl","w",encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("Saved", len(results))
