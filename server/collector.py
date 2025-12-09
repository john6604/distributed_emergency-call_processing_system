# collector.py
import asyncio, json
from redis import asyncio as aioredis

REDIS_URL = "redis://192.168.3.30:6379"
RESULTS_STREAM = "stream:results"
OUT_FILE = "resultados.json"

async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    last = "0-0"
    results = []
    # leer todo el stream
    entry = await r.xrange(RESULTS_STREAM, min=last, max="+")
    for msg_id, fields in entry:
        results.append({"id": fields["id"], "keywords": json.loads(fields["keywords"])})
    # guardar
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("Guardado", len(results))
    await r.close()

if __name__=="__main__":
    asyncio.run(main())
