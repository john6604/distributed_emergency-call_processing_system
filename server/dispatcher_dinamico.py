# dispatcher_rr_fault_tolerant.py
import json
from redis import asyncio as aioredis
import time

REDIS_URL = "redis://192.168.3.30:6379"
JSONL_PATH = "../dataset/conversaciones1.jsonl"
STREAM_PREFIX = "stream:convs"
WORKERS_SET = "workers:active"
WORKERS_TIMEOUT = 30    # considerar inactivos si no hay heartbeat

async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    msgs = 0
    next_index = 0

    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)

            # detectar workers activos
            all_workers = await r.hgetall(WORKERS_SET)
            active_workers = [
                w for w, ts in all_workers.items()
                if int(ts) + WORKERS_TIMEOUT >= int(time.time())
            ]
            if not active_workers:
                print("❌ No hay workers activos, esperando 5s...")
                await asyncio.sleep(5)
                continue

            # round-robin
            active_workers.sort()
            stream_index = next_index % len(active_workers)
            stream_name = f"{STREAM_PREFIX}:{stream_index}"

            await r.xadd(stream_name, {
                "id": str(obj["id"]),
                "text": obj["text"]
            })

            next_index += 1
            msgs += 1

    print(f"📤 Enviados {msgs} mensajes a {len(active_workers)} workers activos")
    await r.aclose()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
