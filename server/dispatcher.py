# dispatcher.py
import asyncio
import aiohttp
import json
from pathlib import Path
from typing import List
import math
import sys

# CONFIG --------------------------------------------------------------
JSONL_PATH = "../dataset/conversaciones.jsonl"  
OUTPUT_JSON = "resultados.json"
SERVERS = [
    "http://IP_MAQUINA_1:8000/keywords", 
    "http://IP_MAQUINA_2:8000/keywords",
]
API_KEY = None  # si usas autenticación, pon el valor aquí (X-API-KEY)
BATCH_SIZE = 8          # cuántos items por petición al servidor
CONCURRENT_REQUESTS = 4 # solicitudes HTTP paralelas desde el dispatcher
REQUEST_TIMEOUT = 60    # segundos
RETRIES = 3
# --------------------------------------------------------------------

def load_jsonl(path: str):
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            obj = json.loads(line)
            items.append(obj)
    return items

def split_shards_round_robin(items: List[dict], n_shards: int):
    shards = [[] for _ in range(n_shards)]
    for i, it in enumerate(items):
        shards[i % n_shards].append(it)
    return shards

async def post_batch(session, url, batch):
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-KEY"] = API_KEY
    payload = {"items": [{"id": it["id"], "text": it["text"], "top_k": it.get("top_k", 6)} for it in batch]}
    for attempt in range(1, RETRIES+1):
        try:
            async with session.post(url, json=payload, timeout=REQUEST_TIMEOUT, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data
                else:
                    text = await resp.text()
                    print(f"[WARN] server {url} status {resp.status}: {text}")
        except Exception as e:
            print(f"[ERROR] post to {url} attempt {attempt} failed: {e}")
        await asyncio.sleep(1 + attempt*2)
    raise RuntimeError(f"Failed to post to {url} after {RETRIES} retries")

async def process_shard(shard, server_url, session, sem):
    results = []
    # send batches
    for i in range(0, len(shard), BATCH_SIZE):
        batch = shard[i:i+BATCH_SIZE]
        # concurrency control for HTTP
        await sem.acquire()
        try:
            data = await post_batch(session, server_url, batch)
            # data expected {"results": [{"id":..., "keywords":[...]} , ...]}
            if "results" in data:
                for r in data["results"]:
                    results.append(r)
            else:
                # fallback: if server returns list
                if isinstance(data, list):
                    results.extend(data)
        except Exception as e:
            # fallback: reassign to other server if exists
            print(f"[ERROR] batch failed on {server_url}: {e}")
            # Try to send to other servers sequentially
            for alt in SERVERS:
                if alt == server_url:
                    continue
                try:
                    data = await post_batch(session, alt, batch)
                    if "results" in data:
                        results.extend(data["results"])
                    else:
                        if isinstance(data, list):
                            results.extend(data)
                    print(f"[INFO] batch reassigned to {alt}")
                    break
                except Exception as e2:
                    print(f"[WARN] alternate {alt} failed: {e2}")
        finally:
            sem.release()
    return results

async def main():
    items = load_jsonl(JSONL_PATH)
    if not items:
        print("No items found.")
        return
    print(f"Loaded {len(items)} conversations.")

    # repartir en shards por servidor con round-robin para balance fino
    shards = split_shards_round_robin(items, len(SERVERS))

    connector = aiohttp.TCPConnector(limit=0)
    timeout = aiohttp.ClientTimeout(total=None)
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for shard, server in zip(shards, SERVERS):
            tasks.append(asyncio.create_task(process_shard(shard, server, session, sem)))
        # esperar y recolectar
        all_results = []
        lists = await asyncio.gather(*tasks)
        for l in lists:
            all_results.extend(l)

    # ordenar por id (opcional)
    all_results_sorted = sorted(all_results, key=lambda x: str(x.get("id")))

    # Guardar en JSON final
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_results_sorted, f, ensure_ascii=False, indent=2)

    print(f"Guardado {len(all_results_sorted)} resultados en {OUTPUT_JSON}")

if __name__ == "__main__":
    asyncio.run(main())
