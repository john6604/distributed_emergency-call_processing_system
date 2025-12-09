# dispatcher_advanced.py

import asyncio
import aiohttp
import json
from pathlib import Path
from typing import List, Dict
import time

# ================================================================
# CONFIG
# ================================================================

JSONL_PATH = "../dataset/conversaciones1.jsonl"
OUTPUT_JSON = "resultados.json"
PROGRESS_FILE = "progress.json"

SERVERS = [
    "http://25.50.175.180:8000/keywords",
    "http://25.50.208.243:8000/keywords",
]

API_KEY = "***REMOVED***"

BATCH_SIZE = 8
CONCURRENT_REQUESTS = 4
BASE_RETRY_DELAY = 2   # segundos
MAX_RETRY_DELAY = 60   # segundos

# ================================================================
# UTILIDADES
# ================================================================

def load_jsonl(path: str) -> List[dict]:
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            items.append(json.loads(line))
    return items


def load_progress() -> Dict:
    if not Path(PROGRESS_FILE).exists():
        return {"processed_ids": []}

    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_progress(progress: Dict):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)


async def health_check(session, server):
    try:
        async with session.get(server.replace("/keywords", "/health"), timeout=5) as r:
            return r.status == 200
    except:
        return False


# ================================================================
# ENVÍO DE LOTES
# ================================================================

async def post_batch(session, server, batch):
    """Envía un batch con reintentos infinitos, nunca se pierde un batch."""
    
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-KEY"] = API_KEY

    payload = {
        "items": [
            {"id": it["id"], "text": it["text"], "top_k": it.get("top_k", 6)}
            for it in batch
        ]
    }

    delay = BASE_RETRY_DELAY

    while True:
        try:
            async with session.post(server, json=payload, timeout=60, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()

                else:
                    msg = await resp.text()
                    print(f"[WARN] Server {server} → {resp.status}: {msg}")

        except Exception as e:
            print(f"[ERROR] Error contacting {server}: {e}")

        print(f"[RETRY] Waiting {delay}s before retry...")
        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RETRY_DELAY)


# ================================================================
# WORKER PRINCIPAL
# ================================================================

async def worker(queue, session, available_servers, progress, results_map):
    """Toma batches de la cola y los envía al primer servidor disponible."""
    
    while True:
        batch = await queue.get()
        if batch is None:
            queue.task_done()
            return

        # escoger servidor disponible
        server = available_servers[int(time.time() * 1000) % len(available_servers)]

        data = await post_batch(session, server, batch)

        # Guardar resultados evitando duplicados
        if "results" in data:
            for r in data["results"]:
                rid = r["id"]
                if rid not in results_map:
                    results_map[rid] = r
                    progress["processed_ids"].append(rid)

        save_progress(progress)

        queue.task_done()


# ================================================================
# MAIN
# ================================================================

async def main():
    print("📌 Cargando dataset…")
    items = load_jsonl(JSONL_PATH)
    progress = load_progress()

    processed = set(progress["processed_ids"])
    items_to_process = [it for it in items if it["id"] not in processed]

    print(f"Total: {len(items)} | Pendientes: {len(items_to_process)}")

    if not items_to_process:
        print("Todo ya está procesado. Saliendo.")
        return

    # session global
    connector = aiohttp.TCPConnector(limit=0)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        # health check
        print("🔍 Verificando servidores...")
        alive = []
        for s in SERVERS:
            ok = await health_check(session, s)
            if ok:
                alive.append(s)
                print(f"  ✔ {s} OK")
            else:
                print(f"  ✖ {s} CAÍDO")

        if not alive:
            print("❌ Ningún servidor está disponible. No se puede continuar.")
            return

        # generar batches
        queue = asyncio.Queue()
        for i in range(0, len(items_to_process), BATCH_SIZE):
            queue.put_nowait(items_to_process[i:i+BATCH_SIZE])

        # mapa de resultados sin duplicados
        results_map = {}

        # lanzar workers
        workers = [
            asyncio.create_task(worker(queue, session, alive, progress, results_map))
            for _ in range(CONCURRENT_REQUESTS)
        ]

        await queue.join()

        # terminar workers
        for _ in workers:
            queue.put_nowait(None)

        await asyncio.gather(*workers)

    # exportar resultados
    final_results = sorted(results_map.values(), key=lambda x: str(x["id"]))

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(final_results, f, indent=2, ensure_ascii=False)

    print(f"✅ Guardado {len(final_results)} resultados en {OUTPUT_JSON}")


if __name__ == "__main__":
    asyncio.run(main())
