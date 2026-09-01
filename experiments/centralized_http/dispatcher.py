# dispatcher_advanced.py

import asyncio
import aiohttp
import json
import os
from pathlib import Path
from typing import List, Dict

from emergency_processing.config import env_int, env_list, env_path

# ================================================================
# CONFIG
# ================================================================

JSONL_PATH = env_path("JSONL_PATH", "data/conversaciones1.jsonl")
OUTPUT_JSON = env_path("OUTPUT_JSON", "outputs/centralized_http_results.json")
PROGRESS_FILE = env_path("PROGRESS_FILE", "outputs/centralized_http_progress.json")

SERVERS = env_list("KEYWORD_SERVER_URLS")
if not SERVERS:
    raise RuntimeError("Falta configurar KEYWORD_SERVER_URLS en el entorno o en .env")

API_KEY = os.getenv("KEYWORDS_API_KEY") or os.getenv("API_KEY")

BATCH_SIZE = env_int("BATCH_SIZE", 8)
CONCURRENT_REQUESTS = env_int("CONCURRENT_REQUESTS", 4)
BASE_RETRY_DELAY = env_int("BASE_RETRY_DELAY", 2)   # segundos
MAX_RETRY_DELAY = env_int("MAX_RETRY_DELAY", 60)   # segundos
HEALTH_RECHECK_INTERVAL = env_int("HEALTH_RECHECK_INTERVAL", 15)

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


class ServerPool:
    def __init__(self, servers: List[str]):
        self._servers = list(servers)
        self._alive_servers = []
        self._counter = 0
        self._lock = asyncio.Lock()
        self._has_alive = asyncio.Event()

    async def update_alive(self, alive_servers: List[str]):
        alive_set = set(alive_servers)
        ordered_alive = [server for server in self._servers if server in alive_set]

        async with self._lock:
            self._alive_servers = ordered_alive
            if self._alive_servers:
                self._counter %= len(self._alive_servers)
                self._has_alive.set()
            else:
                self._counter = 0
                self._has_alive.clear()

    async def next_server(self):
        while True:
            await self._has_alive.wait()

            async with self._lock:
                if not self._alive_servers:
                    self._has_alive.clear()
                    continue

                server = self._alive_servers[self._counter % len(self._alive_servers)]
                self._counter += 1
                return server


async def refresh_server_health(session, server_pool, verbose=False):
    checks = await asyncio.gather(*(health_check(session, server) for server in SERVERS))
    alive = []

    for server, ok in zip(SERVERS, checks):
        if ok:
            alive.append(server)

        if verbose:
            print(f"  {'OK' if ok else 'CAIDO'} {server}")

    await server_pool.update_alive(alive)
    return alive


async def health_rechecker(session, server_pool):
    while True:
        await asyncio.sleep(HEALTH_RECHECK_INTERVAL)
        alive = await refresh_server_health(session, server_pool)
        print(f"[HEALTH] Servidores vivos: {len(alive)}/{len(SERVERS)}")


# ================================================================
# ENVÍO DE LOTES
# ================================================================

async def post_batch(session, server_pool, batch):
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
        server = await server_pool.next_server()

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

async def worker(queue, session, server_pool, progress, results_map):
    """Toma batches de la cola y los envía por round-robin a servidores vivos."""
    
    while True:
        batch = await queue.get()
        if batch is None:
            queue.task_done()
            return

        data = await post_batch(session, server_pool, batch)

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
        server_pool = ServerPool(SERVERS)
        alive = []
        for s in SERVERS:
            ok = await health_check(session, s)
            if ok:
                alive.append(s)
                print(f"  ✔ {s} OK")
            else:
                print(f"  ✖ {s} CAÍDO")

        await server_pool.update_alive(alive)

        if not alive:
            print("❌ Ningún servidor está disponible. No se puede continuar.")
            return

        health_task = asyncio.create_task(health_rechecker(session, server_pool))

        # generar batches
        queue = asyncio.Queue()
        for i in range(0, len(items_to_process), BATCH_SIZE):
            queue.put_nowait(items_to_process[i:i+BATCH_SIZE])

        # mapa de resultados sin duplicados
        results_map = {}

        # lanzar workers
        workers = [
            asyncio.create_task(worker(queue, session, server_pool, progress, results_map))
            for _ in range(CONCURRENT_REQUESTS)
        ]

        await queue.join()

        # terminar workers
        for _ in workers:
            queue.put_nowait(None)

        await asyncio.gather(*workers)
        health_task.cancel()
        await asyncio.gather(health_task, return_exceptions=True)

    # exportar resultados
    final_results = sorted(results_map.values(), key=lambda x: str(x["id"]))

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(final_results, f, indent=2, ensure_ascii=False)

    print(f"✅ Guardado {len(final_results)} resultados en {OUTPUT_JSON}")


if __name__ == "__main__":
    asyncio.run(main())
