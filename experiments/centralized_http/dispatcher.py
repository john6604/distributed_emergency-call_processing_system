import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List

import aiohttp

from emergency_processing.config import env_int, env_list, env_path

JSONL_PATH = env_path("JSONL_PATH", "data/conversaciones1.jsonl")
OUTPUT_JSON = env_path("OUTPUT_JSON", "outputs/centralized_http_results.json")
PROGRESS_FILE = env_path("PROGRESS_FILE", "outputs/centralized_http_progress.json")

SERVERS = env_list("KEYWORD_SERVER_URLS")
if not SERVERS:
    raise RuntimeError(
        "Required setting KEYWORD_SERVER_URLS is missing from the environment "
        "or .env file."
    )

API_KEY = os.getenv("KEYWORDS_API_KEY") or os.getenv("API_KEY")

BATCH_SIZE = env_int("BATCH_SIZE", 8)
CONCURRENT_REQUESTS = env_int("CONCURRENT_REQUESTS", 4)
BASE_RETRY_DELAY = env_int("BASE_RETRY_DELAY", 2)
MAX_RETRY_DELAY = env_int("MAX_RETRY_DELAY", 60)
HEALTH_RECHECK_INTERVAL = env_int("HEALTH_RECHECK_INTERVAL", 15)

def load_jsonl(path: str) -> List[dict]:
    items = []
    with open(path, "r", encoding="utf-8") as input_file:
        for line in input_file:
            if not line.strip():
                continue
            items.append(json.loads(line))
    return items


def load_progress() -> Dict:
    if not Path(PROGRESS_FILE).exists():
        return {"processed_ids": []}

    with open(PROGRESS_FILE, "r", encoding="utf-8") as progress_file:
        return json.load(progress_file)


def save_progress(progress: Dict):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as progress_file:
        json.dump(progress, progress_file, indent=2)


async def health_check(session, server):
    try:
        async with session.get(
            server.replace("/keywords", "/health"), timeout=5
        ) as response:
            return response.status == 200
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
    checks = await asyncio.gather(
        *(health_check(session, server) for server in SERVERS)
    )
    alive = []

    for server, ok in zip(SERVERS, checks):
        if ok:
            alive.append(server)

        if verbose:
            print(f"  {'UP' if ok else 'DOWN'} {server}")

    await server_pool.update_alive(alive)
    return alive


async def health_rechecker(session, server_pool):
    while True:
        await asyncio.sleep(HEALTH_RECHECK_INTERVAL)
        alive = await refresh_server_health(session, server_pool)
        print(f"[HEALTH] Available servers: {len(alive)}/{len(SERVERS)}")


async def post_batch(session, server_pool, batch):
    """Post one batch, retrying indefinitely so it is not discarded."""

    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-KEY"] = API_KEY

    payload = {
        "items": [
            {
                "id": item["id"],
                "text": item["text"],
                "top_k": item.get("top_k", 6),
            }
            for item in batch
        ]
    }

    delay = BASE_RETRY_DELAY

    while True:
        server = await server_pool.next_server()

        try:
            async with session.post(
                server, json=payload, timeout=60, headers=headers
            ) as response:
                if response.status == 200:
                    return await response.json()

                message = await response.text()
                print(
                    f"[WARNING] Server {server} returned "
                    f"{response.status}: {message}"
                )

        except Exception as error:
            print(f"[ERROR] Could not contact {server}: {error}")

        print(f"[RETRY] Waiting {delay}s before retry...")
        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RETRY_DELAY)


async def process_batches(queue, session, server_pool, progress, results_map):
    """Send queued batches to available servers in round-robin order."""

    while True:
        batch = await queue.get()
        if batch is None:
            queue.task_done()
            return

        data = await post_batch(session, server_pool, batch)

        # A retried batch may return an ID already recorded in progress.
        if "results" in data:
            for result in data["results"]:
                result_id = result["id"]
                if result_id not in results_map:
                    results_map[result_id] = result
                    progress["processed_ids"].append(result_id)

        save_progress(progress)

        queue.task_done()


async def main():
    print("Loading dataset...")
    items = load_jsonl(JSONL_PATH)
    progress = load_progress()

    processed = set(progress["processed_ids"])
    items_to_process = [it for it in items if it["id"] not in processed]

    print(f"Tasks: {len(items)} total, {len(items_to_process)} pending.")

    if not items_to_process:
        print("All tasks have already been processed.")
        return

    connector = aiohttp.TCPConnector(limit=0)
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        print("Checking inference servers...")
        server_pool = ServerPool(SERVERS)
        alive = []
        for server in SERVERS:
            ok = await health_check(session, server)
            if ok:
                alive.append(server)
                print(f"  UP {server}")
            else:
                print(f"  DOWN {server}")

        await server_pool.update_alive(alive)

        if not alive:
            print("No inference servers are available; processing cannot continue.")
            return

        health_task = asyncio.create_task(health_rechecker(session, server_pool))

        queue = asyncio.Queue()
        for offset in range(0, len(items_to_process), BATCH_SIZE):
            queue.put_nowait(items_to_process[offset : offset + BATCH_SIZE])

        results_map = {}

        workers = [
            asyncio.create_task(
                process_batches(queue, session, server_pool, progress, results_map)
            )
            for _ in range(CONCURRENT_REQUESTS)
        ]

        await queue.join()

        for _ in workers:
            queue.put_nowait(None)

        await asyncio.gather(*workers)
        health_task.cancel()
        await asyncio.gather(health_task, return_exceptions=True)

    final_results = sorted(results_map.values(), key=lambda x: str(x["id"]))

    with open(OUTPUT_JSON, "w", encoding="utf-8") as output_file:
        json.dump(final_results, output_file, indent=2, ensure_ascii=False)

    print(f"Saved {len(final_results)} results to {OUTPUT_JSON}.")


if __name__ == "__main__":
    asyncio.run(main())
