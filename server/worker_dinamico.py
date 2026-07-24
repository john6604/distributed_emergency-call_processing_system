# worker_rr_heartbeat.py
import asyncio
import os
import json
from redis import asyncio as aioredis
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
import time

try:
    from config import env_int, require_env
except ImportError:
    from .config import env_int, require_env

REDIS_URL = os.getenv("DYNAMIC_REDIS_URL") or require_env("REDIS_URL")
WORKERS_SET = os.getenv("WORKERS_SET", "workers:active")
WORKERS_TIMEOUT = env_int("WORKERS_TIMEOUT", 30)          # segundos antes de considerar muerto
WORKER_HEARTBEAT_SECONDS = env_int("WORKER_HEARTBEAT_SECONDS", 5)
STREAM_PREFIX = os.getenv("STREAM_PREFIX", "stream:convs")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")

MODEL_NAME = os.getenv("MODEL_NAME", "UDA-LIDI/barto_emergency_multi_purpose")
HF_TOKEN = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")
MODEL_AUTH = {"use_auth_token": HF_TOKEN} if HF_TOKEN else {}
device = "cuda" if torch.cuda.is_available() else "cpu"

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, **MODEL_AUTH)
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME, **MODEL_AUTH).to(device)
model.eval()


async def heartbeat(r, worker_id):
    while True:
        await r.hset(WORKERS_SET, worker_id, int(time.time()))
        await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    worker_id = f"{os.getenv('HOSTNAME','node')}-{os.getpid()}"
    print(f"🚀 Worker {worker_id} activo")

    # Lanzar heartbeat
    asyncio.create_task(heartbeat(r, worker_id))

    while True:
        # 1) leer lista de streams activos según workers con heartbeat reciente
        all_workers = await r.hgetall(WORKERS_SET)
        active_workers = [
            w for w, ts in all_workers.items()
            if int(ts) + WORKERS_TIMEOUT >= int(time.time())
        ]
        if worker_id not in active_workers:
            await asyncio.sleep(1)
            continue

        index = sorted(active_workers).index(worker_id)
        stream_name = f"{STREAM_PREFIX}:{index}"

        resp = await r.xread({stream_name: "0"}, block=5000, count=1)
        if not resp:
            continue

        for _, messages in resp:
            for msg_id, fields in messages:
                text = fields.get("text", "")
                conv_id = fields["id"]

                prompt = "Extrae las palabras clave de la emergencia: " + text
                inputs = tokenizer(prompt, return_tensors="pt", max_length=1024, truncation=True).to(device)
                out = model.generate(**inputs, num_beams=2, max_new_tokens=32)
                decoded = tokenizer.decode(out[0], skip_special_tokens=True)

                if "," in decoded:
                    kws = [k.strip() for k in decoded.split(",") if k.strip()]
                else:
                    kws = decoded.split()

                await r.xadd(STREAM_OUT, {
                    "id": conv_id,
                    "keywords": json.dumps(kws)
                })
                await r.xdel(stream_name, msg_id)
                print(f"[{worker_id}] procesado id={conv_id}")

    await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
