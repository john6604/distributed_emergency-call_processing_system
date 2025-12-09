# worker_redis_consumer.py
import asyncio
import json
import os
import time
from redis import asyncio as aioredis
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch

# CONFIG
REDIS_URL = os.getenv("REDIS_URL", "redis://192.168.3.30:6379")
STREAM = "stream:convs"
GROUP = "group:convs"
CONSUMER = f"worker-{os.getenv('HOSTNAME','local')}-{os.getpid()}"
BATCH = 4                 # cuántos mensajes leer por XREADGROUP
CLAIM_MILLIS = 30000      # reclamar mensajes inactivos de > 30s
RECLAIM_INTERVAL = 20     # cada cuántos seg ejecuta reclaim
SLEEP_EMPTY = 1.0

# Modelo
MODEL_NAME = "UDA-LIDI/barto_emergency_multi_purpose"
device = "cuda" if torch.cuda.is_available() else "cpu"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, use_auth_token=True)
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME, use_auth_token=True).to(device)
model.eval()

async def ensure_group(r):
    try:
        # crea el grupo si no existe; "$" para no leer historic por defecto, "0" lee todo
        await r.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        print("Grupo creado:", GROUP)
    except aioredis.exceptions.ResponseError as e:
        # si ya existe lanza error; ignorar
        if "BUSYGROUP" in str(e):
            print("Grupo ya existe.")
        else:
            raise

async def process_message(rid, fields):
    conv_id = fields.get("id")
    text = fields.get("text", "")
    # aquí tu prompt / generación (truncation)
    prompt = "Extrae las palabras clave de la emergencia: " + text
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024).to(device)
    out = model.generate(**inputs, num_beams=2, max_new_tokens=32)
    decoded = tokenizer.decode(out[0], skip_special_tokens=True)
    # normalizar lista (si separa por comas)
    if "," in decoded:
        kws = [k.strip() for k in decoded.split(",") if k.strip()]
    else:
        kws = [k.strip() for k in decoded.split() if k.strip()]
    return {"id": conv_id, "keywords": kws}

async def consumer_loop():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await ensure_group(r)

    async def reclaim_pending():
        # usa XAUTOCLAIM (Redis 6.2+) para reclamar mensajes > CLAIM_MILLIS
        try:
            # XAUTOCLAIM stream group consumer min-idle-time start [COUNT]
            # redis-py exposes xauto_claim
            # Si no disponible, puedes usar XPENDING + XCLAIM
            while True:
                # auto-claim returns (next_id, [{id:{fields}}...])
                res = await r.xautoclaim(STREAM, GROUP, CONSUMER, min_idle_time=CLAIM_MILLIS, start_id="0-0", count=100)
                # res is (next_id, messages)
                if res and len(res) >= 2:
                    msgs = res[1]
                    if msgs:
                        print(f"[{CONSUMER}] reclaimed {len(msgs)} messages")
                await asyncio.sleep(RECLAIM_INTERVAL)
        except Exception as e:
            print("Reclaim error:", e)

    async def read_and_process():
        while True:
            try:
                # XREADGROUP GROUP <group> <consumer> COUNT <BATCH> BLOCK 5000 STREAMS <stream> '>'
                resp = await r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=BATCH, block=5000)
                if not resp:
                    await asyncio.sleep(SLEEP_EMPTY)
                    continue
                # resp is list of (stream, [(id, {field:val})...])
                for stream_name, messages in resp:
                    for msg_id, fields in messages:
                        try:
                            result = await process_message(msg_id, fields)
                            # 1) guardar resultado en Redis hash o en un output stream/file
                            # Aquí, por simplicidad, guarda en un stream de salida
                            await r.xadd("stream:results", {"id": result["id"], "keywords": json.dumps(result["keywords"])})
                            # 2) ACK el message
                            await r.xack(STREAM, GROUP, msg_id)
                            print(f"[{CONSUMER}] processed {result['id']} acked {msg_id}")
                        except Exception as e:
                            print(f"Error processing {msg_id}: {e}")
                            # NO ACK si falla: se quedará en PEL y otro consumer podrá reclamar
            except Exception as e:
                print("Read loop error:", e)
                await asyncio.sleep(1)

    # lanzar tareas
    t_reclaim = asyncio.create_task(reclaim_pending())
    t_read = asyncio.create_task(read_and_process())

    await asyncio.gather(t_read, t_reclaim)

if __name__ == "__main__":
    asyncio.run(consumer_loop())
