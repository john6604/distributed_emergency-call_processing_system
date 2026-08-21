# worker_redis_consumer.py
import asyncio
import json
import os
import time
from redis import asyncio as aioredis
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
from redis.exceptions import ResponseError

try:
    from config import env_float, env_int, require_env
except ImportError:
    from .config import env_float, env_int, require_env

# ============================
# CONFIG
# ============================
REDIS_URL = os.getenv("WORKER_REDIS_URL") or require_env("REDIS_URL")
STREAM = os.getenv("STREAM_IN", "stream:convs")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
GROUP = os.getenv("CONSUMER_GROUP", "group2:convs")
CONSUMER = f"worker-{os.getenv('HOSTNAME','local')}-{os.getpid()}"
BATCH = env_int("CONSUMER_BATCH", 4)
CLAIM_MILLIS = env_int("CLAIM_MILLIS", 30000)       # reclamo mensajes inactivos > 30s
RECLAIM_INTERVAL = env_int("RECLAIM_INTERVAL", 20)      # cada cuántos segundos reclamamos
SLEEP_EMPTY = env_float("SLEEP_EMPTY", 1.0)

# ============================
# MODELO
# ============================
MODEL_NAME = os.getenv("MODEL_NAME", "UDA-LIDI/barto_emergency_multi_purpose")
HF_TOKEN = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")
MODEL_AUTH = {"use_auth_token": HF_TOKEN} if HF_TOKEN else {}
device = "cuda" if torch.cuda.is_available() else "cpu"

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, **MODEL_AUTH)
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME, **MODEL_AUTH).to(device)
model.eval()


# ============================
# GROUP CREATION
# ============================
async def ensure_group(r):
    try:
        # IMPORTANTE: iniciar desde "0" para procesar todos los mensajes nuevos
        await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        print(f"Grupo de consumo '{GROUP}' creado")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Ya existe el grupo de consumo '{GROUP}' — OK")
        else:
            raise


# ============================
# PROCESAMIENTO DEL MENSAJE
# ============================
async def process_message(rid, fields):
    conv_id = fields.get("id")
    text = fields.get("text", "")

    # FIX: si viene como bytes, decodificar
    if isinstance(text, bytes):
        text = text.decode()

    prompt = "Extrae las palabras clave de la emergencia: " + text

    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024).to(device)
    out = model.generate(**inputs, num_beams=2, max_new_tokens=32)
    decoded = tokenizer.decode(out[0], skip_special_tokens=True)

    # Parseo simple en lista
    if "," in decoded:
        kws = [k.strip() for k in decoded.split(",") if k.strip()]
    else:
        kws = [k.strip() for k in decoded.split() if k.strip()]

    return {"id": conv_id, "keywords": kws}


# ============================
# LOOP PRINCIPAL
# ============================
async def consumer_loop():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await ensure_group(r)

    # ------- TAREA: Reclamar mensajes abandonados -------
    async def reclaim_pending():
        try:
            while True:
                res = await r.xautoclaim(
                    STREAM,
                    GROUP,
                    CONSUMER,
                    min_idle_time=CLAIM_MILLIS,
                    start_id="0-0",
                    count=100
                )

                # Redis retorna: (next_id, messages, deleted_msgids)
                next_id, messages, deleted_ids = res

                if messages:
                    print(f"[{CONSUMER}] reclaimed {len(messages)} messages")

                await asyncio.sleep(RECLAIM_INTERVAL)

        except Exception as e:
            print("Reclaim error:", e)



    # ------- TAREA: Leer y procesar mensajes -------
    async def read_and_process():
        while True:
            try:
                # --- Procesar mensajes recuperados (reclaimed) antes de los nuevos ---
                pending = await r.xpending_range(
                    STREAM, GROUP,
                    min="-", max="+",
                    count=BATCH
                )

                processed_pending = False

                if pending:
                    for p in pending:
                        msg_id = p['message_id']
                        # Obtener el mensaje real
                        msg = await r.xclaim(
                            STREAM, GROUP, CONSUMER,
                            min_idle_time=CLAIM_MILLIS,
                            message_ids=[msg_id]
                        )
                        if msg:
                            msg_id, fields = msg[0]

                            try:
                                result = await process_message(msg_id, fields)
                                await r.xadd(STREAM_OUT, {
                                    "id": result["id"],
                                    "keywords": json.dumps(result["keywords"])
                                })
                                await r.xack(STREAM, GROUP, msg_id)
                                print(f"[{CONSUMER}] processed PENDING {result['id']} acked {msg_id}")
                                processed_pending = True
                            except Exception as e:
                                print(f"Error processing reclaimed {msg_id}:", e)

                    # después de procesar pendientes, continuar el loop sin romper tu lógica
                    if processed_pending:
                        continue

                resp = await r.xreadgroup(
                    GROUP, CONSUMER,
                    {STREAM: ">"},
                    count=BATCH,
                    block=5000
                )

                if not resp:
                    await asyncio.sleep(SLEEP_EMPTY)
                    continue

                for stream_name, messages in resp:
                    for msg_id, fields in messages:
                        try:
                            result = await process_message(msg_id, fields)
                            await r.xadd(STREAM_OUT, {
                                "id": result["id"],
                                "keywords": json.dumps(result["keywords"])
                            })
                            await r.xack(STREAM, GROUP, msg_id)
                            print(f"[{CONSUMER}] processed {result['id']} acked {msg_id}")
                        except Exception as e:
                            print(f"Error processing {msg_id}:", e)

            except ResponseError as e:
                # 🔥 FIX: si el grupo no existe → lo recreamos y seguimos
                if "NOGROUP" in str(e):
                    print("No existe un grupo de consumidores, recreándolo.")
                    try:
                        await r.xgroup_create(STREAM, GROUP, id="0-0", mkstream=True)
                        print("Grupo recreado.")
                    except ResponseError as e2:
                        if "BUSYGROUP" in str(e2):
                            pass  # ya fue creado por otro worker
                    continue

            except Exception as e:
                print("Read loop error:", e)
                await asyncio.sleep(1)


    t1 = asyncio.create_task(reclaim_pending())
    t2 = asyncio.create_task(read_and_process())

    await asyncio.gather(t1, t2)


if __name__ == "__main__":
    asyncio.run(consumer_loop())
