import json
import os
from redis import asyncio as aioredis

from ..config import env_path, require_env

REDIS_URL = os.getenv("COLLECTOR_REDIS_URL") or require_env("REDIS_URL")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
OUTPUT_JSONL = env_path("REDIS_RESULTS_JSONL", "outputs/redis_results.jsonl")

async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    print("📥 Leyendo resultados...")

    # 1. Leer todo el stream de resultados
    msgs = await r.xrange(STREAM_OUT, min='-', max='+')

    # 2. Convertir la lista a una estructura manejable
    parsed = []
    for _, fields in msgs:
        # Nos aseguramos de que el ID sea entero para poder ordenar bien
        try:
            order_id = int(fields["id"])
        except:
            order_id = 999999999  # fallback

        parsed.append((order_id, fields))

    # 3. Ordenar por el campo id original
    parsed.sort(key=lambda x: x[0])

    print(f"📦 Guardando {len(parsed)} resultados ordenados por ID...")

    # 4. Guardar en JSONL
    with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
        for _, fields in parsed:
            record = {
                "id": fields["id"],
                "keywords": json.loads(fields.get("keywords", "[]")),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    await r.aclose()
    print(f"✅ Archivo generado: {OUTPUT_JSONL}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
