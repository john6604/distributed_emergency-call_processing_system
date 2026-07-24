import json
from redis import asyncio as aioredis

REDIS_URL = "redis://192.168.3.30:6379"
STREAM_OUT = "stream:results"

async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    print("📥 Leyendo resultados...")

    # 1. Leer todo el stream de resultados
    msgs = await r.xrange(STREAM_OUT, min='-', max='+')

    # 2. Convertir la lista a una estructura manejable
    parsed = []
    for msg_id, fields in msgs:
        # Nos aseguramos de que el ID sea entero para poder ordenar bien
        try:
            order_id = int(fields["id"])
        except:
            order_id = 999999999  # fallback

        parsed.append((order_id, msg_id, fields))

    # 3. Ordenar por el campo id original
    parsed.sort(key=lambda x: x[0])

    print(f"📦 Guardando {len(parsed)} resultados ordenados por ID...")

    # 4. Guardar en JSONL
    with open("resultados_ordenados.jsonl", "w", encoding="utf-8") as f:
        for _, msg_id, fields in parsed:
            # Puedes agregar el redis_id si quieres
            fields["_redis_id"] = msg_id
            f.write(json.dumps(fields, ensure_ascii=False) + "\n")

    await r.aclose()
    print("✅ Archivo generado: resultados_ordenados.jsonl")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
