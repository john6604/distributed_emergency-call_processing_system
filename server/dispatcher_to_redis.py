# dispatcher_to_redis.py
import json
from redis import asyncio as aioredis

REDIS_URL = "redis://25.50.175.180:6379"

STREAM_IN = "stream:convs"
STREAM_OUT = "stream:results"
GROUP = "group2:convs"

JSONL_PATH = "../dataset/conversaciones1.jsonl"


async def cleanup_redis(r):
    print("🧹 Limpiando streams previos...")

    # 1. Eliminar consumer group si existe
    try:
        await r.xgroup_destroy(STREAM_IN, GROUP)
        print("✔ Consumer group eliminado")
    except:
        pass

    # 2. Eliminar streams completos
    await r.delete(STREAM_IN)
    await r.delete(STREAM_OUT)

    print("✔ Streams eliminados (input y output)")


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)

    await cleanup_redis(r)

    print("🚀 Enviando mensajes al stream...")
    count = 0

    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)

            await r.xadd(
                STREAM_IN,
                {
                    "id": str(obj["id"]),
                    "text": obj["text"],
                }
            )
            count += 1

    print(f"📤 Enviados {count} mensajes a {STREAM_IN}")

    await r.aclose()  # evitar warning de deprecated close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
