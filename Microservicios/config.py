import os
from pathlib import Path


def load_env():
    base_dir = Path(__file__).resolve().parent
    for env_path in (base_dir.parent / ".env", base_dir / ".env"):
        if not env_path.exists():
            continue

        with env_path.open("r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta configurar {name} en el entorno o en .env")
    return value


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


load_env()
