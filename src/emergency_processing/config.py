import os
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = REPOSITORY_ROOT / ".env"


def load_env():
    if not ENV_PATH.exists():
        return

    with ENV_PATH.open("r", encoding="utf-8") as env_file:
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


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def env_list(name: str):
    value = os.getenv(name, "")
    return [item.strip() for item in value.split(",") if item.strip()]


def env_path(name: str, default):
    path = Path(os.getenv(name, default))
    if path.is_absolute():
        return path
    return REPOSITORY_ROOT / path


load_env()
