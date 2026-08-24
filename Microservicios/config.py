from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def _load_root_config():
    config_path = Path(__file__).resolve().parents[1] / "config.py"
    spec = spec_from_file_location("_sisdis_config", config_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo cargar {config_path}")

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_config = _load_root_config()

load_env = _config.load_env
require_env = _config.require_env
env_int = _config.env_int
env_float = _config.env_float
env_list = _config.env_list
