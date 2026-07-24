# worker_service.py
import requests
import time
import os
import json
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch

try:
    from config import env_float, require_env
except ImportError:
    from .config import env_float, require_env

ORCH_URL = os.getenv("WORKER_ORCH_URL") or require_env("ORCH_URL")
WORKER_ID = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
SLEEP_NO_TASK = env_float("SLEEP_NO_TASK", 1.0)

MODEL_NAME = os.getenv("MODEL_NAME", "UDA-LIDI/barto_emergency_multi_purpose")
HF_TOKEN = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")
MODEL_AUTH = {"use_auth_token": HF_TOKEN} if HF_TOKEN else {}
device = "cuda" if torch.cuda.is_available() else "cpu"

print("Cargando modelo.")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, **MODEL_AUTH)
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME, **MODEL_AUTH).to(device)
model.eval()
print("Modelo cargado, ID:", WORKER_ID)

def claim_task():
    url = f"{ORCH_URL}/claim"
    try:
        resp = requests.post(url, params={"worker": WORKER_ID}, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if data is None:
                return None
            return data
        else:
            print("Reclamo fallido: ", resp.status_code, resp.text)
            return None
    except Exception as e:
        print("Error en el reclamo: ", e)
        time.sleep(2)
        return None

def post_result(task_id, conv_id, keywords):
    url = f"{ORCH_URL}/result"
    payload = {"task_id": task_id, "conv_id": conv_id, "keywords": keywords, "worker": WORKER_ID}
    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code != 200:
            print("Resultado fallido", resp.status_code, resp.text)
    except Exception as e:
        print("Error de resultado:", e)

def extract_keywords(text):
    prompt = "Extrae las palabras clave de la emergencia: " + text
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024).to(device)
    out = model.generate(**inputs, num_beams=2, max_new_tokens=32)
    decoded = tokenizer.decode(out[0], skip_special_tokens=True)
    if "," in decoded:
        kws = [k.strip() for k in decoded.split(",") if k.strip()]
    else:
        kws = [k.strip() for k in decoded.split() if k.strip()]
    return kws

if __name__ == "__main__":
    while True:
        task = claim_task()
        if not task:
            time.sleep(SLEEP_NO_TASK)
            continue
        task_id = task["task_id"]
        conv_id = task["conv_id"]
        text = task["text"]
        print(f"Tarea reclamada {task_id} conv {conv_id}")
        try:
            kws = extract_keywords(text)
            post_result(task_id, conv_id, kws)
            print(f"ACK - Tarea completada {task_id}")
        except Exception as e:
            print("Error de procesamiento:", e)
