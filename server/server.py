# server.py
from fastapi import FastAPI, UploadFile, HTTPException, Header, Request
from pydantic import BaseModel
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from typing import List, Optional
import torch
import asyncio
import json
import logging
import os

try:
    from keyword_extraction import extract_keywords_from_model
except ImportError:
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from keyword_extraction import extract_keywords_from_model

try:
    from config import load_env
except ImportError:
    from .config import load_env

load_env()

MODEL_NAME = os.getenv("MODEL_NAME", "UDA-LIDI/barto_emergency_multi_purpose")
API_KEY = os.getenv("KEYWORDS_API_KEY") or os.getenv("API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")
MODEL_AUTH = {"use_auth_token": HF_TOKEN} if HF_TOKEN else {}

# Cargar modelo una vez al inicio (tiempo de carga)
device = "cuda" if torch.cuda.is_available() else "cpu"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, **MODEL_AUTH)  # si repo privado
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME, **MODEL_AUTH).to(device)
model.eval()

app = FastAPI(title="KeywordInference", version="1.0")
logger = logging.getLogger(__name__)

class TextItem(BaseModel):
    id: str
    text: str
    top_k: Optional[int] = 6

class BatchRequest(BaseModel):
    items: List[TextItem]

@app.get("/health")
def health():
    return {"status": "ok", "device": device}

def check_api_key(x_api_key: Optional[str]):
    if API_KEY is None:
        return True
    return x_api_key == API_KEY

def _extract_keywords_single_sync(text: str, top_k: int = 6):
    return extract_keywords_from_model(
        text,
        tokenizer,
        model,
        device,
        top_k=top_k,
        num_beams=4,
        max_new_tokens=64,
        early_stopping=True,
        use_batch_decode=True,
    )


async def extract_keywords_single(text: str, top_k: int = 6):
    return await asyncio.to_thread(_extract_keywords_single_sync, text, top_k)

@app.post("/keywords")
async def keywords_endpoint(req: BatchRequest, x_api_key: Optional[str] = Header(None)):
    if not check_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Procesar items en paralelo asincrónico (pero generación es bloqueante GPU/CPU)
    # Hacemos procesado secuencial por item para evitar saturar memoria; si quieres batch interno, cambiar.
    results = []
    for item in req.items:
        try:
            kws = await extract_keywords_single(item.text, item.top_k)
        except Exception:
            logger.warning("Error extrayendo keywords para item id=%s", item.id, exc_info=True)
            kws = []
        results.append({"id": item.id, "keywords": kws})
    return {"results": results}

# Opcional: endpoint para subir jsonl directamente (NO recomendado para dataset grande vía upload)
@app.post("/procesar-jsonl-file")
async def procesar_jsonl_file(file: UploadFile):
    results = []
    for line in file.file:
        obj = json.loads(line.decode("utf-8"))
        kws = await extract_keywords_single(obj["text"])
        results.append({"id": obj["id"], "keywords": kws})
    return {"results": results}
