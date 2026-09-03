import asyncio
import json
import logging
import os
from typing import List, Optional

import torch
from fastapi import FastAPI, Header, HTTPException, UploadFile
from pydantic import BaseModel
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from emergency_processing.config import load_env
from emergency_processing.keyword_extraction import extract_keywords_from_model

load_env()

MODEL_NAME = os.getenv("MODEL_NAME", "UDA-LIDI/barto_emergency_multi_purpose")
API_KEY = os.getenv("KEYWORDS_API_KEY") or os.getenv("API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")
MODEL_AUTH = {"use_auth_token": HF_TOKEN} if HF_TOKEN else {}

# Load one model instance when the service starts.
device = "cuda" if torch.cuda.is_available() else "cpu"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, **MODEL_AUTH)
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


def is_api_key_valid(x_api_key: Optional[str]):
    if API_KEY is None:
        return True
    return x_api_key == API_KEY


def _extract_keywords_sync(text: str, top_k: int = 6):
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


async def extract_keywords_async(text: str, top_k: int = 6):
    return await asyncio.to_thread(_extract_keywords_sync, text, top_k)


@app.post("/keywords")
async def keywords_endpoint(
    request: BatchRequest, x_api_key: Optional[str] = Header(None)
):
    if not is_api_key_valid(x_api_key):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Generate sequentially because model inference is blocking and concurrent
    # calls could exhaust accelerator memory.
    results = []
    for item in request.items:
        try:
            keywords = await extract_keywords_async(item.text, item.top_k)
        except Exception:
            logger.warning(
                "Keyword extraction failed for item id=%s", item.id, exc_info=True
            )
            keywords = []
        results.append({"id": item.id, "keywords": keywords})
    return {"results": results}


@app.post("/procesar-jsonl-file")
async def procesar_jsonl_file(file: UploadFile):
    results = []
    for line in file.file:
        record = json.loads(line.decode("utf-8"))
        keywords = await extract_keywords_async(record["text"])
        results.append({"id": record["id"], "keywords": keywords})
    return {"results": results}
