import torch


KEYWORD_PROMPT_PREFIX = "Extrae las palabras clave de la emergencia: "


def parse_keyword_output(decoded: str):
    if "," in decoded:
        return [keyword.strip() for keyword in decoded.split(",") if keyword.strip()]

    return [keyword.strip() for keyword in decoded.split() if keyword.strip()]


def extract_keywords_from_model(
    text,
    tokenizer,
    model,
    device,
    *,
    top_k=None,
    num_beams=2,
    max_new_tokens=32,
    max_length=1024,
    early_stopping=None,
    use_batch_decode=False,
):
    prompt = KEYWORD_PROMPT_PREFIX + text
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=max_length).to(device)

    generate_kwargs = {
        "num_beams": num_beams,
        "max_new_tokens": max_new_tokens,
    }
    if early_stopping is not None:
        generate_kwargs["early_stopping"] = early_stopping

    with torch.inference_mode():
        output = model.generate(**inputs, **generate_kwargs)

    if use_batch_decode:
        decoded = tokenizer.batch_decode(output, skip_special_tokens=True)[0]
    else:
        decoded = tokenizer.decode(output[0], skip_special_tokens=True)

    keywords = parse_keyword_output(decoded)
    if top_k is not None:
        return keywords[:top_k]

    return keywords
