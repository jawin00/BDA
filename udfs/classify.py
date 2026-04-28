"""GPU zero-shot disaster-type classifier — registered as a Spark pandas UDF.

Model: MoritzLaurer/deberta-v3-base-zeroshot-v2.0 (NLI-style zero-shot, fp16).
Hard requirement: CUDA must be available. We do NOT silently fall back to CPU
because the user explicitly said: if GPU code runs, it must use the GPU or
fail loudly.

Spark SQL UDF tag — see syllabus_mapping.md.
"""

from __future__ import annotations

import os
from typing import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, StructType, StructField, FloatType

LABELS = [
    "earthquake", "flood", "hurricane or cyclone or typhoon", "wildfire",
    "volcano eruption", "landslide", "drought", "tsunami",
    "humanitarian aid response", "infrastructure damage", "not a disaster",
]
SHORT = {
    "earthquake": "earthquake",
    "flood": "flood",
    "hurricane or cyclone or typhoon": "storm",
    "wildfire": "wildfire",
    "volcano eruption": "volcano",
    "landslide": "landslide",
    "drought": "drought",
    "tsunami": "tsunami",
    "humanitarian aid response": "aid",
    "infrastructure damage": "infra_damage",
    "not a disaster": "other",
}

_pipe = None


def _load_pipeline():
    """Lazy-load the HF pipeline once per executor JVM. fp16 on cuda:0."""
    global _pipe
    if _pipe is not None:
        return _pipe
    import torch
    assert torch.cuda.is_available(), (
        "GPU required for classify UDF; no CPU fallback. "
        "Check NVIDIA Container Toolkit / docker-compose GPU reservation."
    )
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
    model_name = os.getenv("CLASSIFY_MODEL",
                           "MoritzLaurer/deberta-v3-base-zeroshot-v2.0")
    tok = AutoTokenizer.from_pretrained(model_name)
    mdl = AutoModelForSequenceClassification.from_pretrained(
        model_name, torch_dtype=torch.float16
    ).to("cuda:0").eval()
    _pipe = pipeline(
        "zero-shot-classification",
        model=mdl, tokenizer=tok, device=0,
        torch_dtype=torch.float16,
    )
    return _pipe


def _classify_batch(texts: list[str]) -> tuple[list[str], list[float]]:
    if not texts:
        return [], []
    pipe = _load_pipeline()
    # Truncate to ~512 tokens (DeBERTa max). Strings are pre-truncated by char.
    cleaned = [(t or "")[:1500] for t in texts]
    results = pipe(cleaned, candidate_labels=LABELS, multi_label=False, batch_size=16)
    if isinstance(results, dict):
        results = [results]
    out_lbl, out_conf = [], []
    for r in results:
        top = r["labels"][0]
        out_lbl.append(SHORT.get(top, top))
        out_conf.append(float(r["scores"][0]))
    return out_lbl, out_conf


_SKIP_GPU = os.getenv("SKIP_GPU", "0") == "1"


@pandas_udf(StructType([
    StructField("event_type", StringType()),
    StructField("classify_conf", FloatType()),
]))
def classify_event_udf(it: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    for batch in it:
        if _SKIP_GPU:
            yield pd.DataFrame({
                "event_type": ["other"] * len(batch),
                "classify_conf": [0.0] * len(batch),
            })
            continue
        texts = batch.fillna("").tolist()
        labels, confs = _classify_batch(texts)
        yield pd.DataFrame({"event_type": labels, "classify_conf": confs})


def register(spark: SparkSession) -> None:
    """Register the UDF with Spark SQL so spark.sql() can call it."""
    spark.udf.register("classify_event", classify_event_udf)
