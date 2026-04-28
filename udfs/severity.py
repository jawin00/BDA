"""Severity-scoring UDF — pure CPU, regex + rule-based.

Maps disaster signals to a 0..1 severity score by parsing magnitudes,
fatality counts, alert levels, and damage keywords from the event text.

This is intentionally simple and explainable so we can defend it at viva
("no labeled training data needed; transparent rules; ~50 lines").
"""

from __future__ import annotations

import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

_MAG_RE = re.compile(r"\bM\s?(\d(?:\.\d)?)", re.IGNORECASE)
_FATALITY_RE = re.compile(
    r"(\d{1,6})\s*(?:dead|deaths|killed|fatalities|casualt(?:y|ies)|missing)",
    re.IGNORECASE,
)
_DISPLACED_RE = re.compile(
    r"(\d{1,7})\s*(?:displaced|evacuated|homeless|affected)",
    re.IGNORECASE,
)
_ALERT_RED = re.compile(r"\bred\s+alert\b", re.IGNORECASE)
_ALERT_ORANGE = re.compile(r"\borange\s+alert\b", re.IGNORECASE)
_DAMAGE_HEAVY = re.compile(
    r"\b(destroyed|devastated|collapsed|wiped out|catastrophic|massive damage)\b",
    re.IGNORECASE,
)


def _score(text: str) -> float:
    if not text:
        return 0.0
    score = 0.0
    t = text[:3000]

    m = _MAG_RE.search(t)
    if m:
        try:
            mag = float(m.group(1))
            score = max(score, min(1.0, (mag - 4.0) / 5.0))  # M4=0, M9=1
        except ValueError:
            pass

    m = _FATALITY_RE.search(t)
    if m:
        try:
            n = int(m.group(1))
            if n >= 1000:
                score = max(score, 1.0)
            elif n >= 100:
                score = max(score, 0.85)
            elif n >= 10:
                score = max(score, 0.6)
            elif n >= 1:
                score = max(score, 0.3)
        except ValueError:
            pass

    m = _DISPLACED_RE.search(t)
    if m:
        try:
            n = int(m.group(1))
            if n >= 100_000:
                score = max(score, 0.9)
            elif n >= 10_000:
                score = max(score, 0.7)
            elif n >= 1_000:
                score = max(score, 0.5)
        except ValueError:
            pass

    if _ALERT_RED.search(t):
        score = max(score, 0.85)
    elif _ALERT_ORANGE.search(t):
        score = max(score, 0.55)

    if _DAMAGE_HEAVY.search(t):
        score = max(score, 0.65)

    return float(round(score, 3))


@udf(returnType=FloatType())
def score_severity_udf(text: str) -> float:
    return _score(text)


def register(spark: SparkSession) -> None:
    spark.udf.register("score_severity", score_severity_udf)
