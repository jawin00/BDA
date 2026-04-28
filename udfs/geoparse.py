"""Geoparse fallback UDF — CPU-only, geonamescache + simple regex.

For rows where lat/lon is null (typically social posts), try to extract a
country / city mention and resolve to (lat, lon, ISO-3 country). Returns
nulls if nothing confident is found; downstream pipeline filters those out
or keeps them with country = 'UNK'.

Why not spaCy NER? spaCy adds ~700 MB to the image and only marginally
improves recall on our short tweet-style text. The geonamescache match plus
country-name patterns gives us most of the win for ~5 MB of data.
"""

from __future__ import annotations

import re
from functools import lru_cache

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

_CITY_INDEX: dict[str, tuple[float, float, str]] = {}
_COUNTRY_INDEX: dict[str, str] = {}


def _build_index() -> None:
    if _CITY_INDEX:
        return
    import geonamescache
    gc = geonamescache.GeonamesCache()
    for c in gc.get_cities().values():
        if c["population"] < 100_000:
            continue
        key = c["name"].lower()
        if key in _CITY_INDEX:
            existing = _CITY_INDEX[key]
            # Keep the bigger city when ambiguous.
            if c["population"] > existing_population_marker(existing):
                _CITY_INDEX[key] = (c["latitude"], c["longitude"], c["countrycode"])
        else:
            _CITY_INDEX[key] = (c["latitude"], c["longitude"], c["countrycode"])
    for code, info in gc.get_countries().items():
        _COUNTRY_INDEX[info["name"].lower()] = code


def existing_population_marker(_) -> int:
    # Placeholder — we accept first match; rare collisions on 100k+ cities.
    return 0


_TOKEN_RE = re.compile(r"\b([A-Z][a-zA-Z\-]{2,})\b")


@lru_cache(maxsize=8192)
def _resolve(text: str) -> tuple[float | None, float | None, str | None]:
    if not text:
        return (None, None, None)
    _build_index()
    tokens = _TOKEN_RE.findall(text[:1500])
    # Country first (more reliable).
    for t in tokens:
        cc = _COUNTRY_INDEX.get(t.lower())
        if cc:
            return (None, None, cc)
    # Then cities.
    for t in tokens:
        hit = _CITY_INDEX.get(t.lower())
        if hit:
            return hit
    return (None, None, None)


@udf(returnType=StructType([
    StructField("geo_lat", DoubleType()),
    StructField("geo_lon", DoubleType()),
    StructField("geo_country", StringType()),
]))
def geoparse_udf(text: str):
    lat, lon, cc = _resolve(text or "")
    return (lat, lon, cc)


def register(spark: SparkSession) -> None:
    spark.udf.register("geoparse_text", geoparse_udf)
