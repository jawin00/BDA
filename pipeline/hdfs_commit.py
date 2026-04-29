"""Windows-friendly Spark-to-HDFS commit helpers.

Spark can read HDFS reliably on this native Windows setup, but direct Parquet
writes to HDFS can hang during the final output-committer rename. These helpers
write to a local completed directory first, then upload it with the Hadoop CLI.
"""

from __future__ import annotations

import os
import posixpath
import shutil
import subprocess
from pathlib import Path


def write_parquet_to_hdfs(df, hdfs_uri: str, stage_name: str, partition_cols=None, files: int | None = None) -> None:
    local_path = _stage_path(stage_name)
    if local_path.exists():
        shutil.rmtree(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if os.getenv("BDA_USE_PYARROW") == "1":
        _write_with_pyarrow(df, local_path, partition_cols)
    else:
        _write_parquet_local(df, local_path, partition_cols=partition_cols, files=files)

    _upload_to_hdfs(local_path, hdfs_uri)


def write_text_to_hdfs(df, hdfs_uri: str, stage_name: str, files: int | None = None) -> None:
    local_path = _stage_path(stage_name)
    if local_path.exists():
        shutil.rmtree(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    out_df = df.coalesce(files) if files else df
    print(f"[hdfs_commit] writing local text stage {local_path}")
    out_df.write.mode("overwrite").text(local_path.resolve().as_uri())
    _upload_to_hdfs(local_path, hdfs_uri)


def write_parts_to_hdfs(
    parts: dict,
    hdfs_uri: str,
    stage_name: str,
    files_per_key: dict | None = None,
    default_files: int = 1,
    expected_rows_per_key: dict | None = None,
) -> None:
    """Write a dict {key: DataFrame} to HDFS as `<hdfs_uri>/<key>/` subdirs.

    Plain subdir names (no Hive-style `key=value`) because the Hadoop CLI
    launcher on Windows strips any argv entry containing `=` (treating it as a
    `-Dkey=value` JVM flag). Readers should use `recursiveFileLookup=true` and
    rely on the data's own `source` column instead of partition discovery.

    files_per_key lets large sources use multiple output partitions instead of
    funneling everything through coalesce(1).
    """
    files_per_key = files_per_key or {}
    expected_rows_per_key = expected_rows_per_key or {}
    root = _stage_path(stage_name)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    target_root = _hdfs_path(hdfs_uri)
    parent = posixpath.dirname(target_root.rstrip("/")) or "/"
    hdfs = _hdfs_cmd()

    subprocess.run([hdfs, "dfs", "-rm", "-r", "-f", target_root], check=False)
    subprocess.run([hdfs, "dfs", "-mkdir", "-p", target_root], check=True)
    if parent and parent != target_root:
        subprocess.run([hdfs, "dfs", "-mkdir", "-p", parent], check=False)

    for key, sub_df in parts.items():
        sub_local = root / key
        n_files = files_per_key.get(key, default_files)
        written_rows = _write_parquet_local(sub_df, sub_local, files=n_files)
        expected_rows = expected_rows_per_key.get(key)
        if expected_rows is not None and written_rows is not None and written_rows != expected_rows:
            raise RuntimeError(
                f"[hdfs_commit] row-count mismatch for {key}: wrote {written_rows}, expected {expected_rows}"
            )
        sub_target = posixpath.join(target_root, key)
        subprocess.run([hdfs, "dfs", "-rm", "-r", "-f", sub_target], check=False)
        subprocess.run([hdfs, "dfs", "-put", str(sub_local), sub_target], check=True)
        print(f"[hdfs_commit] uploaded {sub_target}")


def _upload_to_hdfs(local_path: Path, hdfs_uri: str) -> None:
    target = _hdfs_path(hdfs_uri)
    parent = posixpath.dirname(target.rstrip("/")) or "/"
    hdfs = _hdfs_cmd()
    print(f"[hdfs_commit] uploading {local_path} -> {target}")
    subprocess.run([hdfs, "dfs", "-rm", "-r", "-f", target], check=False)
    subprocess.run([hdfs, "dfs", "-mkdir", "-p", parent], check=True)
    subprocess.run([hdfs, "dfs", "-put", str(local_path), target], check=True)
    print(f"[hdfs_commit] uploaded {target}")


def _stage_path(stage_name: str) -> Path:
    root = Path(os.getenv("BDA_SPARK_STAGE", r"C:\tmp\bda_spark_stage"))
    return root / stage_name


def _write_parquet_local(df, local_path: Path, partition_cols=None, files: int | None = None) -> int | None:
    if local_path.exists():
        shutil.rmtree(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # On native Windows, Spark's local Parquet writer can fail while creating
    # Hadoop's `_temporary` output-committer folders. Stream rows back through
    # the driver and emit Parquet with PyArrow instead, which avoids that code
    # path entirely.
    if os.name == "nt":
        if partition_cols:
            return _write_with_pyarrow_collect(df, local_path, partition_cols)
        else:
            return _write_with_pyarrow_stream(df, local_path, files=files)

    out_df = df.coalesce(files) if files else df
    writer = out_df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    print(f"[hdfs_commit] writing local stage {local_path} (files={files or 'default'})")
    writer.parquet(local_path.resolve().as_uri())
    return None


def _hdfs_path(hdfs_uri: str) -> str:
    for prefix in ("hdfs://localhost:9000", "hdfs://127.0.0.1:9000"):
        if hdfs_uri.startswith(prefix):
            return hdfs_uri[len(prefix):] or "/"
    if hdfs_uri.startswith("hdfs:///"):
        return "/" + hdfs_uri[len("hdfs:///"):]
    return hdfs_uri


def _hdfs_cmd() -> str:
    hadoop_home = os.getenv("HADOOP_HOME", r"C:\hadoop")
    cmd = Path(hadoop_home) / "bin" / "hdfs.cmd"
    return str(cmd) if cmd.exists() else "hdfs"


def _write_with_pyarrow_collect(df, local_path: Path, partition_cols) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    print(f"[hdfs_commit] collecting to pandas for Windows-safe write: {local_path}")
    pdf = df.toPandas()
    local_path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(pdf, preserve_index=False)
    if partition_cols:
        pq.write_to_dataset(table, root_path=str(local_path), partition_cols=list(partition_cols))
    else:
        pq.write_table(table, str(local_path / "part-00000.parquet"))
    (local_path / "_SUCCESS").write_text("", encoding="ascii")
    return len(pdf)


def _write_with_pyarrow_stream(df, local_path: Path, files: int | None = None, batch_rows: int = 20_000) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql import functions as F

    local_path.mkdir(parents=True, exist_ok=True)

    target_files = max(int(files or 1), 1)
    if target_files > 1:
        out_df = df.repartition(target_files)
    else:
        out_df = df.coalesce(1)

    cols = list(df.columns)
    arrow_schema = _arrow_schema(df.schema)
    writer = None
    batch = []
    file_idx = 0
    current_partition = None
    written_rows = 0

    def flush_batch() -> None:
        nonlocal batch, writer
        if not batch:
            return
        table = pa.Table.from_pylist(batch, schema=arrow_schema)
        if writer is None:
            part_path = local_path / f"part-{file_idx:05d}.parquet"
            writer = pq.ParquetWriter(str(part_path), arrow_schema, compression="gzip")
        writer.write_table(table)
        batch = []

    print(f"[hdfs_commit] streaming local stage {local_path} via PyArrow (files~{target_files})")
    row_iter = (out_df
                .withColumn("__bda_partition_id", F.spark_partition_id())
                .select(*cols, "__bda_partition_id")
                .toLocalIterator())

    for row in row_iter:
        partition_id = row[-1]
        if current_partition is None:
            current_partition = partition_id
        elif partition_id != current_partition:
            flush_batch()
            if writer is not None:
                writer.close()
                writer = None
            current_partition = partition_id
            file_idx += 1

        batch.append({name: row[idx] for idx, name in enumerate(cols)})
        written_rows += 1
        if len(batch) >= batch_rows:
            flush_batch()

    flush_batch()
    if writer is not None:
        writer.close()

    (local_path / "_SUCCESS").write_text("", encoding="ascii")
    return written_rows


def _arrow_schema(spark_schema):
    import pyarrow as pa
    from pyspark.sql.types import (
        BooleanType,
        ByteType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        TimestampType,
        TimestampNTZType,
    )

    fields = []
    for field in spark_schema.fields:
        dt = field.dataType
        if isinstance(dt, StringType):
            arrow_type = pa.string()
        elif isinstance(dt, DoubleType):
            arrow_type = pa.float64()
        elif isinstance(dt, FloatType):
            arrow_type = pa.float32()
        elif isinstance(dt, (IntegerType, ShortType, ByteType)):
            arrow_type = pa.int32()
        elif isinstance(dt, LongType):
            arrow_type = pa.int64()
        elif isinstance(dt, BooleanType):
            arrow_type = pa.bool_()
        elif isinstance(dt, (TimestampType, TimestampNTZType)):
            arrow_type = pa.timestamp("us")
        elif isinstance(dt, DateType):
            arrow_type = pa.date32()
        else:
            raise TypeError(f"Unsupported Spark type for PyArrow streaming write: {field.name} -> {dt}")
        fields.append(pa.field(field.name, arrow_type, nullable=field.nullable))
    return pa.schema(fields)
