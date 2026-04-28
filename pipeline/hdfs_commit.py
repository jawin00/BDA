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
        out_df = df.coalesce(files) if files else df
        writer = out_df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        print(f"[hdfs_commit] writing local stage {local_path}")
        writer.parquet(local_path.resolve().as_uri())

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


def write_parts_to_hdfs(parts: dict, hdfs_uri: str, stage_name: str) -> None:
    """Write a dict {key: DataFrame} to HDFS as `<hdfs_uri>/<key>/` subdirs.

    Plain subdir names (no Hive-style `key=value`) because the Hadoop CLI
    launcher on Windows strips any argv entry containing `=` (treating it as a
    `-Dkey=value` JVM flag). Readers should use `recursiveFileLookup=true` and
    rely on the data's own `source` column instead of partition discovery.
    """
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
        sub_local.mkdir(parents=True, exist_ok=True)
        out_df = sub_df.coalesce(1)
        print(f"[hdfs_commit] writing local stage {sub_local}")
        out_df.write.mode("overwrite").parquet(sub_local.resolve().as_uri())
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


def _write_with_pyarrow(df, local_path: Path, partition_cols) -> None:
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
